"""Durable clustering jobs.

The database owns the queue. Redis is only a best-effort wake-up channel, so a
worker crash never loses a request. Computing happens without bale locks; only
the short publish transaction locks the yard and verifies that the input the
model saw is still current.
"""
import hashlib,json,logging,os,time
from datetime import datetime,timezone
from decimal import Decimal,InvalidOperation
from fastapi import HTTPException
from sqlalchemy import select,update,or_,and_,func
from sqlalchemy.exc import IntegrityError
from .database import Session
from .models import Bale,Audit
from .cluster_models import ClusterJob

log=logging.getLogger('huanli.clusters')

LEASE_SECONDS=float(os.getenv('CLUSTER_LEASE_SECONDS','15'))
LEASE_RENEW_SECONDS=float(os.getenv('CLUSTER_LEASE_RENEW_SECONDS','5'))
MAX_ATTEMPTS=int(os.getenv('CLUSTER_MAX_ATTEMPTS','3'))
ACTIVE=('queued','running','publishing')
QUALITY_FIELDS=(('melt_index','熔指'),('moisture','水分'),('pvc_ppm','PVC'))

class LeaseLost(Exception):
    """This executor no longer owns the job; it must not write any result."""

def now_iso():return datetime.now(timezone.utc).isoformat()

def _decimal(v):
    try:return Decimal(str(v))
    except (InvalidOperation,TypeError):return None

def snapshot_bales(bales):
    """A JSON-safe copy of the exact model input. Sorted by bale id."""
    return [{'id':b.id,'code':b.code,'melt_index':str(b.melt_index),'moisture':str(b.moisture),'pvc_ppm':str(b.pvc_ppm)} for b in bales]

def fingerprint(rows):
    h=hashlib.sha256()
    for r in rows:
        for key in ('id','code','melt_index','moisture','pvc_ppm'):
            h.update(str(r.get(key)).encode());h.update(b'|')
        h.update(b';')
    return h.hexdigest()

def all_bales(s):
    return s.scalars(select(Bale).order_by(Bale.id)).all()

def current_version(s):
    """The version selection may use: the newest fully published job.

    Legacy databases created before durable jobs keep working through the
    version stamped on the bales themselves.
    """
    published=s.scalar(select(ClusterJob.version).where(ClusterJob.status=='published',ClusterJob.version.isnot(None)).order_by(ClusterJob.version.desc()).limit(1))
    if published is not None:return published
    return s.scalar(select(func.max(Bale.cluster_version)))

def membership_problem(s,bale):
    """Why this bale may not be picked into a new blend, or None.

    Selection only accepts the newest version that was published as a whole,
    with the quality values that version was published on. Databases created
    before durable jobs have no publication record; there the highest version
    stamped on the bales is the implicit published version, because the legacy
    worker stamped every bale in one transaction.
    """
    job=latest_published(s)
    if job is None:
        legacy=current_version(s)
        if legacy is None:return '尚无完整发布的分群版本，不能选料'
        if bale.cluster_id is None:return f'{bale.code}尚未完成分群检验'
        if bale.cluster_version!=legacy:return f'{bale.code}属于分群版本 {bale.cluster_version}，当前可选用完整发布的版本 {legacy}'
        return None
    if job.version is None:return '尚无完整发布的分群版本，不能选料'
    snap=next((r for r in (job.input_snapshot or []) if r.get('id')==bale.id),None)
    if bale.cluster_id is None:return f'{bale.code}尚未完成分群检验'
    if snap is None:return f'{bale.code}不在版本 {job.version} 的发布名单内，请等待重新分群'
    if bale.cluster_version!=job.version:return f'{bale.code}属于分群版本 {bale.cluster_version}，当前可选用完整发布的版本 {job.version}'
    for field,label in QUALITY_FIELDS:
        if _decimal(snap.get(field))!=_decimal(getattr(bale,field)):return f'{bale.code}的{label}在版本 {job.version} 发布后已复检变化，请等待重新分群后再选料'
    return None

def _audit(s,actor,action,detail):
    s.add(Audit(actor=actor or 'worker',action=action,detail=str(detail),at=now_iso()))

def enqueue(s,user):
    active=s.scalar(select(ClusterJob).where(ClusterJob.status.in_(ACTIVE)).order_by(ClusterJob.id.desc()))
    if active:raise HTTPException(409,f'分群任务 #{active.id} 正在{status_label(active.status)}，请等待完成后再发起')
    bales=all_bales(s)
    rows=snapshot_bales(bales)
    job=ClusterJob(status='queued',active_slot='active',requested_by=user['email'],requested_at=now_iso(),attempt=0,lease_token=0,bale_count=len(rows),bale_ids=[r['id'] for r in rows],fingerprint=fingerprint(rows),input_snapshot=rows)
    s.add(job)
    try:s.flush()
    except IntegrityError as e:
        # Another request inserted the live job between our read and write.
        raise HTTPException(409,'已有分群任务正在计算，请等待完成后再发起') from e
    _audit(s,user['email'],'clusters_queued',f'任务 #{job.id} · {len(rows)} 包')
    return job

def status_label(status):
    return {'queued':'排队待算','running':'计算中','publishing':'发布校验中','published':'已发布','rejected':'已拒绝','failed':'计算失败'}.get(status,status or '空闲')

def job_view(j):
    return {'id':j.id,'status':j.status,'status_label':status_label(j.status),'requested_by':j.requested_by,'requested_at':j.requested_at,'started_at':j.started_at,'finished_at':j.finished_at,'published_at':j.published_at,'attempt':j.attempt,'bale_count':j.bale_count,'version':j.version,'error':j.error,'reason':j.reason,'lease_owner':j.lease_owner}

def active_job(s):
    return s.scalar(select(ClusterJob).where(ClusterJob.status.in_(ACTIVE)).order_by(ClusterJob.id.desc()).limit(1))

def get_job(s,id):
    job=s.get(ClusterJob,id)
    if not job:raise HTTPException(404,'分群任务不存在')
    return job

def latest_job(s):
    return s.scalar(select(ClusterJob).order_by(ClusterJob.id.desc()).limit(1))

def latest_published(s):
    return s.scalar(select(ClusterJob).where(ClusterJob.status=='published',ClusterJob.version.isnot(None)).order_by(ClusterJob.version.desc()).limit(1))

def recent_jobs(s,limit=12):
    return s.scalars(select(ClusterJob).order_by(ClusterJob.id.desc()).limit(limit)).all()

def published_result(job_id):
    """The stored report of a published job, for artifact/notification use."""
    with Session() as s:
        job=s.get(ClusterJob,job_id)
        return dict(job.result) if job is not None and job.status=='published' and job.result else None

# ---------------------------------------------------------------- worker side

def claim(owner,now=None):
    """Atomically take the oldest queued job or a job whose lease has expired.

    The row lock makes concurrent takeovers serial: exactly one executor wins,
    and every win increments the fencing token. A crashed job is retried up to
    MAX_ATTEMPTS; after that it is closed as failed instead of looping forever.
    """
    now=time.time() if now is None else now
    with Session.begin() as s:
        for _ in range(5):
            job=s.scalar(select(ClusterJob).where(or_(ClusterJob.status=='queued',and_(ClusterJob.status.in_(('running','publishing')),or_(ClusterJob.lease_expires_at.is_(None),ClusterJob.lease_expires_at<now)))).order_by(ClusterJob.id).with_for_update(skip_locked=True).limit(1))
            if not job:return None
            if job.status!='queued' and (job.attempt or 0)>=MAX_ATTEMPTS:
                job.status='failed';job.error=f'已自动接管 {job.attempt} 次仍未完成，停止重试';job.finished_at=now_iso()
                job.lease_expires_at=None;job.active_slot=None
                _audit(s,owner,'clusters_failed',f'任务 #{job.id} · {job.error}')
                continue
            job.status='running';job.attempt=(job.attempt or 0)+1
            job.lease_owner=owner;job.lease_token=(job.lease_token or 0)+1
            job.lease_expires_at=now+LEASE_SECONDS
            job.started_at=job.started_at or now_iso()
            _audit(s,owner,'clusters_claimed',f'任务 #{job.id} · 第{job.attempt}次执行 · 令牌{job.lease_token}')
            s.flush()
            return {'id':job.id,'token':job.lease_token,'attempt':job.attempt,'input':job.input_snapshot}
        return None

def renew(job_id,token,now=None):
    """Extend the lease only while this executor's token is still current."""
    now=time.time() if now is None else now
    with Session.begin() as s:
        n=s.execute(update(ClusterJob).where(ClusterJob.id==job_id,ClusterJob.lease_token==token,ClusterJob.status.in_(('running','publishing'))).values(lease_expires_at=now+LEASE_SECONDS)).rowcount
    return n==1

def _still_owner(s,job,token):
    return job is not None and job.lease_token==token and job.status in ('running','publishing')

def _diff(snapshot,bales,locked=None):
    """Compare the roster the model actually used with the yard at publish."""
    snap={r['id']:r for r in (snapshot or [])}
    current={b.id:b for b in bales}
    problems=[]
    added=[b for b in bales if b.id not in snap]
    missing=[bid for bid in snap if bid not in current]
    if added:
        codes='、'.join(b.code for b in added[:5])+('等' if len(added)>5 else '')
        problems.append(f'计算期间新增料包 {codes}（{len(added)}包），参与名单已变化')
    if missing:problems.append('参与料包已不在堆场（编号 '+ '、'.join(str(x) for x in missing[:5]) +'），参与名单已变化')
    checks=[(current[i],r) for i,r in snap.items() if i in current]
    for b,r in checks:
        if r.get('code')!=b.code:
            problems.append(f'{r.get("code")} 的编号已改为 {b.code}，参与名单已变化');continue
        changed=[]
        for field,label in QUALITY_FIELDS:
            old,new=_decimal(r.get(field)),_decimal(getattr(b,field))
            if old!=new:changed.append(f'{label} {r.get(field)}→{getattr(b,field)}')
        if changed:problems.append(f'{b.code} 的'+'、'.join(changed)+'（计算期间被复检），质量快照已变化')
    return problems

def publish(job_id,token,computed,labels,result):
    """Publish labels and the centre report in one short transaction.

    `computed` is the JSON-safe snapshot of the bales the model read. Returns
    the final job status. If validation fails, the old published version is
    left untouched and the reason is recorded on the job. A late executor with
    a stale fencing token raises LeaseLost and writes nothing.
    """
    if not isinstance(labels,(list,tuple)):raise ValueError('标签格式不正确')
    with Session.begin() as s:
        job=s.scalar(select(ClusterJob).where(ClusterJob.id==job_id).with_for_update().execution_options(populate_existing=True))
        if job is None:raise LeaseLost('任务不存在')
        if job.status=='published':return 'published'  # a retry must not publish twice
        if not _still_owner(s,job,token):raise LeaseLost('租约已被其他执行进程接管')
        job.status='publishing';job.computed_snapshot=computed
        # Lock the whole yard for the verification only: this blocks gap-inserted
        # new bales so the roster cannot change between the check and the commit.
        bales=s.scalars(select(Bale).order_by(Bale.id).with_for_update().execution_options(populate_existing=True)).all()
        problems=_diff(computed if computed is not None else job.input_snapshot,bales)
        if len(labels)!=len(bales):problems.append(f'计算结果覆盖 {len(labels)} 包，与当前 {len(bales)} 包不一致')
        if problems:
            job.status='rejected';job.reason='；'.join(problems);job.finished_at=now_iso()
            job.lease_expires_at=None;job.active_slot=None
            _audit(s,job.requested_by,'clusters_rejected',f'任务 #{job.id} · '+job.reason)
            return 'rejected'
        version=job.id
        for b,label in zip(bales,labels):
            b.cluster_id=int(label);b.cluster_version=version
        job.status='published';job.version=version;job.finished_at=now_iso();job.published_at=now_iso()
        job.error=None;job.reason=None;job.lease_expires_at=None;job.active_slot=None
        job.result={**result,'version':version,'job_id':job.id,'bale_count':len(bales),'published_at':job.published_at,'requested_by':job.requested_by}
        _audit(s,job.requested_by,'clusters_published',f'任务 #{job.id} · 版本 {version} · {len(bales)} 包')
        return 'published'

def fail(job_id,token,message):
    """Record a failure only if this executor still owns the job."""
    with Session.begin() as s:
        job=s.scalar(select(ClusterJob).where(ClusterJob.id==job_id).with_for_update())
        if not _still_owner(s,job,token):return False
        job.status='failed';job.error=str(message)[:2000];job.reason=None
        job.finished_at=now_iso();job.lease_expires_at=None;job.active_slot=None
        _audit(s,job.requested_by,'clusters_failed',f'任务 #{job.id} · {str(message)[:300]}')
        return True

# --------------------------------------------------------------- API reads

def overview(s,legacy_result=None,legacy_error=None):
    active=active_job(s);latest=latest_job(s);published=latest_published(s)
    result=published.result if published else legacy_result
    if result is None and legacy_result is not None:result=legacy_result
    if active:status=active.status
    elif latest:status=latest.status
    elif legacy_result is not None:status='published'
    else:status='idle'
    if published:version=published.version
    elif legacy_result is not None:version=(legacy_result or {}).get('version')
    else:version=current_version(s)
    error=latest.error if latest and latest.status=='failed' else (legacy_error if not latest else None)
    reason=latest.reason if latest and latest.status=='rejected' else None
    return {'status':status,'status_label':status_label(status),'active':bool(active),'job':job_view(active) if active else (job_view(latest) if latest else None),'jobs':[job_view(j) for j in recent_jobs(s)],'result':result,'version':version,'error':error,'reason':reason}
