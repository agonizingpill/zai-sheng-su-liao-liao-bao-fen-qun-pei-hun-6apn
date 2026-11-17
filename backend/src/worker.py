"""Clustering worker.

Requests live in MySQL. A Redis list is only a wake-up hint: if the message is
lost, the periodic poll still finds the job. A worker that dies mid-calculation
loses its lease, another worker takes over with a higher fencing token, and the
late original can no longer publish.
"""
import json,logging,os,signal,socket,threading,time
from pathlib import Path
from redis import Redis
from sqlalchemy import select
from .database import Session
from .models import Bale
from .clustering import cluster
from . import cluster_service as jobs

log=logging.getLogger('huanli.worker')
logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s')
r=Redis.from_url(os.getenv('REDIS_URL','redis://redis:6379/0'),decode_responses=True)
HEARTBEAT_PATH=Path(os.getenv('WORKER_HEARTBEAT','/artifacts/poly-heartbeat'))
HEARTBEAT_ENABLED=os.getenv('WORKER_HEARTBEAT_ENABLED','true').lower()!='false'
HEARTBEAT_SECONDS=float(os.getenv('WORKER_HEARTBEAT_SECONDS','5'))
POLL_SECONDS=float(os.getenv('CLUSTER_POLL_SECONDS','2'))
stop=threading.Event()

def owner_name():
    return os.getenv('CLUSTER_WORKER_ID') or f'{socket.gethostname()}:{os.getpid()}'

def heartbeat_path():
    """One aggregate file for the pool plus one file per process.

    A dead replica therefore fails its own healthcheck even while its peer is
    still writing the aggregate heartbeat.
    """
    return HEARTBEAT_PATH, HEARTBEAT_PATH.with_name(HEARTBEAT_PATH.name+'-'+socket.gethostname())

def beat():
    while True:
        now=time.time()
        for path in heartbeat_path():
            try:path.write_text(str(now))
            except OSError:log.warning('heartbeat file not writable: %s',path)
        try:r.set('poly-worker-heartbeat',str(now),ex=60)
        except Exception:log.warning('heartbeat redis unavailable')
        if stop.wait(HEARTBEAT_SECONDS):return

def keep_lease(job_id,token,done):
    """Renew while the calculation runs. A lost lease is not fatal by itself:
    publish will still refuse the stale token, so the result is simply dropped."""
    while not done.wait(jobs.LEASE_RENEW_SECONDS):
        try:
            if not jobs.renew(job_id,token):
                log.warning('job #%s lease taken over; abandoned token %s',job_id,token)
                return
        except Exception:log.exception('lease renew failed')

def write_artifact(result):
    try:Path('/artifacts/clusters.json').write_text(json.dumps(result,ensure_ascii=False))
    except OSError:log.warning('artifact not writable')

def run_job(claim):
    job_id,token=claim['id'],claim['token']
    ids=[x['id'] for x in claim['input'] or []]
    done=threading.Event()
    threading.Thread(target=keep_lease,args=(job_id,token,done),daemon=True).start()
    try:
        with Session() as s:
            bales=s.scalars(select(Bale).where(Bale.id.in_(ids)).order_by(Bale.id)).all() if ids else []
            computed=jobs.snapshot_bales(bales)
        labels,result=cluster(bales)
        status=jobs.publish(job_id,token,computed,labels,result)
        if status=='published':
            # Re-read the stored report: on an idempotent retry the takeover's
            # published result is authoritative, not this process's copy.
            published=jobs.published_result(job_id)
            write_artifact(published if published is not None else {**result,'version':job_id,'job_id':job_id})
            log.info('job #%s attempt %s -> published',job_id,claim['attempt'])
        else:log.warning('job #%s attempt %s -> %s',job_id,claim['attempt'],status)
        return status
    except jobs.LeaseLost as e:
        log.warning('job #%s token %s lost lease: %s',job_id,token,e)
        return 'lease_lost'
    except Exception as e:
        log.exception('job #%s failed',job_id)
        jobs.fail(job_id,token,e)
        return 'failed'
    finally:
        done.set()

def main():
    owner=owner_name()
    if HEARTBEAT_ENABLED:
        threading.Thread(target=beat,daemon=True).start()
    log.info('cluster worker %s ready',owner)
    while not stop.is_set():
        try:
            claimed=False
            for _ in range(5):
                job=jobs.claim(owner)
                if not job:break
                claimed=True;run_job(job)
            if claimed:continue
            try:r.blpop('poly-cluster-wakeup',timeout=POLL_SECONDS)
            except Exception:time.sleep(POLL_SECONDS)
        except Exception:
            log.exception('worker loop error');time.sleep(POLL_SECONDS)
    log.info('cluster worker stopped')

if __name__=='__main__':
    signal.signal(signal.SIGTERM,lambda *_:stop.set())
    signal.signal(signal.SIGINT,lambda *_:stop.set())
    main()
