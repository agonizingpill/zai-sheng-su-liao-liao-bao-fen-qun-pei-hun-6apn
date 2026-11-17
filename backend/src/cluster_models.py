"""Durable clustering jobs: the database, not a Redis list, owns every request."""
from sqlalchemy import String,Integer,BigInteger,Text,JSON,Float,Index
from sqlalchemy.orm import Mapped,mapped_column
from .database import Base

class ClusterJob(Base):
    __tablename__='cluster_jobs'
    __table_args__=(Index('ix_cluster_jobs_status','status'),Index('ix_cluster_jobs_status_version','status','version'))
    id:Mapped[int]=mapped_column(primary_key=True)
    # queued -> running -> publishing -> published | rejected | failed
    status:Mapped[str]=mapped_column(String(20),default='queued')
    # Only one live job may exist. MySQL unique indexes allow many NULLs.
    active_slot:Mapped[str|None]=mapped_column(String(10),nullable=True,unique=True)
    requested_by:Mapped[str]=mapped_column(String(120))
    requested_at:Mapped[str]=mapped_column(String(40))
    started_at:Mapped[str|None]=mapped_column(String(40),nullable=True)
    finished_at:Mapped[str|None]=mapped_column(String(40),nullable=True)
    published_at:Mapped[str|None]=mapped_column(String(40),nullable=True)
    attempt:Mapped[int]=mapped_column(Integer,default=0)
    lease_owner:Mapped[str|None]=mapped_column(String(180),nullable=True)
    # Fencing token: every successful takeover increments it. A late executor
    # may still hold the old token but can no longer publish.
    lease_token:Mapped[int]=mapped_column(BigInteger,default=0)
    lease_expires_at:Mapped[float|None]=mapped_column(Float,nullable=True)
    version:Mapped[int|None]=mapped_column(BigInteger,nullable=True)
    bale_count:Mapped[int]=mapped_column(Integer,default=0)
    bale_ids:Mapped[list]=mapped_column(JSON)
    fingerprint:Mapped[str]=mapped_column(String(64))
    input_snapshot:Mapped[list]=mapped_column(JSON)
    # Exactly what the model read. Compared with the yard at publish time.
    computed_snapshot:Mapped[list|None]=mapped_column(JSON,nullable=True)
    result:Mapped[dict|None]=mapped_column(JSON,nullable=True)
    error:Mapped[str|None]=mapped_column(Text,nullable=True)
    reason:Mapped[str|None]=mapped_column(Text,nullable=True)
