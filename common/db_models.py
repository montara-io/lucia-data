import os
import uuid

from common.config import app_config
from sqlalchemy import Column, VARCHAR, JSON, INTEGER, FLOAT, BIGINT, TIMESTAMP
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase, sessionmaker, scoped_session


environment = os.getenv('ENVIRONMENT', 'development')
engine = create_engine(app_config[environment].DATABASE_URI)
db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))


class Base(DeclarativeBase):
    pass


class RawEvent(Base):
    __tablename__ = 'raw_event'
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_run_id = Column(VARCHAR(255))
    job_id = Column(VARCHAR(255))
    pipeline_id = Column(VARCHAR(255))
    pipeline_run_id = Column(VARCHAR(255))
    event = Column(JSON)


class SparkJobRun(Base):
    __tablename__ = 'spark_job_run'
    id = Column(VARCHAR(255), primary_key=True)
    job_id = Column(VARCHAR(255))
    pipeline_id = Column(VARCHAR(255))
    pipeline_run_id = Column(VARCHAR(255))
    cpu_utilization = Column(INTEGER())
    total_cpu_time_used = Column(FLOAT())
    num_of_executors = Column(INTEGER())
    total_memory_per_executor = Column(FLOAT())
    total_bytes_read = Column(BIGINT())
    total_shuffle_bytes_read = Column(BIGINT())
    total_bytes_written = Column(BIGINT())
    total_shuffle_bytes_written = Column(BIGINT())
    total_cpu_uptime = Column(FLOAT())
    peak_memory_usage = Column(FLOAT())
    total_cores_num = Column(INTEGER())
    start_time = Column(TIMESTAMP())
    end_time = Column(TIMESTAMP())
