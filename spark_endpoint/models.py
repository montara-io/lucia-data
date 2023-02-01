from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Column, VARCHAR, JSON, INTEGER, FLOAT, BIGINT, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID
import uuid

db = SQLAlchemy()


class RawEvent(db.Model):
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_run_id = Column(VARCHAR(255))
    job_id = Column(VARCHAR(255))
    pipeline_id = Column(VARCHAR(255))
    pipeline_run_id = Column(VARCHAR(255))
    event = Column(JSON)


class SparkJobRun(db.Model):
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
