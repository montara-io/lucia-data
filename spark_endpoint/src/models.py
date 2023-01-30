import uuid

from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Column, VARCHAR, JSON
from sqlalchemy.dialects.postgresql import UUID

db = SQLAlchemy()


class RawEvent(db.Model):
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_run_id = Column(VARCHAR(255))
    job_id = Column(VARCHAR(255))
    pipeline_id = Column(VARCHAR(255))
    pipeline_run_id = Column(VARCHAR(255))
    event = Column(JSON)
