from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Column, VARCHAR, JSON, Integer
from sqlalchemy.dialects.postgresql import UUID
import uuid

db = SQLAlchemy()


class RawEvent(db.Model):
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_run_id = Column(VARCHAR(255))
    event = Column(JSON)
