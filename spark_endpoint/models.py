from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Column, VARCHAR, JSON, Integer

db = SQLAlchemy()


class RawEvent(db.Model):
    id = Column(Integer, primary_key=True)
    job_run_id = Column(VARCHAR(255))
    event = Column(JSON)
