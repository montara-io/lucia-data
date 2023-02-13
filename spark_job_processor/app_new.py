import json
import os

from kafka import KafkaConsumer
from sqlalchemy import select

from common.config import app_config
from common.logger import get_logger
from common.models import RawEvent, SparkJobRun, db_session
from spark_job_processor.spark_events_processor import SparkEventsProcessor

TOPIC_NAME = "JOB_RUN_EVENT"

ENVIRONMENT = os.getenv("ENVIRONMENT", "development")
CONFIG = app_config[ENVIRONMENT]
logger = get_logger()


def get_events_from_db(job_run_id: str):
    stmt = select(RawEvent).where(RawEvent.job_run_id == job_run_id)
    return db_session.scalars(stmt)


def insert_metrics_to_db(data: dict):
    spark_job_run = SparkJobRun(**data)
    db_session.add(spark_job_run)
    db_session.commit()


def process_message(job_run_id, job_id, pipeline_id=None, pipeline_run_id=None):
    events = [dict(event) for event in get_events_from_db(job_run_id)]
    spark_events_processor = SparkEventsProcessor()
    application_data = spark_events_processor.process_events(
        events, job_run_id, job_id, pipeline_id, pipeline_run_id
    )
    insert_metrics_to_db(application_data)


def run():
    consumer = KafkaConsumer(
        CONFIG.KAFKA_TOPIC_NAME,
        bootstrap_servers=[CONFIG.KAFKA_BOOTSTRAP_SERVERS],
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    logger.info("Starting to consume messages")
    for msg in consumer:
        try:
            logger.info("Received message: {}".format(msg.value))
            process_message(**msg.value)
        except Exception as e:
            logger.error("Processor error: {}".format(e), exc_info=True)


if __name__ == "__main__":
    run()
