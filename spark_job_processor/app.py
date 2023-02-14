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


def insert_spark_job_run_to_db(data: dict):
    spark_job_run = SparkJobRun(**data)
    db_session.add(spark_job_run)
    db_session.commit()


def run():
    consumer = KafkaConsumer(
        CONFIG.KAFKA_TOPIC_NAME,
        bootstrap_servers=[CONFIG.KAFKA_BOOTSTRAP_SERVERS],
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    logger.info("Starting to consume messages")
    spark_events_processor = SparkEventsProcessor()
    for msg in consumer:
        try:
            logger.info("Received message: {}".format(msg.value))
            events = [dict(raw_event.event) for raw_event in get_events_from_db(msg.value["job_run_id"])]
            application_data = spark_events_processor.process_events(events, **msg.value)
            insert_spark_job_run_to_db(application_data)
        except Exception as e:
            logger.error("Processor error: {}".format(e), exc_info=True)


if __name__ == "__main__":
    run()
