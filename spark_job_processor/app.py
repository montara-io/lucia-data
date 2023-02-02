import json
import os

from common.config import app_config
from kafka import KafkaConsumer

from spark_job_processor.processor import process_message
from common.logger import get_logger

TOPIC_NAME = "JOB_RUN_EVENT"

ENVIRONMENT = os.getenv('ENVIRONMENT', 'development')
CONFIG = app_config[ENVIRONMENT]
logger = get_logger()


def run():
    consumer = KafkaConsumer(
        CONFIG.KAFKA_TOPIC_NAME,
        bootstrap_servers=[CONFIG.KAFKA_BOOTSTRAP_SERVERS],
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    logger.info('Starting to consume messages')
    for msg in consumer:
        try:
            logger.info('Received message: {}'.format(msg.value))
            process_message(**msg.value)
        except Exception as e:
            logger.error('Processor error: {}'.format(e), exc_info=True)


if __name__ == '__main__':
    run()
