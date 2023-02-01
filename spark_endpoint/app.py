import json
import os
from typing import List, Tuple

from flask import Flask, request
from kafka import KafkaProducer

from common.config import app_config
from common.logger import get_logger
from common.models import RawEvent
from common.models import session

logger = get_logger()

APPLICATION_END_EVENT = 'SparkListenerApplicationEnd'
KAFKA_TOPIC_NAME = 'JOB_RUN_EVENT'
ENVIRONMENT = os.getenv('ENVIRONMENT', 'development')
CONFIG = app_config[ENVIRONMENT]


def create_app():
    flask_app = Flask(__name__)
    flask_app.config.from_object(CONFIG)
    producer = KafkaProducer(
        bootstrap_servers=CONFIG.KAFKA_BOOTSTRAP_SERVERS,
        api_version=CONFIG.KAFKA_API_VERSION,
    )
    return flask_app, producer


app, kafka_producer = create_app()


@app.route('/events', methods=['POST'])
def write_events():
    payload = request.get_json()
    job_run_id = payload.get('dmAppId')
    if not job_run_id:
        return 'Missing dmAppId param (job_run_id), cannot process request', 400

    job_id = payload.get('jobId', None)
    pipeline_run_id = payload.get('pipelineRunId', None)
    pipeline_id = payload.get('pipelineId', None)
    try:
        events, app_end_event = parse_events(payload.get('data', ''), job_run_id, job_id, pipeline_id, pipeline_run_id)
    except Exception as e:
        logger.error(f'Error parsing events: {e}')
        return 'Error parsing events', 400

    write_to_db(events)

    if app_end_event:
        logger.info(f"Application {job_run_id} ended, Triggering 'Spark Job Processor'")

        payload = {
            "job_run_id": job_run_id,
            "job_id": job_id,
            "pipeline_run_id": pipeline_run_id,
            "pipeline_id": pipeline_id
        }
        send_to_kafka(payload)

    return 'OK', 200


def parse_events(
        unparsed_events: str,
        job_run_id: str,
        job_id: str,
        pipeline_id: str = None,
        pipeline_run_id: str = None) -> Tuple[List[RawEvent], bool]:

    result = []
    app_end_event = False
    for unparsed_event in unparsed_events.splitlines():
        event = json.loads(unparsed_event)
        result.append(RawEvent(job_run_id=job_run_id,job_id=job_id, pipeline_id=pipeline_id, pipeline_run_id=pipeline_run_id, event=event))
        if event.get('Event') == APPLICATION_END_EVENT:
            app_end_event = True
    return result, app_end_event


def send_to_kafka(payload: dict):
    str_payload = json.dumps(payload)
    encoded_payload = str.encode(str_payload)
    logger.info(f'Sending payload to Kafka, topic: {KAFKA_TOPIC_NAME}, payload: {str_payload}')
    kafka_producer.send(KAFKA_TOPIC_NAME, encoded_payload)
    kafka_producer.flush()


def write_to_db(records: List[RawEvent]):
    session.add_all(records)
    session.commit()
    logger.info(f'Write to DB completed successfully, wrote {len(records)} records')


if __name__ == '__main__':
    app.run()
