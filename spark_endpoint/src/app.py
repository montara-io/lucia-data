import json
import os
from typing import List, Tuple

from flask import Flask, request
from kafka import KafkaProducer
from src.config import app_config
from src.logger import Logger
from src.models import RawEvent
from src.models import db

logger = Logger(log_level=os.getenv('LOG_LEVEL', 'INFO'))

APPLICATION_END_EVENT = 'SparkListenerApplicationEnd'
KAFKA_TOPIC_NAME = 'JOB_RUN_EVENT'


# MODE could be 'development', 'testing', 'production'
MODE = os.getenv('MODE')
if not MODE:
    logger.info("MODE not set, defaulting to development")
    MODE = 'development'
else:
    logger.info(f"MODE set to {MODE}")


def create_app(environment: str):
    flask_app = Flask(__name__)
    flask_app.config.from_object(app_config[environment])
    db.init_app(flask_app)
    producer = KafkaProducer(
        bootstrap_servers=app_config[environment].KAFKA_BOOTSTRAP_SERVERS,
        api_version=app_config[environment].KAFKA_API_VERSION,
    )
    return flask_app, producer


app, kafka_producer = create_app(MODE)

with app.app_context():
    if MODE == 'testing':
        db.create_all()


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

    logger.info(f'Completed Successfully, wrote {len(events)} events')
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
    kafka_producer.send(KAFKA_TOPIC_NAME, encoded_payload)
    kafka_producer.flush()


def write_to_db(records: List[db.Model]):
    db.session.add_all(records)
    db.session.commit()


if __name__ == '__main__':
    app.run()
