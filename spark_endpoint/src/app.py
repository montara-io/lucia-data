import json
import os
from typing import List, Tuple

from flask import Flask, request

from src.config import app_config
from src.logger import Logger
from src.models import RawEvent
from src.models import db

logger = Logger(log_level=os.getenv('LOG_LEVEL', 'INFO'))

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
    return flask_app


app = create_app(MODE)

with app.app_context():
    if MODE == 'testing':
        db.create_all()


@app.route('/events', methods=['POST'])
def write_events():
    payload = request.get_json()
    job_run_id = payload.get('dmAppId')
    if not job_run_id:
        return 'Missing dmAppId param (job_run_id), cannot process request', 400

    try:
        events, app_end_event = parse_events(payload.get('data', ''), job_run_id)
    except Exception as e:
        logger.error(f'Error parsing events: {e}')
        return 'Error parsing events', 400

    write_to_db(events)

    if app_end_event:
        logger.info(f"Application {job_run_id} ended, Triggering 'Spark Job Processor'")
        # TODO: Trigger processor here 

    logger.info(f'Completed Successfully, wrote {len(events)} events')
    return 'OK', 200


def parse_events(unparsed_events: str, job_run_id: str) -> Tuple[List[RawEvent], bool]:
    result = []
    app_end_event = False
    for unparsed_event in unparsed_events.splitlines():
        event = json.loads(unparsed_event)
        result.append(RawEvent(job_run_id=job_run_id, event=event))
        if event.get('Event') == 'SparkListenerApplicationEnd':
            app_end_event = True
    return result, app_end_event


def write_to_db(records: List[db.Model]):
    db.session.add_all(records)
    db.session.commit()


if __name__ == '__main__':
    app.run()
    