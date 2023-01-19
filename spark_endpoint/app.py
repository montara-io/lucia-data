import json
import os

from flask import Flask, request

from spark_endpoint.config import app_config
from spark_endpoint.logger import Logger
from spark_endpoint.models import RawEvent
from spark_endpoint.models import db

logger = Logger(log_level=os.getenv('LOG_LEVEL', 'INFO'))

# mode could be 'development', 'testing', 'production'
mode = os.getenv('MODE')
if not mode:
    logger.info("MODE not set, defaulting to development")
    mode = 'development'
else:
    logger.info(f"MODE set to {mode}")


def create_app(environment: str):
    flask_app = Flask(__name__)
    flask_app.config.from_object(app_config[environment])
    db.init_app(flask_app)
    return flask_app


app = create_app(mode)

with app.app_context():
    db.create_all()


@app.route('/events', methods=['POST'])
def write_events():
    payload = request.get_json()
    job_run_id = payload.get('dmAppId')
    if not job_run_id:
        return 'Missing dmAppId param (job_run_id), cannot process request', 400

    try:
        events = parse_events(payload.get('data', ''), job_run_id)
    except Exception as e:
        logger.error(f'Error parsing events: {e}')
        return 'Error parsing events', 400

    write_to_db(events)
    logger.info(f'Completed Successfully, wrote {len(events)} events')
    return 'OK', 200


def parse_events(unparsed_events: str, job_run_id: str) -> list[dict]:
    result = []
    for unparsed_event in unparsed_events.splitlines():
        event = json.loads(unparsed_event)
        result.append(RawEvent(job_run_id=job_run_id, event=event))
    return result


def write_to_db(records: list[db.Model]):
    db.session.add_all(records)
    db.session.commit()


if __name__ == '__main__':
    app.run()
