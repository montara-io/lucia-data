
import os

from flask import Flask, request
from threading import Thread
import src.processor
import logging
from spark_job_processor.src.models import db
from spark_job_processor.src.config import app_config



# MODE could be 'development', 'testing', 'production'
MODE = os.getenv('MODE')

if not MODE:
    MODE = 'development'

    
def create_app(environment: str):
    flask_app = Flask(__name__)
    flask_app.config.from_object(app_config[environment])
    
    Thread(target=src.processor.load_events, kwargs={'app': flask_app}).start()

    return flask_app


app = create_app(MODE)


if __name__ == '__main__':
    app.run(debug=True)
    app.logger.setLevel(logging.DEBUG)
    
    
    