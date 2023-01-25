
import os

from flask import Flask, request
from threading import Thread
import src.processor
import logging

MODE = os.getenv('MODE')

def create_app(environment: str):
    flask_app = Flask(__name__)
    t = Thread(target=src.processor.load_events)
    t.start()
    return flask_app


app = create_app(MODE)


if __name__ == '__main__':
    app.run(debug=True)
    app.logger.setLevel(logging.DEBUG)
    
    