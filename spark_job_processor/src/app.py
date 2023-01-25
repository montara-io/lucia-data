
import os

from flask import Flask, request

MODE = os.getenv('MODE')


def create_app(environment: str):
    flask_app = Flask(__name__)
    return flask_app


app = create_app(MODE)


if __name__ == '__main__':
    app.run()
    