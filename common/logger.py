import logging
import os


class Logger:
    def __init__(self, log_level):
        self.log_level = log_level
        self.logger = logging.getLogger('spark_endpoint')
        self.logger.setLevel(log_level)
        self.logger.addHandler(logging.StreamHandler())

    def debug(self, msg, *args, **kwargs):
        self.logger.debug(msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        self.logger.warning(msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        self.logger.info(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        self.logger.error(msg, *args, **kwargs)

    def critical(self, msg, *args, **kwargs):
        self.logger.critical(msg, *args, **kwargs)


def get_logger():
    return Logger(log_level=os.getenv('LOG_LEVEL', 'INFO'))
