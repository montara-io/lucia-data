import logging
import os


class Logger:
    def __init__(self, log_level):
        self.logger = logging.getLogger('lucia_data')
        self.logger.setLevel(log_level)
        if not self.logger.handlers:
            self.logger.addHandler(logging.StreamHandler())

    def debug(self, *args, **kwargs):
        self.logger.debug(*args, **kwargs)

    def warning(self, *args, **kwargs):
        self.logger.warning(*args, **kwargs)

    def info(self, *args, **kwargs):
        self.logger.info(*args, **kwargs)

    def error(self, *args, **kwargs):
        self.logger.error(*args, **kwargs)

    def critical(self, *args, **kwargs):
        self.logger.critical(*args, **kwargs)


def get_logger():
    return Logger(log_level=os.getenv('LOG_LEVEL', 'INFO'))
