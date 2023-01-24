import logging


class Logger:
    def __init__(self, log_level):
        self.log_level = log_level
        self.logger = logging.getLogger('spark_endpoint')
        self.logger.setLevel(log_level)
        self.logger.addHandler(logging.StreamHandler())

    def debug(self, msg):
        self.logger.debug(msg)

    def warning(self, msg):
        self.logger.warning(msg)

    def info(self, msg):
        self.logger.info(msg)

    def error(self, msg):
        self.logger.error(msg)

    def critical(self, msg):
        self.logger.critical(msg)
