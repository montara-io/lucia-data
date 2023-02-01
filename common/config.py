import os


class Config:

    # DB config
    DB_USER = os.environ.get('DB_USER', 'postgres')
    DB_PASSWORD = os.environ.get('DB_PASSWORD', 'postgres')
    DB_HOST = os.environ.get('DB_HOST', 'db')
    DB_PORT = os.environ.get('DB_PORT', '5432')
    DB_NAME = os.environ.get('DB_NAME', 'data_pipeline')
    SQLALCHEMY_DATABASE_URI = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

    # Kafka config
    KAFKA_HOST = os.environ.get('KAFKA_HOST', 'kafka1')
    KAFKA_PORT = os.environ.get('KAFKA_PORT', '9092')
    KAFKA_API_VERSION = (0, 11, 15)
    KAFKA_TOPIC_NAME = "JOB_RUN_EVENT"
    KAFKA_BOOTSTRAP_SERVERS = f'{KAFKA_HOST}:{KAFKA_PORT}'


class ProductionConfig(Config):
    DEBUG = False
    TESTING = False


class DevelopmentConfig(Config):
    DEBUG = True


class TestingConfig(Config):
    TESTING = True


app_config = {
    'development': DevelopmentConfig,
    'testing': TestingConfig,
    'production': ProductionConfig
}
