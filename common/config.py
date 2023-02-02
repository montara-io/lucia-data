import os


class Config:

    # DB config
    DB_USER = os.environ.get('DB_USER', 'postgres')
    DB_PW = os.environ.get('DB_PW', 'postgres')
    DB_HOST = os.environ.get('DB_HOST', 'localhost')
    DB_PORT = os.environ.get('DB_PORT', '5432')
    DB_NAME = os.environ.get('DB_NAME', 'data_pipeline')
    DATABASE_URI = f'postgresql://{DB_USER}:{DB_PW}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

    # Kafka config
    KAFKA_HOST = os.environ.get('KAFKA_HOST', 'localhost')
    KAFKA_PORT = os.environ.get('KAFKA_PORT', '29092')
    KAFKA_TOPIC_NAME = 'JOB_RUN_EVENT'
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
