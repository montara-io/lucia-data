import os
import psycopg2


class DataBaseConfig:
    db_user = os.environ.get('DB_USER', 'postgres')
    db_password = os.environ.get('DB_PASSWORD', 'postgres')
    db_host = os.environ.get('DB_HOST', 'db')
    db_port = os.environ.get('DB_PORT', '5432')
    db_name = os.environ.get('DB_NAME', 'data_pipeline')

    conn = psycopg2.connect(host=db_host, user=db_user, password=db_password, dbname=db_name, port=db_port)
