import os

DB_NAME = os.getenv('DB_NAME')
DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PWD = os.getenv('DB_PWD')
DB_PORT = os.getenv('DB_PORT')

MYSQL_DB = f'mysql+pymysql://{DB_USER}:{DB_PWD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4'

SQLALCHEMY_ENGINE_CONFIG = {
    'pool_size': 10,
    'max_overflow': 10,
    'pool_timeout': 10,
    'pool_pre_ping': False,
    'echo': False,
    'pool_recycle': 3600,
}

API_TOKEN = os.getenv('API_TOKEN')
