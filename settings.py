import os

DB_NAME = os.getenv('DB_NAME', 'stockInfo')
DB_HOST = os.getenv('DB_HOST', '127.0.0.1')
DB_USER = os.getenv('DB_USER', 'root')
DB_PWD = os.getenv('DB_PWD', 'root')
DB_PORT = os.getenv('DB_PORT', 3306)

MYSQL_DB = f'mysql+pymysql://{DB_USER}:{DB_PWD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4'

SQLALCHEMY_ENGINE_CONFIG = {
    'pool_size': 10,
    'max_overflow': 10,
    'pool_timeout': 10,
    'pool_pre_ping': False,
    'echo': False,
    'pool_recycle': 3600,
}

API_TOKEN = os.getenv('API_TOKEN', 'XXXXXXXX')
