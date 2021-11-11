from definitions import PROJECT_ROOT_DIR
import configparser
import os

config_files = [
    os.path.join(PROJECT_ROOT_DIR, 'config.ini'),
    os.path.join(PROJECT_ROOT_DIR, 'private_config.ini')
]

_config = configparser.ConfigParser()
_config.read(config_files)

BUCKET_NAME = _config['default']['bucket_name']


WEEKDAYS = _config['cleaning']['weekdays'].split(',')

DB_SERVER = _config['SQL']['server']
DB_NAME = _config['SQL']['database']
DB_USERNAME = _config['SQL']['username']
DB_PASSWORD = _config['SQL']['password']
DB_HOST = _config['SQL_Alchemy']['host']
DB_PORT = _config['SQL_Alchemy']['port']
DB_DRIVER = _config['SQL_Alchemy']['driver']
