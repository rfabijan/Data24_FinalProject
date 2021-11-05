from definitions import PROJECT_ROOT_DIR
import configparser
import os

_config = configparser.ConfigParser()

_config.read(os.path.join(PROJECT_ROOT_DIR, 'config.ini'))

BUCKET_NAME = _config['default']['bucket_name']

WEEKDAYS = _config['default']['weekdays']
