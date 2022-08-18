from final_project.definitions import PROJECT_ROOT_DIR
import configparser
import os

_config = configparser.ConfigParser()

_config.read(os.path.join(PROJECT_ROOT_DIR, 'config.ini'))

DATABASE = _config['default']['database']
SPARTA_DB = _config['default']['sparta_db']
DB_USER = _config['default']['db_user']
DB_PASSWORD = _config['default']['db_password']
HOST = _config['default']['host']
PORT = _config['default']['port']
