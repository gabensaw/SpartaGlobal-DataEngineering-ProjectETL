from definitions import PROJECT_ROOT_DIR
import configparser
import os

_config = configparser.ConfigParser()

_config.read(os.path.join(PROJECT_ROOT_DIR, '../config.ini'))

DATABASE = _config['default']['database']
SPARTA_DB = _config['default']['sparta_db']
DB_USER = _config['default']['db_user']
DB_PASSWORD = _config['default']['db_password']
BUCKET_NAME = _config['default']['bucket_name']
HOST = _config['default']['host']
PORT = _config['default']['port']
CSV_MAIN_TALENT = _config['default']['csv_main_talent']
CSV_APPLICANTS = _config['default']['csv_applicants']
CSV_STRENGTHS = _config['default']['csv_strengths']
CSV_WEAKNESSES = _config['default']['csv_weaknesses']
CSV_ACADEMY = _config['default']['csv_academy']
CSV_ENTRY_TEST = _config['csv_entry_test']
