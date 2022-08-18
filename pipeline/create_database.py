import psycopg2
import config_manager as config

conn_first = psycopg2.connect(database=config.DATABASE, user=config.DB_USER, password=config.DB_PASSWORD, host=config.HOST,
                              port=config.PORT)
conn_first.autocommit = True

cursor = conn_first.cursor()

create_sparta = f"""
    CREATE database {config.SPARTA_DB};
"""

cursor.execute(create_sparta)
conn_first.close()
print("Sparta database created...")
