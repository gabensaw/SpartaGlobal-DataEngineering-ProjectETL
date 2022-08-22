import pandas as pd
from sqlalchemy import create_engine
from transformations import *


# connection to DB
conn_string = 'postgresql://postgres:admin@localhost:5432/postgres'

db = create_engine(conn_string)
connection = db.connect()

# work on DF to prepare them to SQL
pd.set_option('display.max_columns', None)

#### inputing to sql tables functions
def create_uni_table(df):
    df.to_sql('university', con=connection, if_exists='replace', index=False)
    connection.execute('ALTER TABLE university ADD PRIMARY KEY (uni_id);')
    return True


def create_candidates_table(df):
    df.to_sql('candidate', con=connection, if_exists='replace', index=False)
    connection.execute('ALTER TABLE candidate ADD PRIMARY KEY (candidate_id);')
    return True


def create_uni_candidates_table(df):
    df.to_sql('uni_candidate_junction', con=connection, if_exists='replace', index=False)
    connection.execute('ALTER TABLE uni_candidate_junction ADD PRIMARY KEY (uni_id, candidate_id);')
    connection.execute('ALTER TABLE uni_candidate_junction ADD FOREIGN KEY (uni_id) REFERENCES university;')
    connection.execute('ALTER TABLE uni_candidate_junction ADD FOREIGN KEY (candidate_id) REFERENCES candidate;')
    return True


def create_stream_table(df):
    df.to_sql('stream', con=connection, if_exists='replace', index=False)
    connection.execute('ALTER TABLE stream ADD PRIMARY KEY (stream_id);')
    return True


def create_trainer_table(df):
    df.to_sql('trainer', con=connection, if_exists='replace', index=False)
    connection.execute('ALTER TABLE trainer ADD PRIMARY KEY (trainer_id);')
    connection.execute('ALTER TABLE trainer ADD FOREIGN KEY (stream_id) REFERENCES stream;')
    return True


def create_academy_table(df):
    df.to_sql('academy', con=connection, if_exists='replace', index=False)
    connection.execute('ALTER TABLE academy ADD PRIMARY KEY (academy_id);')
    connection.execute('ALTER TABLE academy ADD FOREIGN KEY (trainer_id) REFERENCES trainer;')
    return True


def create_stream_candidate_junction(df):
    df.to_sql('stream_candidate_junction', con=connection, if_exists='replace', index=False)
    connection.execute('ALTER TABLE stream_candidate_junction ADD PRIMARY KEY (candidate_id, stream_id);')
    connection.execute('ALTER TABLE stream_candidate_junction ADD FOREIGN KEY (candidate_id) REFERENCES candidate;')
    connection.execute('ALTER TABLE stream_candidate_junction ADD FOREIGN KEY (stream_id) REFERENCES stream;')
    return True


def create_behaviors_table(df):
    df.to_sql('behaviours', con=connection, if_exists='replace', index=False)
    connection.execute('ALTER TABLE behaviours ADD PRIMARY KEY (behaviour_id);')
    return True


def create_sparta_day_table(df):
    df.to_sql('sparta_day_results', con=connection, if_exists='replace', index=False)
    connection.execute('ALTER TABLE sparta_day_results ADD PRIMARY KEY (scores_id);')
    connection.execute('ALTER TABLE sparta_day_results ADD FOREIGN KEY (candidate_id) REFERENCES candidate;')
    return True

# tables creation
create_candidates_table(candidates_df)
create_uni_table(uni_df)
create_uni_candidates_table(uni_candidates_df)
create_stream_table(stream_df)
create_trainer_table(trainers_df)
create_academy_table(academy_df)
create_stream_candidate_junction(stream_candidates_df)
create_behaviors_table(behaviour_df)
create_sparta_day_table(sparta_day_results_df)

# connection close
connection.close()
