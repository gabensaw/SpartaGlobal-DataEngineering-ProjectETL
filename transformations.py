import pandas as pd
from sqlalchemy import create_engine

# connection to DB
conn_string = 'postgresql://postgres:admin@localhost:5432/postgres'

db = create_engine(conn_string)
connection = db.connect()

# work on DF to prepare them to SQL
pd.set_option('display.max_columns', None)

clean_applicants_df = pd.read_csv('C:/Users/Gaben/Desktop/Data-31-Project-ETL/output/clean_applicants.csv',
                                  dtype={'phone_number': str})
clean_entry_df = pd.read_csv('C:/Users/Gaben/Desktop/Data-31-Project-ETL/output/clean_entry_test_data.csv')
clean_academy_df = pd.read_csv('C:/Users/Gaben/Desktop/Data-31-Project-ETL/output/clean_academy.csv')
clean_main_talent_df = pd.read_csv('C:/Users/Gaben/Desktop/Data-31-Project-ETL/output/clean_main_talent.csv')


def create_candidates_df(df):
    df['first_name'] = clean_applicants_df.name.apply(lambda x: x.split()[0])
    df['middle_name'] = clean_applicants_df.name.apply(lambda x: x.split()[1:-1])
    df['middle_name'] = clean_applicants_df.middle_name.apply(lambda x: ' '.join(x) if x != [] else pd.NA)
    df['last_name'] = clean_applicants_df.name.apply(lambda x: x.split()[-1])
    candidates_df = df[
        ['candidate_id', 'first_name', 'middle_name', 'last_name', 'gender', 'dob', 'email', 'city', 'address',
         'postcode', 'phone_number']]
    return candidates_df


def create_uni_df(df):
    uni_df = df[['uni']].dropna().drop_duplicates()
    uni_df.insert(0, 'uni_id', range(1, 1 + len(uni_df)))
    return uni_df


def create_uni_candidates_junction(u_df, clean_a_df):
    ca_subset_df = clean_a_df[['candidate_id', 'uni', 'degree']]
    merged_inner = pd.merge(left=u_df, right=ca_subset_df, left_on='uni', right_on='uni', how='inner')
    uni_candidate_df = merged_inner[['uni_id', 'candidate_id', 'degree']]
    return uni_candidate_df


def create_stream_df(df):
    df = df[['course_interest', 'date']].drop_duplicates()
    df.insert(0, 'stream_id', range(1, 1 + len(df)))
    df = df.rename(columns={'course_interest': 'course_name', 'date': 'course_start_date'})
    return df


def create_trainers_df(clean_mt_df, stream_df, clean_a_df):
    clean_mt_df = clean_mt_df[['date', 'course_interest', 'name']]
    clean_a_df = clean_a_df[['name', 'trainer']]
    stream_merged = pd.merge(left=stream_df, right=clean_mt_df, left_on=['course_name', 'course_start_date'],
                             right_on=['course_interest', 'date'], how='inner')
    stream_a_merged = pd.merge(left=stream_merged, right=clean_a_df, left_on='name',
                               right_on='name', how='inner')
    trainer_df = stream_a_merged[['stream_id', 'trainer']].drop_duplicates()
    trainer_df.insert(0, 'trainer_id', range(1, 1 + len(trainer_df)))
    return trainer_df


def create_academy_df(clean_e_df, clean_a_df, trainers_df):
    clean_e_df = clean_e_df[['academy', 'name', 'start_date']]
    clean_a_df = clean_a_df[['name', 'trainer']]
    trainers_df = trainers_df[['trainer_id', 'trainer']]
    academy_trainer_merged = pd.merge(left=clean_e_df, right=clean_a_df, left_on='name',
                             right_on='name', how='inner')
    trainers_at_merged = pd.merge(left=trainers_df, right=academy_trainer_merged, left_on='trainer',
                                      right_on='trainer', how='inner')
    trainers_at_merged = trainers_at_merged[['trainer_id', 'academy']].drop_duplicates()
    trainers_at_merged.insert(0, 'academy_id', range(1, 1 + len(trainers_at_merged)))
    trainers_at_merged = trainers_at_merged.rename(columns={'academy': 'location'})
    return trainers_at_merged


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
    return True


# df matching to SQL table
candidates_df = create_candidates_df(clean_applicants_df)
uni_df = create_uni_df(clean_applicants_df)
uni_candidates_df = create_uni_candidates_junction(uni_df, clean_applicants_df)
stream_df = create_stream_df(clean_main_talent_df)
trainers_df = create_trainers_df(clean_main_talent_df, stream_df, clean_academy_df)
academy_df = create_academy_df(clean_entry_df, clean_academy_df, trainers_df)


print(academy_df)
# tables creation
# create_candidates_table(candidates_df)
# create_uni_table(uni_df)
# create_uni_candidates_table(uni_candidates_df)
# create_academy_table(academy_df)
# create_stream_table(stream_df)
# create_trainer_table(trainers_df)

# connection close
connection.close()
