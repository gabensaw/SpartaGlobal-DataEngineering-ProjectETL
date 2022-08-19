import pandas as pd
from sqlalchemy import create_engine

####### connection to DB
conn_string = 'postgresql://postgres:admin@localhost:5432/postgres'

db = create_engine(conn_string)
connection = db.connect()

#### work on DF to prepare them to SQL
pd.set_option('display.max_columns', None)

clean_applicants_df = pd.read_csv('C:/Users/Gaben/Desktop/Data-31-Project-ETL/output/clean_applicants.csv',
                                  dtype={'phone_number': str})
clean_entry_df = pd.read_csv('C:/Users/Gaben/Desktop/Data-31-Project-ETL/output/clean_entry_test_data.csv')
clean_academy_df = pd.read_csv('C:/Users/Gaben/Desktop/Data-31-Project-ETL/output/clean_academy.csv')
clean_main_talent_df = pd.read_csv('C:/Users/Gaben/Desktop/Data-31-Project-ETL/output/clean_main_talent.csv')


def create_candidates_df(df):
    candidates_df = df[
        ['name', 'gender', 'dob', 'email', 'city', 'address', 'postcode', 'phone_number']]
    return candidates_df


def create_uni_df(df):
    uni_df = df[['uni']].dropna().drop_duplicates()
    uni_df.insert(0, 'uni_id', range(1, 1 + len(uni_df)))
    return uni_df


def create_uni_candidates_junction(u_df, ca_df):
    ca_subset_df = ca_df[['candidate_id', 'uni', 'degree']]
    merged_inner = pd.merge(left=u_df, right=ca_subset_df, left_on='uni', right_on='uni', how='inner')
    uni_candidate_df = merged_inner[['uni_id', 'candidate_id', 'degree']]
    return uni_candidate_df

#### to sql functions
def create_uni_table(df, table_name):
    df.to_sql(table_name, con=connection, if_exists='replace', index=False)
    connection.execute(f'ALTER TABLE {table_name} ADD PRIMARY KEY (candidate_id);')
    return True

def create_candidates_table(df):
    df.to_sql('candidates', con=connection, if_exists='replace', index=False)
    connection.execute('ALTER TABLE candidates ADD candidate_id SERIAL PRIMARY KEY;')
    return True

def create_uni_candidates_table(df):

    return True


candidates_df = create_candidates_df(clean_applicants_df)
uni_df = create_uni_df(clean_applicants_df)
uni_candidates_df = create_uni_candidates_junction(uni_df, clean_applicants_df)

create_candidates_table(candidates_df)


######################
# person_df = clean_applicants_df[['name', 'gender', 'dob']]
# academy_df = clean_entry_df[['FullName', 'Academy']].drop_duplicates()


# academy_names_df = clean_academy_df[['name', 'trainer']]

# merged_inner = pd.merge(left=academy_df, right=academy_names_df, left_on='FullName', right_on='name')
# merged_inner = pd.merge(left=academy_names_df, right=academy_df, left_on='name', right_on='FullName')


# print(academy_df)
# print(academy_names_df)
# print(merged_inner)
# print(clean_applicants_df.head())

#


# candidates_df.to_sql('candidates', con=connection, if_exists='replace', index=False)
# uni_df.to_sql('universities', con=connection, if_exists='replace', index=False)
# uni_candidates_df.to_sql('uni_cand_junction', con=connection, if_exists='replace', index=False)

# connection.execute('ALTER TABLE candidates ADD PRIMARY KEY (candidate_id);')
# connection.execute('ALTER TABLE universities ADD PRIMARY KEY (uni_id);')
# connection.execute('ALTER TABLE uni_cand_junction ADD PRIMARY KEY (uni_id, candidate_id);')
# connection.execute('ALTER TABLE uni_cand_junction ADD FOREIGN KEY (uni_id) REFERENCES universities;')
# connection.execute('ALTER TABLE uni_cand_junction ADD FOREIGN KEY (candidate_id) REFERENCES candidates;')

# connection.execute('ALTER TABLE universities ADD uni_id SERIAL PRIMARY KEY;')


connection.close()
