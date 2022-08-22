import pandas as pd
from sqlalchemy import create_engine

conn_string = 'postgresql://postgres:postgres@localhost:5432/sparta_talent'

db = create_engine(conn_string)
connection = db.connect()

pd.set_option('display.max_columns', None)

clean_applicants_df = pd.read_csv('clean_applicants.csv',
                                  dtype={'phone_number': str})


def create_candidates_df(df):
    df['first_name'] = clean_applicants_df.name.apply(lambda x: x.split()[0])
    df['middle_name'] = clean_applicants_df.name.apply(lambda x: x.split()[1:-1])
    df['middle_name'] = clean_applicants_df.middle_name.apply(lambda x: ' '.join(x) if x != [] else pd.NA)
    df['last_name'] = clean_applicants_df.name.apply(lambda x: x.split()[-1])
    candidates_df = df[
        ['first_name', 'middle_name', 'last_name', 'gender', 'dob', 'email', 'city', 'address',
         'postcode', 'phone_number']]
    return candidates_df


def create_candidates_table(df):
    df.to_sql('candidates', con=connection, if_exists='append', index=False)
    return True


candidates_df = create_candidates_df(clean_applicants_df)

create_candidates_table(candidates_df)
connection.close()
