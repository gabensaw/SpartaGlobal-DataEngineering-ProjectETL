import pandas as pd

pd.set_option('display.max_columns', None)

df_talent = pd.read_csv('talent.csv', encoding="ISO-8859-1")


def drop_column(df, column_name):
    df.drop(column_name, inplace=True, axis=1)
    return df


def clean_address(df):
    df.address = df.address.str.lower()
    return df


def formatting_name(df, column):
    df[column] = df[column].str.lower()
    return df


def clean_degree(df):
    df.degree = df.degree.astype('string')
    df.degree = df.degree.apply(lambda x: x.replace('0', '') if x is not pd.NA else x)
    return df


def clean_phone_numbers(df):
    '''
    Function to remove whitespaces and characters
    :return: full number of user with country code (i.e. +44)
    '''

    chars_to_remove = [' ', '(', ')', '-']
    df['phone_number'] = df['phone_number'].astype('string')

    for char in chars_to_remove:
        df['phone_number'] = df['phone_number'].apply(lambda x: x.replace(char, '') if x is not pd.NA else x)

    return df


def clean_day(df):
    df['invited_date'] = df['invited_date'].astype('Int64').astype('string')
    df['invited_date'] = df['invited_date'].apply(lambda x: '0' + x if x is not pd.NA and len(x) == 1 else x)
    return df


def clean_month(df):
    month_dic = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06',
                 'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'}
    df['month'] = df['month'].astype('string')
    df['month'] = df['month'].apply(
        lambda x: month_dic[x.split()[0][:3].title()] + '/' + x.split()[1] if x is not pd.NA else x
    )
    return df


def date_merge(df, day_col_name, month_col_name):
    df['start_date'] = df[day_col_name] + '/' + df[month_col_name]
    return df


drop_column(df_talent, 'id')
formatting_name(df_talent, 'name')
clean_address(df_talent)
clean_phone_numbers(df_talent)
clean_degree(df_talent)
clean_day(df_talent)
clean_month(df_talent)
date_merge(df_talent, 'invited_date', 'month')
drop_column(df_talent, 'invited_date')
drop_column(df_talent, 'month')
formatting_name(df_talent, 'invited_by')

df_talent.to_csv('clean_applicants.csv', index=False)
