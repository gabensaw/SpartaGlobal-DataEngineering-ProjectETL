import pandas as pd

pd.set_option('display.max_columns', None)

df_academy = pd.read_csv('academy.csv')

columns_to_int_transformation = ['Analytic_W1', 'Independent_W1', 'Determined_W1',
                                 'Professional_W1', 'Studious_W1', 'Imaginative_W1', 'Analytic_W2',
                                 'Independent_W2', 'Determined_W2', 'Professional_W2', 'Studious_W2',
                                 'Imaginative_W2', 'Analytic_W3', 'Independent_W3', 'Determined_W3',
                                 'Professional_W3', 'Studious_W3', 'Imaginative_W3', 'Analytic_W4',
                                 'Independent_W4', 'Determined_W4', 'Professional_W4', 'Studious_W4',
                                 'Imaginative_W4', 'Analytic_W5', 'Independent_W5', 'Determined_W5',
                                 'Professional_W5', 'Studious_W5', 'Imaginative_W5', 'Analytic_W6',
                                 'Independent_W6', 'Determined_W6', 'Professional_W6', 'Studious_W6',
                                 'Imaginative_W6', 'Analytic_W7', 'Independent_W7', 'Determined_W7',
                                 'Professional_W7', 'Studious_W7', 'Imaginative_W7', 'Analytic_W8',
                                 'Independent_W8', 'Determined_W8', 'Professional_W8', 'Studious_W8',
                                 'Imaginative_W8', 'Analytic_W9', 'Independent_W9', 'Determined_W9',
                                 'Professional_W9', 'Studious_W9', 'Imaginative_W9', 'Analytic_W10',
                                 'Independent_W10', 'Determined_W10', 'Professional_W10', 'Studious_W10',
                                 'Imaginative_W10']


def formatting_name(df, column):
    df[column] = df[column].str.lower()
    return df


def clean_weeks_columns(df, columns_list):
    for column in columns_list:
        df[column] = df[column].astype('Int64')
    return df


clean_name = formatting_name(df_academy, 'name')
clean_trainer = formatting_name(df_academy, 'trainer')
clean_cols = clean_weeks_columns(df_academy, columns_to_int_transformation)

df_academy.to_csv('clean_academy.csv', index=False)
