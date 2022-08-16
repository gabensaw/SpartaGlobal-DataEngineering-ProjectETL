import boto3
import pandas as pd
import io
import numpy as np
import csv

desired_width=320
pd.set_option('display.width', desired_width)
np.set_printoptions(linewidth=desired_width)
pd.set_option('display.max_columns',10)


s3_client = boto3.client('s3')
s3 = boto3.resource('s3')
my_bucket = s3.Bucket("data-eng-31-final-project")
objects = my_bucket.objects.all()

def getting_text_file_names():  # function to open only text files
    file_names = []
    text_file_names = []
    for object_summary in my_bucket.objects.filter(Prefix="Talent/"):
        file_names.append(object_summary.key)  # add all filenames to list
    for file in file_names:
        if file.endswith('.txt'):  # return only txt files
            text_file_names.append(file)  # add all names to list
    return text_file_names

def extract_info(text_file_names):

    lists_of_all_data = []

    for file in text_file_names:
        obj = s3.Object("data-eng-31-final-project", file)
        body = obj.get()['Body'].read().decode('utf-8')
        body2 = body.split('\r\n')

        people_list = []
        for item in body2:
            sub = item.split(', ')
            people_list.append(sub)

        df = pd.DataFrame(people_list)
        df['Start_Date'] = df[0][0]
        df['Academy'] = df[0][1]
        df = df[3:]
        df = df.reset_index()
        test_df = df[0].str.split('-')
        names_list = []
        for fullname in test_df:
            names_list.append(fullname[0])
        df['Name'] = pd.Series(names_list)
        df['Presentation'] = (df[1].str.split(":").str[1])
        df['Psychometrics'] = (df[0].str.split(":").str[1])
        df.drop([0, 1], axis=1, inplace=True)
        df.drop(['index'], axis=1, inplace=True)

        for rows in range(len(df)):
            startdate = df.iloc[rows, 0]
            academy = df.iloc[rows, 1]
            name = df.iloc[rows, 2]
            presentation =  df.iloc[rows, 3]
            psycho = df.iloc[rows, 4]
            row = [startdate, academy, name, presentation, psycho]
            lists_of_all_data.append(row)

    testing = pd.DataFrame(lists_of_all_data,  columns=['Start_Date', 'Academy', 'FullName', 'Presentation', 'Psychometrics'])
    testing.to_csv('output.csv')
    return testing

test = getting_text_file_names()
extract_info(test)

