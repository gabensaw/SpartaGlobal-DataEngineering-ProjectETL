import boto3
import pandas as pd
import io
import numpy as np
import csv

'''This is to see the table as a whole and not as a cut off table'''
desired_width=320
pd.set_option('display.width', desired_width)
np.set_printoptions(linewidth=desired_width)
pd.set_option('display.max_columns',10)

'''Using boto3 allows us to access the bucket that contains the data'''
s3_client = boto3.client('s3')
s3 = boto3.resource('s3')
my_bucket = s3.Bucket("data-eng-31-final-project")
objects = my_bucket.objects.all()

def getting_text_file_names():  # function to open only text files
    '''To go through all file names and only retrieve .txt files filenames and add it to a list'''

    file_names = [] # empty list to store all filenames
    text_file_names = [] # empty list to store only .txt filenames
    for object_summary in my_bucket.objects.filter(Prefix="Talent/"): # looping through the talent folder
        file_names.append(object_summary.key)  # add all filenames to list
    for file in file_names: # looping through all filenames inside file_names list
        if file.endswith('.txt'):  # return only txt files
            text_file_names.append(file)  # add all names to list
    return text_file_names # returns files_names that only contain .txt filenames

def extract_info(text_file_names):
    '''Extracts all data from all text files and combines them in a csv filename'''

    lists_of_all_data = [] # list to store all data inside as a nested list

    for file in text_file_names: # looping through each file and reading inside the files
        obj = s3.Object("data-eng-31-final-project", file) # stores the data inside textfile as an object
        body = obj.get()['Body'].read().decode('utf-8') # reads the object and changes unicode to fix error
        body2 = body.split('\r\n') # splits the data \r\n

        people_data_row_list = [] # empty list to store the rows of data
        for item in body2: # item is a row from body2 which has all the data inside it
            sub = item.split(', ') # splits the data up using ', '
            people_data_row_list.append(sub) # # add the row of data into an empty list

        df = pd.DataFrame(people_data_row_list) # changes the list into a pandas dataframe
        df['Start_Date'] = df[0][0] # extracts the date column from dataframe and stores it under a new column named Start_Date
        df['Academy'] = df[0][1] # extracts the academy column from dataframe and stores it under a new column named Academy
        df = df[3:] # This gets rid of the first 3 rows that were irrelevant
        df = df.reset_index() # resets the indexing as 3 rows were deleted previously
        test_df = df[0].str.split('-') # makes a temporary dataframe to split the first column that contained test results and full name
        names_list = [] # empty list to store all the full names
        for fullname in test_df: # loop to go through the temporary dataframe to get the fullnames extracted
            names_list.append(fullname[0]) # appends the fullname to the list of fullnames
        df['Name'] = pd.Series(names_list) # turns the full names list into a pandas Series and stores it under a new column called FullName
        df['Presentation'] = (df[1].str.split(":").str[1]) # spilts the result by : and stores the second part of the data under the new column name presentation
        df['Psychometrics'] = (df[0].str.split(":").str[1])# spilts the result by : and stores the second part of the data under the new column name psychometrics
        df.drop([0, 1], axis=1, inplace=True) # drops the columns indexed as 0 and 1
        df.drop(['index'], axis=1, inplace=True) # drops the column named 'index'

        for rows in range(len(df)): # goes through the dataframe 1 row at a time
            startdate = df.iloc[rows, 0] # stores the start date value in variable startdate
            academy = df.iloc[rows, 1]# stores the academy value in variable academy
            name = df.iloc[rows, 2]# stores the name value in variable name
            presentation =  df.iloc[rows, 3]# stores the presentation value in variable pressentation
            psycho = df.iloc[rows, 4]# stores the psychometrics value in variable psycho
            row = [startdate, academy, name, presentation, psycho] # stores all the values as a list called row
            lists_of_all_data.append(row)

    csvfile = pd.DataFrame(lists_of_all_data, columns=['Start_Date', 'Academy', 'FullName', 'Presentation', 'Psychometrics']) # makes a pandas dataframe that contains column names
    csvfile.to_csv('output.csv') # converts the data frame to a csv called output.csv
    return csvfile # returns csv file that contains all the merged data from text files

test = getting_text_file_names()
extract_info(test)

