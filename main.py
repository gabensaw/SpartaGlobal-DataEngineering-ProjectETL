import boto3
import pandas as pd
import io
import numpy as np
import csv
from datetime import datetime

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

def drop_empty_values(csvfile):
    for row in csvfile['FullName']:
        if row == '':
            csvfile.dropna(inplace=True)
    csvfile2 = csvfile.reset_index()
    csvfile2.drop(['index'], axis=1, inplace=True)
    return csvfile2

def clean_start_date(csvfile):
    start_date_list = []
    for entry in csvfile['Start_Date']:
        split_date = entry.split()
        year = split_date[3]
        month = str(monthToNum(split_date[2])).zfill(2)
        day = split_date[1].zfill(2)
        date = str(day) + '/' + str(month) + '/' + str(year)
        x = datetime.strptime(date, '%d/%m/%Y').date()
        clean_date = x.strftime("%d/%m/%Y")
        start_date_list.append(clean_date)

    csvfile['Start_Date'] = pd.Series(start_date_list)

def monthToNum(shortMonth):
    return {
            'January': 1,
            'February': 2,
            'March': 3,
            'April': 4,
            'May': 5,
            'June': 6,
            'July': 7,
            'August': 8,
            'September': 9,
            'October': 10,
            'November': 11,
            'December': 12
    }[shortMonth]

def clean_academy(csvfile):
    academy_list = []
    for academy in csvfile['Academy']:
        academy_name = academy.split()
        academy_name_only = academy_name[0]
        academy_list.append(academy_name_only)
    csvfile['Academy'] = pd.Series(academy_list)

def clean_fullname(csvfile):
    fullname_list = []
    for fullname in csvfile['FullName']:
        clean_fn = fullname.lower().strip()
        fullname_list.append(clean_fn)

    csvfile['FullName'] = pd.Series(fullname_list)

def clean_presentation(csvfile):
    percentage_list = []
    for fraction in csvfile['Presentation']:
        fractions = str(fraction)
        split_fraction = fractions.split('/')
        numerator = split_fraction[0]
        denominator = split_fraction[1]
        ptc_score = round((float(numerator)/float(denominator)) * 100, 2)
        percentage_list.append(ptc_score)

    csvfile['Presentation'] = pd.Series(percentage_list)

def clean_psycho_data(csvfile):
    percentage_list = []
    for fraction in csvfile['Psychometrics']:
        fractions = str(fraction)
        split_fraction = fractions.split('/')
        numerator = split_fraction[0]
        denominator = split_fraction[1]
        ptc_score = (float(numerator) / float(denominator)) * 100
        percentage_list.append(ptc_score)

    csvfile['Psychometrics'] = pd.Series(percentage_list)

test = getting_text_file_names()
csvfile = extract_info(test)
new = drop_empty_values(csvfile)
clean_start_date(new)
clean_academy(new)
clean_fullname(new)
clean_presentation(new)
clean_psycho_data(new)

new.to_csv('output/clean_entry_test_data.csv')

