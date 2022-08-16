import boto3
import json
import pandas as pd

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')
list_of_lists = []


# creating list of lists

def create_strength_df(uid, json_file):  # This creates a dataframe with each persons strengths on eachrow.
    for i in range(len(json_file['strengths'])):
        list = []
        list.append(uid)
        list.append(dict['strengths'][i])
        list_of_lists.append(list)


def create_df(list_of_lists):
    df = pd.DataFrame(list_of_lists,
                      columns=['uid', 'strengths'])
    return (df)


df = create_df(list_of_lists)
df.to_csv('person_strengths.csv')
