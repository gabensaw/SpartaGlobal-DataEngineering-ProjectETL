# Task: go into Amazon S3 > Buckets> data-eng-31-final-project > Talent/
# Extract the weaknesses information from each of the json files in the folder.

import boto3
import json
import pandas as pd
import pprint as pp
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')


"""The function create_weak_df creates a list of lists. Within each child list is the weaknesses of a person. This list 
then gets added to the parent list. Finally, the parent list is turned into a dataframe, where each row represents the 
weaknesses of 1 person and each weakness is in a new cell."""

# def create_weak_df():     # This creates a dataframe with each persons weaknesses on eachrow.
#     list_of_lists = []
#     bucket_name = 'data-eng-31-final-project'
#     bucket_contents = s3_client.list_objects_v2(Bucket=bucket_name, Prefix='Talent')
#     for content in bucket_contents['Contents']:
#         file = content['Key']
#         s3_object = s3_client.get_object(Bucket='data-eng-31-final-project', Key=file)
#         strbody = s3_object['Body'].read()
#         list = []
#         for weakness in range(len(json.loads(strbody)['weaknesses'])):
#             list.append(json.loads(strbody)['weaknesses'][weakness])
#         list_of_lists.append(list)
#     df = pd.DataFrame(list_of_lists,
#                       columns = ['weakness_1','weakness_2','weakness_3'])
#     return(df)

# def create_weak_df():     # This creates a dataframe with 1 weakness per row. The id corrosponds to the person id.
#     list_of_lists = []
#     i = 1
#     bucket_name = 'data-eng-31-final-project'
#     bucket_contents = s3_client.list_objects_v2(Bucket=bucket_name, Prefix='Talent')
#     for content in bucket_contents['Contents']:
#         file = content['Key']
#         s3_object = s3_client.get_object(Bucket='data-eng-31-final-project', Key=file)
#         strbody = s3_object['Body'].read()
#
#         for weakness in range(len(json.loads(strbody)['weaknesses'])):
#             list = []
#             list.append(i)
#             list.append(json.loads(strbody)['weaknesses'][weakness])
#             list_of_lists.append(list)
#         i = i + 1
#
#
#     df = pd.DataFrame(list_of_lists,
#                       columns = ['id','weakness'])
#     return(df)

"""Updating the function to format to fit in with the group code to take in uid and 1 single json file content 
at a time."""

# bucket_name = 'data-eng-31-final-project'
# bucket_contents = s3_client.list_objects_v2(Bucket=bucket_name, Key='Talent/10383.json')
# for content in bucket_contents['Contents']:
#     file = content['Key']
#
# file =  'Talent/10383.json'
#
# s3_object = s3_client.get_object(Bucket='data-eng-31-final-project', Key=file)
# strbody = s3_object['Body'].read()


"""The function create_weak_df takes in a json file and corrosponding uid and adds the uid and weaknesses to the
list of lists. We then have another function which converts the list of lists to a dataframe."""

list_of_lists = []
def create_weak_df(uid, json_file):
    for i in range(len(json_file['weaknesses'])):
        list = []
        list.append(uid)
        list.append(dict['weaknesses'][i])
        list_of_lists.append(list)

# The following function turns the list of lists into a csv.

def create_df(list_of_lists):
    df = pd.DataFrame(list_of_lists,
                           columns = ['uid','weakness'])
    return(df)

df = create_df(list_of_lists)
# df.to_csv('person_weaknesses.csv')