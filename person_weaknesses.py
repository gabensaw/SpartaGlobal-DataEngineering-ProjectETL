# Task: go into Amazon S3 > Buckets> data-eng-31-final-project > Talent/
# Extract the weaknesses information from each of the json files in the folder.

import boto3
import json
import pandas as pd
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

def create_weak_df():     # This creates a dataframe with each persons weaknesses on eachrow.
    list_of_lists = []
    i = 1
    bucket_name = 'data-eng-31-final-project'
    bucket_contents = s3_client.list_objects_v2(Bucket=bucket_name, Prefix='Talent')
    for content in bucket_contents['Contents']:
        file = content['Key']
        s3_object = s3_client.get_object(Bucket='data-eng-31-final-project', Key=file)
        strbody = s3_object['Body'].read()

        for weakness in range(len(json.loads(strbody)['weaknesses'])):
            list = []
            list.append(i)
            list.append(json.loads(strbody)['weaknesses'][weakness])
            list_of_lists.append(list)
        i = i + 1


    df = pd.DataFrame(list_of_lists,
                      columns = ['id','weakness'])
    return(df)


df = create_weak_df()
df.to_csv('person_weaknesses.csv')





