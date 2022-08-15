import boto3
import json
import pandas as pd
import io

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')


# creating list of lists
def create_strength_df():  # This creates a dataframe with each persons weaknesses on eachrow.
    list_of_lists = []
    bucket_name = 'data-eng-31-final-project'
    bucket_contents = s3_client.list_objects_v2(Bucket=bucket_name, Prefix='Talent')
    for content in bucket_contents['Contents']:
        file = content['Key']
        s3_object = s3_client.get_object(Bucket='data-eng-31-final-project', Key=file)
        strbody = s3_object['Body'].read()
        list = []
        for strength in range(len(json.loads(strbody)['strengths'])):
            list.append(json.loads(strbody)['strengths'][strength])
        list_of_lists.append(list)
    df = pd.DataFrame(list_of_lists,
                      columns=['strength_1', 'strength_2', 'strength_3'])
    return (df)


df = create_strength_df()

str_buffer = io.StringIO()
df.to_csv('str_buffer')
s3_client.put_object(
    Body=str_buffer.getvalue(),
    Bucket='data-eng-31-final-project',
    Key='person_strength.csv')
df.to_csv('person_strength.csv')
