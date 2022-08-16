import boto3
import json
import pandas as pd


s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')


# creating list of lists
def create_strength_df():  # This creates a dataframe with each persons weaknesses on eachrow.
    list_of_lists = []
    i = 1
    bucket_name = 'data-eng-31-final-project'
    bucket_contents = s3_client.list_objects_v2(Bucket=bucket_name, Prefix='Talent')
    for content in bucket_contents['Contents']:
        file = content['Key']
        s3_object = s3_client.get_object(Bucket='data-eng-31-final-project', Key=file)
        strbody = s3_object['Body'].read()

        for strengths in range(len(json.loads(strbody)['strengths'])):
            list = []
            list.append(i)
            list.append(json.loads(strbody)['strengths'][strengths])
            list_of_lists.append(list)
        i = i + 1

    df = pd.DataFrame(list_of_lists,
                      columns=['id', 'strengths'])
    return (df)


df = create_strength_df()
df.to_csv('person_strengths.csv')
