import json
import boto3
import pprint as pp


def count_keys_func(json_content):
    for key in json_content:
        if key not in count_keys:
            count_keys[key] = 1
        else:
            count_keys[key] += 1
    return count_keys


count_keys = {}

s3_resource = boto3.resource('s3')

bucket = s3_resource.Bucket('data-eng-31-final-project')
objects = bucket.objects.filter(Prefix='Talent')
for object in objects:
    if object.key.endswith('.json'):
        file_content = object.get()['Body'].read().decode('utf-8')
        json_content = json.loads(file_content)
        print(json_content)
        count_keys_func(json_content)

print(count_keys)
