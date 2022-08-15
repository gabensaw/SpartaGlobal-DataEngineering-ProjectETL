import json
import boto3
import csv
from botocore.exceptions import ClientError

# connect to AWS s3
try:
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket('data-eng-31-final-project')
    objects = bucket.objects.filter(Prefix='Talent')
except ClientError as e:
    print(e)


# Takes in a dictionary, extracts fields and returns a list
def get_fields(id, content):
    return [id,
            content["name"],
            content["date"],
            content["self_development"],
            content["geo_flex"],
            content["financial_support_self"],
            content["result"],
            content["course_interest"]]


# write list of lists to csv
def write_to_csv(list_of_lists, filename="main_talent.csv"):
    try:
        with open(filename, "w", newline='') as csv_file1:
            csv_writer = csv.writer(csv_file1)
            csv_writer.writerow(['id',
                                 'name',
                                 'date',
                                 'self_development',
                                 'geo_flex',
                                 'financial_support_self',
                                 'result',
                                 'course_interest'])
            csv_writer.writerows(list_of_lists)
    except e:
        print(e)
        raise
    finally:
        print("main_talent.csv is created.")


# extract data from json, writes to csv
def extract(talent_object):
    main_talent = []
    for uid, obj in enumerate(talent_object, 1):
        if obj.key.endswith('.json'):
            file_content = obj.get()['Body'].read().decode('utf-8')
            json_content = json.loads(file_content)
            person = get_fields(uid, json_content)
            main_talent.append(person)
    write_to_csv(main_talent)


extract(objects)
