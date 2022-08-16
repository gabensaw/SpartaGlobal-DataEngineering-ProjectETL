import json
import boto3
import csv
from botocore.exceptions import ClientError
import tech_self_score

# connect to AWS s3
try:
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket('data-eng-31-final-project')
    objects = bucket.objects.filter(Prefix='Talent')
except ClientError as e:
    print(e)


# Takes in a dictionary, extracts fields and returns a list
def get_fields(id, content, type):
    if type == "main":
        return [id,
                content["name"],
                content["date"],
                content["self_development"],
                content["geo_flex"],
                content["financial_support_self"],
                content["result"],
                content["course_interest"]]
    elif type == "self-scores":
        return [id,
                content["tech_self_score"]]


# write list of lists to csv
def write_to_csv(list_of_lists, out_type):
    filename = ""
    headers = []
    if out_type == "main":
        filename = "main_talent.csv"
        headers = ['id', 'name', 'date', 'self_development',
                   'geo_flex', 'financial_support_self',
                   'result', 'course_interest']
    elif out_type == "self_scores":
        filename = "tech_self_score.csv"
        headers = ['id', 'skill', 'score']

    try:
        with open(filename, "w", newline='') as csv_file1:
            csv_writer = csv.writer(csv_file1)
            csv_writer.writerow(headers)
            csv_writer.writerows(list_of_lists)
    except e:
        print(e)
        raise
    finally:
        print(filename + " is created.")


# extract data from json, writes to csv
def extract(talent_object):
    main_talent = []
    tech_self_scores = []
    for uid, obj in enumerate(talent_object, 1):
        if obj.key.endswith('.json'):
            file_content = obj.get()['Body'].read().decode('utf-8')
            json_content = json.loads(file_content)
            person = get_fields(uid, json_content, "main")
            main_talent.append(person)
            tech_self_score_sublist = get_fields(uid, json_content, "self-scores")
            tech_self_scores.append(tech_self_score.create_tech_self_score_list(tech_self_score_sublist))

    write_to_csv(main_talent, "main")
    write_to_csv(tech_self_scores, "self_scores")


extract(objects)
