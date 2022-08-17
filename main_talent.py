import json
import boto3
import csv
from botocore.exceptions import ClientError


def create_tech_self_score_list(u_id, self_scores):
    # List of lists to store tech self scores
    self_scores_list = []
    for skill in self_scores[0].keys():
        # One row for each skill
        row = [u_id, skill, self_scores[0][skill]]
        self_scores_list.append(row)
    return self_scores_list


def create_strengths_list(u_id, strengths):
    # List of lists to store strengths
    strengths_list = []

    for strength in strengths[0]:
        # One row for each strength
        row = [u_id, strength]
        strengths_list.append(row)
    return strengths_list


def create_weaknesses_list(u_id, weaknesses):
    # List of lists to store weaknesses
    weaknesses_list = []

    for weakness in weaknesses[0]:
        # One row for each strength
        row = [u_id, weakness]
        weaknesses_list.append(row)
    return weaknesses_list


def get_fields(content, content_type):
    # Headers for main.csv
    if content_type == "main":
        return [content["name"],
                content["date"],
                content["self_development"],
                content["geo_flex"],
                content["financial_support_self"],
                content["result"],
                content["course_interest"]]

    # Headers for tech_self_scores.csv
    elif content_type == "self_scores":

        try:
            return [content["tech_self_score"]]
        except KeyError:
            return False

    # Headers for strengths.csv
    elif content_type == "strengths":
        return [content["strengths"]]

    # Headers for weaknesses.csv
    elif content_type == "weaknesses":
        return [content["weaknesses"]]


# Write list of lists to CSV files
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

    elif out_type == "strengths":
        filename = "strengths.csv"
        headers = ['id', 'strengths']

    elif out_type == "weaknesses":
        filename = "weaknesses.csv"
        headers = ['id', 'weaknesses']

    try:
        with open(filename, "w", newline='') as new_csv_file:
            csv_writer = csv.writer(new_csv_file)
            csv_writer.writerow(headers)
            csv_writer.writerows(list_of_lists)
    except e:
        print(e)
        raise
    finally:
        print(filename + " is created.")


# Extract data from JSON files and write to CSV files.
def extract(talent_object):
    main_talent = []
    tech_self_scores = []
    strengths = []
    weaknesses = []

    for u_id, obj in enumerate(talent_object, 1):
        if obj.key.endswith('.json'):

            # Parse JSON files.
            file_content = obj.get()['Body'].read().decode('utf-8')
            json_content = json.loads(file_content)

            person = get_fields(json_content, "main")
            main_talent.append(person)

            # Skip getting self scores if they are not available.
            if get_fields(json_content, "self_scores") is not False:
                tech_self_score_sublist = get_fields(json_content, "self_scores")
                tech_self_scores.append(create_tech_self_score_list(u_id, tech_self_score_sublist))

            strengths_sublist = get_fields(json_content, "strengths")
            strengths.append(create_strengths_list(u_id, strengths_sublist))

            weaknesses_sublist = get_fields(json_content, "weaknesses")
            weaknesses.append(create_weaknesses_list(u_id, weaknesses_sublist))

    # Join lists within lists.
    tech_self_scores = sum(tech_self_scores, [])
    strengths = sum(strengths, [])
    weaknesses = sum(weaknesses, [])

    # Transfer lists to CSVs.
    write_to_csv(main_talent, "main")
    write_to_csv(tech_self_scores, "self_scores")
    write_to_csv(strengths, "strengths")
    write_to_csv(weaknesses, "weaknesses")


# connect to AWS s3
try:
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket('data-eng-31-final-project')
    talent_files = bucket.objects.filter(Prefix='Talent')
except ClientError as e:
    print(e)

extract(talent_files)
