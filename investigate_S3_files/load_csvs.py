import boto3
import csv


def open_csv_from_s3(bucket, key):
    client = boto3.client('s3')
    return client.get_object(Bucket=bucket, Key=key)


def get_all_csvs_from_s3(bucket, prefix):
    content = ''

    s3_resource = boto3.resource('s3')
    bucket_resource = s3_resource.Bucket(bucket)
    objects = bucket_resource.objects.filter(Prefix=prefix)

    for object in objects:
        if object.key.endswith('.csv'):
            file_content = object.get()['Body'].read().decode('utf-8')
            no_header_content = file_content.split('\n')[1:]
            content += ''.join(no_header_content)

    return content


def create_header(csv_object):
    return csv_object['Body'].read().decode('utf-8').split()[0]


def write_csv(filename, h_data, c_data):
    with open(filename, 'w') as file:
        file.write(h_data)
        file.write('\n')

    with open(filename, 'a') as file:
        file.write(c_data)

    return True


bucket_name = 'data-eng-31-final-project'

# prefix = 'Talent'
# object_key = 'Talent/April2019Applicants.csv'

prefix = 'Academy'
object_key = 'Academy/Engineering_17_2019-02-18.csv'

csv_obj = open_csv_from_s3(bucket_name, object_key)
header = create_header(csv_obj)
content = get_all_csvs_from_s3(bucket_name, prefix)

write_csv('academy.csv', header, content)
# write_csv('talent.csv', header, content)
