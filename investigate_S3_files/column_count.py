"""
# column_count.py
# The script counts the occurrences of column names from CSVs in the Academy folder of the data-eng-31-final-project bucket
# by Ali Shaheed
"""

import csv
import boto3


s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')
bucket = s3_resource.Bucket('data-eng-31-final-project')
objects = bucket.objects.filter(Prefix='Academy')

col_counts = dict()
for obj in objects:
    if obj.key.endswith('.csv'):
        file_content = obj.get()['Body'].read().decode('utf-8')
        csv_content = csv.reader(file_content.split('\r\n'))
        col_list = list()
        for row in csv_content:
            col_list.append(row)
            break
        for header in col_list[0]:
            col_counts[header] = col_counts.get(header, 0) + 1

print(col_counts)



