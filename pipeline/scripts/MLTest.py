# NB there is nothing of use in here (yet...)
# This is NOT PEP8 Compliant
import pandas as pd
import pprint as pp

import boto3
import datetime

client = boto3.client('s3')
resource = boto3.resource('s3')
bucket_name = "data24-final-project"
file_name = "Talent/Sparta Day 14 May 2019.txt"

single_object = client.get_object(Bucket=bucket_name, Key=file_name)
text_str = single_object["Body"].read().decode("utf-8")
text_list = text_str.split('\n')


for i in range(0, len(text_list), 1):
    if i == 0:
        s = text_list[0][8::]
#        print(s[::1])
        d = datetime.datetime.strptime(s, '%d %B %Y')
        print(d.strftime('%Y-%M-%D'))


