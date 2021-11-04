# NB there is nothing of use in here (yet...)
# This is NOT PEP8 Compliant
import pandas as pd
import pprint as pp

import boto3
import datetime

client = boto3.client('s3')
resource = boto3.resource('s3')
bucket_name = "data24-final-project"
file_name = "Talent/Sparta Day 20 June 2019.txt"

single_object = client.get_object(Bucket=bucket_name, Key=file_name)
text_str = single_object["Body"].read().decode("utf-8")
text_list = text_str.split('\n')
#print(text_list)


for i in range(0, len(text_list), 1):
    if i == 0:
        space_index = text_list[i].index(" ") + 1
        s = text_list[i][space_index::].replace("\r", "")
        print(f"S: {s}")
        d = datetime.datetime.strptime(s, '%d %B %Y') #tick
        print(f"d: {d}")
#        print(d.strftime('%Y-%M-%D'))
    elif i == 1:
        print(text_list[i])
    elif i >= 3:
        print(text_list[i])
