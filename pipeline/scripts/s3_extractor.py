"""
This script:
1. Makes a local copy of all the S3 object files (in the 'data24-final-project' bucket)

This script takes a long time to run.
In most cases, you will not need to run it as the Git Repo contains the output of this file in the following directory:
/datafiles
"""

import boto3
import botocore
import os
from pprint import pprint as pp

client = boto3.client('s3')
resource = boto3.resource('s3')
bucket = resource.Bucket('data24-final-project')
objects = [obj.key for obj in bucket.objects.all()]  # All the files/objects in the S3 bucket

save_to_dir = '../../datafiles'

total_files = len(objects)
current_count = 0

# Download all the objects
for obj in objects:
    print(f'Downloading {current_count}/{total_files} ({(current_count/total_files)*100})')

    try:
        filename = obj.replace('/', '-')
        filename = filename.replace(' ', '_')
        local_path = f'{save_to_dir}/{filename}'
        bucket.download_file(obj, local_path)

    # For when we can't download the file
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise

    # If the destination directory does not exist, create it and try agaibn
    except FileNotFoundError as e:
        os.makedirs('../../datafiles')
        bucket.download_file(obj, local_path)

    current_count += 1
