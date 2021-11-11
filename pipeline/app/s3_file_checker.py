import boto3
from datetime import datetime,timedelta
import pytz

s3 = boto3.resource('s3')

# specifies the buckets to be checked
buckets_to_check = ['data24-final-project']

# sets the current time and sets the update check timer
def check_bucket_for_new_files():
    time_now = datetime.utcnow().replace(tzinfo=pytz.UTC)
    delta_hours = time_now - timedelta(hours=1)
    # loops over the bucket names
    for bucket_name in buckets_to_check:
        bucket = s3.Bucket(bucket_name)
        # loops over the objects in the bucket
        for key in bucket.objects.all():
            if key.last_modified >= delta_hours:
                print("There are new files in the bucket:  %s" %bucket)
                print("Files modified: ", key)
                print("Files modified at: ", key.last_modified)
                break
        else:
            print ('No new files found in the bucket in the last hour')


check_bucket_for_new_files()
