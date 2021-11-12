import pprint

import boto3 as b3
import pipeline.config_manager as conf

# parent class to connect to the s3 client using BOTO2
class S3ParentClass:
    def __init__(self):
        self.__client = b3.client('s3')
        self.__resources = b3.resource('s3')
        self.__bucket_name = conf.BUCKET_NAME
        print("Collection successfully established!")
    # returns s3 client info
    @property
    def client(self):
        return self.__client
    # returns s3 resource info
    @property
    def resource(self):
        return self.__resources
    # returns s3 bucket name from which the data will be pulled
    @property
    def bucket_name(self):
        return self.__bucket_name



if __name__ == '__main__':
    test_p_class = S3ParentClass()
    pprint.pprint(test_p_class.bucket_name)
