import pprint

import boto3 as b3
import pipeline.config_manager as conf


class S3ParentClass:
    def __init__(self):
        self.__client = b3.client('s3')
        self.__resources = b3.resource('s3')
        self.__bucket_name = conf.BUCKET_NAME

    @property
    def client(self):
        return self.__client

    @property
    def resource(self):
        return self.__resources

    @property
    def bucket_name(self):
        return self.__bucket_name



if __name__ == '__main__':
    test_p_class = S3ParentClass()
    pprint.pprint(test_p_class.bucket_name)
