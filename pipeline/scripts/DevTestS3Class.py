import boto3 as b3
import pipeline.config_manager as conf


class S3ParentClass:
    def __init__(self):
        self.__client = b3.client('s3')
        self.__resource = b3.resource('s3')
        self.__bucket_name = conf.BUCKET_NAME  # Changed to be in a config later

    #txt list = _____

    #json_list = ____

    #applicant_csv = _____

    #academy_csv = _____

    @property
    def get_client(self):
        return self.__client

    @property
    def get_resource(self):
        return self.__resource

    @property
    def get_bucket_name(self):
        return self.__bucket_name
