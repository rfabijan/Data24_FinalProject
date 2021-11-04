import pprint

import boto3 as b3
import pipeline.config_manager as conf


class S3ParentClass:
    def __init__(self):
        self.__client = b3.client('s3')
        self.__resource = b3.resource('s3')
        self.__bucket_name = conf.BUCKET_NAME  # Changed to be in a config later
        self.__all_files = self.populate_all_files()
        self.__academy_csv = self.get_all_files[0]
        self.__talent_csv = self.get_all_files[1]
        self.__talent_json = self.get_all_files[2]
        self.__talent_txt = self.get_all_files[3]

    @property
    def get_all_files(self):
        return self.__all_files

    @property
    def get_academy_csv(self):
        return self.__academy_csv

    @property
    def get_talent_csv(self):
        return self.__talent_csv

    @property
    def get_talent_json(self):
        return self.__talent_json

    @property
    def get_talent_txt(self):
        return self.__talent_txt

    @property
    def get_txt_list(self):
        return self.__talent_txt

    @property
    def get_client(self):
        return self.__client

    @property
    def get_resource(self):
        return self.__resource

    @property
    def bucket_name(self):
        return self.__bucket_name

    def populate_all_files(self):
        talent_csv_list = []
        talent_json_list = []
        talent_txt_list = []
        academy_csv_list = []
        error_list = []

        paginator = self.get_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.bucket_name)

        for page in pages:
            for i in page["Contents"]:
                key = i["Key"]
                if key.startswith("Academy"):
                    academy_csv_list.append(key)
                elif key.startswith("Talent") and key.endswith(".csv"):
                    talent_csv_list.append(key)
                elif key.startswith("Talent") and key.endswith(".json"):
                    talent_json_list.append(key)
                elif key.startswith("Talent") and key.endswith(".txt"):
                    talent_txt_list.append(key)
                else:
                    print(f"Key {key} does not fit any criteria, it has been added to the end of the output")
                    error_list.append(key)
        return [academy_csv_list, talent_csv_list, talent_json_list, talent_txt_list, error_list]


test_p_class = S3ParentClass()

pprint.pprint(test_p_class.populate_all_files())
