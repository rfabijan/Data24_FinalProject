import csv

import PrototypeS3Class as s3c
import pandas as pd
import pprint as p


class AcademiesCsvExtractor(s3c.S3ParentClass):
    def __init__(self):
        super().__init__()

        self.__keys = self.academy_csv

    @property
    def keys(self):
        return self.__keys

    @staticmethod
    def trainer_name(csv):
        return csv["trainer"][0]


    @staticmethod
    def len_of_rows(csv):
        return len(csv["name"])


    @staticmethod
    def obtain_name(csv):
        list = []
        for student in range(0, len(csv["name"])):
            list.append(csv["name"][student])
        return list

    def extract_row(self, csv, name):
        return csv.iloc[:]

    def singe_csv(self, key):
        single_csv = pd.read_csv(self.client.get_object(Bucket=self.bucket_name, Key=key)["Body"])
        return single_csv



csv_extractor = AcademiesCsvExtractor()

for i in csv_extractor.keys:
    file = csv_extractor.singe_csv(i)
    # print(i, "    ", csv_extractor.trainer_name(file), "   ", csv_extractor.obtain_name(file))
    # print(csv_extractor.extract_row(file, "Seumas Lemonby"))
    for i in range(0, csv_extractor.len_of_rows(file)):
        print(file.iloc[:]["name"][i])

# Produce a dictionary Name : sully, trainer : danny, academy: data 24 wk1:{studios: }}
# for i in csv_extractor.keys:
#     reader = csv.DictReader(open(csv_extractor.client.get_object(Bucket=csv_extractor.bucket_name, Key=i)["Body"]))
#     for row in reader:
#         print(row)
