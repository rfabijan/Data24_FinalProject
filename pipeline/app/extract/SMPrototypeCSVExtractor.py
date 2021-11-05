import csv


import pipeline.app.extract.PrototypeS3Class as s3c

import pandas as pd
import pprint as p


class AcademiesCsvExtractor(s3c.S3ParentClass):
    def __init__(self):
        super().__init__()

        self.__keys = self.academy_csv

    @property
    def keys(self):
        return self.__keys

    def singe_csv(self, key):
        single_csv = pd.read_csv(self.client.get_object(Bucket=self.bucket_name, Key=key)["Body"])
        return single_csv

    @staticmethod
    def len_of_rows(csv):
        return len(csv["name"])

    @staticmethod
    def extract_name(file_name, row_number):
        return file_name.iloc[:]["name"][row_number]

    @staticmethod
    def extract_trainer(file_name, row_number):
        return file_name.iloc[:]["trainer"][row_number]

    @staticmethod
    def extract_weeks(columns):
        string = columns[-1].split("_")
        num_of_weeks = string[1].split("W")
        return int(num_of_weeks[1])

    @staticmethod
    def extract_skill_value(file_name, column_name, row_number):
        return file_name.iloc[:][column_name][row_number]


if __name__ == '__main__':

    dict_holder = {}
    csv_extractor = AcademiesCsvExtractor()
    for i in csv_extractor.keys:
        file = csv_extractor.singe_csv(i)
        # print(csv_extractor.len_of_rows(file))
        for row_num in range(0, csv_extractor.len_of_rows(file) + 1):
            # print(row_num)
            dict_holder[row_num] = {}
            for numb in range(1, csv_extractor.extract_weeks(file.columns)):
                var = "W" + str(numb)
                dict_holder[row_num][var] = {}
                for column in file.columns:
                    print(column)
                    if column.endswith(var):
                        dict_holder[row_num][var][column] =  [1]#[csv_extractor.extract_skill_value(file, column, row_num)]
    #print(dict_holder)

    # for numb in range(1, csv_extractor.extract_weeks(file.columns) + 1):
    #     var = "W" + str(numb)
    #     dict_holder[var] = {}
    #     for row_number in range(0, csv_extractor.len_of_rows(file)):
    #         dict_holder[var][row_number] = {}
    #         for column in file.columns:
    #             if column.endswith(var):
    #                 dict_holder[var][row_number][column] = [csv_extractor.extract_skillvalue(file, column,
    #                                                                                          row_number)]
    # p.pprint(dict_holder)

    # for i in csv_extractor.keys:

    #     file = csv_extractor.single_csv(i)
    #
    #     # print(i, "    ", csv_extractor.trainer_name(file), "   ", csv_extractor.obtain_name(file))
    #     # print(csv_extractor.extract_row(file, "Seumas Lemonby"))
    #     for i in range(0, csv_extractor.len_of_rows(file)):
    #         weeks = csv_extractor.extract_weeks(file.columns)
    #         print(csv_extractor.obtain_name(file, i), "   ", csv_extractor.extract_Analytics(file, i, weeks))

    # will move to the transform stage.
    # for i in csv_extractor.keys:
    #     file = csv_extractor.singe_csv(i)
    #     for i in range(0, csv_extractor.len_of_rows(file)):
    #         print(file.iloc[:]["name"][i])

    # @staticmethod
    # def trainer_name(csv):
    #     return csv["trainer"][0]
    #
    #

    #
    #
    # @staticmethod
    # def obtain_name(csv):
    #     list = []
    #     for student in range(0, len(csv["name"])):
    #         list.append(csv["name"][student])
    #     return list

# Produce a dictionary Name : sully, trainer : danny, academy: data 24 wk1:{studios: }}
# for i in csv_extractor.keys:
#     reader = csv.DictReader(open(csv_extractor.client.get_object(Bucket=csv_extractor.bucket_name, Key=i)["Body"]))
#     for row in reader:
#         print(row)


# for i in range(0, csv_extractor.len_of_rows(file)):
# print(file.iloc[:]["Analytic%"][i])
