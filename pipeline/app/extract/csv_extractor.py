import csv
import pipeline.app.extract.s3_connector as s3c
import pandas as pd
import pprint as p


class AcademiesCsvExtractor(s3c.S3ParentClass):
    def __init__(self):
        super().__init__()
        self.__keys = self.academy_csv

    # keys property as assigned in init
    @property
    def keys(self):
        return self.__keys

    # Returns the number of rows in a csv file.
    @staticmethod
    def len_of_rows(csv):
        return len(csv["name"])

    # Returns the name for a given row in a csv file.
    @staticmethod
    def extract_name(file_name, row_number: int) -> str:
        return file_name.iloc[:]["name"][row_number]

    # Returns the trainer name for a given row in a csv file.
    @staticmethod
    def extract_trainer(file_name, row_number: int) -> str:
        return file_name.iloc[:]["trainer"][row_number]

    # Returns the number of training weeks in the academy csv files.
    @staticmethod
    def extract_weeks(columns) -> int:
        string = columns[-1].split("_")
        num_of_weeks = string[1].split("W")
        return int(num_of_weeks[1])

    # Returns the value for a given column and given name.
    @staticmethod
    def extract_skill_value(file_name, column_name: str, row_number: int) -> str:
        return file_name.iloc[:][column_name][row_number]

    # Returns a single csv file.
    def single_csv(self, key):
        single_csv = pd.read_csv(self.client.get_object(Bucket=self.bucket_name, Key=key)["Body"])
        return single_csv

    # Produces a dictionary in the form of name:{wk:{skill:}} for a given csv file.
    def extract_skill_values_per_person_per_week(self, csv):
        dict_holder = {}
        for rows in range(0, self.len_of_rows(csv)):
            dict_holder[self.extract_name(csv, rows)] = {}
            for numb in range(1, self.extract_weeks(csv.columns) + 1):
                var = "W" + str(numb)
                dict_holder[self.extract_name(csv, rows)][var] = {}
                for columns in csv.columns:
                    if columns.endswith(var):
                        dict_holder[self.extract_name(csv, rows)][var][columns] = \
                            self.extract_skill_value(csv, columns, rows)
        return dict_holder

    @staticmethod
    def extract_course_name(key) -> str:
        split_1 = key.split("/")[1]
        academy = split_1.split("_")[0] + " " + split_1.split("_")[1]
        return academy

    @staticmethod
    def extract_date(key) -> str:
        date = key.split("_")[2].split(".")[0]
        return date


class ApplicantsCsvExtractor(AcademiesCsvExtractor):
    def __init__(self):
        super().__init__()
        self.__keys = self.talent_csv

    # keys property as assigned in init
    @property
    def keys(self):
        return self.__keys

    # Returns the id for a given row in a csv file.
    @staticmethod
    def extract_id(file_name, row_number: int) -> str:
        return file_name.iloc[:]["id"][row_number]

    # Returns the gender for a given row in a csv file.
    @staticmethod
    def extract_gender(file_name, row_number: int) -> str:
        return file_name.iloc[:]["gender"][row_number]

    # Returns the dob for a given row in a csv file.
    @staticmethod
    def extract_dob(file_name, row_number: int) -> str:
        return file_name.iloc[:]["dob"][row_number]

    # Returns the email for a given row in a csv file.
    @staticmethod
    def extract_email(file_name, row_number: int) -> str:
        return file_name.iloc[:]["email"][row_number]

    # Returns the city for given row in a csv file.
    @staticmethod
    def extract_city(file_name, row_number: int) -> str:
        return file_name.iloc[:]["city"][row_number]

    # Returns the address for a given row in a csv file.
    @staticmethod
    def extract_address(file_name, row_number: int) -> str:
        return file_name.iloc[:]["address"][row_number]

    # Returns the postcode for a given row in a csv file.
    @staticmethod
    def extract_postcode(file_name, row_number: int) -> str:
        return file_name.iloc[:]["postcode"][row_number]

    # Returns the phone number for a given row in a csv file.
    @staticmethod
    def extract_phone_number(file_name, row_number: int) -> str:
        return file_name.iloc[:]["phone_number"][row_number]

    # Returns the university name for a given row in a csv file.
    @staticmethod
    def extract_university(file_name, row_number: int) -> str:
        return file_name.iloc[:]["uni"][row_number]

    # Returns the degree classification for a given row in a csv file.
    @staticmethod
    def extract_degree(file_name, row_number: int) -> str:
        return file_name.iloc[:]["degree"][row_number]

    # Returns the date an applicant was invited to in a csv file.
    @staticmethod
    def extract_invited_date(file_name, row_number: int) -> str:
        return file_name.iloc[:]["invited_date"][row_number]

    # Returns the month an applicant was invited to in a csv file
    @staticmethod
    def extract_month(file_name, row_number: int) -> str:
        return file_name.iloc[:]["month"][row_number]

    # Returns the name of the person who invited an applicant in a csv file.
    @staticmethod
    def extract_invited_by(file_name, row_number: int) -> str:
        return file_name.iloc[:]["invited_by"][row_number]

    @staticmethod
    def extract_title_date(key) -> str:
        month = key.lstrip("Talent/").rstrip("Applicants.csv")
        date = month.split("2")
        return date[0] + "-2" + date[1]


if __name__ == '__main__':
    # Dictionary Name:{"Wk":{"Analytic": , "Independent"}}
    test = ApplicantsCsvExtractor()
    print(test.extract_title_date('Talent/April2019Applicants.csv'))
    # p.pprint(test.extract_city(test.singe_csv('Talent/April2019Applicants.csv'), 36))
    csv_extractor = AcademiesCsvExtractor()
    # p.pprint(dict_holder)
    # p.pprint(csv_extractor.extract_skill_values_per_person_per_week(
    #     csv_extractor.singe_csv("Academy/Engineering_29_2019-12-30.csv")))
