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
    def extract_csv_name(file_name, row_number: int) -> str:
        return file_name.iloc[:]["name"][row_number]

    # Returns the trainer name for a given row in a csv file.
    @staticmethod
    def extract_academies_trainer(file_name, row_number: int) -> str:
        return file_name.iloc[:]["trainer"][row_number]

    # Returns the number of training weeks in the academy csv files.
    @staticmethod
    def extract_academies_weeks(columns) -> int:
        string = columns[-1].split("_")
        num_of_weeks = string[1].split("W")
        return int(num_of_weeks[1])

    # Returns the value for a given column and given name.
    @staticmethod
    def extract_academies_skill_value(file_name, column_name: str, row_number: int) -> float:
        return float(file_name.iloc[:][column_name][row_number])

    # Returns a single csv file.
    def single_csv(self, key):
        single_csv = pd.read_csv(self.client.get_object(Bucket=self.bucket_name, Key=key)["Body"])
        return single_csv

    @staticmethod
    def clean_skill_value(score: int) -> int or None:
        if score in range(1, 9):
            return score
        else:
            return None

    # Produces a dictionary in the form of name:{wk:{skill:}} for a given csv file.
    def extract_academies_skill_values_per_person_per_week(self, csv):
        dict_holder = {}
        for rows in range(0, self.len_of_rows(csv)):
            dict_holder[self.extract_csv_name(csv, rows)] = {}
            for numb in range(1, self.extract_academies_weeks(csv.columns) + 1):
                var = "W" + str(numb)
                dict_holder[self.extract_csv_name(csv, rows)][var] = {}
                for columns in csv.columns:
                    if columns.endswith(var):
                        dict_holder[self.extract_csv_name(csv, rows)][var][columns] = \
                            self.clean_skill_value(self.extract_academies_skill_value(csv, columns, rows))
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
    def extract_id(csv_body, row_number: int) -> str:
        return ApplicantsCsvExtractor.extract_value_from_rowcolumn(csv_body, "id", row_number)

    # Returns the gender for a given row in a csv file.
    @staticmethod
    def extract_gender(csv_body, row_number: int) -> str:
        return ApplicantsCsvExtractor.extract_value_from_rowcolumn(csv_body, "gender", row_number)

    # Returns the dob for a given row in a csv file.
    @staticmethod
    def extract_dob(csv_body, row_number: int) -> str:
        return ApplicantsCsvExtractor.extract_value_from_rowcolumn(csv_body, "dob", row_number)

    # Returns the email for a given row in a csv file.
    @staticmethod
    def extract_email(csv_body, row_number: int) -> str:
        return ApplicantsCsvExtractor.extract_value_from_rowcolumn(csv_body, "email", row_number)

    # Returns the city for given row in a csv file.
    @staticmethod
    def extract_city(csv_body, row_number: int) -> str:
        return ApplicantsCsvExtractor.extract_value_from_rowcolumn(csv_body, "city", row_number)

    # Returns the address for a given row in a csv file.
    @staticmethod
    def extract_address(csv_body, row_number: int) -> str:
        return ApplicantsCsvExtractor.extract_value_from_rowcolumn(csv_body, "address", row_number)

    # Returns the postcode for a given row in a csv file.
    @staticmethod
    def extract_postcode(csv_body, row_number: int) -> str:
        return ApplicantsCsvExtractor.extract_value_from_rowcolumn(csv_body, "postcode", row_number)

    # Returns the phone number for a given row in a csv file.
    @staticmethod
    def extract_phone_number(csv_body, row_number: int) -> str:
        return ApplicantsCsvExtractor.extract_value_from_rowcolumn(csv_body, "phone_number", row_number)

    # Returns the university name for a given row in a csv file.
    @staticmethod
    def extract_university(csv_body, row_number: int) -> str:
        return ApplicantsCsvExtractor.extract_value_from_rowcolumn(csv_body, "uni", row_number)

    # Returns the degree classification for a given row in a csv file.
    @staticmethod
    def extract_degree(csv_body, row_number: int) -> str:
        return ApplicantsCsvExtractor.extract_value_from_rowcolumn(csv_body, "degree", row_number)

    # Returns the date an applicant was invited to in a csv file.
    @staticmethod
    def extract_invited_date(csv_body, row_number: int) -> str:
        return ApplicantsCsvExtractor.extract_value_from_rowcolumn(csv_body, "invited_date", row_number)

    # Returns the month an applicant was invited to in a csv file
    @staticmethod
    def extract_month(csv_body, row_number: int) -> str:
        return ApplicantsCsvExtractor.extract_value_from_rowcolumn(csv_body, "month", row_number)

    # Returns the name of the person who invited an applicant in a csv file.
    @staticmethod
    def extract_invited_by(csv_body, row_number: int) -> str:
        return ApplicantsCsvExtractor.extract_value_from_rowcolumn(csv_body, "invited_by", row_number)

    @staticmethod
    def extract_title_date(key) -> str or None:
        try:
            month = key.lstrip("Talent/").rstrip("Applicants.csv")
            date = month.split("2")
            return date[0] + "-2" + date[1]
        except IndexError:
            return None

    # Returns the value in a column from a .csv file
    @staticmethod
    def extract_value_from_rowcolumn(csv_body: pd.DataFrame, column: str, row: int):
        try:
            # return csv_body.iloc[:][column][row]
            return csv_body.iloc[row][column]
        except:
            return None
