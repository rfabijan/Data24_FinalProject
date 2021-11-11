import pipeline.app.extract.csv_extractor as extractor

import datetime as dt
import pandas as pd
import pprint as p


class AcademyCleaner(extractor.AcademiesCsvExtractor):
    def __init__(self):
        super(AcademyCleaner, self).__init__()
        self.__csv_academy_df = pd.DataFrame
        self.__unique_cs_list = []
        self.__core_skill_list = []
        self.__error_names = set()
        self.__error_trainer_names = set()

    # returns the csv dataframe to use in the final database
    @property
    def csv_academy_df(self):
        return self.__csv_academy_df

    @property
    def unique_cs_list(self):
        return self.__unique_cs_list

    # return the list for the CoreSkills dataframe in pre_load_formatter
    @property
    def core_skill_list(self):
        return self.__core_skill_list

    # returns names appended to the error_names set
    @property
    def error_names(self):
        return self.__error_names

    # returns names appended to the error_trainer_names set
    @property
    def error_trainer_names(self):
        return self.__error_trainer_names

    # returns cleaned name in a tuple format
    def clean_name(self, name: str) -> tuple:
        name = name.title()
        if not name.isalpha():
            self.error_names.add(name)
        if name.count(" ") > 1 or "-" in name:
            self.error_names.add(name)
        if " " in name:
            space_index = name.index(" ")
            name_list = [name[0:space_index], name[space_index + 1:]]
            clean_name = name_list[0], name_list[1]
            return clean_name
        else:
            print(name)

    # returns cleaned trainer name in a tuple format
    def clean_trainer(self, trainer_name: str) -> tuple:
        trainer_name = trainer_name.title()
        if not trainer_name.isalpha():
            self.error_names.add(trainer_name)
        if trainer_name.count(" ") > 1 or "-" in trainer_name:
            self.error_names.add(trainer_name)
        if " " in trainer_name:
            space_index = trainer_name.index(" ")
            name_list = [trainer_name[0:space_index], trainer_name[space_index + 1:]]
            return name_list[0], name_list[1]
        else:
            print(trainer_name)

    @staticmethod
    def clean_course_start_date(date: str) -> dt.datetime:
        return dt.datetime.strptime(date, '%Y-%m-%d')

    # returns skill value score in a specific range
    @staticmethod
    def create_unique_academy_key(clean_name: tuple) -> str:
        unique = str(clean_name[0] + clean_name[1])
        return unique

    # Setter for the final dataframe
    def set_csv_academy_df(self, new_df: pd.DataFrame):
        self.__csv_academy_df = new_df

    @staticmethod
    def single_csv_academies_dict(name: tuple, trainer: tuple, date: dt.datetime,
                                  cname, clength, skill_value: int) -> dict:
        return {"Name": name,
                "Trainer": trainer,
                "Course Start Date": date,
                "Course Name": cname,
                "Course Length": clength,
                "Skill Value": skill_value}

    @staticmethod
    def extract_skill_from_dict_key(dict_key: str):
        return dict_key.split("_")[0]

    def final_academy_csv_dict_appender(self):
        csv_dict = {}
        print(f"Starting loading the {len(list(self.keys))} academy CSV files.\n")

        for keys in self.keys[:10]:
            print(f"Currently loading {keys}...")
            csv_body = self.single_csv(keys)
            for row in range(0, self.len_of_rows(csv_body)):
                name = self.clean_name(self.extract_csv_name(csv_body, row))
                trainer = self.clean_trainer(self.extract_academies_trainer(csv_body, row))
                date = self.clean_course_start_date(self.extract_academies_date(keys))
                course_name = self.extract_academies_course_name(keys)
                course_length = self.extract_academies_weeks(csv_body.columns)
                skill_value = self.extract_academies_skill_values_per_person_per_week(csv_body)[name[0] + " " + name[1]]
                unique_key = self.create_unique_academy_key(name)

                csv_dict[unique_key] = self.single_csv_academies_dict(name, trainer, date, course_name,
                                                                      course_length, skill_value)
            print(f"{keys} loaded.\n")
        self.set_csv_academy_df(pd.DataFrame.from_dict(csv_dict).transpose())
        return csv_dict

    def populate_final_academy_df(self):
        print(f"Dictionary being loaded into dataframe...\n")
        original_dict = self.final_academy_csv_dict_appender()
        intermediate_dict = {}

        for first_key in list(original_dict.keys())[:10]:
            au_key = first_key
            clean_name = original_dict[first_key]["Name"]
            clean_date = original_dict[first_key]["Course Start Date"]
            course_name = original_dict[first_key]["Course Name"]
            course_length = original_dict[first_key]["Course Length"]
            clean_trainer = original_dict[first_key]["Trainer"]
            for week_key in list(original_dict[first_key]["Skill Value"].keys()):
                week = int(week_key[1:])
                analytic_score = original_dict[first_key]["Skill Value"][week_key]["Analytic" + "_" + week_key]
                determined_score = original_dict[first_key]["Skill Value"][week_key]["Determined" + "_" + week_key]
                imaginative_score = original_dict[first_key]["Skill Value"][week_key]["Imaginative" + "_" + week_key]
                professional_score = original_dict[first_key]["Skill Value"][week_key]["Professional" + "_" + week_key]
                studious_score = original_dict[first_key]["Skill Value"][week_key]["Studious" + "_" + week_key]
                independent_score = original_dict[first_key]["Skill Value"][week_key]["Independent" + "_" + week_key]
                skill_dict = {
                    "Analytic": analytic_score,
                    "Determined": determined_score,
                    "Imaginative": imaginative_score,
                    "Professional": professional_score,
                    "Studious": studious_score,
                    "Independent": independent_score
                }
                for skill_key in list(original_dict[first_key]["Skill Value"][week_key].keys()):
                    if self.extract_skill_from_dict_key(skill_key) not in self.unique_cs_list:
                        self.unique_cs_list.append(self.extract_skill_from_dict_key(skill_key))

                    this_row_id = clean_name[0] + clean_name[1] \
                                  + str(clean_date.day) + str(clean_date.month) + str(clean_date.year)\
                                  + str(week) + skill_key

                    if analytic_score is not None and imaginative_score is not None:
                        intermediate_dict[this_row_id] = {
                            "Academy Unique Key": au_key,
                            "Name": clean_name,
                            "Date": clean_date,
                            "Trainer First Name": clean_trainer[0],
                            "Trainer Last Name": clean_trainer[1],
                            "Course Name": course_name,
                            "Course Length": course_length,
                            "Week": week,
                            "Core Skill": self.extract_skill_from_dict_key(self.extract_skill_from_dict_key(skill_key)),
                            "Skill Score": skill_dict[self.extract_skill_from_dict_key(skill_key)]
                        }
        self.set_csv_academy_df(pd.DataFrame.from_dict(intermediate_dict).transpose())

if __name__ == "__main__":
    test = AcademyCleaner()
    test.populate_final_academy_df()
    print(test.csv_academy_df.columns)
#    print(f"and here comes the dataframe (maybe)\n{test.csv_academy_df}")
