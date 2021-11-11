import pipeline.app.transform.cleaning_talent_sparta_day as tsd
import pipeline.app.transform.cleaning_talent as t
import pipeline.app.transform.cleaning_talent_applicants as ta
import pipeline.app.transform.cleaning_academy_course as ca

import pipeline.config_manager as conf

import datetime as dt
import pandas
import numpy as np
import pandas as pd


# main class
class PreLoadFormatter(tsd.TxtCleaner, t.JsonCleaner, ta.Applicants_Cleaner, ca.AcademyCleaner):
    def __init__(self):
        super(PreLoadFormatter, self).__init__()
        self.__academy_df = pandas.DataFrame
        self.__sparta_day_df = pandas.DataFrame
        self.__app_sparta_day_jt_df = pandas.DataFrame
        self.__weakness_df = pandas.DataFrame
        self.__app_weakness_jt_df = pandas.DataFrame
        self.__strengths_df = pandas.DataFrame
        self.__app_strengths_jt_df = pandas.DataFrame
        self.__tech_skills_df = pandas.DataFrame
        self.__tech_self_score_jt_df = pandas.DataFrame
        self.__applicants_df = pandas.DataFrame
        self.__streams_df = pandas.DataFrame
        self.__invitors_df = pandas.DataFrame
        self.__address_df = pandas.DataFrame
        self.__spartans_df = pandas.DataFrame
        self.__tracker_jt_df = pandas.DataFrame
        self.__core_skills_df = pandas.DataFrame
        self.__course_df = pandas.DataFrame
        self.__course_trainer_jt_df = pandas.DataFrame
        self.__trainer_df = pandas.DataFrame

        # A list containing all the dataframes, to allow all to be accessed at once if needed
        self.__all_dataframes = [self.academy_df,
                                 self.sparta_day_df,
                                 self.app_sparta_day_jt_df,
                                 self.weakness_df,
                                 self.app_weakness_jt_df,
                                 self.strengths_df,
                                 self.app_strengths_jt_df,
                                 self.tech_skills_df,
                                 self.tech_self_score_jt_df,
                                 self.applicants_df,
                                 self.streams_df,
                                 self.invitors_df,
                                 self.address_df,
                                 self.spartans_df,
                                 self.tracker_jt_df,
                                 self.core_skills_df,
                                 self.course_df,
                                 self.course_trainer_jt_df,
                                 self.trainer_df
                                 ]
        # The functions to fill the dataframes with data from S3 (via extract and transform functions)
        print("BEGINNING IMPORT TO DATAFRAMES...\n")
        self.fill_txt_dict_df()
        print("TXT FILES COMPLETED!\n")
        self.populate_json_df()
        print("JSON FILES COMPLETED!\n")
        self.populate_talent_csv_file()
        print("TALENT CSV FILES COMPLETED!\n")
        self.populate_final_academy_df()
        print("ACADEMY CSV FILES COMPLETED!\n")

    # Getters and setters for each dataframe - I won't comment each one as it it fairly self explanatory as to
    # what they do
    @property
    def all_dataframes(self):
        return self.__all_dataframes

    # returns dataframes with info in relation to the academy
    @property
    def academy_df(self):
        return self.__academy_df

    def set_academy_df(self, new_df):
        self.__academy_df = new_df

    # returns dataframes with info in relation to the sparta day
    @property
    def sparta_day_df(self):
        return self.__sparta_day_df

    def set_sparta_day_df(self, new_df):
        self.__sparta_day_df = new_df

    @property
    def app_sparta_day_jt_df(self):
        return self.__app_sparta_day_jt_df

    def set_app_sparta_day_jt_df(self, new_df):
        self.__app_sparta_day_jt_df = new_df

    # returns dataframes with info in relation to the applicants weaknesses
    @property
    def weakness_df(self):
        return self.__weakness_df

    def set_weakness_df(self, new_df):
        self.__weakness_df = new_df

    @property
    def app_weakness_jt_df(self):
        return self.__app_weakness_jt_df

    def set_app_weakness_jt_df(self, new_df):
        self.__app_weakness_jt_df = new_df

    # returns dataframes with info in relation to the applicants strengths
    @property
    def strengths_df(self):
        return self.__streams_df

    def set_strengths_df(self, new_df):
        self.__streams_df = new_df

    @property
    def app_strengths_jt_df(self):
        return self.__app_strengths_jt_df

    def set_app_strengths_jt_df(self, new_df):
        self.__app_strengths_jt_df = new_df

    # returns dataframes with info in relation to the applicants tech skills scores
    @property
    def tech_skills_df(self):
        return self.__tech_skills_df

    def set_tech_skills_df(self, new_df):
        self.__tech_skills_df = new_df

    @property
    def tech_self_score_jt_df(self):
        return self.__tech_self_score_jt_df

    def set_tech_self_score_jt_df(self, new_df):
        self.__tech_self_score_jt_df = new_df

    # returns dataframes with info in relation to the applicant
    @property
    def applicants_df(self):
        return self.__applicants_df

    def set_applicants_df(self, new_df):
        self.__applicants_df = new_df

    # returns dataframes with info in relation to the streams
    @property
    def streams_df(self):
        return self.__streams_df

    def set_streams_df(self, new_df):
        self.__streams_df = new_df

    # returns dataframes with info in relation to the sparta day invitors
    @property
    def invitors_df(self):
        return self.__invitors_df

    def set_invitors_df(self, new_df):
        self.__invitors_df = new_df

    # returns dataframes with info in relation to the applicants address
    @property
    def address_df(self):
        return self.__address_df

    def set_address_df(self, new_df):
        self.__address_df = new_df

    # returns dataframes with info in relation to the spartans
    @property
    def spartans_df(self):
        return self.__spartans_df

    def set_spartans_df(self, new_df):
        self.__spartans_df = new_df

    # returns dataframes with info in relation to the applicants tracker
    @property
    def tracker_jt_df(self):
        return self.__tracker_jt_df

    def set_tracker_jt_df(self, new_df):
        self.__tracker_jt_df = new_df

    # returns dataframes with info in relation to the applicants core skills
    @property
    def core_skills_df(self):
        return self.__core_skills_df

    def set_core_skills_df(self, new_df):
        self.__core_skills_df = new_df

    # returns dataframes with info in relation to the course
    @property
    def course_df(self):
        return self.__course_df

    def set_course_df(self, new_df):
        self.__course_df = new_df

    # returns dataframes with info in relation to the course trainer
    @property
    def course_trainer_jt_df(self):
        return self.__course_trainer_jt_df

    def set_course_trainer_jt_df(self, new_df):
        self.__course_trainer_jt_df = new_df

    # returns dataframes with info in relation to the specific trainer
    @property
    def trainer_df(self):
        return self.__trainer_df

    def set_trainer_df(self, new_df):
        self.__trainer_df = new_df

    ####################################################################################################################
    # returns concatinated dataframes
    @staticmethod
    def concat_new_df(data: list, keys: list) -> pandas.DataFrame:
        return pandas.concat(objs=data, axis=1, keys=keys, join='inner')

    @staticmethod
    def reset_index(df):
        df.index = np.arange(1, len(df) + 1)

    # checks for an unique key and sets it as the index
    @staticmethod
    def set_key_as_index(df):
        if "Unique Key" in list(df.columns):
            df.set_index("Unique Key", inplace=True)

    def populate_from_one_df(self, dataframe, key_list, output_dataframe, reindex=True):
        data_list = []
        for key in key_list:
            data_list.append(dataframe[key])
        eval(f"self.set_{output_dataframe}")((self.concat_new_df(data_list, key_list))
                                             .drop_duplicates(subset=key_list))
        if reindex:
            self.reset_index(eval(f"self.{output_dataframe}"))
            self.set_key_as_index(eval(f"self.{output_dataframe}"))

    def populate_from_two_df(self, df1: pd.DataFrame, df2: pd.DataFrame,
                             key_list: list, output_dataframe: str, reindex=True):
        data_list = []

        for key in key_list:
#            print(f"Key {key} is in")
            if key in df1.keys():
#                print("DF1")
                data_list.append(df1[key])
            elif key in df2.keys():
#                print("DF2")
                data_list.append(df2[key])
            else:
#                print("Neither")
#            print(f"begin data_list: {data_list}\n")
#            print("\n")
#            print(f"Here is the key list: {key_list}")
        eval(f"self.set_{output_dataframe}")(
            (self.concat_new_df(data_list, key_list)))
        eval(f"self.{output_dataframe}").drop_duplicates(subset=key_list)
        if reindex:
            self.reset_index(eval(f"self.{output_dataframe}"))
            self.set_key_as_index(eval(f"self.{output_dataframe}"))

    def populate_from_one_list(self, this_list: list, column_title: str, output_dataframe: str, reindex=True):
        eval(f"self.set_{output_dataframe}")(
            pd.DataFrame(this_list, columns=[column_title]).drop_duplicates(ignore_index=True))
        if reindex:
            self.reset_index(eval(f"self.{output_dataframe}"))
        # eval(f"self.{output_dataframe}").drop_duplicates(ignore_index=True)

    def create_final_dataframes(self):
        print("Creating Academy dataframe.\n")
        self.populate_from_one_df(self.txt_df,
                                  ["Academy"], "academy_df")

        print("Creating Sparta Day dataframe.\n")
        self.populate_from_one_df(self.txt_df,
                                  ["Academy", "Date"], "sparta_day_df")

        print("Creating App Sparta Day JT dataframe.\n")
        self.populate_from_one_df(self.txt_df,
                                  ["Unique Key", "Academy", "Date", "Psychometrics", "Presentation"],
                                  "app_sparta_day_jt_df")

        print("Creating Strengths dataframe. \n")
        self.populate_from_one_list(self.unique_s_list,
                                    "Strengths", "strengths_df")

        print("Creating App Strengths JT dataframe. \n")
        self.populate_from_one_df(self.json_df,
                                  ["Unique Key", "Strengths"], "app_strengths_jt_df")

        print("Creating Weakness dataframe. \n")
        self.populate_from_one_list(self.unique_w_list,
                                    "Weaknesses", "weakness_df")

        print("Creating App Weakness JT dataframe.\n")
        self.populate_from_one_df(self.json_df,
                                  ["Unique Key", "Weaknesses"], "app_weakness_jt_df")

        print("Creating Tech Skills dataframe.\n")
        self.populate_from_one_list(self.unique_ts_list,
                                    "Tech Score Topics", "tech_skills_df")

        print("Creating Tech Self Score JT dataframe.\n")
        self.populate_from_one_df(self.json_df,
                                  ["Unique Key", "Tech_score_keys", "Tech_score_values"], "tech_self_score_jt_df")

        print("Creating Applicants dataframe.\n")
        self.populate_from_two_df(self.csv_talent_df, self.json_df,
                                  ["Unique Key", "Course_interest", "Invited By", "Address",
                                   "Postcode", "City", "First Name", "Last Name", "Gender", "DoB", "Email",
                                   "Phone Number", "Uni", "Degree", "Geo_flex", "Financial_support_self", "Result"],
                                  "applicants_df")

        print("Creating streams dataframe.\n")
        self.populate_from_one_df(self.json_df,
                                  ["Course_interest"], "streams_df")

        print("Creating Invitors dataframe.\n")
        self.populate_from_one_list(self.unique_i_list,
                                    "Invitors",
                                    "invitors_df")

        print("Creating Address dataframe.\n")
        self.populate_from_one_df(self.csv_talent_df,
                                  ["Address", "Postcode", "City"],
                                  "address_df")

        print("Creating Spartans dataframe.\n")                                #ToDo <== This one just doesn't work :(
        self.populate_from_two_df(self.csv_talent_df, self.csv_academy_df,
                                  ["Academy Unique Key", "Course Name"],
                                  "spartans_df")

        print("Creating Course dataframe.\n")
        self.populate_from_one_df(self.csv_academy_df,
                                  ["Course Name", "Course Length", "Date"],
                                  "course_df")

        print("Creating Course Trainer JT dataframe.\n")
        self.populate_from_one_df(self.csv_academy_df,
                                  ["Course Name", "Trainer First Name", "Trainer Last Name"],
                                  "course_trainer_jt_df")

        print("Creating Trainer dataframe.\n")
        self.populate_from_one_df(self.csv_academy_df,
                                  ["Trainer First Name", "Trainer Last Name"],
                                  "trainer_df")

        print("Creating Tracker JT dataframe.\n")
        self.populate_from_one_df(self.csv_academy_df,
                                  ["Academy Unique Key", "Core Skill", "Week", "Skill Score"],
                                  "tracker_jt_df")

        print("Creating Core Skills dataframe.\n")
        self.populate_from_one_list(self.unique_cs_list, "Core Skill", "core_skills_df")


if __name__ == '__main__':
    test_table_formatter = PreLoadFormatter()

    test_table_formatter.create_final_dataframes()
    print("###########################################################################################################")
    pd.set_option('display.max_columns', None)
    print(test_table_formatter.app_weakness_jt_df)
