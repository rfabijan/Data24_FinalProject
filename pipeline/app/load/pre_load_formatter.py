import pipeline.app.transform.cleaning_talent_sparta_day as tsd
import pipeline.app.transform.cleaning_talent as t
import pipeline.app.transform.cleaning_talent_applicants as ta
import pipeline.app.transform.cleaning_academy_course as ca

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
        print("Now formatting the dataframes into individual tables...\n")
        self.create_final_dataframes()
        print("Complete.\n")

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
        return self.__strengths_df

    def set_strengths_df(self, new_df):
        self.__strengths_df = new_df

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
            df.set_index("Unique Key", inplace=True, drop=False)

    # The following three functions will take columns from dataframes/lists (depending on the
    # required format) and feeds them into concat (above) to get out a final dataframe, which
    # will represent one table
    def populate_from_one_df(self, dataframe, key_list, output_dataframe,
                             pk_column_name="", reindex=True, generate_key=False):
        data_list = []
        for key in key_list:
            data_list.append(dataframe[key])
        eval(f"self.set_{output_dataframe}")((self.concat_new_df(data_list, key_list))
                                             .drop_duplicates(subset=key_list))
        if reindex:
            self.reset_index(eval(f"self.{output_dataframe}"))
            self.set_key_as_index(eval(f"self.{output_dataframe}"))
        if generate_key:
            rename_dict = {"index": pk_column_name}
            eval(f"(self.{output_dataframe}.reset_index(level=0, inplace=True))")
            eval(f"self.{output_dataframe}.rename({rename_dict}, inplace=True)")

    # The options at the bottom will generate keys where needed and manipulate the indexes
    # to become keys

    def populate_from_two_df(self, df1: pd.DataFrame, df2: pd.DataFrame,
                             key_list: list, output_dataframe: str,
                             reindex=True, join_index=None, generate_key=False, pk_column_name=""):
        data_list = []
        if join_index is not None:
            print(f"Resetting index to {join_index}")
            keylist1 = []
            keylist2 = []
            df1 = df1.set_index(join_index, drop=False)
            df2 = df2.set_index(join_index, drop=False)
            for key in key_list:
                if key in df1.keys():
                    keylist1.append(key)
                if key in df2.keys():
                    keylist2.append(key)

            df1.drop_duplicates(subset=keylist1, inplace=True)
            df2.drop_duplicates(subset=keylist2, inplace=True)

        for key in key_list:
            if key in df1.keys():
                data_list.append(df1[key])
            elif key in df2.keys():
                data_list.append(df2[key])

        eval(f"self.set_{output_dataframe}")((self.concat_new_df(data_list, key_list)))
        eval(f"self.{output_dataframe}")

        if reindex:         # Setting the index if needed
            self.reset_index(eval(f"self.{output_dataframe}"))
            self.set_key_as_index(eval(f"self.{output_dataframe}"))

        if generate_key:    # Using the above index (sometimes) to make the primary key where applicable
            rename_dict = {"index": pk_column_name}
            eval(f"(self.{output_dataframe}.reset_index(level=0, inplace=True))")
            eval(f"self.{output_dataframe}.rename({rename_dict}, inplace=True)")

    # Certain datasets are held in lists, and as such need to be treated differently, as is below
    def populate_from_one_list(self, this_list: list, column_title: str, output_dataframe: str, reindex=True,
                               generate_key=False, pk_column_name=""):
        eval(f"self.set_{output_dataframe}")(
            pd.DataFrame(this_list, columns=[column_title]).drop_duplicates(ignore_index=True))
        if reindex:
            self.reset_index(eval(f"self.{output_dataframe}"))
        if generate_key:
            rename_dict = {"index": pk_column_name}
            eval(f"(self.{output_dataframe}.reset_index(level=0, inplace=True))")
            eval(f"self.{output_dataframe}.rename({rename_dict}, inplace=True)")

    # Calls the above functions for each dataframe required, and explodes tuples where needed
    def create_final_dataframes(self):
        print("Creating Academy dataframe.\n")
        self.populate_from_one_df(self.txt_df,
                                  ["Academy"], "academy_df",
                                  generate_key=True,
                                  pk_column_name="AcademyID")
    # pk_column_name is a remnant of renaming the index, however it wasn't necessarily needed and as such has been
    # abandoned (for now at least)

        print("Creating Sparta Day dataframe.\n")
        self.populate_from_one_df(self.txt_df,
                                  ["Academy", "Date"], "sparta_day_df",
                                  generate_key=True)

        print("Creating App Sparta Day JT dataframe.\n")
        self.populate_from_one_df(self.txt_df,
                                  ["Unique Key", "Academy", "Date", "Psychometrics", "Presentation"],
                                  "app_sparta_day_jt_df")

        print("Creating Strengths dataframe. \n")
        self.populate_from_one_list(self.unique_s_list,
                                    "Strengths", "strengths_df",
                                    generate_key=True)

        print("Creating App Strengths JT dataframe. \n")
        self.populate_from_one_df(self.json_df,
                                  ["Unique Key", "Strengths"], "app_strengths_jt_df")
        self.set_app_strengths_jt_df(self.app_strengths_jt_df.explode("Strengths"))

        print("Creating Weakness dataframe. \n")
        self.populate_from_one_list(self.unique_w_list,
                                    "Weaknesses", "weakness_df",
                                    generate_key=True)

        print("Creating App Weakness JT dataframe.\n")
        self.populate_from_one_df(self.json_df,
                                  ["Unique Key", "Weaknesses"], "app_weakness_jt_df")
        self.set_app_weakness_jt_df(self.app_weakness_jt_df.explode("Weaknesses"))

        print("Creating Tech Skills dataframe.\n")
        self.populate_from_one_list(self.unique_ts_list,
                                    "Tech Score Topics", "tech_skills_df",
                                    generate_key=True)

        print("Creating Tech Self Score JT dataframe.\n")
        self.populate_from_one_df(self.json_skills_df,
                                  ["Unique Key", "Tech Skills", "Tech Score Value"], "tech_self_score_jt_df")

        print("Creating Applicants dataframe.\n")
        self.populate_from_two_df(self.csv_talent_df, self.json_df,
                                  ["Academy Unique Key", "Unique Key", "Course Interest", "Invited By", "Address",
                                   "Postcode", "City", "First Name", "Last Name", "Gender", "DoB", "Email",
                                   "Phone Number", "Uni", "Degree", "Geo Flex", "Financial Support Self", "Result"],
                                  "applicants_df")

        print("Creating streams dataframe.\n")
        self.populate_from_one_df(self.json_df,
                                  ["Course Interest"], "streams_df",
                                  generate_key=True)

        print("Creating Invitors dataframe.\n")
        self.populate_from_one_list(self.unique_i_list,
                                    "Invited By",
                                    "invitors_df",
                                    generate_key=True)

        print("Creating Address dataframe.\n")
        self.populate_from_one_df(self.csv_talent_df,
                                  ["Address", "Postcode", "City"],
                                  "address_df",
                                  generate_key=True)

        print("Creating Spartans dataframe.\n")
        self.populate_from_two_df(self.csv_talent_df, self.csv_academy_df,
                                  ["Academy Unique Key", "Unique Key", "Course Name"],
                                  "spartans_df", reindex=False, join_index="Academy Unique Key")

        print("Creating Course dataframe.\n")
        self.populate_from_one_df(self.csv_academy_df,
                                  ["Course Name", "Course Length", "Start Date"],
                                  "course_df",
                                  generate_key=True)

        print("Creating Course Trainer JT dataframe.\n")
        self.populate_from_one_df(self.csv_academy_df,
                                  ["Course Name", "Trainer First Name", "Trainer Last Name"],
                                  "course_trainer_jt_df")

        print("Creating Trainer dataframe.\n")
        self.populate_from_one_df(self.csv_academy_df,
                                  ["Trainer First Name", "Trainer Last Name"],
                                  "trainer_df",
                                  generate_key=True)

        print("Creating Tracker JT dataframe.\n")
        self.populate_from_one_df(self.csv_academy_df,
                                  ["Academy Unique Key", "Core Skill", "Week", "Skill Score"],
                                  "tracker_jt_df")

        print("Creating Core Skills dataframe.\n")
        self.populate_from_one_list(self.unique_cs_list, "Core Skill", "core_skills_df",
                                    generate_key=True)

        print("ALl dataframes now filled.\n\n")


if __name__ == '__main__':
    test_table_formatter = PreLoadFormatter()

    print("###########################################################################################################")

    pd.set_option('display.max_columns', None)
    print(test_table_formatter.app_sparta_day_jt_df)

    print(test_table_formatter.academy_df)
