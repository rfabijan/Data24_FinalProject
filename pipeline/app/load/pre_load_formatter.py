import pipeline.app.transform.cleaning_talent_sparta_day as tsd
import pipeline.app.transform.cleaning_talent as t
import pipeline.app.transform.cleaning_talent_applicants as ta
import pipeline.app.transform.cleaning_academy_course as ca

import pipeline.config_manager as conf

import datetime as dt
import pandas
import numpy as np
import pandas as pd


class PreLoadFormatter(tsd.TxtCleaner, t.JsonCleaner, ta.Applicants_Cleaner, ca.AcademyCleaner):
    def __init__(self):
        super(PreLoadFormatter, self).__init__()
        self.__academy_df = pandas.DataFrame
        self.__sparta_day_df = pandas.DataFrame
        self.__app_sparta_day_df = pandas.DataFrame
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

        self.fill_txt_dict_df()
        self.populate_json_df()

    @property
    def academy_df(self):
        return self.__academy_df

    def set_academy_df(self, new_df):
        self.__academy_df = new_df

    @property
    def sparta_day_df(self):
        return self.__sparta_day_df

    def set_sparta_day_df(self, new_df):
        self.__sparta_day_df = new_df

    @property
    def app_sparta_day_df(self):
        return self.__app_sparta_day_df

    def set_app_sparta_day_df(self, new_df):
        self.__app_sparta_day_df = new_df

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

    @property
    def applicants_df(self):
        return self.__applicants_df

    def set_applicants_df(self, new_df):
        self.__applicants_df = new_df

    @property
    def streams_df(self):
        return self.__streams_df

    def set_streams_df(self, new_df):
        self.__streams_df = new_df

    @property
    def invitors_df(self):
        return self.__invitors_df

    def set_invitors_df(self, new_df):
        self.__invitors_df = new_df

    @property
    def address_df(self):
        return self.__address_df

    def set_address_df(self, new_df):
        self.__address_df = new_df

    @property
    def spartans_df(self):
        return self.__spartans_df

    def set_spartans_df(self, new_df):
        self.__spartans_df = new_df

    @property
    def tracker_jt_df(self):
        return self.__tracker_jt_df

    def set_tracker_df(self, new_df):
        self.__tracker_jt_df = new_df

    @property
    def core_skills_df(self):
        return self.__core_skills_df

    def set_core_skills_df(self, new_df):
        self.__core_skills_df = new_df

    @property
    def course_df(self):
        return self.__course_df

    def set_course_df(self, new_df):
        self.__course_df = new_df

    @property
    def course_trainer_jt_df(self):
        return self.__course_trainer_jt_df

    def set_course_trainer_jt_df(self, new_df):
        self.__course_trainer_jt_df = new_df

    @property
    def trainer_df(self):
        return self.__trainer_df

    def set_trainer_df(self, new_df):
        self.__trainer_df = new_df

    ####################################################################################################################

    @staticmethod
    def concat_new_df(data: list, keys: list) -> pandas.DataFrame:
        # long_df = pd.DataFrame
        # for obj in data:
        #     if type(obj) == list:
        #         long_df.append(pd.DataFrame(obj))
        #     data[data.index(obj)] = long_df
        return pandas.concat(objs=data, axis=1, keys=keys, join='inner')

    @staticmethod
    def reset_index(df):
        df.index = np.arange(1, len(df) + 1)

    @staticmethod
    def set_key_as_index(df):
        if "Unique Key" in list(df.columns):
            df.set_index("Unique Key", inplace=True)

    def populate_from_one_df(self, dataframe, key_list, output_dataframe):
        data_list = []
        for key in key_list:
            data_list.append(dataframe[key])
        eval(f"self.set_{output_dataframe}")((self.concat_new_df(data_list, key_list)).drop_duplicates(subset=key_list))
        self.reset_index(eval(f"self.{output_dataframe}"))
        self.set_key_as_index(eval(f"self.{output_dataframe}"))

    def populate_from_two_df(self, df1: pd.DataFrame, df2: pd.DataFrame, key_list: list, output_dataframe: str):
        data_list = []
        for key in key_list:
            if key in df1.keys():
                data_list.append(df1[key])
            elif key in df2.keys():
                data_list.append(df2[key])
        eval(f"self.set_{output_dataframe}")((self.concat_new_df(data_list, key_list)).drop_duplicates(subset=key_list))
        self.reset_index(eval(f"self.{output_dataframe}"))

    def populate_from_one_list(self, this_list: list, column_title: str, output_dataframe: str):
        eval(f"self.set_{output_dataframe}")(pd.DataFrame(this_list, columns=[column_title]))
        self.reset_index(eval(f"self.{output_dataframe}"))

    def create_final_dataframes(self):
        print("Creating Academy dataframe.\n")
        self.populate_from_one_df(self.txt_df,
                                  ["Academy"], "academy_df")

        print("Creating Sparta Day dataframe.\n")
        self.populate_from_one_df(self.txt_df,
                                  ["Academy", "Date"], "sparta_day_df")

        print("Creating App Sparta Day JT dataframe.\n")
        # self.populate_from_two_df(self.applicants_csv_df, self.txt_df,
        #                         ["Unique Key", "Academy", "Date", "Psychometrics", "Presentation"]

        print("Creating Applicants dataframe.\n")
        # self.populate_from_two_df(self.applicants_csv_df, self.json_df,   # ToDo COMPLETE WHEN DATAFRAME FOR APPLICANTS IS IN
        #                         ["Unique Key",

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


if __name__ == '__main__':
    test_table_formatter = PreLoadFormatter()

    test_table_formatter.create_final_dataframes()

    print(test_table_formatter.weakness_df)
