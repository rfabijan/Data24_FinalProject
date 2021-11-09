import pandas as pd

import pipeline.app.transform.cleaning_talent_sparta_day as tsd
import pipeline.app.transform.cleaning_talent as t
import pipeline.app.transform.cleaning_talent_applicants as ta
import pipeline.app.transform.cleaning_academy_course as ca

import pipeline.config_manager as conf

import datetime as dt
import pandas
import numpy as np
import pyodbc



class PreLoadFormatter(tsd.TxtCleaner, t.JsonCleaner): #, ta.csv_cleaner1, ca.csv_cleaner1):
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

        # self.__server = 'localhost,1433'
        # self.__database = conf.DB_NAME
        # self.__username = 'SA'
        # self.__password = 'Passw0rd2018'
        # self.data24etl_db = pyodbc.connect(
        #     'DRIVER={SQL Server};SERVER=' + self.__server + ';DATABASE=' + self.__database
        #     + ';UID=' + self.__username + ';PWD=' + self.__password)

        self.fill_txt_dict_df()

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

    #######################################################################################################################

    @staticmethod
    def concat_new_df(data: list, keys: list) -> pandas.DataFrame:
        return pandas.concat(objs=data, axis=1, keys=keys)

    @staticmethod
    def reset_index(df):
        df.index = np.arange(1, len(df) + 1)

    def populate_from_one_df(self, dataframe, key_list, output_dataframe):
        data_list = []
        for key in key_list:
            data_list.append(dataframe[key])
        eval(f"self.set_{output_dataframe}")((self.concat_new_df(data_list, key_list)).drop_duplicates(subset=key_list))
        self.reset_index(eval(f"self.{output_dataframe}"))

    def populate_from_two_df(self, df1: pd.DataFrame, df2: pd.DataFrame, key_list: list, output_dataframe: str):
        data_list = []
        for key in key_list:
            if key in df1.keys():
                data_list.append(df1[key])
            if key in df2.keys():
                data_list.append(df2[key])
        eval(f"self.set_{output_dataframe}")((self.concat_new_df(data_list, key_list)).drop_duplicates(subset=key_list))
        self.reset_index(eval(f"self.{output_dataframe}"))


if __name__ == '__main__':
    test_table_formatter = PreLoadFormatter()

    test_table_formatter.populate_from_one_df(test_table_formatter.txt_df, ["Academy"], "academy_df")
    test_table_formatter.populate_from_one_df(test_table_formatter.txt_df, ["Academy", "Date"], "sparta_day_df")

    print(test_table_formatter.sparta_day_df)

    # print(test_table_formatter.academy_df.to_sql(name="Academy",
    #                                              con=test_table_formatter.data24etl_db,
    #                                              if_exists='replace'))
