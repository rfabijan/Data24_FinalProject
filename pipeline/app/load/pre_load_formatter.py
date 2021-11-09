import pipeline.app.transform.cleaning_talent_sparta_day as tsd
import pipeline.app.transform.cleaning_talent as t
import pipeline.app.transform.cleaning_talent_applicants as ta
import pipeline.app.transform.cleaning_academy_course as ca

import datetime as dt
import pandas

class pre_load_formatter(tsd.txt_cleaner, t.JsonCleaner, ta.csv_cleaner1, ca.csv_cleaner1):
    def __init__(self):
        super(pre_load_formatter).__init__()
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

    @property
    def academy_df(self):
        return self.__academy_df

    @property
    def sparta_day_df(self):
        return self.__sparta_day_df

    @property
    def app_sparta_day_df(self):
        return self.__app_sparta_day_df

    @property
    def weakness_df(self):
        return self.__weakness_df

    @property
    def app_weakness_jt_df(self):
        return self.__app_weakness_jt_df

    @property
    def strengths_df(self):
        return self.__streams_df

    @property
    def app_strengths_jt_df(self):
        return self.__app_strengths_jt_df

    @property
    def tech_skills_df(self):
        return self.__tech_skills_df

    @property
    def tech_self_score_jt_df(self):
        return self.__tech_self_score_jt_df

    @property
    def applicants_df(self):
        return self.__applicants_df

    @property
    def streams_df(self):
        return self.__streams_df

    @property
    def invitors_df(self):
        return self.__invitors_df

    @property
    def address_df(self):
        return self.__address_df

    @property
    def spartans_df(self):
        return self.__spartans_df

    @property
    def tracker_jt_df(self):
        return self.__tracker_jt_df

    @property
    def core_skills_df(self):
        return self.__core_skills_df

    @property
    def course_df(self):
        return self.__course_df

    @property
    def course_trainer_jt_df(self):
        return self.__course_trainer_jt_df

    @property
    def trainer_df(self):
        return self.__trainer_df

#######################################################################################################################

    @staticmethod
    def concat_new_df(data: list, keys: list) -> pandas.DataFrame:
        return pandas.concat(data, axis = 1, keys=keys).drop_duplicates


    def set_academy_df