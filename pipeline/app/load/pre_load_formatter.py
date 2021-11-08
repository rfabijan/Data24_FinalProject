import pipeline.app.transform.cleaning_talent_sparta_day as tsd
import pipeline.app.transform.cleaning_talent as t
import pipeline.app.transform.cleaning_talent_applicants as ta
import pipeline.app.transform.cleaning_academy_course as ca

import datetime as dt

class pre_load_formatter(tsd.txt_cleaner, t.json_cleaner, ta.csv_cleaner1, ca.csv_cleaner1):
    def __init__(self):
        super(pre_load_formatter).__init__()
        self.__academy_list = []
        self.__sparta_day_list = []
        self.__app_sparta_day_list = []
        self.__weakness_list = []
        self.__app_weakness_jt_list = []
        self.__strengths_list = []
        self.__app_strengths_jt_list = []
        self.__tech_skills_list = []
        self.__tech_self_score_jt_list = []
        self.__applicants_list = []
        self.__streams_list = []
        self.__invitors_list = []
        self.__address_list = []
        self.__spartans_list = []
        self.__tracker_jt_list = []
        self.__core_skills_list = []
        self.__course_list = []
        self.__course_trainer_jt_list = []
        self.__trainer_list = []

    @property
    def academy_list(self):
        return self.__academy_list

    @property
    def sparta_day_list(self):
        return self.__sparta_day_list

    @property
    def app_sparta_day_list(self):
        return self.__app_sparta_day_list

    @property
    def weakness_list(self):
        return self.__weakness_list

    @property
    def app_weakness_jt_list(self):
        return self.__app_weakness_jt_list

    @property
    def strengths_list(self):
        return self.__streams_list

    @property
    def app_strengths_jt_list(self):
        return self.__app_strengths_jt_list

    @property
    def tech_skills_list(self):
        return self.__tech_skills_list

    @property
    def tech_self_score_jt_list(self):
        return self.__tech_self_score_jt_list

    @property
    def applicants_list(self):
        return self.__applicants_list

    @property
    def streams_list(self):
        return self.__streams_list

    @property
    def invitors_list(self):
        return self.__invitors_list

    @property
    def address_list(self):
        return self.__address_list

    @property
    def spartans_list(self):
        return self.__spartans_list

    @property
    def tracker_jt_list(self):
        return self.__tracker_jt_list

    @property
    def core_skills_list(self):
        return self.__core_skills_list

    @property
    def course_list(self):
        return self.__course_list

    @property
    def course_trainer_jt_list(self):
        return self.__course_trainer_jt_list

    @property
    def trainer_list(self):
        return self.__trainer_list

#######################################################################################################################

    def add_to_academy_list(self, cleaned_academy: str) -> None:
        if cleaned_academy not in self.academy_list:
            self.academy_list.append(cleaned_academy)

    def add_to_sparta_day_list(self, cleaned_academy: str, cleaned_date: dt.datetime) -> None:
        if cleaned_academy in self.academy_list:
            cleaned_academy = str(self.academy_list.index(cleaned_academy))
        list_to_add = [cleaned_academy, cleaned_date]
        if list_to_add not in self.sparta_day_list:
            self.sparta_day_set.append(list_to_add)


            