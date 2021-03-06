"""
The question is, should these functions just clean strings and stuff, or the whole dictionary/dataframe/json/whatever?

Basically, how do we test correct extraction? As part of transformation (here), or before it?
If here, then tell us to change our test input to json objects or whatever.
I think ideally it'll be before (which we'll need to test with your guidance), and these will be very tiny functions.
"""
from pipeline.app.extract.json_extractor import JSONExtractor
import datetime as dt
import pandas as pd
from pprint import pprint


class JsonCleaner(JSONExtractor):
    def __init__(self):
        super(JsonCleaner, self).__init__()
        self.__json_df = pd.DataFrame
        self.__json_skills_df = pd.DataFrame
        self.__unique_s_list = []
        self.__unique_w_list = []
        self.__unique_ts_list = []

    @property
    def json_df(self):
        return self.__json_df

    @property
    def json_skills_df(self):
        return self.__json_skills_df

    @property
    def unique_s_list(self):
        return self.__unique_s_list

    @property
    def unique_w_list(self):
        return self.__unique_w_list

    @property
    def unique_ts_list(self):
        return self.__unique_ts_list

    def set_json_df(self, new_df: pd.DataFrame):
        self.__json_df = new_df

    def set_json_skills_df(self, new_df: pd.DataFrame):
        self.__json_skills_df = new_df

    def create_unique_key(self, name: tuple, date: dt) -> str:
        if date:
            year = date.year
            month = date.month
            day = date.day
            unique = str(name[0] + name[1] + str(day) + str(month) + str(year))
            unique = unique.replace(" ", "")
            return unique
        else:
            return (name[0] + name[1]).replace(" ", "")


    @staticmethod
    def clean_json_name(name: str):
        # 'Judy' needs cleaning into a tuple of ('Judy','')
        # 'joNNY O Sullivan-Weddeburn' is cleaned to ('Jonny',  'O' Sullivan-Weddeburn') or Sullivan - Weddeburn
        error_names = []
        name = name.title()
        if name.count(" ") > 1 or "-" in name:
            error_names.append(name)
        if " " in name:
            space_index = name.index(" ")
            name_list = [name[0:space_index], name[space_index + 1:]]
            return name_list[0], name_list[1]
        else:
            print(name)

    @staticmethod
    def clean_json_date(date: str):
        #  '01/02/2003' converts to datetime.date(2003, 2, 1)
        if not date:
            return None
        else:
            date = date.split("/")
            if len(date) == 3:
                datetime = dt.date(int(date[2]), int(date[1]), int(date[0]))
                return datetime
            elif len(date) > 3:
                idx = date.index("")
                date.pop(idx)
                datetime = dt.date(int(date[2]), int(date[1]), int(date[0]))
                return datetime

    @staticmethod
    def clean_json_geo_flex(geo_flex: str):
        # 'Yes' converts to True
        if geo_flex == "Yes":
            return True
        else:
            return False

    @staticmethod
    def clean_json_result(result):
        # 'Pass' converts to True
        if result == "Pass":
            return True
        else:
            return False

    @staticmethod
    def clean_json_self_development(self_development):
        # 'Yes' converts to True
        if self_development == "Yes":
            return True
        else:
            return False
        pass

    @staticmethod
    def clean_json_course_interest(course_interest: str):
        # 'Data' stays as it is, as a string
        # Honestly idk if there's any bad values to REALLY clean_json here
        return course_interest.title()

    @staticmethod
    def clean_json_tech_self_score(tech_self_scores: dict):
        # {"R": 4} goes in as a dictionary/json/whatever, comes out as a dictionary where dict["R"] == 4
        # Pretty sure this doesn't really get back input, but I'll test with bad numbers that convert to None
        return tech_self_scores

    @staticmethod
    def clean_json_strengths(strengths: list):
        # Input is a list, output is a list of strings
        # Must have the first letter only be capitalised
        clean_strengths = []
        for i in strengths:
            i = str(i).title()
            clean_strengths.append(i)
        return clean_strengths

    @staticmethod
    def clean_json_weaknesses(weaknesses):
        # Input is a list, output is a list of strings
        # Must have the first letter only be capitalised
        clean_weaknesses = []
        for i in weaknesses:
            i = str(i).title()
            clean_weaknesses.append(i)
        return clean_weaknesses

    @staticmethod
    def clean_json_financial_support_self(financial_support_self):
        if financial_support_self == "Yes":
            return True
        else:
            return False
        pass

    """
    Cleaning process and creating a directory with unique id to match data across multiple files
    uses json_extractor class to extract data and self cleaning function to clean them
    Unique Key is created from name and date in an example format: "StillmannCastano22082019"
    # cleaning order:
    # - name
    # - date
    # - tech_dict
    # - list_of_strengths
    # - list_of_weaknesses
    # - self_development
    # - geo_flex
    # - financial_support_self
    # - result
    # - course_interest
    """
    def create_unique_dict_from_json(self, json_file):
        name = self.clean_json_name(self.extract_json_name(json_file))
        date = self.clean_json_date(self.extract_json_date(json_file))
        tech_dict = self.clean_json_tech_self_score(self.extract_json_tech_self_score(json_file))
        list_of_strengths = self.clean_json_strengths(self.extract_json_strengths(json_file))
        list_of_weaknesses = self.clean_json_weaknesses(self.extract_json_weaknesses(json_file))
        self_development = self.clean_json_self_development(self.extract_json_self_development(json_file))
        geo_flex = self.clean_json_geo_flex(self.extract_json_geo_flex(json_file))
        financial_support_self = self.clean_json_financial_support_self(self.extract_json_financial_support_self(json_file))
        result = self.clean_json_result(self.extract_json_result(json_file))
        course_interest = self.clean_json_course_interest(self.extract_json_course_interest(json_file))
        # CREATE the unique key
        unique_key = self.create_unique_key(name, date)
        # CREATE a final dictonary
        final_dictonary = {unique_key:
                               {"Name": name
                                , "Date": date
                                , "Tech Self Score": tech_dict
                                , "Strenghts": list_of_strengths
                                , "Weaknesses": list_of_weaknesses
                                , "Self Development": self_development
                                , "Geo Flex": geo_flex
                                , "Financial Support Self": financial_support_self
                                , "Result": result
                                , "Course Interest": course_interest
                                }
                           }
        return final_dictonary

    def populate_json_df(self):
        main_dict = {}
        intermediate_dict = {}
        print(f"Beginning processing all {len(self.extract_json_keys)} JSON files...\n")
        i = 0
        for key in self.extract_json_keys[:10]:
            json_file = self.pull_single_json(key)
            name = self.clean_json_name(self.extract_json_name(json_file))
            date = self.clean_json_date(self.extract_json_date(json_file))
            tech_dict = self.clean_json_tech_self_score(self.extract_json_tech_self_score(json_file))
            list_of_strengths = tuple(self.clean_json_strengths(self.extract_json_strengths(json_file)))
            list_of_weaknesses = tuple(self.clean_json_weaknesses(self.extract_json_weaknesses(json_file)))
            self_development = self.clean_json_self_development(self.extract_json_self_development(json_file))
            geo_flex = self.clean_json_geo_flex(self.extract_json_geo_flex(json_file))
            financial_support_self = self.clean_json_financial_support_self(self.extract_json_financial_support_self(json_file))
            result = self.clean_json_result(self.extract_json_result(json_file))
            course_interest = self.clean_json_course_interest(self.extract_json_course_interest(json_file))
            unique_key = self.create_unique_key(name, date)
            main_dict[unique_key] = {"Unique Key": unique_key
                                               , "Name": name
                                               , "Date": date
                                               , "Tech_self_score": tech_dict
                                               , "Strengths": list_of_strengths
                                               , "Weaknesses": list_of_weaknesses
                                               , "Self Development": self_development
                                               , "Geo Flex": geo_flex
                                               , "Financial_support_self": financial_support_self
                                               , "Result": result
                                               , "Course Interest": course_interest
                                               }
            for key in list(tech_dict.keys()):
                dict_key = unique_key+str(tech_dict[key])
                intermediate_dict[dict_key] = {"Unique Key": unique_key
                                               , "Name": name
                                               , "Date": date
                                               , "Tech_self_score": tech_dict
                                               , "Tech Skills": key
                                               , "Tech Score Value": tech_dict[key]
                                               , "Strengths": list_of_strengths
                                               , "Weaknesses": list_of_weaknesses
                                               , "Self Development": self_development
                                               , "Geo Flex": geo_flex
                                               , "Financial_support_self": financial_support_self
                                               , "Result": result
                                               , "Course Interest": course_interest
                                               }
            for strength in list_of_strengths:
                if strength not in self.unique_s_list:
                    self.unique_s_list.append(strength)

            for weakness in list_of_weaknesses:
                if weakness not in self.unique_w_list:
                    self.unique_w_list.append(weakness)

            for skill in list(tech_dict.keys()):
                if skill not in self.__unique_ts_list:
                    self.unique_ts_list.append(skill)

            i += 1
            if i % 50 == 0:
                print(f"{i} JSON files completed...")
        self.set_json_df(pd.DataFrame.from_dict(main_dict).transpose())
        self.set_json_skills_df(pd.DataFrame.from_dict(intermediate_dict).transpose())
        print(f"All JSON files completed\n")





if __name__ == '__main__':
    cleaner = JsonCleaner()
    #keys = cleaner.extract_json_keys
    #for i in keys:
    #    file = cleaner.pull_single_json(i)
    #    pprint(cleaner.create_unique_dict_from_json(file))
    cleaner.populate_json_df()
    pd.set_option('display.max_columns', None)
    print(cleaner.unique_s_list)