import pipeline.app.extract.txt_extractor as ext

import datetime as dt
import pprint as pp
import pandas as pd


class TxtCleaner(ext.TxtExtractor):
    def __init__(self):
        super(TxtCleaner, self).__init__()
        self.__final_dict = {}
        self.__txt_df = pd.DataFrame
        self.__txt_error_names = set()

    @property
    def final_dict(self):
        return self.__final_dict

    @property
    def txt_df(self):
        return self.__txt_df

    def set_txt_df(self, new_df: pd.DataFrame):
        self.__txt_df = new_df

    @property
    def error_names(self):
        return self.__txt_error_names

    # Takes the raw name from the file and returns a tuple of first name, surname
    # and appends any that don't follow "normal" naming conventions to a list
    def clean_txt_name(self, raw_name: str) -> tuple:
        raw_name = raw_name.title()
        if raw_name.count(" ") > 1 or "-" in raw_name:
            self.error_names.add(raw_name)
        if " " in raw_name:
            space_index = raw_name.index(" ")
            name_list = [raw_name[0:space_index], raw_name[space_index + 1:]]
            return name_list[0], name_list[1]
        else:
            print(raw_name)

    # Returns the date in a datetime format
    @staticmethod
    def clean_txt_date(raw_date_str: str) -> dt.datetime:
        space_index = raw_date_str.index(" ") + 1
        s_ins = raw_date_str[space_index::].replace("\r", "")
        return dt.datetime.strptime(s_ins, '%d %B %Y')

    # Takes the float out of the scores string
    @staticmethod
    def __clean_txt_scores(raw_score_str: str) -> float:
        colon_index = raw_score_str.index(":")
        slash_index = raw_score_str.index("/")
        numerator = int(raw_score_str[colon_index+1:slash_index])
        denominator = int(raw_score_str[slash_index+1:])
        return numerator/denominator

    # Calls above method for the two scores given
    def clean_txt_psychometrics(self, raw_psychometrics: str) -> float:
        return self.__clean_txt_scores(raw_psychometrics)

    def clean_txt_presentation(self, raw_presentation: str) -> float:
        return self.__clean_txt_scores(raw_presentation)

    # Can be updated to check if academy is in a list of academies potentially
    @staticmethod
    def clean_txt_academy(raw_academy: str) -> str:
        return raw_academy.replace("\r", "").title()

    # Takes the name and datetime to make the unique key
    @staticmethod
    def key_generator(clean_name: tuple, clean_date: dt.datetime) -> str:
        return clean_name[0].replace(" ", "") + \
               clean_name[1].replace(" ", "") + \
               str(clean_date.day) + \
               str(clean_date.month) + \
               str(clean_date.year)

    @staticmethod
    def single_dict_maker(key: str, name: tuple, academy: str, date: dt.datetime, psy: float, pres: float) -> dict:
        return {"Unique Key" : key,
                "Name": name,
                "Academy": academy,
                "Date": date,
                "Psychometrics": psy,
                "Presentation": pres}

    def final_dict_appender(self, this_key: str):
        list_instance = self.pull_text_object_as_list(this_key)
        for i in range(3, len(list_instance)):
            if len(list_instance[i]) > 0:
                raw_name_line = self.extract_txt_name_line(list_instance, i)
                cleaned_name = self.clean_txt_name(self.extract_txt_name_from_line(raw_name_line))
                cleaned_academy = self.clean_txt_academy(self.extract_txt_academy(list_instance))
                cleaned_date = self.clean_txt_date(self.extract_txt_date(list_instance))
                cleaned_psychometric = self.clean_txt_psychometrics(self.extract_txt_psychometric_from_line(raw_name_line))
                cleaned_presentation = self.clean_txt_presentation(self.extract_txt_presentation_from_line(raw_name_line))
                unique_key = self.key_generator(cleaned_name, cleaned_date)

                self.final_dict[unique_key] = self.single_dict_maker(unique_key,
                                                                     cleaned_name,
                                                                     cleaned_academy,
                                                                     cleaned_date,
                                                                     cleaned_psychometric,
                                                                     cleaned_presentation)

    def fill_txt_dict_df(self):
        print(f"Beginning processing {len(self.txt_keys)} files...\n")
        for this_key in self.txt_keys:
            print(f"Processing .txt file {this_key}.\n")
            self.final_dict_appender(this_key)
        self.set_txt_df(pd.DataFrame.from_dict(self.final_dict).transpose())
        print(f"Finished processing all {len(self.txt_keys)} txt files.\n\n")

if __name__ == '__main__':

    testcleaner = TxtCleaner()

    testcleaner.fill_txt_dict_df()

    pp.pprint(testcleaner.txt_df.columns)
