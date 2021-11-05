import pprint


from pipeline.app.extract import PrototypeS3Class as s3c
import pipeline.config_manager as conf
import datetime as dt


class TxtExtractor(s3c.S3ParentClass):
    def __init__(self):
        super().__init__()
        self.__keys = self.talent_txt
        self.__sparta_day_dict = {}

    @property
    def keys(self):
        return self.__keys

    @property
    def sparta_day_dict(self):
        return self.__sparta_day_dict

    def pull_text_object_as_list(self, key: str) -> list:
        object = self.client.get_object(Bucket=self.bucket_name, Key=key)
        text_str = object["Body"].read().decode("utf-8")
        return text_str.split('\n')

    @staticmethod
    def extract_date(txt_list: list) -> str:
        if txt_list[0].title() in conf.WEEKDAYS:
            return txt_list[0]

    @staticmethod
    def extract_academy(txt_list: list) -> str:
        return txt_list[1]

    @staticmethod
    def extract_name_line(txt_list: list, i: int) -> str:
        if i >= 3 and len(txt_list[i]) > 0:
            return txt_list[i]

    @staticmethod
    def extract_name_from_line(name_line: str) -> str:
        if name_line and len(name_line) > 0:
            hyphen_index = name_line.index("-")
            intermediate = name_line[0:hyphen_index - 1]
            if intermediate:
                return intermediate

    @staticmethod
    def extract_psychometric_from_line(name_line: str) -> str:
        hyphen_index = name_line.index("-")
        return name_line[hyphen_index + 2:hyphen_index + 23]

    @staticmethod
    def extract_presentation_from_line(name_line: str) -> str:
        hyphen_index = name_line.index("-")
        return name_line[hyphen_index + 26:]

    # def extract_all_info(self) -> dict:
    #     output_dict = {}
    #     for key in self.keys:
    #
    #         object_instance = self.client.get_object(Bucket=self.bucket_name, Key=key)
    #         list_instance = self.read_text_object_as_list(object_instance)
    #
    #         for i in range(0, len(list_instance), 1):
    #             print(list_instance)
    #             if len(list_instance[i]) > 0:
    #                 if i == 0:
    #                     space_index = list_instance[i].index(" ") + 1
    #                     s_ins = list_instance[i][space_index::].replace("\r", "")
    #                     datetime_instance = dt.datetime.strptime(s_ins, '%d %B %Y')
    #                 elif i == 1:
    #                     academy_instance = (list_instance[i])
    #                 elif i >= 3 and list_instance:
    #                     hyphen_index = list_instance[i].index("-")
    #                     name_instance = list_instance[i][0:hyphen_index - 1]
    #                     name_instance = name_instance.title()
    #
    #                     psychometric_instance = list_instance[i][hyphen_index + 2:hyphen_index + 23].replace("\r", "")
    #                     presentation_instance = list_instance[i][hyphen_index + 26:].replace("\r", "")
    #
    #                     key_name_instance = name_instance.replace(" ", "")
    #                     date_key = str(datetime_instance.day) +\
    #                                str(datetime_instance.month) +\
    #                                str(datetime_instance.year)
    #                     unique_key = key_name_instance+date_key
    #
    #                     output_dict[unique_key] = {
    #                         "Name": name_instance,
    #                         "Academy": academy_instance,
    #                         "Date": datetime_instance,
    #                         "Psychometric": psychometric_instance,
    #                         "Presentation": presentation_instance}
    #     return output_dict


if __name__ == '__main__':
    testTxt = TxtExtractor()
    for key in testTxt.keys:
        list_instance = testTxt.pull_text_object_as_list(key)

        for i in range(3, len(list_instance)):
            if len(list_instance[i]) > 0:
                #print(i)
                test_name_line = testTxt.extract_name_line(list_instance, i)
                #print(test_name_line)
                test_name = testTxt.extract_name_from_line(test_name_line)
                print(test_name)

