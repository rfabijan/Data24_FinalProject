import pprint

import PrototypeS3Class as s3c
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

    @staticmethod
    def read_text_object(object) -> list:
        text_str = object["Body"].read().decode("utf-8")
        return text_str.split('\n')

    def extract_all_info(self) -> dict:
        output_dict = {}
        for i in self.keys:
            object_instance = self.client.get_object(Bucket=self.bucket_name, Key=i)
            list_instance = self.read_text_object(object_instance)
            for j in range(0, len(list_instance), 1):
                if len(list_instance[j]) > 0:
                    if j == 0:
                        space_index = list_instance[j].index(" ") + 1
                        s_ins = list_instance[j][space_index::].replace("\r", "")
                        datetime_instance = dt.datetime.strptime(s_ins, '%d %B %Y')
                    elif j == 1:
                        academy_instance = (list_instance[j]).replace("\r", "")
                    elif j >= 3 and list_instance:
                        hyphen_index = list_instance[j].index("-")
                        name_instance = list_instance[j][0:hyphen_index-1]
                        name_instance = name_instance.title()

                        psychometric_instance = list_instance[j][hyphen_index+2:hyphen_index+23].replace("\r", "")
                        presentation_instance = list_instance[j][hyphen_index+26:]

                        key_name_instance = name_instance.replace(" ", "")
                        date_key = str(datetime_instance.day) +\
                                   str(datetime_instance.month) +\
                                   str(datetime_instance.year)
                        unique_key = key_name_instance+date_key

                        output_dict[unique_key] = {
                            "Name": name_instance,
                            "Academy": academy_instance,
                            "Date": datetime_instance,
                            "Psychometric": psychometric_instance,
                            "Presentation": presentation_instance}
        return output_dict


if __name__ == '__main__':
    testTxt = TxtExtractor()

    pprint.pprint(testTxt.extract_all_info())