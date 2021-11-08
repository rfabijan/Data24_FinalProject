from pipeline.app.extract import PrototypeS3Class as s3c
import pipeline.config_manager as conf


class TxtExtractor(s3c.S3ParentClass):
    def __init__(self):
        super().__init__()
        self.__keys = self.talent_txt

    # keys property as assigned in init
    @property
    def keys(self):
        return self.__keys

    # takes in the key from the list defined previously, and returns the corresponding txt file,
    # but broken down into a list
    def pull_text_object_as_list(self, this_key: str) -> list:
        this_object = self.client.get_object(Bucket=self.bucket_name, Key=this_key)
        text_str = this_object["Body"].read().decode("utf-8")
        return text_str.split('\n')

    # Returns the date in string form from the list above
    @staticmethod
    def extract_date(txt_list: list) -> str:
        space_index = txt_list[0].index(" ")
        if txt_list[0][0:space_index] .title() in conf.WEEKDAYS:
            return txt_list[0]

    # Returns the academy name from the list
    @staticmethod
    def extract_academy(txt_list: list) -> str:
        return txt_list[1]

    # Extracts a specific line form the list, used in extracting name, psychometric and presentation scores
    @staticmethod
    def extract_name_line(txt_list: list, this_i: int) -> str:
        if len(txt_list[this_i]) > 0:
            return txt_list[this_i]

    # Extracts the name form the lines above
    @staticmethod
    def extract_name_from_line(name_line: str) -> str:
        if name_line and len(name_line) > 0:
            if name_line.count("-") == 1:
                hyphen_index = name_line.index("-")
            else:
                hyphen_index_skip = name_line.index("-")
                hyphen_index = name_line.index("-", hyphen_index_skip+1)
            intermediate = name_line[0:hyphen_index - 1]
            if intermediate:
                return intermediate

# Extracts the psychometric section from the line pulled above
    @staticmethod
    def extract_psychometric_from_line(name_line: str) -> str:
        if name_line.count("-") == 1:
            hyphen_index = name_line.index("-")
        else:
            hyphen_index_skip = name_line.index("-")
            hyphen_index = name_line.index("-", hyphen_index_skip + 1)
        return name_line[hyphen_index + 3:hyphen_index + 24]

    # Extracts the presentation section from the name line above
    @staticmethod
    def extract_presentation_from_line(name_line: str) -> str:
        if name_line.count("-") == 1:
            hyphen_index = name_line.index("-")
        else:
            hyphen_index_skip = name_line.index("-")
            hyphen_index = name_line.index("-", hyphen_index_skip + 1)
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

    # An example of accessing all the data needed from the txt files
    for key in testTxt.keys:
        list_instance = testTxt.pull_text_object_as_list(key)

        for i in range(3, len(list_instance)):
            if len(list_instance[i]) > 0:
                test_name_line = testTxt.extract_name_line(list_instance, i)

                test_academy = testTxt.extract_academy(list_instance)
                test_date = testTxt.extract_date(list_instance)
                test_name = testTxt.extract_name_from_line(test_name_line)
                test_psychometric = testTxt.extract_psychometric_from_line(test_name_line)
                test_presentation = testTxt.extract_presentation_from_line(test_name_line)

                #print(test_academy)
                #print(test_date)
                if ":" not in test_name_line:
                    print(test_name_line)
                #print(test_name)
                #print(test_psychometric)
                #print(test_presentation)
                #print("\n")
