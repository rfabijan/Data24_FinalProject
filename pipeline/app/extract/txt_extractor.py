from botocore.exceptions import ClientError
from pipeline.app.extract import s3_connector as s3c
import pipeline.config_manager as conf


class TxtExtractor(s3c.S3ParentClass):
    def __init__(self):
        super(TxtExtractor, self).__init__()
        self.__txt_keys = self.populate_txt_files()

    def populate_txt_files(self):
        paginator = self.client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.bucket_name)
        files_list = []
        for page in pages:

            for i in page["Contents"]:
                key = i["Key"]

                if key.startswith("Talent") and key.endswith(".txt"):
                    files_list.append(key)

        return files_list

    # keys property as assigned in init
    @property
    def txt_keys(self):
        return self.__txt_keys

    # takes in the key from the list defined previously, and returns the corresponding txt file,
    # but broken down into a list
    def pull_text_object_as_list(self, this_key: str) -> list or None:
        try:
            this_object = self.client.get_object(Bucket=self.bucket_name, Key=this_key)
            text_str = this_object["Body"].read().decode("utf-8").rstrip()
            return text_str.split('\r\n')
        except ClientError:
            print(f"File {this_key} not found")
            return None

    # Returns the date in string form from the list above
    @staticmethod
    def extract_txt_date(txt_list: list) -> str:
        space_index = txt_list[0].index(" ")
        if txt_list[0][0:space_index] .title() in conf.WEEKDAYS:
            return txt_list[0]

    # Returns the academy name from the list
    @staticmethod
    def extract_txt_academy(txt_list: list) -> str:
        return txt_list[1]

    # Extracts a specific line form the list, used in extracting name, psychometric and presentation scores
    @staticmethod
    def extract_txt_name_line(txt_list: list, this_i: int) -> str:
        if len(txt_list[this_i]) > 0:
            return txt_list[this_i]

    # Extracts the name form the lines above
    @staticmethod
    def extract_txt_name_from_line(name_line: str) -> str:
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
    def extract_txt_psychometric_from_line(name_line: str) -> str:
        if name_line.count("-") == 1:
            hyphen_index = name_line.index("-")
        else:
            hyphen_index_skip = name_line.index("-")
            hyphen_index = name_line.index("-", hyphen_index_skip + 1)
        return name_line[hyphen_index + 3:hyphen_index + 24]

    # Extracts the presentation section from the name line above
    @staticmethod
    def extract_txt_presentation_from_line(name_line: str) -> str:
        if name_line.count("-") == 1:
            hyphen_index = name_line.index("-")
        else:
            hyphen_index_skip = name_line.index("-")
            hyphen_index = name_line.index("-", hyphen_index_skip + 1)
        return name_line[hyphen_index + 26:]


if __name__ == '__main__':
    testTxt = TxtExtractor()

    # An example of accessing all the data needed from the txt files
    for key in testTxt.txt_keys:
        list_instance = testTxt.pull_text_object_as_list(key)

        for i in range(3, len(list_instance)):
            if len(list_instance[i]) > 0:
                test_name_line = testTxt.extract_txt_name_line(list_instance, i)

                test_academy = testTxt.extract_txt_academy(list_instance)
                test_date = testTxt.extract_txt_date(list_instance)
                test_name = testTxt.extract_txt_name_from_line(test_name_line)
                test_psychometric = testTxt.extract_txt_psychometric_from_line(test_name_line)
                test_presentation = testTxt.extract_txt_presentation_from_line(test_name_line)

                #print(test_academy)
                #print(test_date)
                #if ":" not in test_name_line:
                #    print(test_name_line)
                #print(test_name)
                #print(test_psychometric)
                #print(test_presentation)
                #print("\n")
