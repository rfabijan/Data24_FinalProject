import json
from pipeline.app.extract import s3_connector as s3c


class JSONExtractor(s3c.S3ParentClass):
    def __init__(self):
        super(JSONExtractor, self).__init__()
        self.__json_keys = self.talent_json

    # Static method that returns a list of keys
    # Keys are necessary to extract a specific JSON file from a bucket on S3
    @property
    def extract_json_keys(self):
        return self.__json_keys


    # Returns a single dict file pulled from bucket based on passed key
    def pull_single_json(self, key: str):
        # Loading as in a JSON format
        jsonfile = json.load(self.client.get_object(Bucket=self.bucket_name, Key=key)["Body"])
        return jsonfile


    # Returns a name if exists as a key in dictonary
    @staticmethod
    def extract_name(json_file) -> str:
        if "name" in json_file.keys():
            return json_file["name"]
        else:
            return None

    # Returns a date if exists as a key in dictonary
    @staticmethod
    def extract_date(json_file) -> str:
        if "date" in json_file.keys():
            return json_file["date"]
        else:
            return None

    # Returns a tech_self_score
    @staticmethod
    def extract_tech_self_score(json_file) -> dict:
        if "tech_self_score" in json_file.keys():
            return json_file["tech_self_score"]
        else:
            return None

    # Returns a strengths
    @staticmethod
    def extract_strengths(json_file) -> list:
        return json_file["strengths"]

    # Returns a weaknesses
    @staticmethod
    def extract_weaknesses(json_file) -> list:
        return json_file["weaknesses"]

    # Returns a self_development
    @staticmethod
    def extract_self_development(json_file) -> str:
        return json_file["self_development"]

    # Returns a geo_flex
    @staticmethod
    def extract_geo_flex(json_file) -> str:
        return json_file["geo_flex"]

    # Returns a financial_support_self
    @staticmethod
    def extract_financial_support_self(json_file) -> str:
        return json_file["financial_support_self"]

    # Returns a result
    @staticmethod
    def extract_result(json_file) -> str:
        return json_file["result"]

    # Returns a course_interest
    @staticmethod
    def extract_course_interest(json_file) -> str:
        return json_file["course_interest"]





if __name__ == "__main__":
    extractor = JSONExtractor()
    for i in extractor.extract_json_keys:
        file = extractor.pull_single_json(str(i))
        print(extractor.extract_tech_self_score(file))
