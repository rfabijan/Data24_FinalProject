import json
from pipeline.app.extract import PrototypeS3Class as s3c


class JSONExtractor(s3c.S3ParentClass):
    def __init__(self):
        super().__init__()
        self.__keys = self.get_talent_json

    # Static method that returns a list of keys
    # Keys are necessary to extract a specific JSON file from a bucket on S3
    @property
    def get_keys(self):
        return self.__keys


    # Returns a single dict file pulled from bucket based on passed key
    def pull_single_json(self, key: str):
        # Loading as in a JSON format
        jsonfile = json.load(self.get_client.get_object(Bucket=self.bucket_name, Key=key)["Body"])
        return jsonfile

    # Returns a name if exists as a key in dictonary
    def get_name(self, json_file) -> str:
        if "name" in json_file.keys():
            return json_file["name"]
        else:
            return None

    # Returns a date if exists as a key in dictonary
    def get_date(self, json_file) -> str:
        if "date" in json_file.keys():
            return json_file["date"]
        else:
            return None

    # Returns a tech_self_score
    def get_tech_self_score(self, json_file) -> dict:
        if "tech_self_score" in json_file.keys():
            return json_file["tech_self_score"]
        else:
            return None

    # Returns a strengths
    def get_strengths(self, json_file) -> list:
        return json_file["strengths"]

    # Returns a weaknesses
    def get_weaknesses(self, json_file) -> list:
        return json_file["weaknesses"]

    # Returns a self_development
    def get_self_development(self, json_file) -> str:
        return json_file["self_development"]

    # Returns a geo_flex
    def get_geo_flex(self, json_file) -> str:
        return json_file["geo_flex"]

    # Returns a financial_support_self
    def get_financial_support_self(self, json_file) -> str:
        return json_file["financial_support_self"]

    # Returns a result
    def get_result(self, json_file) -> str:
        return json_file["result"]

    # Returns a course_interest
    def get_course_interest(self, json_file) -> str:
        return json_file["course_interest"]





if __name__ == "__main__":
    extractor = JSONExtractor()
    for i in extractor.get_keys:
        file = extractor.pull_single_json(str(i))
        print(extractor.get_tech_self_score(file))