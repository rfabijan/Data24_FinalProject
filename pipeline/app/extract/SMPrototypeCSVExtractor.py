import PrototypeS3Class as s3c
import pandas as pd


class AcademiesCsvExtractor(s3c.S3ParentClass):
    def __init__(self):
        super().__init__()
        self.keys = self.get_talent_csv

    def extract_csv(self):
        dictionary_of_csv = {}
        for i in self.get_academy_csv:
            csv = pd.read_csv(self.get_client.get_object(Bucket=self.bucket_name, Key=i)["Body"])
            dictionary_of_csv[i] = csv["name"]

        return dictionary_of_csv.keys()


test2 = AcademiesCsvExtractor()
print(test2.extract_csv())
