import PrototypeS3Class as s3c


class TxtExtractor(s3c.S3ParentClass):
    def __init__(self):
        super().__init__()
        self.__keys = self.get_talent_txt
        self.__SpartaDayDict = {}

    @property
    def get_keys(self):
        return self.__keys

    def pull_single_txt(self, key: str):
        text_str = self.get_object["Body"].read().decode("utf-8")
        return text_str.split('\n')

    def functiongoeshere.exe:
        self.get_client.get_object(Bucket=self.bucket_name, Key=key)

testTxt = TxtExtractor()