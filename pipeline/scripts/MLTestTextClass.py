import DevTestS3Class as s3c


class TxtExtractor(s3c.S3ParentClass):
    def __init__(self):
        super().__init__()
        self.keys = self.get_talent_txt
        self.__object = functiongoeshere.exe



    @property
    def get_object(self):
        return self.__object

    def pull_something_please(self):
        text_str = self.get_object["Body"].read().decode("utf-8")
        return text_str.split('\n')

    def functiongoeshere.exe:
        self.get_client.get_object(Bucket=self.bucket_name, Key=key)

testTxt = TxtExtractor()

print(testTxt.pull_something_please())
