import DevTestS3Class as s3c


class TxtExtractor(s3c.S3ParentClass):
    def __init__(self, key):
        super().__init__()
        self.__object = self.get_client.get_object(Bucket=self.get_bucket_name, Key=key)

    @property
    def get_object(self):
        return self.__object

    def pull_something_please(self):
        text_str = self.get_object["Body"].read().decode("utf-8")
        return text_str.split('\n')


testTxt = TxtExtractor("Talent/Sparta Day 20 June 2019.txt")

print(testTxt.pull_something_please())
