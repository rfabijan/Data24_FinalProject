import PrototypeS3Class as s3c


class TxtExtractor(s3c.S3ParentClass):
    def __init__(self):
        super().__init__()
        self.__keys = self.get_talent_txt
        self.__SpartaDayDict = {}

    @property
    def get_keys(self):
        return self.__keys

    def read_text_object(self, object):
        text_str = object["Body"].read().decode("utf-8")
        return text_str.split('\n')

    def functiongoeshere(self):
        for i in self.get_keys:
            object_instance = self.get_client.get_object(Bucket=self.bucket_name, Key=i)
            object_list = self.read_text_object(object_instance)
            print(object_list)

#testTxt = TxtExtractor()