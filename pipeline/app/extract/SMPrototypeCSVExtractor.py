import PrototypeS3Class as s3c

class AcademiesCsvExtractor(s3c.S3ParentClass):
    def __init__(self):
        super().__init__()
        self.keys = self.get_talent_txt
        self.__object = functiongoeshere.exe