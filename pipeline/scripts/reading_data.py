import io
import pandas as pd
import pprint
import boto3
from pipeline import config_manager as conf


class S3(object):

    def __init__(self, file_name: str):
        self.__client = boto3.client("s3")
        self.__resources = boto3.resource("s3")
        self.__bucket_name = conf.BUCKET_NAME
        self.__object = self.__client.get_object(Bucket=self.__bucket_name, Key=f"Talent/{file_name}")


    def get_client(self):
        return self.__client

    def get_resources(self):
        return self.__resources

    def get_object(self):
        return self.__object

    def get_bucket_name(self):
        return self.__bucket_name

    def get_key_name(self):
        return self.__key_name

    def get_object_body(self):
        return self.get_object()["Body"]



# Read CSV functions
def get_csv(bucket_object_body: object):
    return pd.read_csv(bucket_object_body)


def get_columns(csv_file):
    return csv_file.columns


def save_csv_file(dataframe):
    str_buffer = io.StringIO()
    dataframe.to_csv(str_buffer)
    return str_buffer


def put_object(str_buffer, client, csv_file):
    client.get_client().put_object(Body=str_buffer.getvalue(), Bucket=client.get_bucket_name(),
                                   Key=f"Data100/fish/Robert.csv")




# Data Investigation from S3
client = S3("April2019Applicants.csv")
file = get_csv(client.get_object_body())
df = pd.DataFrame.to_dict(file)
# df = pd.read_csv("D:\Documents\Sparta Global\Data24_FinalProject\datafiles\combined\TalentApplicants.csv")
# df = pd.DataFrame.to_dict(df)
print(df.keys())
# # pprint.pprint(df)

for i in range(100):
    # if np.isnan(df["month"][i]):
    #     print("Run you fool!")
    # else:
    pprint.pprint(df["month"][i])

# x = "my nAme is Rob"
# print(x.title())