from pipeline.app.extract import PrototypeS3Class as PS3


values = PS3.S3ParentClass()

def test_get_talent_csv():
    get_values = values.get_talent_csv
    assert type(get_values) is list
    assert len(get_values) >= 12
    assert 'Talent/Aug2019Applicants.csv' in get_values


def test_get_client():
    client = "<botocore.client.S3 object"
    assert client.startswith('<botocore.client.S3 object')
