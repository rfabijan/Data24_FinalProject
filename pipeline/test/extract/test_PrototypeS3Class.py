from pipeline.app.extract import PrototypeS3Class as psc

extractor = psc.S3ParentClass()


def test_talent_csv():
    get_values = extractor.talent_csv
    assert type(get_values) is list
    assert len(get_values) >= 12
    assert 'Talent/Aug2019Applicants.csv' in get_values


# TODO: Change
def test_client():
    client = extractor.client

    # Check if the 'data24-final-project' bucket is available
    available_buckets = [bucket['Name'] for bucket in client.list_buckets()['Buckets']]
    assert 'data24-final-project' in available_buckets


def test_all_files():
    return_value = extractor.all_files
    assert type(return_value) is list
    assert type(return_value[3]) is list
    assert len(return_value) == 5
    assert return_value[0] == extractor.academy_csv


def test_talent_json():
    return_value = extractor.talent_json
    assert type(return_value) is list
    assert return_value[0].startswith('Talent')
    assert return_value[-1].endswith('.json')
    assert len(return_value) >= 3105

    # Check for a few known files:
    check_for = ["Talent/10403.json", "Talent/10511.json", "Talent/10671.json", "Talent/10682.json"]
    assert all(file in return_value for file in check_for)


def test_resource():
    res = extractor.resource
    assert 'Bucket' in res.get_available_subresources()
    assert 'Object' in res.get_available_subresources()
    assert res.Bucket('data24-final-project').name == 'data24-final-project'


def test_populate_all_files():
    return_value = extractor.populate_all_files()
    assert type(return_value) is list
    assert type(return_value[3]) is list
    assert len(return_value) == 5
    assert return_value[0] == extractor.academy_csv


def test_academy_csv():
    return_value = extractor.academy_csv
    assert type(return_value) is list
    assert len(return_value) >= 36
    assert 'Academy/Data_33_2019-08-05.csv' in return_value


def test_talent_txt():
    return_value = extractor.talent_txt
    assert type(return_value) is list
    assert len(return_value) >= 152
    assert 'Talent/Sparta Day 10 April 2019.txt' in return_value


def test_bucket_name():
    return_value = extractor.bucket_name
    # 's3://data24-final-project/Academy/Data_33_2019-08-05.csv'
    assert return_value == 'data24-final-project'
