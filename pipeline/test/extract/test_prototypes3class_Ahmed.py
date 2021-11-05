import boto3
from pipeline.app.extract import PrototypeS3Class as psc

extractor = psc.S3ParentClass()


def test_get_all_files():
    return_value = extractor.get_all_files
    assert type(return_value) is list
    assert type(return_value[3]) is list
    assert len(return_value) == 5
    assert return_value[0] == extractor.get_academy_csv


def test_get_talent_json():
    return_value = extractor.get_talent_json
    assert type(return_value) is list
    assert return_value[0].startswith('Talent')
    assert return_value[-1].endswith('.json')
    assert len(return_value) >= 3105

    # Check for a few known files:
    check_for = ["Talent/10403.json", "Talent/10511.json", "Talent/10671.json", "Talent/10682.json"]
    assert all(file in return_value for file in check_for)


def test_get_resource():
    res = extractor.get_resource
    assert 'Bucket' in res.get_available_subresources()
    assert 'Object' in res.get_available_subresources()
    assert res.Bucket('data24-final-project').name == 'data24-final-project'


def test_populate_all_files():
    return_value = extractor.populate_all_files()
    assert type(return_value) is list
    assert type(return_value[3]) is list
    assert len(return_value) == 5
    assert return_value[0] == extractor.get_academy_csv

