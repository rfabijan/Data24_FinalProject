from pipeline.app.extract import PrototypeS3Class as psc

extractor = psc.S3ParentClass()


def test_get_academy_csv():
    return_value = extractor.get_academy_csv
    assert type(return_value) is list
    assert len(return_value) >= 36
    assert 'Data_33_2019-08-05.csv' in return_value


def test_get_talent_txt():
    return_value = extractor.get_talent_txt
    assert type(return_value) is list
    assert len(return_value) > 152
    assert 'Sparta Day 10 April 2019.txt' in return_value


def test_bucket_name():
    return_value = extractor.bucket_name
    # 's3://data24-final-project/Academy/Data_33_2019-08-05.csv'
    assert return_value == 'data24_final-project'
