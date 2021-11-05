import pprint

from pipeline.app.extract import MLPrototypeTXTClass as ptc

txtextractor = ptc.TxtExtractor()


def test_keys():
    return_value = txtextractor.keys
    assert type(return_value) is list
    assert len(return_value) >= 152
    assert 'Talent/Sparta Day 12 September 2019.txt' in return_value


def test_sparta_day_dict():
    return_value = txtextractor.sparta_day_dict
    """
    NEEDS TESTING
    """
    pass


def test_read_text_object():
    i = txtextractor.keys[0]
    object_instance = txtextractor.client.get_object(Bucket=txtextractor.bucket_name, Key=i)
    return_value = txtextractor.read_text_object(object_instance)
    assert 'HILARY WILLMORE -  Psychometrics: 51/100, Presentation: 19/32\r' in return_value


def test_extract_all_info():
    return_value = txtextractor.extract_all_info()
    assert return_value['ChloFair2122019']['Academy'] == 'Birmingham Academy'
