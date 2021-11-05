from pipeline.app.extract import MLPrototypeTXTClass as ptc

txtextractor = ptc.TxtExtractor()
# Hi Rob!
list_instance = txtextractor.pull_text_object_as_list(txtextractor.keys[0])  # These two are for all the extract funcs
test_name_line = txtextractor.extract_name_line(list_instance, 3)


def test_keys():
    return_value = txtextractor.keys
    assert 'Talent/Sparta Day 12 September 2019.txt' in return_value


def test_pull_text_object_as_list():
    testing_key = txtextractor.keys[0]
    return_value = txtextractor.pull_text_object_as_list(testing_key)
    assert 'HILARY WILLMORE -  Psychometrics: 51/100, Presentation: 19/32\r' in return_value


def test_extract_academy():
    assert txtextractor.extract_academy(list_instance) == 'Birmingham Academy\r'


def test_extract_date():
    assert txtextractor.extract_date(list_instance) == 'Thursday 1 August 2019\r'


def test_extract_name_from_line():
    assert txtextractor.extract_name_from_line(test_name_line) == 'HILARY WILLMORE'


def test_extract_psychometric_from_line():
    assert txtextractor.extract_psychometric_from_line(test_name_line) == 'Psychometrics: 51/10'


def test_extract_presentation_from_line():
    assert txtextractor.extract_presentation_from_line(test_name_line) == 'Presentation: 19/32\r'
