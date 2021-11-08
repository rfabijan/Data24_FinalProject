from pipeline.app.extract import txt_extractor

# instantiation of the class
extractor = txt_extractor.TxtExtractor()


def test_keys():
    function_return = extractor.keys

    assert type(function_return) is list
    assert len(function_return) >= 152
    assert 'Talent/Sparta Day 12 September 2019.txt' in function_return


def test_pull_text_object_as_list():
    function_return = extractor.pull_text_object_as_list('Talent/Sparta Day 29 January 2019.txt')

    assert function_return[0] == 'Tuesday 29 January 2019'  # Reading a valid file
    assert extractor.pull_text_object_as_list('ThisFileDoesNotExist.txt') is None


def test_extract_date():
    # Check from file read
    file_body = extractor.pull_text_object_as_list('Talent/Sparta Day 30 May 2019.txt')
    assert extractor.extract_date(file_body) == 'Thursday 30 May 2019'

    # Check from custom parameter
    file_body = ["Monday 08 November 2021", "London Academy"]
    assert extractor.extract_date(file_body) == 'Monday 08 November 2021'

    # Check from invalid parameter
    file_body = ["NATA BOHMAN", "BENNIE LATAN"]
    assert extractor.extract_date(file_body) is None


def test_extract_academy():
    # Check from file read
    file_body = extractor.pull_text_object_as_list('Talent/Sparta Day 31 October 2019.txt')
    assert extractor.extract_academy(file_body) == 'Birmingham Academy'

    # Check from custom parameter
    file_body = ["Monday 08 November 2021", "London Academy"]
    assert extractor.extract_academy(file_body) == 'London Academy'


def test_extract_name_line():
    # Check from file read
    file_body = extractor.pull_text_object_as_list('Talent/Sparta Day 27 March 2019.txt')
    expected_output = 'FRANCISKUS LETHARDY -  Psychometrics: 55/100, Presentation: 20/32'
    assert extractor.extract_name_line(file_body, 3) == expected_output

    # Check from custom parameter
    file_body = ["Monday 08 November 2021", "London Academy", "MATT LYONS - Baking: 10/10"]
    assert extractor.extract_name_line(file_body, 1) == 'London Academy'


def test_extract_name_from_line():
    # Check from file read
    file_body = extractor.pull_text_object_as_list('Talent/Sparta Day 28 May 2019.txt')
    name_line = extractor.extract_name_line(file_body, 13)
    assert extractor.extract_name_from_line(name_line) == 'GAV RANTOUL'

    # Check from custom parameter
    file_body = ["MATT LYONS - 10/10", "ROB FABIJAN - 11/10", "SULLY MAHMOOD - ?/10"]
    name_line = extractor.extract_name_line(file_body, 1)
    assert extractor.extract_name_from_line(name_line) == 'ROB FABIJAN'


def test_extract_psychometric_from_line():
    # Check from file read
    file_body = extractor.pull_text_object_as_list('Talent/Sparta Day 29 January 2019.txt')
    name_line = extractor.extract_name_line(file_body, 17)
    assert extractor.extract_psychometric_from_line(name_line) == '60/100'

    # Check from custom parameter
    file_body = [
        "SILEAS GOLSON -  Psychometrics: 10/100, Presentation: 16/32",
        "SALOMON COSTELLOW -  Psychometrics: 20/100, Presentation: 27/32",
        "GAYEL MEINEKING -  Psychometrics: 30/100, Presentation: 19/32"
    ]
    name_line = extractor.extract_name_line(file_body, 2)
    assert extractor.extract_psychometric_from_line(name_line) == '30/100'


def test_extract_presentation_from_line():
    # Check from file read
    file_body = extractor.pull_text_object_as_list('Talent/Sparta Day 28 August 2019.txt')
    name_line = extractor.extract_name_line(file_body, 27)
    assert extractor.extract_presentation_from_line(name_line) == '19/32'

    # Check from custom parameter
    file_body = [
        "SILEAS GOLSON -  Psychometrics: 10/100, Presentation: 16/32",
        "SALOMON COSTELLOW -  Psychometrics: 20/100, Presentation: 27/32",
        "GAYEL MEINEKING -  Psychometrics: 30/100, Presentation: 19/32"
    ]
    name_line = extractor.extract_name_line(file_body, 0)
    assert extractor.extract_psychometric_from_line(name_line) == '16/32'
