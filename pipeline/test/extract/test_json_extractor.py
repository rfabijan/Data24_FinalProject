from pipeline.app.extract import json_extractor

# instantiation of the class
extractor = json_extractor.JSONExtractor()


# function to test the JSON key extraction, testing for the type, length and values in the output
def test_extract_keys():
    function_return = extractor.extract_keys

    # Check return type
    assert type(function_return) is list  # Check return type
    assert len(function_return) >= 3105  # Check length of returned value is correct
    assert 'Talent/10385.json' in function_return  # Check if known file is in the returned list


# function to test extracting data from a single JSON file, testing for type, length, values and keys in the output
def test_pull_single_json():
    function_return = extractor.pull_single_json('Talent/10384.json')

    assert type(function_return) is dict  # Check return type
    assert len(function_return) == 10  # Check # keys in dictionary
    assert function_return['name'] == 'Hilary Willmore'  # Check correct file was read
    assert 'Python' in function_return['tech_self_score']  # Check 2 for correct file was read


# function to test name extraction from a single JSON file, testing for type and values of the output
def test_extract_name():
    file = extractor.pull_single_json('Talent/10383.json')
    function_return = extractor.extract_name(file)

    assert function_return == 'Stillmann Castano'  # Check return value from file dictionary
    assert extractor.extract_name({'name': 'John Doe'}) == 'John Doe'  # Check return value from custom dictionary
    assert extractor.extract_name({'age': 1}) is None  # Check return value on invalid dictionary


# function to test extraction of the date from a single JSON file, testing for value of the output
def test_extract_date():
    file = extractor.pull_single_json('Talent/10450.json')
    function_return = extractor.extract_date(file)

    assert function_return == '15/08/2019'
    assert extractor.extract_name({'date': '01/01/2021'}) == '01/01/2021'
    assert extractor.extract_name({'dob': '01/01/2021'}) is None


# function to test extraction of the tech_self_score from a single JSON file
# testing for type, length, values and keys of the output
def test_extract_tech_self_score():
    file = extractor.pull_single_json('Talent/10458.json')
    function_return = extractor.extract_tech_self_score(file)

    # Check dictionary based on valid file
    assert type(function_return) is dict
    assert len(function_return) == 4
    assert function_return['SPSS'] == 3
    assert 'Ruby' in function_return

    # Check custom dictionary
    function_return = extractor.extract_tech_self_score({'tech_self_score': {'C++': 1, 'Python': 3}})
    assert len(function_return) == 2
    assert function_return['Python'] == 3
    assert 'C++' in function_return

    # Check invalid dictionary
    function_return = extractor.extract_tech_self_score({'strengths': ['Organisation', 'Courteous']})
    assert function_return is None


# function to test extraction of the strengths from a single JSON file
# testing for type, length and values of the output
def test_extract_strengths():
    file = extractor.pull_single_json('Talent/10474.json')
    function_return = extractor.extract_strengths(file)

    # Check dictionary based on valid file
    assert type(function_return) is list
    assert len(function_return) == 1
    assert 'Listening' in function_return

    # Check custom dictionary
    function_return = extractor.extract_strengths({"strengths": ["Programming", "Gaming"]})
    assert len(function_return) == 2
    assert 'Programming' in function_return

    # Check invalid dictionary
    function_return = extractor.extract_strengths({'tech_self_score': {'C++': 1, 'Python': 3}})
    assert function_return is None


# function to test extraction of the weaknesses from a single JSON file
# testing for type, length and values of the output
def test_extract_weaknesses():
    file = extractor.pull_single_json('Talent/10661.json')
    function_return = extractor.extract_weaknesses(file)

    # Check dictionary based on valid file
    assert type(function_return) is list
    assert len(function_return) == 3
    assert 'Sensitive' in function_return

    # Check custom dictionary
    function_return = extractor.extract_strengths({"weaknesses": ["Procrastination", 'Distractable']})
    assert len(function_return) == 2
    assert 'Distractable' in function_return


# function to test extraction of the self_development from a single JSON file
# testing value of the output
def test_extract_self_development():
    file = extractor.pull_single_json('Talent/10836.json')
    function_return = extractor.extract_self_development(file)

    # Check dictionary based on valid file
    assert function_return == "No"


# function to test extraction of the goe_flex from a single JSON file, testing for type and values of the output
def test_extract_geo_flex():
    file = values.pull_single_json('Talent/10887.json')
    get_geo_flex = values.extract_geo_flex(file)
    assert type(get_geo_flex) is str
    assert 'Yes' in get_geo_flex


# function to test extraction of the financial_support_self from a single JSON file, testing for type and values of the output
def test_extract_financial_support_self():
    file = values.pull_single_json('Talent/11040.json')
    get_finance_supp = values.extract_financial_support_self(file)
    assert type(get_finance_supp) is str
    assert 'Yes' in get_finance_supp


# function to test extraction of the result from a single JSON file, testing for type and values of the output
def test_extract_result():
    file = values.pull_single_json('Talent/11098.json')
    get_result = values.extract_result(file)
    assert type(get_result) is str
    assert 'Pass' in get_result


# function to test extraction of the course interest from a single JSON file, testing for type and values of the output
def test_extract_course_interest():
    file = values.pull_single_json('Talent/11579.json')
    get_course_interest = values.extract_course_interest(file)
    assert type(get_course_interest) is str
    assert 'Data' in get_course_interest
