from pipeline.app.extract import RFPrototypeJSONExtractor as rf

# instantiation of the class
values = rf.JSONExtractor()

# function to test the JSON key extraction, testing for the type, length and values in the output
def test_extract_keys():
    get_values = values.extract_keys
    assert type(get_values) is list
    assert len(get_values) >= 3105
    assert 'Talent/10385.json' in get_values

# function to test extracting data from a single JSON file, testing for type, length, values and keys in the output
def test_extract_single_json():
    get_values = values.pull_single_json('Talent/10384.json')
    assert type(get_values) is dict
    assert len(get_values) >= 10
    assert get_values['name'] == 'Hilary Willmore'
    assert 'Python' in get_values['tech_self_score']

# function to test name extraction from a single JSON file, testing for type and values of the output
def test_extract_name():
    file = values.pull_single_json('Talent/10383.json')
    get_name = values.extract_name(file)
    assert get_name == 'Stillmann Castano'
    assert type(get_name) is str

# function to test extraction of the date from a single JSON file, testing for type and values of the output
def test_extract_date():
    file = values.pull_single_json('Talent/10450.json')
    get_date = values.extract_date(file)
    assert get_date == ('15/08/2019')
    assert type(get_date) is str

# function to test extraction of the tech_self_score from a single JSON file, testing for type, length, values and keys of the output
def test_extract_tech_self_score():
    file = values.pull_single_json('Talent/10458.json')
    get_tech_score = values.extract_tech_self_score(file)
    assert type(get_tech_score) is dict
    assert len(get_tech_score) >= 4
    assert get_tech_score['SPSS'] == 3
    assert 'Ruby' in get_tech_score

# function to test extraction of the strengths from a single JSON file, testing for type, length and values of the output
def test_extract_strengths():
    file = values.pull_single_json('Talent/10474.json')
    get_strengths = values.extract_strengths(file)
    assert type(get_strengths) is list
    assert len(get_strengths) >= 1
    assert 'Listening' in get_strengths

# function to test extraction of the weaknesses from a single JSON file, testing for type, length and values of the output
def test_extract_weaknesses():
    file = values.pull_single_json('Talent/10661.json')
    get_weakness = values.extract_weaknesses(file)
    assert type(get_weakness) is list
    assert len(get_weakness) >= 3
    assert 'Sensitive' in get_weakness

# function to test extraction of the self_development from a single JSON file, testing for type and values of the output
def test_extract_self_development():
    file = values.pull_single_json('Talent/10836.json')
    get_self_dev = values.extract_self_development(file)
    assert type(get_self_dev) is str
    assert 'No' in get_self_dev

# function to test extraction of the goe_flex from a single JSON file, testing for type and values of the output
def test_extract_geo_flex():
    file = values.pull_single_json('Talent/10887.json')
    get_geo_flex= values.extract_geo_flex(file)
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
