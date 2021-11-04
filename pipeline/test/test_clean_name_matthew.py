from datetime import *
# from cleaning import *


def test_clean_name():
    assert clean_name('Judy Finders') == 'Judy', 'Finders'
    assert isinstance(talent12345["name"], str)


def test_clean_date():
    assert clean_name('01/02/2003') == date(2003, 2, 1)
    assert isinstance(talent12345["date"], date)


def test_clean_geo_flex():
    assert isinstance(talent12345["geo_flex"], bool)
    assert clean_geo_flex('Yes')


def test_clean_result():
    assert isinstance(talent12345["result"], bool)
    assert clean_result('Pass')


def test_clean_self_development():
    assert isinstance(talent12345["result"], bool)
    assert clean_self_development('Yes')


def test_clean_course_interest():
    assert isinstance(talent12345["course_interest"], str)  # Check it's in the list


def test_clean_tech_self_score():
    assert isinstance(talent12345["tech_self_score"], dict)  # Check it's in the list


def test_clean_strengths():
    assert isinstance(talent12345["strengths"], list)  # Check it's in the list


def test_clean_weaknesses():
    assert isinstance(talent12345["weaknesses"], list)  # Check it's in the list
