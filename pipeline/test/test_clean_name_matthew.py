from datetime import *
# from cleaning import *

"""
Need to test NULL values and other extrema
Bit cruel to the devs?
"""


def test_clean_name():
    assert clean_name('Judy Finders') == 'Judy', 'Finders'


def test_clean_date():
    assert clean_name('01/02/2003') == date(2003, 2, 1)


def test_clean_geo_flex():
    assert clean_geo_flex('Yes')


def test_clean_result():
    assert clean_result('Pass')


def test_clean_self_development():
    assert clean_self_development('Yes')


def test_clean_course_interest():
    assert clean_course_interest("Data    ") == "Data"
    assert "Data" in list_of_courses


def test_clean_tech_self_score():
    assert clean_tech_self_score({"R": 4})["R"] == 4  # Needs to actually test the cleaning abilities
    assert "R" in list_of_tech_self_skills
    # Needs a test for tech_self_score_ID


def test_clean_strengths():
    assert clean_strengths(["Versatile"])[1] in list_of_strengths  # Needs to actually check the cleaning capabilities
    # Needs a test for strengthID


def test_clean_weaknesses():
    assert clean_weaknesses(["Versatile"])[1] in list_of_weaknesses  # Needs to actually check the cleaning capabilities
    # Needs a test for weaknessID
