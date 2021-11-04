from datetime import *
from pipeline.app.transform.cleaning_talent import *

"""
Need to test NULL values and other extrema
Bit cruel to the devs?
"""


def test_clean_name():
    # Normal values
    assert clean_name('Judy Finders') == ('Judy', 'Finders')

    # Bad values
    assert clean_name('Dominic') == ('Dominic', None)
    assert clean_name('jAck Flash-Gordon') == ('Jack', 'Flash-Gordon')


def test_clean_date():
    assert clean_date('01/02/2003') == date(2003, 2, 1)


def test_clean_geo_flex():
    assert clean_geo_flex('Yes')


def test_clean_result():
    assert clean_result('Pass')


def test_clean_self_development():
    assert clean_self_development('Yes')


def test_clean_course_interest():
    assert clean_course_interest("Data") == "Data"
    # assert "Data" in list_of_courses


def test_clean_tech_self_score():
    assert clean_tech_self_score({"R": 4})["R"] == 4  # Needs to actually test the cleaning abilities

    assert clean_tech_self_score({"R": 1000}) is None


def test_clean_strengths():
    # assert clean_strengths(["Versatile"])[1] in list_of_strengths

    assert clean_strengths(["Versatile", "PATIENT"])[2] == 'Patient'


def test_clean_weaknesses():
    # assert clean_weaknesses(["Impatient"])[1] in list_of_weaknesses

    assert clean_weaknesses(["dUMB"])[1] == "Dumb"
