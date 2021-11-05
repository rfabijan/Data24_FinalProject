import pytest
from datetime import datetime
from pipeline.app.transform import cleaning_talent_sparta_day as clean


def test_clean_psychometrics():
    new_value = clean.clean_psychometrics("Psychometrics: 54/100")
    assert clean.clean_psychometrics("Psychometrics: 54/100") == 0.54
    assert type(new_value) is float


def test_clean_presentation():
    new_value = clean.clean_presentation("Presentation: 12/32")
    assert clean.clean_presentation("Presentation: 12/32") == 0.375
    assert type(new_value) is float


def test_clean_name():
    new_value = clean.clean_name("MICHEIL ROTLAUF")
    assert clean.clean_name("MICHEIL ROTLAUF") == ("Micheil", "Rotlauf")
    assert type(new_value) is tuple


def test_clean_academy():
    new_value = clean.clean_academy("London Academy")
    assert clean.clean_academy("London Academy") == "London Academy"
    assert type(new_value) is str


def test_clean_date():
    new_value = clean.clean_date("1 May 2019")
    assert clean.clean_date("1 May 2019") == datetime.datetime(2019, 5, 1)
    assert type(new_value) is datetime.datetime

