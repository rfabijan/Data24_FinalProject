import pytest
from pipeline.app.transform import cleaning_academy_course as clean


def test_clean_name():
    new_value = clean.clean_name("Rossie Caitlin")
    assert clean.clean_name("Rossie Caitlin") == ("Rossie", "Caitlin")
    assert type(new_value) is tuple


def test_clean_trainer():
    new_value = clean.clean_trainer("Bruce Lugo")
    assert clean.clean_trainer("Bruce Lugo") == ("Bruce", "Lugo")
    assert type(new_value) is tuple


def test_clean_skill_value():
    pass
