import pytest


def test_clean_psychometrics():
    assert clean_psychometric("Psychometrics: 54/100") == ("Psychometrics :", 0.54)


def test_clean_presentation():
    assert clean_presentation("Presentation: 12/32") == ("Presentation :", 12)


def test_clean_name():
    assert test_clean("MICHEIL ROTLAUF") == ("Micheil", "Rotlauf")
