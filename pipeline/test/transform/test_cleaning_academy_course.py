from pipeline.app.transform import cleaning_academy_course as clean


def test_clean_name():
    testy = clean.AcademyCleaner()
    new_value = testy.clean_name("Rossie Caitlin")
    assert testy.clean_name("Rossie Caitlin") == ("Rossie", "Caitlin")
    assert type(new_value) is tuple


def test_clean_trainer():
    testy = clean.AcademyCleaner()
    new_value = testy.clean_trainer("Bruce Lugo")
    assert testy.clean_trainer("Bruce Lugo") == ("Bruce", "Lugo")
    assert type(new_value) is tuple


# not finalised
def test_clean_skill_value():
    pass
