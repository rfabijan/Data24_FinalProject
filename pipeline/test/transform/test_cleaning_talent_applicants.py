from pipeline.app.transform import cleaning_talent_applicants as clean


def test_clean_id():
    # testing good value(s)
    assert clean.clean_id('1') == 1

    # testing bad value(s)
    assert clean.clean_id('one') is None


def test_clean_name():
    # testing good value(s)
    return_value = clean.clean_name('Esme Trusslove')
    assert return_value == ('Esme', 'Trusslove')
    assert type(return_value) is tuple

    # testing bad value(s)
    assert clean.clean_name('esMe tRusSloVe') == ('Esme', 'Trusslove')
    assert clean.clean_name('esme') == ('Esme', '')
    assert clean.clean_name('Alberto O Sullivan') == ('Alberto', 'O Sullivan')
    assert clean.clean_name('Lester Weddeburn - Scrimgeour') == ('Lester', 'Weddeburn - Scrimgeour')


def test_clean_gender():
    # testing good value(s)
    assert clean.clean_gender('Female') == 'Female'

    # testing bad value(s)
    assert clean.clean_gender('male') == 'Male'
    assert clean.clean_gender('feMALE') == 'Female'
    assert clean.clean_gender('') is None
    assert clean.clean_gender('invalid') is None


def test_clean_dob():
    pass


def test_clean_email():
    pass


def test_clean_city():
    pass


def test_clean_address():
    pass


def test_clean_postcode():
    pass


def test_clean_phone_number():
    pass


def test_clean_uni():
    pass


def test_clean_degree():
    pass


def test_clean_invited_date():
    pass


def test_clean_month():
    pass


def test_clean_invited_by():
    pass
