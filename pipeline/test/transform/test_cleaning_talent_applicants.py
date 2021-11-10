from pipeline.app.transform import cleaning_talent_applicants as clean
import datetime


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
    assert clean.clean_name('esme') == ('Esme', None)
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
    # testing good value(s)
    assert clean.clean_dob("04/08/1994") == datetime.datetime(1994, 8, 4)

    # testing bad value(s)
    assert clean.clean_dob("") is None


def test_clean_email():
    # testing good value(s)
    assert clean.clean_email('etrusslove0@google.es') == 'etrusslove0@google.es'

    # testing bad value(s)
    assert clean.clean_email('') is None
    assert clean.clean_email('etrusslove0google.es') is None  # No @ in email
    assert clean.clean_email('etrusslove0googlees') is None  # No . after @


def test_clean_city():
    # testing good value(s)
    assert clean.clean_city('Twyford') == 'Twyford'

    # testing bad value(s)
    assert clean.clean_city() is None
    assert clean.clean_city(' ') is None
    assert clean.clean_city('tWyfORD') == 'Twyford'


def test_clean_address():
    # testing good value(s)
    return_value = clean.clean_address('052 Ruskin Point')
    assert return_value == (52, 'Ruskin Point')
    assert type(return_value) is tuple
    assert len(return_value) == 2

    # testing bad value(s)
    assert clean.clean_address('  ') == (None, None)
    assert clean.clean_address() == (None, None)
    assert clean.clean_address('052Ruskin Point') == (None, 'Point')
    assert clean.clean_address('0 Shoshone Crossing') == (0, 'Shoshone Crossing')


def test_clean_postcode():
    # testing good value(s)
    assert clean.clean_postcode('WC1B') == 'WC1B'

    # testing bad value(s)
    assert clean.clean_postcode() is None
    assert clean.clean_postcode('  ') is None
    assert clean.clean_postcode('wc1b  ') == 'WC1B'
    assert clean.clean_postcode('1wcb ') is None


def test_clean_phone_number():
    # testing good value(s)
    assert clean.clean_phone_number('+449183488810') == '+449183488810'

    # testing bad value(s)
    assert clean.clean_phone_number() is None
    assert clean.clean_phone_number('+44-588-749-6002') == '+445887496002'
    assert clean.clean_phone_number('+44 (723) 247-1004') == '+447232471004'
    assert clean.clean_phone_number('+4472324710049425') is None


def test_clean_uni():
    # testing good value(s)
    assert clean.clean_uni('University of Leicester') == 'University of Leicester'

    # testing bad value(s)
    assert clean.clean_uni() is None
    assert clean.clean_uni('   ') is None
    assert clean.clean_uni('university Of leicester') == 'University of Leicester'


def test_clean_degree():
    # testing good value(s)
    assert clean.clean_degree('2:2') == '2:2'
    assert clean.clean_degree('1st') == '1st'
    assert clean.clean_degree('3rd') == '3rd'

    # testing bad value(s)
    assert clean.clean_degree() is None
    assert clean.clean_degree(' ') is None
    assert clean.clean_degree('2:0') is None
    assert clean.clean_degree('first') == '1st'
    assert clean.clean_degree('third') == '3rd'
    assert clean.clean_degree('two One') == '2:1'
    assert clean.clean_degree('two two') == '2:2'
    assert clean.clean_degree('two too') is None


def test_clean_invited_date():
    # testing good value(s)
    assert clean.clean_invited_date('18.0') == 18

    # testing bad value(s)
    assert clean.clean_invited_date() is None
    assert clean.clean_invited_date('   ') is None
    assert clean.clean_invited_date('one') is None
    assert clean.clean_invited_date('32') is None
    assert clean.clean_invited_date('-1') is None


def test_clean_month():
    # testing good value(s)
    assert clean.clean_month('April 2019') == datetime.datetime(2019, 4, 1)

    # testing bad value(s)
    assert clean.clean_month() is None
    assert clean.clean_month(' ') is None
    assert clean.clean_month('', 'Talent/April2019Applicants.csv') == datetime.datetime(2019, 4, 1)
    assert clean.clean_month(None, 'Talent/June2019Applicants.csv') == datetime.datetime(2019, 6, 1)


def test_clean_invited_by():
    # testing good value(s)
    return_value = clean.clean_name('Fifi Eton')
    assert return_value == ('Fifi', 'Eton')
    assert type(return_value) is tuple

    # testing bad value(s)
    assert clean.clean_name() is None
    assert clean.clean_name('') is None
    assert clean.clean_name('fifi eton') == ('Fifa', 'Eton')
    assert clean.clean_name('fifi') == ('Fifi', None)
    assert clean.clean_name('fiFI h ETon') == ('Fifa', 'H Eton')
