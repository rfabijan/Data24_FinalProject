from pipeline.app.extract.csv_extractor import ApplicantsCsvExtractor
import pandas as pd
import numpy as np

# instantiation of the class
extractor = ApplicantsCsvExtractor()


def test_extract_id():
    # Check from valid file
    file = extractor.single_csv('Talent/Oct2019Applicants.csv')
    function_return = extractor.extract_id(file, 2)
    assert type(function_return) == np.int64
    assert function_return == 3

    # Check from custom file
    file = pd.DataFrame({"id": [10, 21, 32]})
    assert extractor.extract_id(file, 2) == 32
    assert extractor.extract_id(file, -10) is None
    assert extractor.extract_id(file, 23) is None

    # Check from invalid file
    file = pd.DataFrame({"name": ["Tom", "Dick", "Harry"]})
    assert extractor.extract_id(file, 2) is None


def test_extract_gender():
    # Check from valid file
    file = extractor.single_csv('Talent/Feb2019Applicants.csv')
    assert extractor.extract_gender(file, 4) == 'Male'

    # Check from custom file
    file = pd.DataFrame({"gender": ['Male', 'Female', 'Female']})
    assert extractor.extract_gender(file, 1) == 'Female'
    assert extractor.extract_gender(file, -5) is None
    assert extractor.extract_gender(file, 5) is None

    # Check from invalid file
    file = pd.DataFrame({"name": ["Tom", "Dick", "Harry"]})
    assert extractor.extract_gender(file, 2) is None


def test_extract_dob():
    # Check from valid file
    file = extractor.single_csv('Talent/Aug2019Applicants.csv')
    assert extractor.extract_dob(file, 5) == '15/12/2000'

    # Check from custom file
    file = pd.DataFrame({"dob": ['01/01/2001', '10/10/2010', '20/12/2020']})
    assert extractor.extract_dob(file, 2) == '20/12/2020'
    assert extractor.extract_dob(file, -5) is None
    assert extractor.extract_dob(file, 5) is None

    # Check from invalid file
    file = pd.DataFrame({"name": ["Tom", "Dick", "Harry"]})
    assert extractor.extract_dob(file, 2) is None


def test_extract_email():
    # Check from valid file
    file = extractor.single_csv('Talent/Sept2019Applicants.csv')
    assert extractor.extract_email(file, 1) == 'dduffil1@exblog.jp'

    # Check from custom file
    file = pd.DataFrame({"email": ['tom@gmail.com', 'tim@icloud.com', 'ted@outlook.com']})
    assert extractor.extract_email(file, 1) == 'tim@icloud.com'
    assert extractor.extract_dob(file, -7) is None
    assert extractor.extract_dob(file, 3) is None

    # Check from invalid file
    file = pd.DataFrame({"name": ["Tom", "Dick", "Harry"]})
    assert extractor.extract_dob(file, 2) is None


def test_extract_city():
    # Check from valid file
    file = extractor.single_csv('Talent/Aug2019Applicants.csv')
    assert extractor.extract_city(file, 1) == 'Sutton'

    # Check from custom file
    file = pd.DataFrame({"city": ['London', 'Birmingham', 'Manchester']})
    assert extractor.extract_city(file, 0) == 'London'
    assert extractor.extract_city(file, -2) is None
    assert extractor.extract_city(file, 6) is None

    # Check from invalid file
    file = pd.DataFrame({"name": ["Tom", "Dick", "Harry"]})
    assert extractor.extract_city(file, 2) is None


def test_extract_address():
    # Check from valid file
    file = extractor.single_csv('Talent/May2019Applicants.csv')
    assert extractor.extract_address(file, 5) == '799 Vera Hill'

    # Check from custom file
    file = pd.DataFrame({"address": ['1 High Street', '2 Main Road', '3 London Road']})
    assert extractor.extract_address(file, 1) == '2 Main Road'
    assert extractor.extract_address(file, -4) is None
    assert extractor.extract_address(file, 11) is None

    # Check from invalid file
    file = pd.DataFrame({"name": ["Tom", "Dick", "Harry"]})
    assert extractor.extract_address(file, 2) is None


def test_extract_postcode():
    # Check from valid file
    file = extractor.single_csv('Talent/Feb2019Applicants.csv')
    assert extractor.extract_postcode(file, 2) == 'GU32'

    # Check from custom file
    file = pd.DataFrame({"postcode": ['EC2Y', 'NW2', 'WV3']})
    assert extractor.extract_postcode(file, 2) == 'WV3'
    assert extractor.extract_postcode(file, -1) is None
    assert extractor.extract_postcode(file, 4) is None

    # Check from invalid file
    file = pd.DataFrame({"name": ["Tom", "Dick", "Harry"]})
    assert extractor.extract_postcode(file, 0) is None


def test_extract_phone_number():
    # Check from valid file
    file = extractor.single_csv('Talent/March2019Applicants.csv')
    function_return = extractor.extract_phone_number(file, 3)
    assert function_return == '+44-900-852-4771'
    assert (len(function_return)) <= 16

    # Check from custom file
    file = pd.DataFrame({"phone_number": ['+44 (1)', '+44 234-567', '+448754']})
    assert extractor.extract_phone_number(file, 0) == '+44 (1)'
    assert extractor.extract_phone_number(file, -6) is None
    assert extractor.extract_phone_number(file, 5) is None

    # Check from invalid file
    file = pd.DataFrame({"name": ["Tom", "Dick", "Harry"]})
    assert extractor.extract_phone_number(file, 0) is None


def test_extract_university():
    # Check from valid file
    file = extractor.single_csv('Talent/Dec2019Applicants.csv')
    assert extractor.extract_university(file, 1) == 'University of Bradford'

    # Check from custom file
    file = pd.DataFrame({"uni": ['Oxford', 'Cambridge', 'Imperial College']})
    assert extractor.extract_university(file, 1) == 'Cambridge'
    assert extractor.extract_university(file, -2) is None
    assert extractor.extract_university(file, 8) is None

    # Check from invalid file
    file = pd.DataFrame({"name": ["Tom", "Dick", "Harry"]})
    assert extractor.extract_university(file, 0) is None


def test_extract_degree():
    # Check from valid file
    file = extractor.single_csv('Talent/Oct2019Applicants.csv')
    assert extractor.extract_degree(file, 4) == '2:2'

    # Check from custom file
    file = pd.DataFrame({"degree": ['1st', '2:1', '3rd']})
    assert extractor.extract_degree(file, 2) == '3rd'
    assert extractor.extract_degree(file, -8) is None
    assert extractor.extract_degree(file, 12) is None

    # Check from invalid file
    file = pd.DataFrame({"name": ["Tom", "Dick", "Harry"]})
    assert extractor.extract_degree(file, 0) is None


def test_extract_invited_date():
    # Check from valid file
    file = extractor.single_csv('Talent/April2019Applicants.csv')
    assert extractor.extract_invited_date(file, 5) == '18'

    # Check from custom file
    file = pd.DataFrame({"invited_date": ["0", "1", "2"]})
    assert extractor.extract_invited_date(file, 0) == '0'
    assert extractor.extract_invited_date(file, -10) is None
    assert extractor.extract_invited_date(file, 23) is None

    # Check from invalid file
    file = pd.DataFrame({"name": ["Tom", "Dick", "Harry"]})
    assert extractor.extract_invited_date(file, 2) is None


def test_extract_month():
    # Check from valid file
    file = extractor.single_csv('Talent/Oct2019Applicants.csv')
    assert extractor.extract_month(file, 88) == 'OCTOBER 2019'

    # Check from custom file
    file = pd.DataFrame({"month": ["APRIL 2000", "MAY 2001", "JUNE 2020"]})
    assert extractor.extract_month(file, 1) == 'MAY 2001'
    assert extractor.extract_month(file, -1) is None
    assert extractor.extract_month(file, 3) is None

    # Check from invalid file
    file = pd.DataFrame({"name": ["Tom", "Dick", "Harry"]})
    assert extractor.extract_month(file, 2) is None


def test_extract_invited_by():
    # Check from valid file
    file = extractor.single_csv('Talent/June2019Applicants.csv')
    assert extractor.extract_invited_by(file, 11) == 'Doris Bellasis'

    # Check from custom file
    file = pd.DataFrame({"invited_by": ["Tom", "Dick", "Harry"]})
    assert extractor.extract_invited_by(file, 2) == 'Harry'
    assert extractor.extract_invited_by(file, -1) is None
    assert extractor.extract_invited_by(file, 9) is None

    # Check from invalid file
    file = pd.DataFrame({"name": ["Tom", "Dick", "Harry"]})
    assert extractor.extract_invited_by(file, 2) is None


def test_extract_title_date():
    assert extractor.extract_title_date('Talent/June2019Applicants.csv') == 'June-2019'  # valid filename
    assert extractor.extract_title_date('Talent/Jan2050Applicants.csv') == 'Jan-2050'  # custom parameter
    assert extractor.extract_title_date('invalidfilename') is None
