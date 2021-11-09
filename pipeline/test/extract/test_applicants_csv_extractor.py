from pipeline.app.extract.csv_extractor import ApplicantsCsvExtractor
import pandas as pd


# instantiation of the class
extractor = ApplicantsCsvExtractor()


def test_extract_title_date():
    assert extractor.extract_title_date('Talent/June2019Applicants.csv') == 'June-2019'  # valid filename
    assert extractor.extract_title_date('Talent/Jan2050Applicants.csv') == 'Jan-2050'  # custom parameter
    assert extractor.extract_title_date('invalidfilename') is None


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
