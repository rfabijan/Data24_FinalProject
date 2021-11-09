from pipeline.app.extract import csv_extractor as cx
import pandas as pd
import numpy as np

values = cx.AcademiesCsvExtractor()
values1 = cx.ApplicantsCsvExtractor()

def test_len_of_rows():
    output = values.len_of_rows(values.single_csv("Academy/Engineering_29_2019-12-30.csv"))
    assert output >= 9 or output <= 9
    assert type(output) is int

def test_extract_name():
    output = values.extract_name(values.single_csv("Academy/Data_28_2019-02-18.csv"),3)
    assert type(output) is str
    assert 'Aida Bothams' in output
    assert 'aida Bothams' not in output
    assert 'Aida bothams' not in output
    assert 'aida bothams' not in output

def test_extract_trainer():
    output = values.extract_trainer(values.single_csv("Academy/Business_20_2019-02-11.csv"),4)
    assert type(output) is str
    assert 'Gregor Gomez' in output
    assert 'Gregor gomez' not in output
    assert 'gregor Gomez' not in output
    assert 'gregor gomez' not in output

def test_extract_skill_value():
    output = values.extract_skill_value(values.single_csv("Academy/Data_31_2019-05-20.csv"),'Determined_W1',1)
    assert output >= 7 or output <= 7
    assert type(output) == np.int64

def test_singe_csv():
    output = values.single_csv('Academy/Engineering_21_2019-07-15.csv')
    output2 = output['name'][1]
    output3 = output['trainer'][5]
    assert 'name' in output
    assert 'trainer' in output
    assert output['name'][1] == 'William Tomlett'
    assert output['trainer'][5] == 'Macey Broughton'
    assert type(output2) is str
    assert type(output3) is str
    assert type(output) == pd.core.frame.DataFrame

def test_extract_skill_values_per_person_per_week():
    output = values.extract_skill_values_per_person_per_week(values.single_csv('Academy/Business_29_2019-11-18.csv'))
    assert type(output) is dict
    assert len(output) == 10
    assert 'Rossie Caitlin' in output

def test_extract_course_name():
    output = values.extract_course_name('Academy/Engineering_29_2019-12-30.csv')
    assert type(output) is str
    assert 'Engineering 29' in output

def test_extract_date():
    output = values.extract_date('Academy/Business_27_2019-09-16.csv')
    assert type(output) is str
    assert '2019-09-16' in output
    assert output[0:4] == '2019'
    assert output[5:7] == '09'
    assert output[8::] == '16'




def test_extract_id():
    output = values1.extract_id(values.single_csv("Talent/Oct2019Applicants.csv"), 2)
    assert type(output) == np.int64
    assert output == 3

def test_extract_gender():
    output = values1.extract_gender(values.single_csv("Talent/Feb2019Applicants.csv"), 4)
    assert type(output) is str
    assert 'Male' in output

def test_extract_dob():
    output = values1.extract_dob(values.single_csv("Talent/Aug2019Applicants.csv"),5)
    assert type(output) is str
    assert '15/12/2000' in output
    assert output[0:2] == '15'
    assert output[3:5] == '12'
    assert output[6::] == '2000'

def test_extract_email():
    output = values1.extract_email(values.single_csv("Talent/Sept2019Applicants.csv"), 1)
    assert type(output) is str
    assert 'dduffil1@exblog.jp' in output
    assert '@' in output

def test_extract_city():
    output = values1.extract_city(values.single_csv("Talent/Aug2019Applicants.csv"), 1)
    assert type(output) is str
    assert 'Sutton' in output
    assert 'sutton' not in output

def test_extract_address():
    output = values1.extract_address(values.single_csv("Talent/May2019Applicants.csv"), 5)
    assert type(output) is str
    assert '799 Vera Hill' in output
    assert output[0:3] == '799'
    assert output[4:8] == 'Vera'
    assert output[9::] == 'Hill'

def test_extract_postcode():
    output = values1.extract_postcode(values.single_csv("Talent/Feb2019Applicants.csv"), 2)
    assert type(output) is str
    assert 'GU32' in output
    assert 'gu32' not in output
    assert 'Gu32' not in output
    assert 'gU32' not in output

def test_extract_phone_number():
    output = values1.extract_phone_number(values.single_csv("Talent/March2019Applicants.csv"), 3)
    assert type(output) is str
    assert '+44-900-852-4771' in output
    assert output[0:3] == '+44'
    assert len(output) <= 16
    assert '+44' in output

def test_extract_university():
    output = values1.extract_university(values.single_csv("Talent/Dec2019Applicants.csv"), 1)
    assert type(output) is str
    assert 'University of Bradford' in output
    assert 'university of Bradford' not in output
    assert 'University of bradford' not in output
    assert 'university Of bradford' not in output

def test_extract_degree():
    output = values1.extract_degree(values.single_csv("Talent/Oct2019Applicants.csv"), 4)
    assert type(output) is str
    assert '2:2' in output
    assert output[0] == '2'
    assert output[2] == '2'
