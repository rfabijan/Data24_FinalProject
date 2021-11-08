from pipeline.app.extract import csv_extractor as cx
import pandas as pd
import numpy as np

values = cx.AcademiesCsvExtractor()
values1 = cx.ApplicantsCsvExtractor()

print(values1.extract_dob(values.singe_csv("Talent/Aug2019Applicants.csv"),5))
# print(values.extract_date('Academy/Business_27_2019-09-16.csv'))
# change single csv name

def test_len_of_rows():
    output = values.len_of_rows(values.singe_csv("Academy/Engineering_29_2019-12-30.csv"))
    assert output >= 9 or output <= 9
    assert type(output) is int

def test_extract_name():
    output = values.extract_name(values.singe_csv("Academy/Data_28_2019-02-18.csv"),3)
    assert type(output) is str
    assert 'Aida Bothams' in output

def test_extract_trainer():
    output = values.extract_trainer(values.singe_csv("Academy/Business_20_2019-02-11.csv"),4)
    assert type(output) is str
    assert 'Gregor Gomez' in output

# def test_extract_weeks(): to do

def test_extract_skill_value():
    output = values.extract_skill_value(values.singe_csv("Academy/Data_31_2019-05-20.csv"),'Determined_W1',1)
    assert output >= 7 or output <= 7
    assert type(output) == np.int64

def test_singe_csv():
    output = values.singe_csv('Academy/Engineering_21_2019-07-15.csv')
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
    output = values.extract_skill_values_per_person_per_week(values.singe_csv('Academy/Business_29_2019-11-18.csv'))
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
    pass



def test_extract_gender():
    pass



def test_extract_dob():
    output = values1.extract_dob(values.singe_csv("Talent/Aug2019Applicants.csv"),5)
    assert type(output) is str
    assert '15/12/2000' in output
    assert output[0:2] == '15'
    assert output[3:5] == '12'
    assert output[6::] == '2000'

# def count(x):
#     key = 0
#     for i in x:
#         key += 1
#     return key
#
# print(count(values.extract_skill_values_per_person_per_week(values.singe_csv('Academy/Business_29_2019-11-18.csv'))))
#













