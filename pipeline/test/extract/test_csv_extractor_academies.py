from pipeline.app.extract import csv_extractor as cx
import pandas as pd
import numpy as np

values = cx.AcademiesCsvExtractor()

# tests the length of the rows returned in the output as well as its type
def test_len_of_rows():
    output = values.len_of_rows(values.single_csv("Academy/Engineering_29_2019-12-30.csv"))
    assert output >= 9 or output <= 9
    assert type(output) is int


# tests for the type and specific value format of the output
def test_extract_name():
    output = values.extract_csv_name(values.single_csv("Academy/Data_28_2019-02-18.csv"), 3)
    assert type(output) is str
    assert 'Aida Bothams' in output
    assert 'aida Bothams' not in output
    assert 'Aida bothams' not in output
    assert 'aida bothams' not in output


# tests for the type and specific value format of the output
def test_extract_trainer():
    output = values.extract_academies_trainer(values.single_csv("Academy/Business_20_2019-02-11.csv"), 4)
    assert type(output) is str
    assert 'Gregor Gomez' in output
    assert 'Gregor gomez' not in output
    assert 'gregor Gomez' not in output
    assert 'gregor gomez' not in output


# tests for the type and values in the output
def test_extract_skill_value():
    output = values.extract_academies_skill_value(values.single_csv("Academy/Data_31_2019-05-20.csv"), 'Determined_W1', 1)
    assert output >= 7 or output <= 7
    assert type(output) == float

# tests for specific values within the dict and the output types
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

# tests for the type, length and specific values in the output
def test_extract_skill_values_per_person_per_week():
    output = values.extract_academies_skill_values_per_person_per_week(values.single_csv('Academy/Business_29_2019-11-18.csv'))
    assert type(output) is dict
    assert len(output) == 10
    assert 'Rossie Caitlin' in output

# tests for the type and specific value in the output
def test_extract_course_name():
    output = values.extract_academies_course_name('Academy/Engineering_29_2019-12-30.csv')
    assert type(output) is str
    assert 'Engineering 29' in output

# tests for the specific values at each provided index and type of the final output
def test_extract_date():
    output = values.extract_academies_date('Academy/Business_27_2019-09-16.csv')
    assert type(output) is str
    assert '2019-09-16' in output
    assert output[0:4] == '2019'
    assert output[5:7] == '09'
    assert output[8::] == '16'
