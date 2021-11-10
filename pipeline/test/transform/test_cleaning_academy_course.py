from pipeline.app.transform import cleaning_academy_course as clean

output = clean.AcademyCleaner()

# tests to ensure correct output format and type
def test_clean_name():
    new_value = output.clean_name("rossie caitlin")
    assert new_value == ("Rossie", "Caitlin")
    assert type(new_value) is tuple

# tests to ensure correct output format and type
def test_clean_trainer():
    new_value = output.clean_trainer("bruce lugo")
    assert new_value == ("Bruce", "Lugo")
    assert type(new_value) is tuple

# tests to ensure the output is in correct format, type and specific range of values
def test_clean_skill_value():
    new_value = output.clean_skill_value(3)
    assert new_value == 3
    assert type(new_value) is int
    assert new_value in range(1,9)


# have not tested the error_name functions because they return empty sets
