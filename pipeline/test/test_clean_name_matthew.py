from datetime import *


def test_clean_name():
    assert clean_name('Judy Finders') == 'Judy', 'Finders'


def test_clean_talent():
    assert isinstance(talent12345["name"], str)
    assert isinstance(talent12345["date"], date)                # Check with example like above
    assert isinstance(talent12345["tech_self_score"], dict)     # Check it's in the list
    assert isinstance(talent12345["strengths"], list)           # Check it's in the list
    assert isinstance(talent12345["weaknesses"], list)          # Check it's in the list
    assert isinstance(talent12345["self_development"], bool)
    assert isinstance(talent12345["geo_flex"], bool)
    assert isinstance(talent12345["result"], bool)
    assert isinstance(talent12345["course_interest"], str)      # Check it's in the list
