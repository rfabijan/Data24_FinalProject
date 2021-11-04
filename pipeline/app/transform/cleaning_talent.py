"""
The question is, should these functions just clean strings and stuff, or the whole dictionary/dataframe/json/whatever?

Basically, how do we test correct extraction? As part of transformation (here), or before it?
If here, then tell us to change our test input to json objects or whatever.
I think ideally it'll be before (which we'll need to test with your guidance), and these will be very tiny functions.
"""


def clean_name(name):
    # 'Judy' needs cleaning into a tuple of ('Judy','')
    # 'joNNY O Sullivan-Weddeburn' is cleaned to ('Jonny',  'O' Sullivan-Weddeburn') or Sullivan - Weddeburn
    pass


def clean_date(date):
    #  '01/02/2003' converts to datetime.date(2003, 2, 1)
    pass


def clean_geo_flex(geo_flex):
    # 'Yes' converts to True
    pass


def clean_result(result):
    # 'Pass' converts to True
    pass


def clean_self_development(self_development):
    # 'Yes' converts to True
    pass


"""
Don't know how to test for a list of courses, tech skills, strengths or weaknesses
"""


def clean_course_interest(course_interest):
    # 'Data' stays as it is, as a string
    # Honestly idk if there's any bad values to REALLY clean here
    pass


def clean_tech_self_score(tech_self_scores):
    # {"R": 4} goes in as a dictionary/json/whatever, comes out as a dictionary where dict["R"] == 4
    # Pretty sure this doesn't really get back input, but I'll test with bad numbers that convert to None
    pass


def clean_strengths(strengths):
    # Input is a list, output is a list of strings
    # Must have the first letter only be capitalised
    pass


def clean_weaknesses(weaknesses):
    # Input is a list, output is a list of strings
    # Must have the first letter only be capitalised
    pass
