import pyodbc
import pipeline.config_manager as conf


# TODO: Validation on method parameters


def insert(cursor, db_name, table_name, values):
    # TODO This is inefficient. You can insert all values in one statement.
    for value in values:
        query = f"""
        INSERT INTO {table_name}
        VALUES ({value})
        """
        cursor.execute(query)


def insert_into_applicants(cursor, values=None, db_name=conf.DB_NAME):
    return insert(cursor, db_name=conf.DB_NAME, table_name="Applicants", values=values)


def insert_into_academy(cursor, values=None, db_name=conf.DB_NAME):
    return insert(cursor, db_name=conf.DB_NAME, table_name="Academy", values=values)


def insert_into_spartaday(cursor, values=None, db_name=conf.DB_NAME):
    return insert(cursor, db_name=conf.DB_NAME, table_name="SpartaDay", values=values)


def insert_data(insert_records_query, records_rows):
    with data24etl_db.cursor() as cursor:
        cursor.executemany(insert_records_query, records_rows)
        data24etl_db.commit()


server = 'localhost,1433'
database = conf.DB_NAME
username = 'SA'
password = 'Passw0rd2018'

data24etl_db = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID='
                              + username + ';PWD=' + password)

# cursor = data24etl_db.cursor()

"""List of tuples, with each tuple being a row in the database"""
# records_rows = [(a, b),
#                 (a, b),
#                 (a, b),
#                 (a, b)]

list_of_tables = ("techskill", "strengths", "weaknesses", "academy", "spartaday", "trainer", "course", "coursetrainer",
                  "coreskills", "streams", "invitors", "addresses", "applicants", "applicantsspartaday",
                  "techselfscore", "applicantsstrengths", "applicantsweaknesses", "spartans", "tracker")

#

insert_techskill_query = """
INSERT INTO techskill
(techskill_id, skill_name)
VALUES (%?, %?)
"""

insert_strengths_query = """
INSERT INTO strengths
(strength_id, skill_name)
VALUES (%?, %?)
"""

insert_weaknesses_query = """
INSERT INTO weaknesses
(weakness_id, weakness_name)
VALUES (%?, %?)
"""

insert_academy_query = """
INSERT INTO academy
(academy_id, academy_name)
VALUES (%?, %?)
"""

insert_spartaday_query = """
INSERT INTO spartaday
(spartaday_id, academy_id, spartaday_date)
VALUES (%?, %?, %?)
"""

insert_trainer_query = """
INSERT INTO trainer
(trainer_id, first_name, last_name)
VALUES (%?, %?, %?)
"""

insert_course_query = """
INSERT INTO course
(course_id, course_name, week_length, start_date)
VALUES (%?, %?, %?, %?)
"""

insert_coursetrainer_query = """
INSERT INTO coursetrainer
(course_id, trainer_id)
VALUES (%?, %?)
"""

insert_coreskills_query = """
INSERT INTO coreskills
(coreskill_id, coreskill_name)
VALUES (%?, %?)
"""

insert_streams_query = """
INSERT INTO streams
(stream_id, stream_name)
VALUES (%?, %?)
"""

insert_invitors_query = """
INSERT INTO invitors
(invitor_id, first_name, last_name)
VALUES (%?, %?, %?)
"""

insert_addresses_query = """
INSERT INTO addresses
(address_id, house_number, address_line, postcode, city)
VALUES (%?, %?, %?, %?, %?)
"""

insert_applicants_query = """
INSERT INTO invitors
(applicant_id, stream_interest_id, invited_by_id, address_id, first_name, last_name, gender, dob, email, phonenumber, uni, degree, geoflex, financial_support_self, result)
VALUES (%?, %?, %?, %?, %?, %?, %?, %?, %?, %?, %?, %?, %?, %?, %?)
"""

insert_applicantsspartaday_query = """
INSERT INTO applicantsspartaday
(applicant_id, spartaday_id, psychometric_score, presentation_score)
VALUES (%?, %?, %?, %?)
"""

insert_techselfscore_query = """
INSERT INTO techselfscore
(applicant_id, techskill_id, score)
VALUES (%?, %?, %?)
"""

insert_applicantsstrengths_query = """
INSERT INTO applicantsstrengths
(applicant_id, strength_id)
VALUES (%?, %?)
"""

insert_applicantsweaknesses_query = """
INSERT INTO applicantsweaknesses
(applicant_id, weakness_id)
VALUES (%?, %?)
"""

insert_spartans_query = """
INSERT INTO spartans
(spartan_id, applicant_id, course_id)
VALUES (%?, %?, %?)
"""

insert_tracker_query = """
INSERT INTO tracker
(spartan_id, coreskill_id, week, skill_value)
VALUES (%?, %?, %?, %?)
"""
