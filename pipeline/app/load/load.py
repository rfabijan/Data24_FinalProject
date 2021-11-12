from pipeline.app.transform.extract_transform import create_dataframes, write_dataframe_to_csv
import pyodbc
import pandas as pd
import pipeline.config_manager as conf
import ast
from pprint import pprint as pp

DBNAME = "Data24ETLTest"


def connect_to_database():
    connection_config = f'DRIVER={{ODBC Driver 17 for SQL Server}};' \
                        f'SERVER={conf.DB_HOST};' \
                        f'DATABASE=master;' \
                        f'UID={conf.DB_USERNAME};' \
                        f'PWD={conf.DB_PASSWORD};'
    return pyodbc.connect(connection_config, autocommit=True)


def extract_query_from_file(file_path):
    with open(file_path, 'r') as file:
        query_return = file.read()
    return query_return


def build_database(cursor):
    cursor.execute(extract_query_from_file('sql_scripts/db_creator.sql'))
    cursor.execute(extract_query_from_file('sql_scripts/table_creator.sql'))


def print_query_number(table, count, total):
    print(f'Inserting into {table}: {count}/{total} ({round((count / total) * 100, 2)}%)')
    return count + 1


def pandas_string_to_set(series: pd.Series):
    return_set = set()
    for item in series:
        list(map(return_set.add, ast.literal_eval(item)))
    return return_set


def generate_key_from_name_date(firstname: pd.Series, lastname: pd.Series, date_col: pd.Series):
    date_split = date_col.str.split('-', expand=True)
    return firstname + lastname + date_split[0] + date_split[1]


def generate_key_from_df(df: pd.DataFrame, date_col: str):
    return generate_key_from_name_date(df['firstname'], df['lastname'], df[date_col])


def insert_df(df: pd.DataFrame, tablename, cursor, db_name='Data24ETLTest'):
    for index, row in df.iterrows():
        inner_value = '('
        for column in row:
            inner_value += f"'{column}'   "
        inner_value = inner_value.rstrip()
        inner_value = inner_value.replace('   ', ', ')
        inner_value += ') '

        query = f"""INSERT INTO [{db_name}].[dbo].[{tablename}] VALUES {inner_value};"""
        cursor.execute(query)


def insert_into_academies(all_academies: pd.Series, db_name=DBNAME):
    for academy in all_academies:
        query = f"INSERT INTO [{DBNAME}].[dbo].[Academy] VALUES ('{academy}');"
        cursor.execute(query)


def insert_into_sparta_day(df: pd.DataFrame, db_name=DBNAME):
    # Loop through the dataframe
    for index, row in df.iterrows():
        query = f"SELECT AcademyID FROM [{DBNAME}].[dbo].[Academy] WHERE AcademyName='{row['Academy']}'"
        acad_id = cursor.execute(query).fetchone()[0]

        query = f"INSERT INTO [{DBNAME}].[dbo].[SpartaDay] VALUES ({acad_id} , '{row['Date']}')"
        cursor.execute(query)


def insert_into_streams(series: pd.Series, db_name=DBNAME):
    for course in series:
        query = f"INSERT INTO [{DBNAME}].[dbo].[Streams] VALUES ('{course}')"
        cursor.execute(query)


def insert_into_addresses(df: pd.DataFrame, db_name=DBNAME):
    counter = 0
    for index, row in df.iterrows():
        counter = print_query_number('Address', counter, len(df))
        query = f"INSERT INTO [{DBNAME}].[dbo].[Addresses] " \
                f"VALUES ('{row['address']}', '{row['postcode']}', '{row['city']}')"
        cursor.execute(query)


def insert_into_invitors(df: pd.DataFrame, db_name=DBNAME):
    counter = 0
    # Loop through the dataframe
    for index, row in df.iterrows():
        counter = print_query_number('Invitors', counter, len(df))
        query = f"INSERT INTO [{DBNAME}].[dbo].[Invitors] VALUES ('{row['invitorfirstname']}','{row['invitorlastname']}')"
        cursor.execute(query)


def insert_into_trainers(df: pd.DataFrame, db_name=DBNAME):
    counter = 0
    # Loop through the dataframe
    for index, row in df.iterrows():
        counter = print_query_number('Invitors', counter, len(df))
        query = f"INSERT INTO [{DBNAME}].[dbo].[Trainer] " \
                f"VALUES ('{row['trainerfirstname']}','{row['trainerlastname']}')"
        cursor.execute(query)


def insert_into_core_skills(db_name=DBNAME):
    query = f"INSERT INTO [{DBNAME}].[dbo].[CoreSkills] VALUES ('Analytic')," \
            f" ('Independent'),('Determined'),('Professional'),('Studios'),('Imaginative')"
    cursor.execute(query)


def insert_into_course(df: pd.DataFrame, db_name=DBNAME):
    counter = 0
    # Loop through the dataframe
    for index, row in df.iterrows():
        counter = print_query_number('Course', counter, len(df))
        query = f"INSERT INTO [{DBNAME}].[dbo].[Course] " \
                f"VALUES ('{row['course']}',{row['courselength']}, '{row['coursestartdate']}')"
        cursor.execute(query)


def insert_into_course_trainer(df: pd.DataFrame, db_name=DBNAME):
    counter = 0
    # Loop through the dataframe
    for index, row in df.iterrows():
        counter = print_query_number('Course', counter, len(df))

        query = f"SELECT CourseID FROM [{DBNAME}].[dbo].[Course] WHERE CourseName='{row['course']}'"
        course_id = cursor.execute(query).fetchone()[0]

        query = f"SELECT TrainerID FROM [{DBNAME}].[dbo].[Trainer] WHERE FirstName='{row['trainerfirstname']}' AND LastName='{row['trainerlastname']}'"
        trainer_id = cursor.execute(query).fetchone()[0]

        query = f"INSERT INTO [{DBNAME}].[dbo].[CourseTrainer] " \
                f"VALUES ({course_id}, {trainer_id})"
        cursor.execute(query)


def insert_into_weaknesses(weaknesses: set, db_name=DBNAME):
    counter = 0
    for w in weaknesses:
        counter = print_query_number('Weaknesses', counter, len(weaknesses))
        query = f"INSERT INTO [{DBNAME}].[dbo].[Weaknesses] VALUES ('{w}')"
        cursor.execute(query)


def insert_into_strengths(strengths: set, db_name=DBNAME):
    counter = 0
    for s in strengths:
        counter = print_query_number('Strengths', counter, len(strengths))
        query = f"INSERT INTO [{DBNAME}].[dbo].[Strengths] VALUES ('{s}')"
        cursor.execute(query)


def insert_into_techskills(tech_skills: set, db_name=DBNAME):
    counter = 0
    for ts in tech_skills:
        counter = print_query_number('Tech Skill', counter, len(tech_skills))
        query = f"INSERT INTO [{DBNAME}].[dbo].[TechSkill] VALUES ('{ts}')"
        cursor.execute(query)


def insert_into_applicants(df: pd.DataFrame, db_name=DBNAME):
    counter = 0
    for index, row in df.iterrows():
        counter = print_query_number('Applicants', counter, len(df))

        # Foreign Keys
        stream_id = invitor_id = address_id = None

        # Applicant may not have attended Sparta Day, we do not have all the information required
        if row['course_interest'] is not None:
            query = f"SELECT StreamID FROM [{DBNAME}].[dbo].[Streams] WHERE StreamName='{row['course_interest']}'"
            stream_id = cursor.execute(query).fetchone()[0]

        if row['invitorlastname'] is not None:
            query = f"SELECT InvitorID FROM [{DBNAME}].[dbo].[Invitors] " \
                    f"WHERE FirstName='{row['invitorfirstname']}' AND LastName='{row['invitorlastname']}'"
            invitor_id = cursor.execute(query).fetchone()[0]

        # TODO: Remove
        row['postcode'] = None

        if row['postcode'] is not None:
            query = f"SELECT AddressID FROM [{DBNAME}].[dbo].[Addresses] " \
                    f"WHERE AddressLine='{row['address']}' AND Postcode='{row['postcode']}'"
            # address_id = cursor.execute(query).fetchone()[0]

        phone_number = '+' + str(row['phone_number']).split('.')[0]
        result = 0 if row['result'] is None else row['result']

        query = f"INSERT INTO [{DBNAME}].[dbo].[Applicants] VALUES ({', '.join('?' * 15)})"
        params = [row['key'], stream_id, invitor_id, address_id, row['firstname_csv'], row['lastname_csv'],
                  row['gender'], row['dob'], row['email'], phone_number, row['uni'], row['degree'], row['geo_flex'],
                  row['financial_support_self'], result]

        cursor.execute(query, params)


def insert_into_spartans(df: pd.DataFrame, db_name=DBNAME):
    counter = 0
    applicant_ids = pd.Series()
    for index, row in df.iterrows():
        counter = print_query_number('Spartans', counter, len(df))

        query = f"SELECT CourseID FROM [{DBNAME}].[dbo].[Course] WHERE CourseName='{row['course']}'"
        course_id = cursor.execute(query).fetchone()[0]

        try:
            query = f"SELECT ApplicantID FROM [{DBNAME}].[dbo].[Applicants] WHERE " \
                    f"FirstName='{row['firstname']}' AND LastName='{row['lastname']}'"
            applicant_id = cursor.execute(query).fetchone()[0]
            applicant_ids.append(applicant_id)
        except:
            applicant_id = 'AshHannon201904'

        query = f"INSERT INTO [{DBNAME}].[dbo].[Spartans] " \
                f"VALUES ('{applicant_id}', '{course_id}')"
        cursor.execute(query)

    df['ApplicantID'] = applicant_ids
    return df


def insert_into_tracker(df: pd.DataFrame, db_name=DBNAME):
    counter = 0
    for index, row in df.iterrows():
        counter = print_query_number('Spartans', counter, len(df))

        row.dropna(inplace=True)
        print(row)
        break


# if __name__ == "__main__":
def database_builder():
    txt_df, json_df, csv_df_acad, csv_df_talent = create_dataframes()

    # 1. Connect to and build the Database schema
    cnxn = connect_to_database()
    cursor = cnxn.cursor()
    build_database(cursor)

    # 2. Start populating tables
    academies = txt_df['Academy'].unique()

    # 2.2. SpartaDay
    sparta_days = txt_df[['Date', 'Academy']].drop_duplicates()

    # 2.3. Streams
    streams_series = json_df['course_interest'].drop_duplicates()

    # 2.4. Addresses
    address_df = csv_df_talent[['address', 'postcode', 'city']].drop_duplicates()

    # 2.5. Invitors
    invitors_df = csv_df_talent[["invitorfirstname", "invitorlastname"]].drop_duplicates()

    # 2.6 Trainers
    trainers_df = csv_df_acad[["trainerfirstname", "trainerlastname"]].drop_duplicates()

    # 2.7. Weaknesses
    weaknesses_set = pandas_string_to_set(series=json_df['weaknesses'])

    # 2.8. Strengths
    strength_set = pandas_string_to_set(series=json_df['strengths'])

    # 2.9. Tech Skills
    tech_skills_set = set()
    for skill in json_df['tech_self_score']:
        try:
            res = ast.literal_eval(skill)
            for key in res.keys():
                tech_skills_set.add(key)
        except ValueError:
            # String is empty so cant convert to dict
            continue

    # 2.10. Core Skills
    # Values entered manually

    # 2.11. Course
    course_df = csv_df_acad[["course", "courselength", "coursestartdate"]].drop_duplicates()

    # 2.12. CourseTrainer (JT)
    course_trainer_df = csv_df_acad[["trainerfirstname", "trainerlastname", "course"]].drop_duplicates()

    # 2.13. Applicants
    csv_df_talent['key'] = generate_key_from_df(csv_df_talent, 'month')
    json_df['key'] = generate_key_from_df(json_df, 'date')
    applicants_df = csv_df_talent.join(json_df.set_index('key'), on='key', lsuffix='_csv', rsuffix='_json')
    applicants_df.drop_duplicates(subset='key', keep="last", inplace=True)
    applicants_df = applicants_df.where(pd.notnull(applicants_df), None)  # Replace NaN with None

    # 2.14. Spartans
    spartans_df = csv_df_acad[["firstname", "lastname", "course"]].drop_duplicates()

    # 2.15. Tracker
    csv_df_acad['key'] = csv_df_acad['firstname'] + csv_df_acad['lastname'] + csv_df_acad['course']
    spartans_df['key'] = spartans_df['firstname'] + spartans_df['lastname'] + spartans_df['course']

    # 3. Inserts
    insert_into_academies(academies)
    insert_into_sparta_day(df=sparta_days)
    insert_into_streams(streams_series)
    insert_into_invitors(df=invitors_df)
    insert_into_weaknesses(weaknesses_set)
    insert_into_strengths(strength_set)
    insert_into_techskills(tech_skills_set)
    insert_into_trainers(trainers_df)
    insert_into_course(course_df)
    insert_into_course_trainer(course_trainer_df)
    insert_into_core_skills()
    insert_into_addresses(address_df)
    insert_into_applicants(applicants_df)
