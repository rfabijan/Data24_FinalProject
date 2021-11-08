import pyodbc
import pipeline.config_manager as conf


# TODO: Validation on method parameters


def insert_data(insert_records_query, records_rows):  # Takes the SQL query and the table of data
    with data24etl_db.cursor() as cursor:
        cursor.executemany(insert_records_query, records_rows)  # Executes the query on each row in the table
        data24etl_db.commit()


"""
This bit connects to the database
"""
server = 'localhost,1433'
database = conf.DB_NAME
username = 'SA'
password = 'Passw0rd2018'

data24etl_db = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID='
                              + username + ';PWD=' + password)


# list_of_tables = ("techskill", "strengths", "weaknesses", "academy", "spartaday", "trainer", "course",
# "coursetrainer", "coreskills", "streams", "invitors", "addresses", "applicants", "applicantsspartaday",
# "techselfscore", "applicantsstrengths", "applicantsweaknesses", "spartans", "tracker")

# TODO: The number of columns in each table in the dictionary below needs to be reduced because it currently counts
#  ID columns, which aren't in the data tables
# Key = table name, Value = Number of columns
dict_of_tables = {"techskill": 2, "strengths": 2, "weaknesses": 2, "academy": 2, "spartaday": 3, "trainer": 3,
                  "course": 4, "coursetrainer": 2, "coreskills": 2, "streams": 2, "invitors": 3, "addresses": 5,
                  "applicants": 15, "applicantsspartaday": 4, "techselfscore": 3, "applicantsstrengths": 2,
                  "applicantsweaknesses": 2, "spartans": 3, "tracker": 4}

for item in dict_of_tables.items():  # Each item is a tuple of (Key, Value)
    tablename = item[0]
    number_of_columns = item[1]

    # These 2 lines make a string like, for example, "(?,?,?)" for a 3 column table.
    # This is the empty row allows the SQL query to execute the data into it
    column_string = "(" + ("?,"*number_of_columns)
    column_string = column_string[:-1] + ")"

    insert_query = f"""
    INSERT INTO {tablename}
    VALUES {column_string}
    """
    # TODO: Don't have any tables of data
    insert_data(insert_query, records_rows)

"""records_rows is a list of tuples (or lists, I don't think it matters), with each tuple being a row in the database"""
# records_rows = [(a, b),
#                 (a, b),
#                 (a, b),
#                 (a, b)]
