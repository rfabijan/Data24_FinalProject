import pyodbc
import pipeline.config_manager as conf
import pipeline.app.load.convert_id_columns as cic
import urllib
import sqlalchemy
# TODO: Validation on method parameters

driver = "ODBC Driver 17 for SQL Server"
database = conf.DB_NAME
password = 'Passw0rd2018'
server = 'localhost,1433'
username = 'SA'
params = urllib.parse.quote_plus(
    'Driver=%s;' % driver +
    'Server=tcp:%s,1433;' % server +
    'Database=%s;' % database +
    'Uid=%s;' % username +
    'Pwd={%s};' % password +
    'Encrypt=yes;' +
    'TrustServerCertificate=yes;' +
    'Connection Timeout=30;')
conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
engine = sqlalchemy.create_engine(conn_str)

def insert_data_df(df, tablename, connection, identity_insert_on_sql, identity_insert_off_sql):
    connection.cursor.execute(identity_insert_on_sql)  # Allows us to insert ID rows
    df.to_sql(tablename, connection, if_exists='append', index=False, index_label=None)
    connection.cursor.execute(identity_insert_off_sql)  # Reverts the ID rows allowance to close the fabric of spacetime
    connection.commit()


"""
list of datframes = []
list of table_names = [techskill, strengths, weaknesses, academy, spartaday, trainer,
                      course, coursetrainer, coreskills, streams, invitors, addresses,
                      applicants, applicantsspartaday, techselfscore, applicantsstrengths,
                      applicantsweaknesses, spartans, tracker
                      ]
                      
for i in range(19):
    insert(list_of_dataframes[i], list_of_tablenames[i])
"""


def insert(df, tablename):
    """
    This bit connects to the database
    """
    # server = 'localhost,1433'
    # database = conf.DB_NAME
    # username = 'SA'
    # password = 'Passw0rd2018'
    #
    # data24etl_db = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID='
    #                               + username + ';PWD=' + password)

    identity_insert_on_sql = f"""SET IDENTITY_INSERT {tablename} ON"""
    identity_insert_off_sql = f"""SET IDENTITY_INSERT {tablename} OFF"""

    insert_data_df(df, tablename, engine, identity_insert_on_sql, identity_insert_off_sql)

    # TODO: The number of columns in each table in the dictionary below needs to be reduced because it currently counts
    #  ID columns, which aren't in the data tables
    # Key = table name, Value = Number of columns
    # dict_of_tables = {"techskill": 2, "strengths": 2, "weaknesses": 2, "academy": 2, "spartaday": 3, "trainer": 3,
    #                   "course": 4, "coursetrainer": 2, "coreskills": 2, "streams": 2, "invitors": 3, "addresses": 5,
    #                   "applicants": 15, "applicantsspartaday": 4, "techselfscore": 3, "applicantsstrengths": 2,
    #                   "applicantsweaknesses": 2, "spartans": 3, "tracker": 4}
    #
    # for item in dict_of_tables.items():  # Each item is a tuple of (Key, Value)
    #     tablename = item[0]
    #     number_of_columns = item[1]
    #
    #     # These 2 lines make a string like, for example, "(?,?,?)" for a 3 column table.
    #     # This is the empty row allows the SQL query to execute the data into it
    #     column_string = "(" + ("?,"*number_of_columns)
    #     column_string = column_string[:-1] + ")"
    #
    #     identity_insert_on_sql = f"""SET IDENTITY_INSERT {tablename} ON"""
    #     identity_insert_off_sql = f"""SET IDENTITY_INSERT {tablename} OFF"""
    #
    #     # TODO: Alternative dataframe function usage
    #     insert_data_df(df, tablename, data24etl_db, identity_insert_on_sql, identity_insert_off_sql)


"""records_rows is a list of tuples (or lists, I don't think it matters), with each tuple being a row in the database"""
# records_rows = [(a, b),
#                 (a, b),
#                 (a, b),
#                 (a, b)]
