import pyodbc
import pipeline.config_manager as conf
import urllib
import sqlalchemy
import pandas as pd
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
engine = sqlalchemy.create_engine(conn_str, pool_pre_ping=True)


def insert_data_df(df, tablename, connection, identity_insert_on_sql, identity_insert_off_sql):
    connection.execute(identity_insert_on_sql)  # Allows us to insert ID rows
    # df.to_sql(tablename, connection, if_exists='append', index=False, index_label=None, method='multi')
    for each_item in df.index:
        df.loc[each_item].to_sql(name=each_item, con=connection, if_exists='append', index=False)
    connection.execute(identity_insert_off_sql)  # Reverts the ID rows allowance to close the fabric of spacetime
    connection.commit()


def insert(df, tablename):
    """
    This bit connects to the database
    """

    server = 'localhost,1433'
    database = conf.DB_NAME
    username = 'SA'
    password = 'Passw0rd2018'

    # server = 'localhost,1433'
    # database = conf.DB_NAME
    # username = 'SA'
    # password = 'Passw0rd2018'
    #
    # data24etl_db = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID='
    #                               + username + ';PWD=' + password)

    identity_insert_on_sql = f"SET IDENTITY_INSERT {tablename} ON"
    identity_insert_off_sql = f"SET IDENTITY_INSERT {tablename} OFF"


    data24etl_db = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID='
                                  + username + ';PWD=' + password)


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

        identity_insert_on_sql = f"""SET IDENTITY_INSERT {tablename} ON"""
        identity_insert_off_sql = f"""SET IDENTITY_INSERT {tablename} OFF"""

        # TODO: Alternative dataframe function usage
        insert_data_df(df, tablename, data24etl_db, identity_insert_on_sql, identity_insert_off_sql)

"""
def insert_df(df: pd.DataFrame, tablename, cursor, db_name='Data24ETL'):
    values = ''
    for index, row in df.iterrows():
        inner_value = '('
        for column in row:
            inner_value += f"'{column}' "
        inner_value = inner_value.rstrip()
        inner_value = inner_value.replace(' ', ', ')
        inner_value += ') '
        values += inner_value

    values = values.rstrip()
    values = values.replace(') ', '), ')

    query = f"INSERT INTO [{db_name}].[dbo].[{tablename}] VALUES {values};"
    #print(query)
    cursor.execute(query)
"""


def insert_df(df: pd.DataFrame, tablename, cursor, db_name='Data24ETL'):
    for index, row in df.iterrows():
        inner_value = '('
        for column in row:
            inner_value += f"'{column}' "
        inner_value = inner_value.rstrip()
        inner_value = inner_value.replace(' ', ', ')
        inner_value += ') '
        query = f"""INSERT INTO [{db_name}].[dbo].[{tablename}] VALUES {inner_value};"""
        cursor.execute(query)

# if __name__ == "__main__":
#     df = pd.DataFrame({"AcademyName": ["London", "Birmingham"]})
#     insert_df(df=df, tablename='Academy', cursor=cursor, db_name='Data24ETL')
