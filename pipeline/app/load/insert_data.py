import pyodbc
import pipeline.config_manager as conf
import pipeline.app.load.convert_id_columns as cic
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

# AHMED - Using pyodbc
# import pipeline.config_manager as conf
# connection_config = f'DRIVER={{ODBC Driver 17 for SQL Server}};' \
#                     f'SERVER={conf.DB_SERVER};' \
#                     f'DATABASE=master;' \
#                     f'UID={conf.DB_USERNAME};' \
#                     f'PWD={conf.DB_PASSWORD}'
# db_connection = pyodbc.connect(connection_config, autocommit=True)
# cursor = db_connection.cursor()


# Insert an academy dataframe which should be a DF with a single column: AcademyName
def insert_academy_df(df, tablename, cursor, db_name='Data24ETL'):
    value = ''
    for academy in list(df['AcademyName']):
        value += f"('{academy}') "

    value = value.rstrip()
    value = value.replace(' ', ', ')

    query = f"""INSERT INTO [{db_name}].[dbo].[{tablename}] VALUES {value};"""
    print(query)
    cursor.execute(query)


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


if __name__ == "__main__":
    df = pd.DataFrame({"AcademyName": ["London", "Birmingham", "Manchester"], "Column2": [1, 2, 8], "Column3": [4, 5, 8]})
    insert_df(df=df, tablename='Academy', cursor=cursor, db_name='Data24ETL')


def insert_data_df(df, tablename, connection, identity_insert_on_sql, identity_insert_off_sql):
    connection.execute(identity_insert_on_sql)  # Allows us to insert ID rows
    for row in df:
        row.to_sql(tablename, connection, if_exists='append', index=False, index_label=None)
    df.to_sql(tablename, connection, if_exists='append', index=False, index_label=None, method='multi')
    connection.execute(identity_insert_off_sql)  # Reverts the ID rows allowance to close the fabric of spacetime
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

    identity_insert_on_sql = f"SET IDENTITY_INSERT {tablename} ON"
    identity_insert_off_sql = f"SET IDENTITY_INSERT {tablename} OFF"

    insert_data_df(df, tablename, engine, identity_insert_on_sql, identity_insert_off_sql)
