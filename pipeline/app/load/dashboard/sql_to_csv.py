"""
Pre-req:
pip install xlsxwriter
"""

import pyodbc
import sqlalchemy
import pandas as pd
import pipeline.config_manager as conf
from pprint import pprint as pp

# 1. Connect to Database
connection_config = f'DRIVER={{ODBC Driver 17 for SQL Server}};' \
                    f'SERVER=localhost,1433;' \
                    f'DATABASE=master;' \
                    f'UID=SA;' \
                    f'PWD=Passw0rd2018;'
cnxn = pyodbc.connect(connection_config, autocommit=True)
engine = sqlalchemy.create_engine(
    f'mssql+pyodbc://{conf.DB_USERNAME}:{conf.DB_PASSWORD}@{conf.DB_HOST}:{conf.DB_PORT}'
    f'/master?driver={conf.DB_DRIVER}'
)
cursor = cnxn.cursor()


# 2. Create database from script
def extract_query_from_file(file_path):
    with open(file_path, 'r') as file:
        query_return = file.read()
    return query_return


# Execute the queries:
cursor.execute(extract_query_from_file('db_creator.sql'))
cursor.execute(extract_query_from_file('table_creator.sql'))
cursor.execute(extract_query_from_file('db_populator.sql'))
cursor.close()  # Connection must be closed to allow pandas to query db connection


# 3. SQL to CSV
# Helper function(s)
def tablename_to_dataframe(table_name: str):
    return pd.read_sql(sql=f'SELECT * FROM {table_name};', con=cnxn)


# 3.1. Get all the table names from the Database
query = "SELECT TABLE_NAME FROM [Data24ETLTest].[INFORMATION_SCHEMA].[TABLES] WHERE TABLE_TYPE = 'BASE TABLE'"
tables = pd.read_sql(sql=query, con=cnxn)['TABLE_NAME']

# Iterate through the tables and build the dataframe
xlsx_writer = pd.ExcelWriter('Database.xlsx', engine='xlsxwriter')
for table in tables:
    df = tablename_to_dataframe(table)
    df.to_excel(xlsx_writer, sheet_name=table, index=False)

xlsx_writer.save()


# 4. Business Questions
# 4.1. Which members of talent team are performing best?
