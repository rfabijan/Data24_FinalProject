"""
Pre-req:
pip install xlsxwriter

Dashboard preparer
"""

import pyodbc
import pandas as pd
from pprint import pprint as pp


def connect_to_database():
    connection_config = f'DRIVER={{ODBC Driver 17 for SQL Server}};' \
                        f'SERVER=localhost,1433;' \
                        f'DATABASE=master;' \
                        f'UID=SA;' \
                        f'PWD=Passw0rd2018;'
    return pyodbc.connect(connection_config, autocommit=True)


def extract_query_from_file(file_path):
    with open(file_path, 'r') as file:
        query_return = file.read()
    return query_return


def build_database(cursor):
    cursor.execute(extract_query_from_file('sql_scripts/db_creator.sql'))
    cursor.execute(extract_query_from_file('sql_scripts/table_creator.sql'))
    cursor.execute(extract_query_from_file('sql_scripts/db_populator.sql'))

    # Create the Views that will assist querying:
    views_creator_query = extract_query_from_file('sql_scripts/views_creator.sql')
    for subquery in views_creator_query.split('--\n'):
        cursor.execute(subquery)


def table_to_dataframe(from_sql: str, con: pyodbc.Connection):  # from_sql should be a Table or View name
    return pd.read_sql(sql=f'SELECT * FROM {from_sql};', con=con)


def views_to_workbook(views: list, workbook_name: str, con: pyodbc.Connection):
    xlsx_writer = pd.ExcelWriter(f'excel/sourcedata/{workbook_name}.xlsx', engine='xlsxwriter')

    for view in views:
        table_to_dataframe(view, con).to_excel(xlsx_writer, sheet_name=view, index=False)

    xlsx_writer.save()


if __name__ == "__main__":
    # 1. Establish Database Connection:
    cnxn = connect_to_database()
    cursor = cnxn.cursor()

    # 2. Build the database:
    build_database(cursor)

    # 3. Close the cursor - queries will now be handled by pandas
    cursor.close()

    # 4. Answering Business Questions:
    # 4.1. Which members of talent team are performing best?
    views = ['SpartansCompleteTrackerSummary']
    views_to_workbook(views=views, workbook_name='q1_Best_Talent', con=cnxn)

    # 4.2. How many trainees are removed at each stage of the course?
    views = ['ExpectedSpartansPerWeek', 'ActualSpartansPerWeek']
    views_to_workbook(views=views, workbook_name='q2_Trainees_Removed_Stage', con=cnxn)

    # 4.3. What factors influence removal?
    views = ['SpartansRemoved', 'SpartansRemovedStrength', 'SpartansRemovedTechSkills', 'SpartansRemovedTestScores']
    views_to_workbook(views=views, workbook_name='q3_Factors_Trainees_Removed', con=cnxn)

    # 4.4. What factors at the interview stage predict high performance on the course?
    views = ['SpartansComplete', 'SpartansCompleteStrength', 'SpartansCompleteTechSkills', 'SpartansCompleteTestScores']
    views_to_workbook(views=views, workbook_name='q4_Factors_Trainees_Complete', con=cnxn)

    # 4.5. How are courses on a given date performing?

