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

if __name__ == "__main__":
    connection = pyodbc.connect(
        f'DRIVER={{ODBC Driver 17 for SQL Server}};' \
        f'SERVER=localhost,1433;' \
        f'DATABASE=master;' \
        f'UID={username};' \
        f'PWD={password};'
    )