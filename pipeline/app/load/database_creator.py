import pyodbc
import pipeline.config_manager as conf


def connect_to_database(server: str = conf.DB_SERVER, username: str = conf.DB_USERNAME,
                        password: str = conf.DB_PASSWORD,  db_name: str = 'master') -> pyodbc.Connection:
    connection_config = f'DRIVER={{SQL Server}};' \
                        f'SERVER={server};' \
                        f'DATABASE={db_name};' \
                        f'UID={username};' \
                        f'PWD={password};'
    return pyodbc.connect(connection_config, autocommit=True)


def create_database(cursor: pyodbc.Cursor, db_name: str = 'Data24ETL') -> bool:
    try:
        cursor.execute(f'CREATE DATABASE [{db_name}];')
    except:
        return False

    cursor.execute(f'USE [{db_name}];')
    return True


def check_db_exists(cursor, db_name):
    return cursor.execute(f"SELECT * FROM sys.databases WHERE name = '{db_name}'").fetchone() is not None


def reset_database(cursor, db_name):
    # Use master database (prevents dropping active database)
    cursor.execute('USE master;')
    cursor.execute(f'DROP DATABASE IF EXISTS [{db_name}];')
    create_database(cursor, db_name)

    return check_db_exists(cursor, db_name)


# ToDo: Add validation so tests pass
def run_script(cursor, script_file_path=None):
    # Read the script
    with open(script_file_path, 'r') as file:
        query = file.read()

    # Execute the query within:
    cursor.execute(query)


if __name__ == "__main__":
    cursor = connect_to_database('localhost,1433', 'SA', 'Passw0rd2018', 'master').cursor()
    print(f"Create Database: {create_database(cursor, 'Data24ETL')}")
    print(f"Reset Database: {reset_database(cursor, 'Data24ETL')}")

    print("Building Database using Script")
    run_script(cursor, 'database_creator.sql')
