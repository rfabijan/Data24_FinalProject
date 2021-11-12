import pyodbc
import pipeline.config_manager as conf

"""
This class is estabilishes connection with SQL Server by using pyodbc
First it connects to master database and creates a cursor to make a SQL querying possible
"""
class DatabaseCreator:
    def __init__(self):
        self.__server = conf.DB_SERVER
        self.__database = "master"
        self.__username = conf.DB_USERNAME
        self.__password = conf.DB_PASSWORD
        self.__data24etl_db = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER=' + self.__server
                                             + ';DATABASE=' + self.__database
                                             + ';UID=' + self.__username
                                             + ';PWD=' + self.__password, autocommit=True)
        self.cursor = self.__data24etl_db.cursor()

    # This method creates a database
    def create_database(self) -> bool:
        try:
            print("DATABASE CREATION IN PROGRESS ..... ")
            # Check if database does not exist on the server
            if not self.check_db_exists():
                self.cursor.execute(f'CREATE DATABASE [{conf.DB_NAME}];')
                print("DATABASE HAS BEEN CREATED SUCCESSFULLY")
            else:
                # If exists uses an existing database
                self.cursor.execute(f'USE [{conf.DB_NAME}];')
        except:
            return False
        return True

    # This method checks if database does not exist on the server
    def check_db_exists(self):
        return self.cursor.execute(f"SELECT * FROM sys.databases WHERE name = '{conf.DB_NAME}'").fetchone() is not None

    # This method remove existing database
    def reset_database(self):
        # Uses master database (prevents dropping active database)
        self.cursor.execute('USE master;')
        self.cursor.execute(f'DROP DATABASE IF EXISTS [{conf.DB_NAME}];')
        print("DATABASE HAS BEEN DELETED SUCCESSFULLY!")

    # ToDo: Add validation so tests pass
    def run_script(self, script_file_path=None):
        # Reads the script
        with open(script_file_path, 'r') as file:
            query = file.read()

        # Executes the sql query in load folder:
        self.cursor.execute(query)
        # Commits the executions of the script
        self.cursor.commit()



if __name__ == "__main__":
    database_connector = DatabaseCreator()
    database_connector.reset_database()
    database_connector.create_database()
    database_connector.run_script("database_creator.sql")
