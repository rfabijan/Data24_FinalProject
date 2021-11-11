import pyodbc
import pipeline.config_manager as conf
from pipeline.app.transform.cleaning_talent import JsonCleaner
from pipeline.app.transform.cleaning_talent_sparta_day import TxtCleaner
from pipeline.app.transform.cleaning_talent_applicants import applicants_cleaner
from pprint import pprint
import urllib

class loader():
    def __init__(self):
        # self.__server = 'localhost,1433'
        # self.__database = conf.DB_NAME
        # self.__username = 'SA'
        # self.__password = 'Passw0rd2018'
        # self.__data24etl_db = pyodbc.connect('DRIVER={SQL Server};SERVER=' + self.__server + ';DATABASE=' + self.__database
        #                               + ';UID=' + self.__username + ';PWD=' + self.__password)
        # self.cursor = self.__data24etl_db.cursor()
        self.driver = "ODBC Driver 17 for SQL Server"
        self.database = conf.DB_NAME
        self.password = 'Passw0rd2018'
        self.server = 'localhost,1433'
        self.username = 'SA'
        self.params = urllib.parse.quote_plus(
            'Driver=%s;' % self.driver +
            'Server=tcp:%s,1433;' % self.server +
            'Database=%s;' % self.database +
            'Uid=%s;' % self.username +
            'Pwd={%s};' % self.password +
            'Encrypt=yes;' +
            'TrustServerCertificate=yes;' +
            'Connection Timeout=30;')
        self.conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(self.params)
        self.json_object = JsonCleaner()
        self.txt_object = TxtCleaner()
        self.csv_object = applicants_cleaner()



    def find_match_files(self, json_file):
        unique_key_json = self.json_object.create_unique_dict_from_json(json_file).keys()
        csv_keys = self.csv_object.keys
        for csv_file in csv_keys:
            len_of_rows = self.csv_object.len_of_rows(csv_file)
            for row in range(1, len_of_rows):
                unique_key_csv = self.csv_object.final_dict(csv_file, row).keys()
                if unique_key_json in unique_key_csv:
                    print("FOUND A MATCH!")

    # def insert_applicants(self, json_file):
    #     unique_key_json = self.json_object.create_unique_dict_from_json(json_file).keys()
    #     csv_keys = self.csv_object.keys
    #     for csv_file in csv_keys:
    #         len_of_rows = self.csv_object.len_of_rows(csv_file)
    #         for row in range(1, len_of_rows):
    #             unique_key_csv = self.csv_object.final_dict(csv_file, row).keys()
    #             if unique_key_json in unique_key_csv:
    #                 print("FOUND A MATCH!")
        # unique_key = self.json_object.create_unique_key(name, date)
        # insert_querry = "INSERT INTO Academy VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"


    def insert_tech_self_score(self):
        insert_querry = "INSERT INTO TechSelfScore VALUES (?, ?)"

    def insert_strengths(self, ):
        insert_querry = "INSERT INTO TechSelfScore VALUES (?, ?)"

    def insert_weaknesses(self, ):
        insert_querry = "INSERT INTO TechSelfScore VALUES (?, ?)"




    def select_applicants(self, querry):
        self.cursor.execute(querry)
        row = self.cursor.fetchone()
        while row:
            print(row[0])
            row = self.cursor.fetchone()



loader = loader()
file = loader.pull_single_json("Talent/10388.json")
data = loader.get_clean_data(file)
pprint(loader.select_applicants("SELECT * FROM Academy"))
