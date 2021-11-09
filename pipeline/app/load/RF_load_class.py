import pyodbc
import pipeline.config_manager as conf
from pipeline.app.transform.cleaning_talent import JsonCleaner
from pprint import pprint

class loader(JsonCleaner):
    def __init__(self):
        super().__init__()
        self.__server = 'localhost,1433'
        self.__database = conf.DB_NAME
        self.__username = 'SA'
        self.__password = 'Passw0rd2018'
        self.__data24etl_db = pyodbc.connect('DRIVER={SQL Server};SERVER=' + self.__server + ';DATABASE=' + self.__database
                                      + ';UID=' + self.__username + ';PWD=' + self.__password)
        self.cursor = self.__data24etl_db.cursor()


    def get_clean_data(self, json_file):
        return self.create_unique_dict_from_json(json_file)

    def



loader = loader()
file = loader.pull_single_json("Talent/10388.json")
data = loader.get_clean_data(file)
pprint(data)
