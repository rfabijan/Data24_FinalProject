import pipeline.config_manager as conf
from pipeline.app.load.database_creator import DatabaseCreator
from pipeline.app.load.pre_load_formatter import PreLoadFormatter
from pipeline.app.load.load import database_builder
from pipeline.app.load.insert_data import *
from pipeline.app.load.convert_id_columns import *

if __name__ == "__main__":

    database_builder()
