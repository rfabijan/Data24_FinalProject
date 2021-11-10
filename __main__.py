import pipeline.config_manager as conf
from pipeline.app.transform.cleaning_talent import JsonCleaner
from pipeline.app.transform.cleaning_academy_course import AcademyCleaner
from pipeline.app.transform.cleaning_talent_applicants import Applicants_Cleaner
from pipeline.app.transform.cleaning_talent_sparta_day import TxtCleaner
from pipeline.app.load.database_creator import DatabaseCreator

if __name__ == "__main__":

    print("Connecting to S3...")
    # Receiving data from S3 and cleaning
    clean_json = JsonCleaner()
    clean_academy = AcademyCleaner()
    clean_applicants = Applicants_Cleaner()
    clean_txt = TxtCleaner()
    print("CONNECTION ESTABLISHED!")

    print("\nFILE EXTRACTION!")
    json_keys = clean_json.extract_json_keys
    applicants_keys = clean_applicants.keys
    academy_keys = clean_academy.keys
    txt_keys = clean_txt.txt_keys


    # print(clean_json.extract_json_keys)
    # print(clean_academy.keys)
    # print(clean_applicants.keys)
    # print(clean_txt.)

    # Database creation stage
    # database = DatabaseCreator()
    # database.reset_database()
    # database.create_database()
    # database.run_script(conf.SQL_SCRIPT)


