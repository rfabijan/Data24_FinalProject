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
    print("JSON file extraction ...")
    for json in json_keys:
        file = clean_json.pull_single_json(json)
    print("JSON file extraction done!\n")

    applicants_keys = clean_applicants.keys
    print("Applicants CSV file extraction ...")
    for app in applicants_keys:
        file = clean_applicants.single_csv(app)
    print("Applicants CSV file extraction done!\n")

    academy_keys = clean_academy.keys
    print("Academy CSV file extraction ...")
    for academy in applicants_keys:
        file = clean_academy.single_csv(academy)
    print("Academy CSV file extraction done!\n")

    txt_keys = clean_txt.txt_keys
    print("TXT file extraction ...")
    for txt in txt_keys:
        file = clean_txt.pull_text_object_as_list(txt)
    print("TXT file extraction done!\n")

    # Database creation stage
    database = DatabaseCreator()
    database.reset_database()
    database.create_database()
    database.run_script(conf.SQL_SCRIPT)

    # print(clean_json.extract_json_keys)
    # print(clean_academy.keys)
    # print(clean_applicants.keys)
    # print(clean_txt.)



