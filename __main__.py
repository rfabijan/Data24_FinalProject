import pipeline.config_manager as conf
from pipeline.app.transform.cleaning_talent import JsonCleaner
from pipeline.app.transform.cleaning_academy_course import AcademyCleaner
from pipeline.app.transform.cleaning_talent_applicants import Applicants_Cleaner
from pipeline.app.transform.cleaning_talent_sparta_day import TxtCleaner
from pipeline.app.load.database_creator import DatabaseCreator

if __name__ == "__main__":

<<<<<<< Updated upstream
    print("Connecting to S3...")
    # Receiving data from S3 and cleaning
    clean_json = JsonCleaner()
    clean_academy = AcademyCleaner()
    clean_applicants = Applicants_Cleaner()
    clean_txt = TxtCleaner()
    print("CONNECTION ESTABLISHED!")
=======
    formatter = PreLoadFormatter()

    sparta_day_df = ci_app_sparta_day(formatter)
    course_trainer_jt_df = ci_course_trainer_jt(formatter)
    applicants_df = ci_applicants(formatter)
    app_sparta_day_df = ci_app_sparta_day(formatter)
    tech_self_score_jt_df = ci_tech_self_score_jt(formatter)
    app_strengths_jt_df = ci_app_strengths_jt(formatter)
    app_weaknesses_jt_df = ci_app_weaknesses_jt(formatter)
    spartans_df = ci_spartans(formatter)
    tracker_df = ci_tracker_jt(formatter)
    print(applicants_df.columns)

    # Database creation stage
    database = DatabaseCreator()
    database.reset_database()
    database.create_database()
    database.run_script(conf.SQL_SCRIPT)

    insert_df(formatter.academy_df, "Academy", database.cursor, conf.DB_NAME)
    insert_df(formatter.address_df, "Addresses", database.cursor, conf.DB_NAME)
    insert_df(applicants_df, "Applicants", database.cursor, conf.DB_NAME)
    insert_df(app_sparta_day_df , "ApplicantSpartaDay", database.cursor, conf.DB_NAME)
    insert_df(app_strengths_jt_df , "ApplicantStrengths", database.cursor, conf.DB_NAME)
    insert_df(app_weaknesses_jt_df, "ApplicantWeaknesses", database.cursor, conf.DB_NAME)
    insert_df(formatter.core_skills_df, "CoreSkills", database.cursor, conf.DB_NAME)
    insert_df(formatter.course_df, "Course", database.cursor, conf.DB_NAME)
    insert_df(course_trainer_jt_df, "CourseTrainer", database.cursor, conf.DB_NAME)
    insert_df(formatter.invitors_df, "Invitors", database.cursor, conf.DB_NAME)
    insert_df(sparta_day_df, "SpartaDay", database.cursor, conf.DB_NAME)
    insert_df(spartans_df, "Spartans", database.cursor, conf.DB_NAME)
    insert_df(formatter.streams_df, "Streams", database.cursor, conf.DB_NAME)
    insert_df(formatter.strengths_df, "Strengths", database.cursor, conf.DB_NAME)
    insert_df(formatter.tech_skills_df, "TechSkill", database.cursor, conf.DB_NAME)
    insert_df(tech_self_score_jt_df, "TechSelfScore", database.cursor, conf.DB_NAME)
    insert_df(tracker_df, "Tracker", database.cursor, conf.DB_NAME)
    insert_df(formatter.trainer_df, "Trainer", database.cursor, conf.DB_NAME)
    insert_df(formatter.weakness_df, "Weaknesses", database.cursor, conf.DB_NAME)
>>>>>>> Stashed changes

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


