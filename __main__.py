import pipeline.config_manager as conf
from pipeline.app.load.database_creator import DatabaseCreator
from pipeline.app.load.pre_load_formatter import PreLoadFormatter
from pipeline.app.load.insert_data import *
from pipeline.app.load.convert_id_columns import *

if __name__ == "__main__":

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

    # Database creation stage
    database = DatabaseCreator()
    database.reset_database()
    database.create_database()
    database.run_script(conf.SQL_SCRIPT)

    insert(formatter.academy_df, "Academy")
    insert(formatter.address_df, "Addresses")
    insert(applicants_df, "Applicants")
    insert(app_sparta_day_df , "ApplicantSpartaDay")
    insert(app_strengths_jt_df , "ApplicantStrengths")
    insert(app_weaknesses_jt_df, "ApplicantWeaknesses")
    insert(formatter.core_skills_df, "CoreSkills")
    insert(formatter.course_df, "Course")
    insert(course_trainer_jt_df, "CourseTrainer")
    insert(formatter.invitors_df, "Invitors")
    insert(sparta_day_df, "SpartaDay")
    insert(spartans_df, "Spartans")
    insert(formatter.streams_df, "Streams")
    insert(formatter.strengths_df, "Strengths")
    insert(formatter.tech_skills_df, "TechSkill")
    insert(tech_self_score_jt_df, "TechSelfScore")
    insert(tracker_df, "Tracker")
    insert(formatter.trainer_df, "Trainer")
    insert(formatter.weakness_df, "Weaknesses")




    # print(clean_json.extract_json_keys)
    # print(clean_academy.keys)
    # print(clean_applicants.keys)
    # print(clean_txt.)



