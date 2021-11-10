import pipeline.app.load.pre_load_formatter as plf

# TODO: Make sure this is actually taking in the populated databases

# TODO: Praise Jesus because we're uploading data with ApplicantID attatched
#  Or is it?
#  If it isn't, fix the junction tables by demanding FirstName, LastName, DOB be included in all tables with ApplicantID

# TODO: The Spartans and Tracker tables, probably by demanding that ApplicantID be included to allow for conversion


def convert_id_columns():
    x = plf.PreLoadFormatter()
    sparta_day_df = ci_app_sparta_day(x)
    course_trainer_jt_df = ci_course_trainer_jt(x)
    applicants_df = ci_applicants(x)
    app_sparta_day_df = ci_app_sparta_day(x)
    tech_self_score_jt_df = ci_tech_self_score_jt(x)
    app_strengths_jt_df = ci_app_strengths_jt(x)
    app_weaknesses_jt_df = ci_app_weaknesses_jt(x)
    spartans_df = ci_spartans(x)
    tracker_df = ci_tracker_jt(x)


def ci_sparta_day(x):
    academy_df = x.academy_df
    sparta_day_df = x.sparta_day_df
    sparta_day_df['AcademyID'] = sparta_day_df['AcademyID'].map(academy_df.set_index('AcademyName')['AcademyID'])
    return sparta_day_df


def ci_course_trainer_jt(x):
    course_trainer_jt_df = x.course_trainer_jt_df
    course_df = x.course_df
    trainer_df = x.trainer_df
    # course_trainer_jt_df['CourseID'] = course_trainer_jt_df['CourseID'].map(course_df.set_index('CourseName')['CourseID'])  # Also not totally sure
    course_trainer_jt_df = course_trainer_jt_df.merge(course_df, on=['CourseName', 'WeekLength', 'StartDate'], suffixes=['', '', '_2']).drop(['CourseName', 'WeekLength', 'StartDate'], axis=1).rename(columns={"id_2": "CourseID"})
    # course_trainer_jt_df['TrainerID'] = course_trainer_jt_df['TrainerID'].map(trainer_df.set_index('FirstName')['TrainerID'])
    course_trainer_jt_df = course_trainer_jt_df.merge(trainer_df, on=['FirstName', 'LastName'], suffixes=['', '_2']).drop(['FirstName', 'LastName'], axis=1).rename(columns={"id_2": "TrainerID"})
    return course_trainer_jt_df


def ci_applicants(x):
    applicants_df = x.applicants_df
    streams_df = x.streams_df
    invitors_df = x.invitors_df
    address_df = x.address_df
    applicants_df['StreamInterestID'] = applicants_df['StreamInterestID'].map(streams_df.set_index('StreamName')['StreamID'])
    # applicants_df['InvitedByID'] = applicants_df['InvitedByID'].map(invitors_df.set_index('FirstName')['InvitedByID'])
    applicants_df = applicants_df.merge(invitors_df, on=['FirstName', 'LastName'], suffixes=['', '_2']).drop(['FirstName', 'LastName'], axis=1).rename(columns={"id_2": "InvitorID"})
    # applicants_df['AddressID'] = applicants_df['AddressID'].map(address_df.set_index('HouseNumber')['AddressID'])
    applicants_df = applicants_df.merge(address_df, on=['HouseNumber', 'AddressLine', 'Postcode', 'City'], suffixes=['', '', '', '_2']).drop(['HouseNumber', 'AddressLine', 'Postcode', 'City'], axis=1).rename(columns={"id_2": "Address_id"})
    return applicants_df


def ci_app_sparta_day(x):
    app_sparta_day_df = x.app_sparta_day_df
    # applicants_df = x.applicants_df
    sparta_day_df = x.sparta_day_df
    # app_sparta_day_df['ApplicantID'] = app_sparta_day_df['ApplicantID'].map(applicants_df.set_index('matchingvalues')['ApplicantID'])
    # app_sparta_day_df = app_sparta_day_df.merge(applicants_df, on=['AcademyID', 'c2'], suffixes=['', '_2']).drop(['c1', 'c2'], axis=1).rename(columns={"id_2": "df2_id"})
    # app_sparta_day_df['SpartaDayID'] = app_sparta_day_df['SpartaDayID'].map(sparta_day_df.set_index('matchingvalues')['SpartaDayID'])
    app_sparta_day_df = app_sparta_day_df.merge(sparta_day_df, on=['AcademyID', 'SpartaDayDate'], suffixes=['', '_2']).drop(['AcademyID', 'SpartaDayDate'], axis=1).rename(columns={"id_2": "SpartaDayID"})
    return app_sparta_day_df


def ci_tech_self_score_jt(x):
    tech_skills_df = x.tech_skills_df
    tech_self_score_jt_df = x.tech_self_score_jt_df
    # applicants_df = x.applicants_df
    tech_self_score_jt_df['TechSkillID'] = tech_self_score_jt_df['TechSkillID'].map(tech_skills_df.set_index('SkillName')['TechSkillID'])
    # tech_self_score_jt_df['ApplicantID'] = tech_self_score_jt_df['ApplicantID'].map(applicants_df.set_index('matchingvalues')['ApplicantID'])
    return tech_self_score_jt_df


def ci_app_strengths_jt(x):
    app_strengths_jt_df = x.app_strengths_jt_df
    strengths_df = x.strengths_df
    # applicants_df = x.applicants_df
    app_strengths_jt_df['StrengthID'] = app_strengths_jt_df['StrengthID'].map(strengths_df.set_index('Strength')['StrengthID'])
    # app_strengths_jt_df['ApplicantID'] = app_strengths_jt_df['ApplicantID'].map(applicants_df.set_index('matchingvalues')['ApplicantID'])
    return app_strengths_jt_df


def ci_app_weaknesses_jt(x):
    app_weaknesses_jt_df = x.app_weakness_jt_df
    weaknesses_df = x.weakness_df
    # applicants_df = x.applicants_df
    app_weaknesses_jt_df['WeaknessID'] = app_weaknesses_jt_df['WeaknessID'].map(weaknesses_df.set_index('Weakness')['WeaknessID'])
    # app_weaknesses_jt_df['ApplicantID'] = app_weaknesses_jt_df['ApplicantID'].map(applicants_df.set_index('matchingvalues')['ApplicantID'])
    return app_weaknesses_jt_df


def ci_spartans(x):
    spartans_df = x.spartans_df
    course_df = x.course_df
    # applicants_df = x.applicants_df
    # spartans_df['CourseID'] = spartans_df['CourseID'].map(course_df.set_index('CourseName')['CourseID'])  # Also not totally sure
    spartans_df = spartans_df.merge(course_df, on=['CourseName', 'WeekLength', 'StartDate'],
                                                      suffixes=['', '', '_2']).drop(
        ['CourseName', 'WeekLength', 'StartDate'], axis=1).rename(columns={"id_2": "CourseID"})
    # spartans_df['ApplicantID'] = spartans_df['ApplicantID'].map(applicants_df.set_index('matchingvalues')['ApplicantID'])
    return spartans_df


def ci_tracker_jt(x):
    tracker_jt_df = x.tracker_jt_df
    spartans_df = x.spartans_df
    core_skills_df = x.core_skills_df
    # tracker_jt_df['SpartanID'] = tracker_jt_df['SpartanID'].map(spartans_df.set_index('matchingvalues')['SpartanID'])
    tracker_jt_df['CoreSkillID'] = tracker_jt_df['CoreSkillID'].map(core_skills_df.set_index('SkillName')['CoreSkillID'])
    return tracker_jt_df
