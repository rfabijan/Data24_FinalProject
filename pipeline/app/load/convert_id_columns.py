import pipeline.app.load.pre_load_formatter as plf

# TODO: Make sure this is actually taking in the populated databases

# TODO: Praise Jesus because we're uploading data with ApplicantID attatched
#  therefore all need to replace ApplicantID with a foreign key is unnecessary and only the commented-out ashes remain

# TODO: The Spartans and Tracker tables, probably by demanding that ApplicantID be included to allow for conversion


def convert_id_columns():
    x = plf.PreLoadFormatter()  # x instantiates the object with all the dataframes TODO: Make sure this is right

    # Runs all the functions below this one
    sparta_day_df = ci_app_sparta_day(x)
    course_trainer_jt_df = ci_course_trainer_jt(x)
    applicants_df = ci_applicants(x)
    app_sparta_day_df = ci_app_sparta_day(x)
    tech_self_score_jt_df = ci_tech_self_score_jt(x)
    app_strengths_jt_df = ci_app_strengths_jt(x)
    app_weaknesses_jt_df = ci_app_weaknesses_jt(x)
    spartans_df = ci_spartans(x)
    tracker_df = ci_tracker_jt(x)

    return sparta_day_df, course_trainer_jt_df, applicants_df, app_sparta_day_df, tech_self_score_jt_df, app_strengths_jt_df, app_weaknesses_jt_df, spartans_df, tracker_df


# TODO: Read the comments in these next 2 functions, because they should explain the rest


def ci_sparta_day(x):
    # Row like these get the dataframes
    academy_df = x.academy_df
    sparta_day_df = x.sparta_day_df

    # This map function takes a column called 'AcademyID' in sparta_day_df that is filled with Academy Names
    # It then replaces them with the Academy IDs of the corresponding Academy Names that they match in the Academy DF
    sparta_day_df['Academy'] = sparta_day_df['Academy'].map(academy_df.set_index('Academy')['index'])

    return sparta_day_df


def ci_course_trainer_jt(x):
    course_trainer_jt_df = x.course_trainer_jt_df
    course_df = x.course_df
    trainer_df = x.trainer_df

    # Here and below I have included the old map functions in many places where they have been replaced by merges,
    # because I am unsure if they work
    #
    # The merges assume there are X (e.g. 2) columns in the dataframe that should be replaced by an ID, but are useful
    # because they match the same columns in the table we're getting the ID from
    #
    # This is done through the merge, which is a kind of inner join 'on' the X columns, that we then delete after
    # adding the ID column from the other table

    # course_trainer_jt_df['CourseID'] = course_trainer_jt_df['CourseID'].map(course_df.set_index('Course Name')['CourseID'])
    course_trainer_jt_df = course_trainer_jt_df.merge(course_df, on=['Course Name'], suffixes=['_2']).drop(['Course Name'], axis=1).rename(columns={"id_2": "CourseID"})
    # course_trainer_jt_df['TrainerID'] = course_trainer_jt_df['TrainerID'].map(trainer_df.set_index('FirstName')['TrainerID'])
    course_trainer_jt_df = course_trainer_jt_df.merge(trainer_df, on=['First Name', 'Last Name'], suffixes=['', '_2']).drop(['First Name', 'Last Name'], axis=1).rename(columns={"id_2": "TrainerID"})
    return course_trainer_jt_df


def ci_applicants(x):
    applicants_df = x.applicants_df
    streams_df = x.streams_df
    invitors_df = x.invitors_df
    address_df = x.address_df
    applicants_df['Course_interest'] = applicants_df['Course_interest'].map(streams_df.set_index('Course_interest')['StreamID'])
    # applicants_df['InvitedByID'] = applicants_df['InvitedByID'].map(invitors_df.set_index('FirstName')['InvitedByID'])
    applicants_df = applicants_df.merge(invitors_df, on=['Invitors'], suffixes=['_2']).drop(['Invitors'], axis=1).rename(columns={"id_2": "InvitorID"})
    # applicants_df['AddressID'] = applicants_df['AddressID'].map(address_df.set_index('HouseNumber')['AddressID'])
    applicants_df = applicants_df.merge(address_df, on=['AddressLine', 'Postcode', 'City'], suffixes=['', '', '_2']).drop(['AddressLine', 'Postcode', 'City'], axis=1).rename(columns={"id_2": "Address_id"})
    return applicants_df


def ci_app_sparta_day(x):
    app_sparta_day_df = x.app_sparta_day_jt_df
    # applicants_df = x.applicants_df
    sparta_day_df = x.sparta_day_df
    # app_sparta_day_df['ApplicantID'] = app_sparta_day_df['ApplicantID'].map(applicants_df.set_index('matchingvalues')['ApplicantID'])
    # app_sparta_day_df = app_sparta_day_df.merge(applicants_df, on=['AcademyID', 'c2'], suffixes=['', '_2']).drop(['c1', 'c2'], axis=1).rename(columns={"id_2": "df2_id"})
    # app_sparta_day_df['SpartaDayID'] = app_sparta_day_df['SpartaDayID'].map(sparta_day_df.set_index('matchingvalues')['SpartaDayID'])
    app_sparta_day_df = app_sparta_day_df.merge(sparta_day_df, on=['Academy', 'Date'], suffixes=['', '_2']).drop(['Academy', 'Date'], axis=1).rename(columns={"id_2": "SpartaDayID"})
    return app_sparta_day_df


def ci_tech_self_score_jt(x):
    tech_skills_df = x.tech_skills_df
    tech_self_score_jt_df = x.tech_self_score_jt_df
    # applicants_df = x.applicants_df
    tech_self_score_jt_df['Tech Skills'] = tech_self_score_jt_df['Tech Skills'].map(tech_skills_df.set_index('Tech Score Topics')['index'])
    # tech_self_score_jt_df['ApplicantID'] = tech_self_score_jt_df['ApplicantID'].map(applicants_df.set_index('matchingvalues')['ApplicantID'])
    return tech_self_score_jt_df


def ci_app_strengths_jt(x):
    app_strengths_jt_df = x.app_strengths_jt_df
    strengths_df = x.strengths_df
    # applicants_df = x.applicants_df
    app_strengths_jt_df['Strengths'] = app_strengths_jt_df['Strengths'].map(strengths_df.set_index('Strengths')['index'])
    # app_strengths_jt_df['ApplicantID'] = app_strengths_jt_df['ApplicantID'].map(applicants_df.set_index('matchingvalues')['ApplicantID'])
    return app_strengths_jt_df


def ci_app_weaknesses_jt(x):
    app_weaknesses_jt_df = x.app_weakness_jt_df
    weaknesses_df = x.weakness_df
    # applicants_df = x.applicants_df
    app_weaknesses_jt_df['Weaknesses'] = app_weaknesses_jt_df['Weaknesses'].map(weaknesses_df.set_index('Weaknesses')['index'])
    # app_weaknesses_jt_df['ApplicantID'] = app_weaknesses_jt_df['ApplicantID'].map(applicants_df.set_index('matchingvalues')['ApplicantID'])
    return app_weaknesses_jt_df


def ci_spartans(x):
    spartans_df = x.spartans_df
    course_df = x.course_df
    # applicants_df = x.applicants_df
    # spartans_df['CourseID'] = spartans_df['CourseID'].map(course_df.set_index('CourseName')['CourseID'])
    spartans_df = spartans_df.merge(course_df, on=['Course Name'],
                                                      suffixes=['_2']).drop(
        ['Course Name'], axis=1).rename(columns={"id_2": "CourseID"})
    # spartans_df['ApplicantID'] = spartans_df['ApplicantID'].map(applicants_df.set_index('matchingvalues')['ApplicantID'])
    return spartans_df


def ci_tracker_jt(x):
    tracker_jt_df = x.tracker_jt_df
    spartans_df = x.spartans_df
    core_skills_df = x.core_skills_df
    # tracker_jt_df['SpartanID'] = tracker_jt_df['SpartanID'].map(spartans_df.set_index('matchingvalues')['SpartanID'])
    tracker_jt_df['Core Skill'] = tracker_jt_df['Core Skill'].map(core_skills_df.set_index('Core Skill')['index'])
    return tracker_jt_df

