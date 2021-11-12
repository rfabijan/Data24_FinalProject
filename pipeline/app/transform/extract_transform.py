import functools

from pipeline.app.transform.cleaning_talent_sparta_day import TxtCleaner
from pipeline.app.transform.cleaning_talent import JsonCleaner
from pipeline.app.transform.cleaning_academy_course import AcademyCleaner
from pipeline.app.transform.cleaning_talent_applicants import Applicants_Cleaner
import pandas as pd
import os


def write_dataframe_to_csv(dataframe: pd.DataFrame, csv_name: str):
    dataframe.to_csv(f'cleaned_csvs/{csv_name}.csv', index=False, header=True)


def print_progress(file_type: str, counter: int, total: int, key: str):
    print(f'Extracting: {file_type} \t Progress: {counter}/{total} ({round((counter / total) * 100, 2)}%) \t-\t {key}')
    return counter + 1


def extract_and_clean_all_txts_to_dataframe(read_limit=None):
    cleaner = TxtCleaner()  # The cleaner has extraction capabilities
    all_txts = list()  # A list of dictionaries
    keys = cleaner.txt_keys
    counter = 1  # A counter for console feedback

    for key in keys:
        # Check if loop should continue and print console feedback
        if read_limit and counter > read_limit:
            break
        counter = print_progress('TXT', counter, len(keys), key)

        txt_as_list = cleaner.pull_text_object_as_list(key)  # The TXT file as a Python list

        # Extract the columns
        date = cleaner.clean_txt_date(cleaner.extract_txt_date(txt_as_list))
        academy = cleaner.clean_txt_academy(cleaner.extract_txt_academy(txt_as_list))

        # Read the Applicants details
        for i in range(3, len(txt_as_list)):
            line = cleaner.extract_txt_name_line(txt_as_list, i)
            name = cleaner.clean_txt_name(cleaner.extract_txt_name_from_line(line))
            psychometrics = cleaner.clean_txt_psychometrics(cleaner.extract_txt_psychometric_from_line(line))
            presentation = cleaner.clean_txt_presentation(cleaner.extract_txt_presentation_from_line(line))

            # Populate the DataFrame/Dictionary/List
            all_txts.append({"Date": date, "Academy": academy, "FirstName": name[0], "LastName": name[1],
                             "Psychometrics": psychometrics, "Presentation": presentation})

    return pd.DataFrame(all_txts)


def extract_and_clean_all_jsons_to_dataframe(read_limit=None):
    cleaner = JsonCleaner()
    all_jsons = list()
    keys = cleaner.extract_json_keys
    counter = 1  # A counter for console feedback

    for key in keys:
        if read_limit and counter > read_limit:
            break
        counter = print_progress('JSON', counter, len(keys), key)

        # Extract the S3 JSON file as a Python dictionary and clean
        file = cleaner.pull_single_json(key=key)

        # Seperate name into two different fields
        file['name'] = cleaner.clean_json_name(file['name'])
        file['firstname'] = file['name'][0]
        file['lastname'] = file['name'][1]
        del file['name']

        # Capture the rest of the data
        file['date'] = cleaner.clean_json_date(file.get('date'))
        file['tech_self_score'] = cleaner.clean_json_tech_self_score(file.get('tech_self_score'))
        file['strengths'] = cleaner.clean_json_strengths(file.get('strengths'))
        file['weaknesses'] = cleaner.clean_json_weaknesses(file.get('weaknesses'))
        file['self_development'] = cleaner.clean_json_self_development(file.get('self_development'))
        file['geo_flex'] = cleaner.clean_json_geo_flex(file.get('geo_flex'))
        file['financial_support_self'] = cleaner.clean_json_financial_support_self(file.get('financial_support_self'))
        file['result'] = cleaner.clean_json_result(file.get('result'))
        file['course_interest'] = cleaner.clean_json_course_interest(file.get('course_interest'))

        all_jsons.append(file)  # Add this file to the complete list

    return pd.DataFrame(all_jsons)


def extract_and_clean_all_academy_csv_to_dataframe(read_limit=None):
    cleaner = AcademyCleaner()
    all_csvs = pd.DataFrame()
    keys = cleaner.keys
    counter = 1  # A counter for console feedback

    for key in keys:
        if read_limit and counter > read_limit:
            break
        counter = print_progress('CSV (Academy)', counter, len(keys), key)

        file = cleaner.single_csv(key)  # Store the CSV into a Dataframe
        nones = [None] * len(file)

        file['courselength'] = cleaner.extract_academies_weeks(list(file.columns))

        file['name'] = list(map(cleaner.clean_name, file.get('name', nones)))
        file['firstname'] = [item[0] for item in file['name']]
        file['lastname'] = [item[1] for item in file['name']]

        file['trainer'] = list(map(cleaner.clean_trainer, file.get('trainer', nones)))
        file['trainerfirstname'] = [item[0] for item in file['trainer']]
        file['trainerlastname'] = [item[1] for item in file['trainer']]
        file['course'] = cleaner.extract_academies_course_name(key)
        file['coursestartdate'] = cleaner.clean_course_start_date(cleaner.extract_academies_date(key))
        all_csvs = all_csvs.append(file)

    return all_csvs


def extract_and_clean_all_talent_csv_to_dataframe(read_limit=None):
    cleaner = Applicants_Cleaner()
    all_csvs = pd.DataFrame()
    keys = cleaner.applicants_keys
    counter = 1  # A counter for console feedback

    for key in keys:
        if read_limit and counter > read_limit:
            break
        counter = print_progress('CSV (Academy)', counter, len(keys), key)

        file = cleaner.single_csv(key)  # Store the CSV into a Dataframe
        nones = [None] * len(file)

        file['name'] = list(map(cleaner.clean_applicants_name, file.get('name', nones)))
        file['firstname'] = [item[0] for item in file['name']]
        file['lastname'] = [item[1] for item in file['name']]

        file['gender'] = list(map(cleaner.clean_applicants_gender, file.get('gender', nones)))
        file['dob'] = list(map(cleaner.clean_applicants_dob, file.get('dob', nones)))
        file['email'] = list(map(cleaner.clean_applicants_email, file.get('email', nones)))
        file['city'] = list(map(cleaner.clean_applicants_city, file.get('city', nones)))
        file['address'] = list(map(cleaner.clean_applicants_address, file.get('address', nones)))
        file['postcode'] = list(map(cleaner.clean_applicants_postcode, file.get('postcode', nones)))
        file['phone_number'] = list(map(cleaner.clean_applicants_phone_number, file.get('phone_number', nones)))
        file['uni'] = list(map(cleaner.clean_applicants_uni, file.get('uni', nones)))
        file['degree'] = list(map(cleaner.clean_applicants_degree, file.get('degree', nones)))
        file['invited_date'] = list(map(cleaner.clean_applicants_invited_date, file.get('invited_date', nones)))

        file['invited_by'] = list(map(cleaner.clean_applicants_invited_by, file.get('invited_by', nones)))
        file['invitorfirstname'] = [item[0] for item in file['invited_by']]
        file['invitorlastname'] = [item[1] for item in file['invited_by']]

        clean_month = functools.partial(cleaner.clean_applicants_month, filename=key)
        file['month'] = list(map(clean_month, file.get('month', nones)))

        all_csvs = all_csvs.append(file)

    return all_csvs


def create_dataframes_from_s3():
    txt_df = extract_and_clean_all_txts_to_dataframe()
    write_dataframe_to_csv(txt_df, "txt_sparta_days")

    json_df = extract_and_clean_all_jsons_to_dataframe()
    write_dataframe_to_csv(json_df, "json_talents")

    csv_df_acad = extract_and_clean_all_academy_csv_to_dataframe()
    write_dataframe_to_csv(csv_df_acad, "csv_academy")

    csv_df_talent = extract_and_clean_all_talent_csv_to_dataframe()
    write_dataframe_to_csv(csv_df_talent, "csv_talent")

    return txt_df, json_df, csv_df_acad, csv_df_talent


def dataframe_from_file(filepath, dataframe_from_function):
    try:
        return pd.read_csv(filepath)
    except FileNotFoundError:
        df = dataframe_from_function()
        write_dataframe_to_csv(df, (filepath.split('/')[-1]).split('.')[0])
        return df


def create_dataframes():
    root = 'cleaned_csvs'
    txt_df = dataframe_from_file(f'{root}/txt_sparta_days.csv', extract_and_clean_all_txts_to_dataframe)
    json_df = dataframe_from_file(f'{root}/json_talents.csv', extract_and_clean_all_jsons_to_dataframe)
    csv_df_acad = dataframe_from_file(f'{root}/csv_academy.csv', extract_and_clean_all_academy_csv_to_dataframe)
    csv_df_talent = dataframe_from_file(f'{root}/csv_talent.csv', extract_and_clean_all_talent_csv_to_dataframe)

    return txt_df, json_df, csv_df_acad, csv_df_talent


if __name__ == "__main__":
    # Create dataframe from CSVs if possible, else S3
    txt_df, json_df, csv_df_acad, csv_df_talent = create_dataframes()
