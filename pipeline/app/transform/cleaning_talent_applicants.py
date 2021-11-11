import datetime as dt
import pipeline.app.extract.csv_extractor as ext
import pprint as p
import pandas as pd
import re


class Applicants_Cleaner(ext.ApplicantsCsvExtractor):
    def __init__(self):
        super().__init__()
        self.__final_dict_applicants = {}
        self.__csv_talent_df = pd.DataFrame
        self.__unique_i_list = []
        self.__error_names = set()
        self.__error_phone_numbers = set()
        self.__error_degree = set()
        self.__error_inv_date = set()
        self.__error_id = set()
        self.__error_city = set()  # Change all of these at the end pls.
        self.__error_address = set()
        self.__error_postcode = set()
        self.__error_uni = set()
        self.__error_month = set()
        self.__error_name = set()

    @property
    def final_dict_applicants(self):
        return self.__final_dict_applicants

    @property
    def csv_talent_df(self):
        return self.__csv_talent_df

    @property
    def unique_i_list(self):
        return self.__unique_i_list

    @property
    def error_name_applicants(self):
        return self.__error_name

    @property
    def error_month_applicants(self):
        return self.__error_month

    @property
    def error_uni_applicants(self):
        return self.__error_uni

    @property
    def error_postcode_applicants(self):
        return self.__error_postcode

    @property
    def error_phone_numbers_applicants(self):
        return self.__error_phone_numbers

    @property
    def error_degree_applicants(self):
        return self.__error_degree

    @property
    def error_id_applicants(self):
        return self.__error_id

    @property
    def error_city_applicants(self):
        return self.__error_city

    @property
    def error_address_applicants(self):
        return self.__error_address

    def set_csv_talent_df(self, new_df: pd.DataFrame):
        self.__csv_talent_df = new_df

    def clean_applicants_id(self, raw_id: str) -> int or None:
        raw_id = str(raw_id)
        if raw_id.isnumeric():
            cleaned_id = int(raw_id)
            return cleaned_id
        else:
            self.__error_id.add(raw_id)
            return None

    # Returns a cleaned name as a tuple.
    @staticmethod
    def clean_applicants_name(name: str) -> tuple:
        name = name.title()
        if " " in name:
            first_name = name.split(" ", 1)[0]
            last_name = name.split(" ", 1)[1]
            return first_name, last_name
        else:
            return name, None

    # Returns a cleaned gender as a string or returns None.
    @staticmethod
    def clean_applicants_gender(gender: str) -> str or None:
        gender = str(gender)
        if gender == "nan":
            pass
        else:
            cleaned_gender = gender.title()
            if cleaned_gender.isalpha() and (cleaned_gender == "Male" or cleaned_gender == "Female"):
                return cleaned_gender
            else:
                return None

    # Returns dob as datetime or if no dob returns None.
    @staticmethod
    def clean_applicants_dob(dob: str) -> dt.datetime or None:
        dob = str(dob)
        if len(dob) > 3:
            date_dob = dt.datetime.strptime(dob, "%d/%m/%Y")
            cleaned_dob = date_dob
            return cleaned_dob
        else:
            return None

    # Returns a cleaned email as a string and checks to see if @ and . are present.
    @staticmethod
    def clean_applicants_email(email: str) -> str or None:
        cleaned_email = str(email)
        if "@" in cleaned_email and "." in cleaned_email:
            return cleaned_email
        else:
            return None

    # Cleans city and either outputs it as a cleaned string or None value. If None value adds to the error list.
    def clean_applicants_city(self, city: str) -> str or None:
        city = str(city)
        if city == "nan":
            pass
        else:
            if city.isalpha():
                return str(city).capitalize()
            elif len(city.replace(" ", "")) < 2:
                self.__error_city.add(city)
                return None
            else:
                self.__error_city.add(city)
                return None

    # Cleans the address and splits into house number and road name within a tuple.
    def clean_applicants_address(self, address: str) -> tuple or None:
        address = str(address)
        if address == "nan":
            pass
        else:
            no_spaces_address = address.replace(" ", "")
            if no_spaces_address.isalnum():
                number = address.split(" ", 1)[0]
                road_name = address.split(" ", 1)[1]
                if number.isnumeric() and road_name.replace(" ", "").isalpha():
                    address_tuple = (int(number), road_name)
                    return address_tuple
                elif road_name.replace(" ", "").isalpha():
                    non_alpha_road = (None, road_name)
                    self.__error_address.add(address)
                    return non_alpha_road
                elif number.isnumeric():
                    non_numeric_number = (int(number), None)
                    self.__error_address.add(address)
                    return non_numeric_number
            else:
                self.__error_address.add(address)
                error_tuple = (None, None)
                return error_tuple

    # Cleans the postcode. Produces are string or None. If None then appends to a list of error postcodes.
    def clean_applicants_postcode(self, postcode: str) -> str or None:
        postcode = str(postcode)
        if postcode == "nan":
            pass
        else:
            clean_postcode = postcode.replace(" ", "").upper()
            if clean_postcode.isalnum():
                if clean_postcode[0].isnumeric():
                    self.__error_postcode.add(postcode)
                    return None
                else:
                    return clean_postcode
            else:
                self.__error_postcode.add(postcode)
                return None

    # Cleans the phone number column. Produces a string of the number or None. If None then appends to an error list.
    def clean_applicants_phone_number(self, phone_number: str) -> str or None:
        no_spaces = str(phone_number).replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
        if len(no_spaces) == 13:
            return no_spaces
        else:
            self.__error_phone_numbers.add(no_spaces)
            return None

    # Cleans the university name column. Returns a string or None. If None then adds to a error set.
    def clean_applicants_uni(self, uni: str) -> str or None:
        uni = str(uni)
        if uni == "nan":
            pass
        else:
            no_spaces_uni = uni.replace(' ', "")
            if no_spaces_uni.isalpha() and uni.count(" ") > 1:
                clean_uni = uni.split()
                start = clean_uni[0].title()
                middle = clean_uni[1].lower()
                end = clean_uni[2].title()
                uni_name = start + " " + middle + " " + end
                return uni_name
            elif no_spaces_uni.isalpha():
                clean_uni = uni.split()
                start = clean_uni[0].title()
                end = clean_uni[1].title()
                uni_name = start + " " + end
                return uni_name
            else:
                self.__error_uni.add(uni)
                return None

    # Returns a degree classification as a string or return None. If none adds to a error set.
    def clean_applicants_degree(self, degree: str) -> str or None:
        degree = str(degree)
        if degree == "nan":
            pass
        else:
            lower_degree = degree.lower()
            dictionary_of_grades = {"two one": "2:1", "two two": "2:2", "first": "1st", "third": "3rd",
                                    "2:2": "2:2", "1st": "1st", "3rd": "3rd"}
            if lower_degree in dictionary_of_grades.keys():
                return dictionary_of_grades[lower_degree]
            else:
                self.__error_degree.add(degree)
                return None

    # Returns a invited date as a int or None. If None then adds to an error set.
    def clean_applicants_invited_date(self, invited_date: str, filename=None) -> int or None:
        invited_date = str(invited_date)
        invited_date = invited_date.split(".")[0]
        if invited_date == "" or invited_date == "None":
            return None
        else:
            if invited_date.isdigit():
                cleaned_inv_date = int(float(invited_date))
                if 31 > cleaned_inv_date > 0:
                    return cleaned_inv_date
            else:
                self.__error_inv_date.add(invited_date)
                return None

    # Returns a date as datetime or returns None. If None then adds to an error set.
    def clean_applicants_month(self, month: str or None, filename=None) -> dt.datetime or None:
        month = str(month)
        if month == "" or month is None or month == "nan":
            month = filename.split("/")[1].split("Applicants")[0].split("2019")[0]
            month = month[:3]
            year = filename.split("/")[1].split("Applicants")[0].split("2")[1]
            month_year = str(month) + " " + "2" + str(year)
            datetime_month_year = dt.datetime.strptime(month_year, "%b %Y")
            date_month_year = datetime_month_year#.date()
            return date_month_year
        elif month.replace(" ", "").isalnum():
            month = month[:3] + " " + month[-4:]
            datetime_strp = dt.datetime.strptime(month, "%b %Y")
            date = datetime_strp#.date()
            return date
        else:
            self.__error_month.add(month)
            return None

    # Split by first name last name
    @staticmethod
    def clean_applicants_invited_by(invited_by: str) -> tuple or None:
        invited_by = str(invited_by)
        name = invited_by.title()
        if name == "nan":
            return None
        if " " in name:
            first_name = name.split(" ", 1)[0]
            last_name = name.split(" ", 1)[1]
            return first_name, last_name
        elif len(name) > 2:
            return name, None
        else:
            return None, None

    @staticmethod
    def applicants_key_generator(clean_applicants_name: tuple, clean_applicants_invited_date, clean_applicants_month):
        return clean_applicants_name[0].replace(" ", "") + clean_applicants_name[1].replace(" ", "") + \
               str(clean_applicants_invited_date) + str(clean_applicants_month.month) + \
               str(clean_applicants_month.year)

    @staticmethod
    def single_dict_maker_applicants(id, au_key, unique_key, name, gender, dob, email, city, address, postcode, phone_number,
                                     uni, degree, invited_date, month, invited_by):
        return {"id": id,
                "Academy Unique Key": au_key,
                "Unique Key": unique_key,
                "First Name": name[0],
                "Last Name": name[1],
                "Gender": gender,
                "DoB": dob,
                "Email": email,
                "City": city,
                "Address": address,
                "Postcode": postcode,
                "Phone Number": phone_number,
                "Uni": uni,
                "Degree": degree,
                "Invited Day": invited_date,
                "Month": month,
                "Invited By": invited_by}

    def final_dict_appender_applicants(self):
        applicant_dictionary = {}
        print(f"Beginning work on {len(list(self.applicants_keys))} keys.")
        for keys in self.applicants_keys[:10]:
            print(f"Currently loading file {keys}...")
            csv_body = self.single_csv(keys)
            for row in range(0, self.len_of_rows(csv_body)):
                applicant_id = self.clean_applicants_id(self.extract_applicants_id(csv_body, row))
                applicant_name = self.clean_applicants_name(self.extract_csv_name(csv_body, row))
                applicant_gender = self.clean_applicants_gender(self.extract_applicants_gender(csv_body, row))
                applicant_dob = self.clean_applicants_dob(self.extract_applicants_dob(csv_body, row))
                applicant_email = self.clean_applicants_email(self.extract_applicants_email(csv_body, row))
                applicant_city = self.clean_applicants_city(self.extract_applicants_city(csv_body, row))
                applicant_address = self.clean_applicants_address(self.extract_applicants_address(csv_body, row))
                applicant_postcode = self.clean_applicants_postcode(self.extract_applicants_postcode(csv_body, row))
                applicant_phone_number = self.clean_applicants_phone_number \
                    (self.extract_applicants_phone_number(csv_body, row))
                applicant_university = self.clean_applicants_uni(self.extract_applicants_university(csv_body, row))
                applicant_degree = self.clean_applicants_degree(self.extract_applicants_degree(csv_body, row))
                applicant_invited_date = self.clean_applicants_invited_date \
                    (self.extract_applicants_invited_date(csv_body, row), keys)
                applicant_month = self.clean_applicants_month(self.extract_applicants_month(csv_body, row), keys)
                applicant_invited_by = self.clean_applicants_invited_by(
                    self.extract_applicants_invited_by(csv_body, row))
                applicant_unique_key = self.applicants_key_generator(applicant_name, applicant_invited_date,
                                                                     applicant_month)
                academy_applicant_key = (applicant_name[0]+applicant_name[1]).replace(" ", "")

                if applicant_invited_by not in self.unique_i_list and\
                        not applicant_invited_by == ("Nan", None) and\
                        applicant_invited_by is not None:
                    self.unique_i_list.append(applicant_invited_by[0] +" "+ applicant_invited_by[1])

                applicant_dictionary[applicant_unique_key] = \
                    self.single_dict_maker_applicants(applicant_id,
                                                      academy_applicant_key,
                                                      applicant_unique_key,
                                                      applicant_name,
                                                      applicant_gender,
                                                      applicant_dob,
                                                      applicant_email,
                                                      applicant_city,
                                                      applicant_address,
                                                      applicant_postcode,
                                                      applicant_phone_number,
                                                      applicant_university,
                                                      applicant_degree,
                                                      applicant_invited_date,
                                                      applicant_month,
                                                      applicant_invited_by)
                print(f"File {keys} - {applicant_unique_key} loaded into dictionary.\n")
        return applicant_dictionary

    def populate_talent_csv_file(self):
        self.set_csv_talent_df(pd.DataFrame.from_dict(self.final_dict_appender_applicants()).transpose())


if __name__ == '__main__':
    test = Applicants_Cleaner()
    test.populate_talent_csv_file()
    pd.set_option('display.max_columns', None)
    p.pprint(test.csv_talent_df["Academy Unique Key"])
