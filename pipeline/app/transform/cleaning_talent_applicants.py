import datetime as dt
import pipeline.app.extract.csv_extractor as ext


class Applicants_Cleaner(ext.ApplicantsCsvExtractor):
    def __init__(self):
        super().__init__()
        self.__final_dict_applicants = {}
        self.__error_names = set()
        self.__error_phone_numbers = set()
        self.__error_degree = set()
        self.__error_inv_date = set()
        self.__error_id = set()
        self.__error_city = set()           # Change all of these at the end pls.
        self.__error_address = set()
        self.__error_postcode = set()
        self.__error_uni = set()

    @property
    def final_dict(self):
        return self.__final_dict_applicants

    @property
    def error_uni(self):
        return self.__error_uni
    @property
    def error_postcode(self):
        return self.__error_postcode

    @property
    def error_phone_numbers(self):
        return self.__error_phone_numbers

    @property
    def error_degree(self):
        return self.__error_degree

    @property
    def error_id(self):
        return self.__error_id

    @property
    def error_city(self):
        return self.__error_city

    @property
    def error_address(self):
        return self.__error_address

    def clean_id(self, raw_id: str) -> int or None:
        if raw_id.isdigit():
            cleaned_id = int(raw_id)
            return cleaned_id
        else:
            self.error_id.append(int(raw_id))
            return None

    @staticmethod
    def clean_name(name: str) -> tuple:
        pass

    @staticmethod
    def clean_gender(gender: str) -> str or None:
        cleaned_gender = gender.title()
        if cleaned_gender.isalpha() and (cleaned_gender == "Male" or cleaned_gender == "Female"):
            return cleaned_gender
        else:
            return None

    @staticmethod
    def clean_dob(dob: str) -> dt.datetime:
        cleaned_dob = dt.datetime.strptime(dob, "%d/%m/%y")
        return cleaned_dob

    # Do we want to add the optional stuff in?
    def clean_email(email: str) -> str or None:
        cleaned_email = str(email)
        if "@" in cleaned_email and "." in cleaned_email:
            return cleaned_email
        else:
            return None

    # Cleans city and either outputs it as a cleaned string or None value. If None value adds to the error list.
    def clean_city(self, city: str) -> str or None:
        if city.isalpha():
            return str(city).capitalize()
        else:
            self.__error_city.append(city)
            return None

    # Cleans the address and splits into house number and road name within a tuple.
    def clean_address(self, address: str) -> tuple or None:
        no_spaces_address = address.replace(" ", "")
        if no_spaces_address.isalnum():
            number = address.split(" ", 1)[0]
            road_name = address.split(" ", 1)[1]
            if number.isnumeric() and road_name.replace(" ", "").isalpha():
                address_tuple = (int(number), road_name)
                return address_tuple
            elif road_name.replace(" ", "").isalpha():
                non_alpha_road = (None, road_name)
                self.error_address.append(address)
                return non_alpha_road
            elif number.isnumeric():
                non_numeric_number = (int(number), None)
                self.error_address.append(address)
                return non_numeric_number
        else:
            self.error_address.append(address)
            error_tuple = (None, None)
            return error_tuple

    # Cleans the postcode. Produces are string or None. If None then appends to a list of error postcodes.
    def clean_postcode(self, postcode: str) -> str or None:
        clean_postcode = postcode.replace(" ", "").upper()
        if clean_postcode.isalnum():
            if clean_postcode[0].isnumeric():
                self.error_postcode.append(postcode)
                return None
            else:
                return clean_postcode
        else:
            self.error_postcode.append(postcode)
            return None

    # Cleans the phone number column. Produces a string of the number or None. If None then appends to an error list.
    def clean_phone_number(self, phone_number: str) -> str or None:
        no_spaces = str(phone_number).replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
        if len(no_spaces) == 13:
            return no_spaces
        else:
            self.__error_phone_numbers.append(no_spaces)
            return None

    # --------------------------------------------------------------------------------------------------
    def clean_uni(self, uni: str) -> str or None:
        no_spaces_uni = uni.replace(' ', "")
        if no_spaces_uni.isalpha():
            clean_uni = uni.split()
            start = clean_uni[0].title()
            middle = clean_uni[1].lower()
            end = clean_uni[2].title()
            uni_name = start + " " + middle + " " + end
            return uni_name
        else:
            self.error_uni.add(uni)
            return None

    def clean_degree(self, degree: str) -> str:
        if degree in ["1st", "2:1", "2:2", "3rd"]:
            return str(degree)
        else:
            self.__error_degree.append(str(degree))

    def clean_invited_date(self, invited_date: str) -> int:
        if int(invited_date) < 31 and int(invited_date) > 0:
            return int(invited_date)
        else:
            self.__error_inv_date.append(int(invited_date))

    def clean_month(month: str, filename=None) -> dt.datetime:
        pass

    # Split by first name last name
    def clean_invited_by(invited_by: str) -> tuple:
        pass

# Dictionary


if __name__ == '__main__':
    test = Applicants_Cleaner()
    print(test.clean_uni('University Of Leicester'))

