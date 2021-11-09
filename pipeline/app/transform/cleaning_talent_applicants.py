import datetime as dt
import pipeline.app.extract.csv_extractor as ext


class applicants_cleaner(ext.ApplicantsCsvExtractor):
    def __init__(self):
        super().__init__()
        self.__final_dict_applicants = {}
        self.__error_names = []
        self.__error_phone_numbers = []
        self.__error_degree = []
        self.__error_inv_date = []
        self.__error_id = []

    @property
    def final_dict(self):
        return self.__final_dict_applicants

    @property
    def error_phone_numbers(self):
        return self.__error_phone_numbers

    @property
    def error_degree(self):
        return self.__error_degree

    @property
    def error_id(self):
        return self.__error_id

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

    def clean_city(city: str) -> str:
        return str(city).capitalize()

    # Split the address
    def clean_address(address: str) -> tuple:
        pass

    def clean_postcode(postcode: str) -> str:
        pass

    def clean_phone_number(self, phone_number: str) -> str:
        no_spaces = str(phone_number).replace(" ", "")
        if len(no_spaces) == 13 or len(no_spaces) == 11:
            return no_spaces
        else:
            self.__error_phone_numbers.append(no_spaces)

    @staticmethod
    def clean_uni(uni: str) -> str:
        return str(uni)

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


if __name__ == '__main__':
    test = applicants_cleaner

    print(test.clean_gender("male"))
