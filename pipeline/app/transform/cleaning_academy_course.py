import pipeline.app.extract.csv_extractor as extractor
import pandas as pd


class AcademyCleaner(extractor.AcademiesCsvExtractor):
    def __init__(self):
        super(AcademyCleaner, self).__init__()
        self.__error_names = set()
        self.__error_trainer_names = set()

    @property
    def error_names(self):
        return self.__error_names

    @property
    def error_trainer_names(self):
        return self.__error_trainer_names

    def clean_name(self, name: str) -> tuple:
        name = name.title()
        if name.isalpha() == False:
            self.error_names.add(name)
        if name.count(" ") > 1 or "-" in name:
            self.error_names.add(name)
        if " " in name:
            space_index = name.index(" ")
            name_list = [name[0:space_index], name[space_index + 1:]]
            clean_name = name_list[0], name_list[1]
            return clean_name
        else:
            print(name)

    def clean_trainer(self, trainer_name: str) -> tuple:
        trainer_name = trainer_name.title()
        if trainer_name.isalpha() == False:
            self.error_names.add(trainer_name)
        if trainer_name.count(" ") > 1 or "-" in trainer_name:
            self.error_names.add(trainer_name)
        if " " in trainer_name:
            space_index = trainer_name.index(" ")
            name_list = [trainer_name[0:space_index], trainer_name[space_index + 1:]]
            return name_list[0], name_list[1]
        else:
            print(trainer_name)



    @staticmethod
    def create_unique_key(clean_name: tuple) -> str:
        unique = str(clean_name[0] + clean_name[1])
        return unique

    def set_csv_df(self, new_df: pd.DataFrame):
        self.__csv_df = new_df

    @staticmethod
    def single_csv_academies_dict(unique_key: str, name: tuple, trainer: tuple, skill_value: int) -> dict:
        return {"Unique Key": unique_key,
                "Name": name,
                "Trainer": trainer,
                "Skill Value": skill_value}

    def final_academy_csv_dict_appender(self):
        csv_dict = {}
        for keys in self.keys:
            csv_body = self.single_csv(keys)
            for row in range(0, self.len_of_rows(csv_body) + 1):
                name = self.clean_name(self.extract_csv_name(csv_body, 1))
                trainer = self.clean_trainer(self.extract_academies_trainer(csv_body, 1))
                skill_value = self.extract_academies_skill_values_per_person_per_week(csv_body)
                # file_name, column_name: str, row_number: int
                unique_key = self.create_unique_key(name)

                csv_dict[unique_key] = {"Unique Key": unique_key,
                                        "Name": name,
                                        "Trainer Name": trainer,
                                        "Skill Value": skill_value}

        self.set_csv_df(pd.DataFrame.from_dict(csv_dict).transpose())


if __name__ == "__main__":
    test = AcademyCleaner()
    print(test.final_academy_csv_dict_appender())


