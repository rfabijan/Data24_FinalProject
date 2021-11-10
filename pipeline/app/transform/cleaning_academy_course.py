import pipeline.app.extract.csv_extractor as extractor


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
            return name_list[0], name_list[1]
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
    def clean_skill_value(score: int) -> int or None:
        if score in range(1, 9):
            return score

        else:
            return None


if __name__ == "__main__":
    test = AcademyCleaner()
    print(test.clean_name("Rossie Caitlin"))



# def clean_name(name):
#     name_errors = []
#     name = name.title()                                                     # capitalises the first letter of each word
#
#     if name.count(" ") > 1 or "-" in name:                                  # checks for multiple spaces and hyphens
#         name_errors.append(name)                                            # adds names with errors to the error list
#         return f"These names contain errors: {name_errors}"                 # returns the names with errors
#
#     if " " in name:                                                         # checks for a space in the name
#         space_index = name.index(" ")                                       # gets the index number of the space
#         list_of_names = [name[0: space_index], name[space_index + 1:]]      # separates first and last name
#         return list_of_names[0], list_of_names[1]                           # returns first and last name as a tuple
#
#     else:
#         return name
#
#
# def clean_trainer(trainer_name):
#     trainer_name_errors = []
#     trainer_name = trainer_name.title()
#
#     if trainer_name.count(" ") > 1 or "-" in trainer_name:
#         trainer_name_errors.append(trainer_name)
#         return f"These names contain errors: {trainer_name_errors}"
#
#     if " " in trainer_name:
#         space_index = trainer_name.index(" ")
#         list_of_trainer_names = [trainer_name[0: space_index], trainer_name[space_index + 1:]]
#         return list_of_trainer_names[0], list_of_trainer_names[1]
#
#     else:
#         return trainer_name
#
#
# def clean_skill_value(score):
#     if score in range(1, 9):
#         return score
#
#     else:
#         return None

