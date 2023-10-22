from abc import ABC, abstractmethod
from typing import Type

from db_loader.structs.structure import Structure, create_dynamic_class


class StrategyExtractor(ABC):
    structure_class = None
    column_indexes = None

    @classmethod
    def define_structure(cls, structure: Type[Structure]) -> None:
        """
        This method is used to define the structure of the class.
        :param structure: Structure class.
        :return: None
        """
        cls.structure_class = structure

    @classmethod
    def define_fields(cls, column_indexes: dict) -> None:
        """
        This method is used to define the fields of the class.
        :param column_indexes: Dictionary with the indexes of the columns to be extracted.
        :return: None
        """
        cls.column_indexes = column_indexes
        new_structure = create_dynamic_class(column_indexes)
        cls.define_structure(new_structure)

    @classmethod
    def extract_from_csv(cls, indexes: dict, data: list, row: list[str]) -> tuple:
        """
        This method is used to extract data from the csv file.
        Its necessary to override this method in the child class.
        Its necessary to return a tuple with the extracted data sorted in the same order as the dictionary keys.
        :param indexes: Dictionary with the indexes of the columns to be extracted.
        :param data: List of data from the csv file.
        :param row: List of data from the current row.
        :return: Tuple with the extracted data.
        """
        pass

    def perform_extractor(self, indexes: dict, data: list, row: list[str]) -> tuple:
        """
            This method is used to perform the extraction of data from the csv file.
            First checks if the row is valid, then calls the extract_from_csv method.
            :param indexes: Dictionary with the indexes of the columns to be extracted.
            :param data: List of data from the csv file.
            :param row: List of data from the current row.
            :return: Tuple with the extracted data.
        """
        if not self.is_valid(indexes=indexes, row=row):
            return ()
        extracted_data = self.extract_from_csv(indexes=indexes, data=data, row=row)
        return extracted_data

    @classmethod
    def is_valid(self, row: list[str] = None, indexes: dict = None) -> bool:
        """
            This method is used to check if the row is valid.
            (OPTIONAL) Override this method in the child class to perform the validation.
            :param indexes: Dictionary with the indexes of the columns to be extracted.
            :param row: List of data from the current row.
            :return: Boolean value.
        """
        return True

    @classmethod
    def generate_struct(cls, struct_data: list) -> list:
        """
        This method is used to generate the structure.
        :param struct_data: List of tuples with the data to be structured.
        :return: List of instances of the structure class.
        """
        return cls.structure_class.generate(struct_data)

    @classmethod
    def extract_value(cls, row: list, indexes: dict, key: str):
        """
        This method is used to extract a value from the row.
        """
        return row[indexes[key]]
