from abc import ABC, abstractmethod
from typing import List, Type


class Structure(ABC):
    """
        This class is used to define the structure of the classes that will be generated.
    """
    @classmethod
    def generate(cls, struct_data: List[tuple]):
        return [cls(*data) for data in struct_data]


def create_dynamic_class(column_indexes) -> Type[Structure]:
    """
    This method is used to create a dynamic class with dynamic attributes.
    :param column_indexes: Dictionary with the indexes of the columns to be extracted.
    :return: Dynamic class with dynamic attributes.
    """
    attribute_list = list(column_indexes.keys())

    # Create a data class with dynamic attributes.
    class DynamicStructure(Structure):
        """
            This class is used to create a dynamic class with dynamic attributes.
        """

        def __init__(self, **kwargs):
            for key in attribute_list:
                # Set the attributes of the class based on the keys of the COLUMN_INDEXES dictionary.
                setattr(self, key, kwargs.get(key))

        @classmethod
        def generate(cls, struct_data: List[tuple]):
            # Create a list of objects based on the data in the struct_data list.
            # The data is sorted in the same order as the keys of the COLUMN_INDEXES dictionary.
            return [cls(**dict(zip(attribute_list, data))) for data in struct_data]

    return DynamicStructure
