from datetime import datetime

from db_loader.structs.struct_strategy import StrategyExtractor
from db_loader.structs.structure import Structure
from db_loader.migration import migrate
import os

"""
    This is an example of how to use the db_loader package.
    This example will migrate the data from the csv to the database.
    Creating a custom structure to add custom fields to the query.
"""

# Define the database credentials, must be a dictionary
# with the following keys: DB_HOST, DB_NAME, DB_USERNAME, DB_PASSWORD, DB_PORT
DB_CREDENTIALS = {
    'DB_HOST': os.getenv('DB_HOST'),
    'DB_NAME': os.getenv('DB_NAME'),
    'DB_USERNAME': os.getenv('DB_USERNAME'),
    'DB_PASSWORD': os.getenv('DB_PASSWORD'),
    'DB_PORT': os.getenv('DB_PORT'),
}

# Define the column indexes, must be a dictionary
COLUMN_INDEXES = {
    "code": 2,
    "country_code": 4,
}

# Define the query to be executed, must be a string
QUERY_PRODUCT = "INSERT INTO product(code, created_at) " \
                "SELECT {code}, '{created_at}'"

# Define the queries to be executed, must be a list of strings
QUERIES_TO_EXECUTE = [QUERY_PRODUCT]


# Define the structure of the class
class CustomStructure(Structure):
    def __init__(self, code: int):
        self.code = code
        self.created_at = datetime.now()


# Define the strategy to extract the data from the csv, must be a class that inherits from StrategyExtractor
# and implements the extract_from_csv method.
class StrategyExtractorExample(StrategyExtractor):
    def extract_from_csv(self, indexes: dict, data: list, row: list) -> tuple:
        # Implement your own logic here
        country_code = self.extract_value(row=row, indexes=indexes, key='country_code')
        code = country_code + "_" + self.extract_value(row=row, indexes=indexes, key='code')
        return code,  # Now is necessary to return a tuple with the CustomStructure fields sorted like CustomStructure.__init__ arguments.

        # Is valid is not implemented in this example, but it's necessary to implement it in your own strategy.


# Define the strategy to extract the data from the csv, must be a class that inherits from StrategyExtractor
strategy = StrategyExtractorExample()
# Define the structure of the class
strategy.define_fields(COLUMN_INDEXES)
strategy.define_structure(CustomStructure)

# Finally, call the migrate function to start the migration
migrate(db_credentials=DB_CREDENTIALS, csv_path='products_example.csv', strategy=strategy, indexes=COLUMN_INDEXES,
        queries=QUERIES_TO_EXECUTE, starter_row=1, delimiter=",", logs=True, only_show_queries=True)
