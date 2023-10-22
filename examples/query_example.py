from datetime import datetime

from db_loader.structs.struct_strategy import StrategyExtractor
from db_loader.migration import migrate
import os

"""
    This is an example of how to use the db_loader package.
    This example will migrate the data from the csv to the database.
    The method is_valid will be used to check if the row is valid.
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
    "name": 1,
    "code": 2,
    "due_date": 7,
    "country_code": 4,
    "client_email": 5,
    "sold": 6
}

# Define the query to be executed, must be a string
QUERY_PRODUCT = "INSERT INTO product(name, code, due_date) " \
                "SELECT '{name}', {code}, '{due_date}' "

QUERY_UPDATE_CLIENT_PURCHASE_COUNT = "UPDATE client SET purchase_count = purchase_count + 1 WHERE email = '{client_email}'"

# Define the queries to be executed, must be a list of strings
QUERIES_TO_EXECUTE = [QUERY_PRODUCT, QUERY_UPDATE_CLIENT_PURCHASE_COUNT]


# Define the strategy to extract the data from the csv, must be a class that inherits from StrategyExtractor
# and implements the extract_from_csv method.
class StrategyExtractorExample(StrategyExtractor):
    def extract_from_csv(self, indexes: dict, data: list, row: list) -> tuple:
        # Implement your own logic here
        name = self.extract_value(row=row, indexes=indexes, key='name')  # Use the extract_value method to extract the value from the row.
        country_code = self.extract_value(row=row, indexes=indexes, key='country_code')
        code = country_code + "_"+ self.extract_value(row=row, indexes=indexes, key='code')
        due_date = self.extract_value(row=row, indexes=indexes, key='due_date')
        client_email = ''
        sold = self.extract_value(row=row, indexes=indexes, key='sold')
        if sold == 'yes':
            client_email = self.extract_value(row=row, indexes=indexes, key='client_email')
        return name, code, due_date, country_code, client_email, sold  # It's necessary to return a tuple with ALL the extracted data sorted in the same order as the dictionary keys.

    def is_valid(self, indexes: dict, row: list[str]) -> bool:
        # The implementation of this method is optional to check if the row is valid, if not, the row will be skipped.
        due_date_str = self.extract_value(row=row, indexes=indexes, key='due_date')
        due_date = datetime.strptime(due_date_str, "%Y-%m-%d %H:%M:%S")
        some_date_limit = datetime(2023, 9, 24)
        if due_date > some_date_limit:
            return False
        return True


# Define the strategy to extract the data from the csv, must be a class that inherits from StrategyExtractor
strategy = StrategyExtractorExample()
# Define the structure of the class
strategy.define_fields(COLUMN_INDEXES)

# Finally, call the migrate function to start the migration
migrate(db_credentials=DB_CREDENTIALS, csv_path='products_example.csv', strategy=strategy, indexes=COLUMN_INDEXES,
        queries=QUERIES_TO_EXECUTE, starter_row=1, delimiter=",", logs=True, only_show_queries=True)
