# CSV Mysql Loader
[![PyPI version](https://img.shields.io/pypi/v/csv-db-loader)](https://pypi.org/project/csv-db-loader/)

**csv_db_loader** is a Python tool that allows you to easily load data from CSV files into a database with flexibility for custom data manipulation. You can transform and manipulate the data before storing it in the database, making it a versatile solution for data import projects.

## Installation

To install **CSVDBLoader**, use pip:

```bash
pip install csv-db-loader
```

## Getting Started
First define your database credentials into a dictionary like this:
```python
DB_CREDENTIALS = {
    'DB_HOST': 'your_host',
    'DB_NAME': 'your_db_name',
    'DB_USERNAME': 'your_db_username',
    'DB_PASSWORD': 'your_db_password',
    'DB_PORT': 'your_db_port',
}
```
Remember the name of the keys of the dictionary must be the same as the ones in the example. Then you have to define the column indexes of the CSV file you want to import into the database. For example, if you have a CSV file with the following columns:
```bash
id, name, email, address, number_code
0, John, john@mai.com, 1234, 1234
```
And you want to import only the name, email and number_code, columns, you have to define the indexes of the columns you want to import into the database. In this case, the indexes of the name and email columns are 1 and 2 respectively. So you have to define the indexes in a list like this:

```python
COLUMN_INDEXES = {
    "name": 1,
    "email": 2,
    "number_code": 4,
}
```
Now you have to define the query/queries that will be executed to insert the data into the database. For example, if you want to insert the data into a table called "users", you have to define the query like this:
```python
QUERY1 = "INSERT INTO users (name, email, code) SELECT ('{name}', '{email}'. {number_code})"
```
Once you have define all the queries you have to set them into a list like this:
```python
QUERIES = [QUERY1, QUERY2, QUERY3]
```
Now you have to create your own Strategy class that will be used to manipulate the data before storing it into the database. Remember that the class must inherit from the StrategyExtractor class. For example:
```python
from db_loader.structs.struct_strategy import StrategyExtractor
class StrategyExtractorExample(StrategyExtractor):
    def extract_from_csv(self, indexes: dict, data: list, row: list) -> tuple:
        # Implement your own logic here
        name = self.extract_value(row=row, indexes=indexes, key='name')  # Use the extract_value method to extract the value from the row.
        email = self.extract_value(row=row, indexes=indexes, key='email')
        number_code = "PARTNER-N-"+ self.extract_value(row=row, indexes=indexes, key='number_code')
        return name, email, number_code
```
extract_from_csv method is mandatory and must return a tuple with the values that will be used to replace the placeholders in the query/queries. Remember you need to use extract_value class method to extract the value of the row and the tuple must be sorted  in the same order as the COLUMN_INDEXES dictionary. In this case, the tuple must be like this:
```python
    (name, email, number_code)
```
We also have is_valid method that is optional and can be used to validate the data before storing it into the database. For example:
```python
def is_valid(self, indexes: dict, row: list[str]) -> bool:
    email = self.extract_value(row=row, indexes=indexes, key='email')
    my_company_domain = "@mycompany.com"
    if not email.endswith(my_company_domain):
        return False
    return True
```
If a row is not valid, the row will be skipped and won't be stored into the database.

Now you have to create an instance of the StrategyExtractorExample class and define the fields to be used from the COLUMN_INDEXES dictionary. For example:
```python
# Define the strategy to extract the data from the csv, must be a class that inherits from StrategyExtractor
strategy = StrategyExtractorExample()
# Define the structure of the class
strategy.define_fields(COLUMN_INDEXES)
```
Now you have everything you need you just need to import and run the migrate function like this:
```python
from db_loader.migration import migrate
migrate(db_credentials=DB_CREDENTIALS, csv_path='your/path/to/your.csv', strategy=strategy, indexes=COLUMN_INDEXES,
        queries=QUERIES_TO_EXECUTE, starter_row=1)
```
The starter_row parameter is optional and can be used to skip the first n rows of the CSV file. For example, if you want to skip the first row of the CSV file, you have to set the starter_row parameter to 1.

## More

### Optionals parameters for migrate function
- **starter_row**: defines the starter row to start reading csv, default value  = 0, 
- **delimiter**: define the csv delimiter, default value  = ','
- **double_query**: used to use chain query, default value  = False
- **logs**: used to print error logs, default value  = True
- **only_show_queries**: used to not excecute queryt only log the built query, default value  = False 
- **encoding**: used to define the csv encoding, default value  = 'ISO-8859-1'

## CHAIN QUERY
Using the double_query = True parameter you can use chain query. 

The only proupose of this is to use the result of the first query as a parameter of the second query. For example:
```python
QUERY_PRODUCT = "INSERT INTO product(code) " \
                "SELECT {code}"

QUERY_CLIENT_PURCHASES_MIDDLE_TABLE = "INSERT INTO client_purchases_middle_table(client_id, product_id) " \
                                      "SELECT " \
                                      "(SELECT id FROM client WHERE email = '{client_email}'), " \
                                      "LAST_INSERT_ID()"
QUERIES_TO_EXECUTE = [QUERY_PRODUCT, QUERY_CLIENT_PURCHASES_MIDDLE_TABLE]
```
In this case, the QUERY_CLIENT_PURCHASES_MIDDLE_TABLE query uses the result of the QUERY_PRODUCT query as a parameter. So, if you want to use chain query, you have to set the double_query parameter to True.

## More Examples
You can find more examples in the implementation examples folder.