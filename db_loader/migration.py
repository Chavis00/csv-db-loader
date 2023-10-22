from db_loader.structs.migration import CsvReader
from db_loader.structs.struct_strategy import StrategyExtractor
from db_loader.mysql_connector.db_connector import migrate_to_db


def migrate(db_credentials: dict, csv_path: str, strategy: StrategyExtractor, indexes: dict, queries: list,
            starter_row: int = 0, delimiter: str = ',', double_query: bool = False, logs: bool = True,
            only_show_queries: bool = False, encoding: str = 'ISO-8859-1') -> None:
    """
    Migrate the csv to the database
    :type db_credentials: dict
    :type csv_path: str
    :type strategy: StrategyExtractor
    :type indexes: dict
    :type queries: list
    :type starter_row: int
    :type delimiter: str
    :type double_query: bool
    :type logs: bool
    :type only_show_queries: bool
    :type encoding: str
    :param db_credentials: The database credentials
    :param csv_path: The csv path
    :param strategy: The strategy to use
    :param indexes: The indexes to use
    :param queries: The queries to use
    :param starter_row : The starter row (optional, default is 0)
    :param delimiter: The delimiter (optional, default is ',')
    :param double_query: If true, you can execute a second query after the first one using the id of the first query.(optional, default is False)
    :param logs: If the logs should be shown (optional, default is True)
    :param only_show_queries: If only the queries should be shown (optional, default is False)
    :param encoding: The encoding (optional, default is 'ISO-8859-1')
    :return: None
    """
    csv_reader = CsvReader(csv_path=csv_path, strategy=strategy, delimiter=delimiter, encoding=encoding)
    csv_reader.generate_structs_from_csv(indexes=indexes, starter_row=starter_row)
    migrate_to_db(db_credentials=db_credentials, csv_reader=csv_reader, queries=queries, double_query=double_query, logs=logs, only_show_queries=only_show_queries)
