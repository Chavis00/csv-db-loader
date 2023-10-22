import mysql.connector
from alive_progress import alive_bar
import logging
from db_loader.structs.structure import Structure
from db_loader.structs.migration import CsvReader

logging.basicConfig(level=logging.DEBUG)
# Crea un registro
logger = logging.getLogger("CSV-DB-LOADER")
keys_to_check = ["DB_HOST", "DB_NAME", "DB_USERNAME", "DB_PASSWORD", "DB_PORT"]


def any_key_missing(db_credentials) -> bool:
    """
    This method is used to check if any key is missing in the db credentials dictionary.
    :param db_credentials: Dictionary with the db credentials.
    :return: True if any key is missing, False otherwise.
    """
    at_least_one_missing = False
    for key in keys_to_check:
        if key not in db_credentials:
            at_least_one_missing = True
            break  # Exit the loop as soon as a missing key is found
    return at_least_one_missing


def migrate_to_db(db_credentials: dict, csv_reader: CsvReader, queries: list[str], double_query: bool, logs: bool, only_show_queries: bool = False):
    """
    This method is used to migrate data from a CSV file to a MySQL database.
    :param db_credentials: Dictionary with the db credentials.
    :param csv_reader: CsvReader instance.
    :param queries: List of queries to execute.
    :param double_query: If True, you can execute a second query after the first one using the id of the first query.
    :param logs: If True, the errors will be logged.
    :param only_show_queries: If True, the queries will be printed but not executed.
    :return: None
    """
    if len(queries) > 2 and double_query:
        logger.error("double_query = True only support a max of 2 queries")
        return
    if any_key_missing(db_credentials):
        logger.error("Your keys in your db credentials dictionary must be DB_HOST, DB_NAME, DB_USERNAME, DB_PASSWORD, DB_PORT")
        return
    db = MysqlConnector(db_credentials['DB_HOST'], db_credentials['DB_NAME'], db_credentials['DB_USERNAME'],
                        db_credentials['DB_PASSWORD'], port=db_credentials['DB_PORT'])
    db.connect()
    db.premigrar(csv_reader)
    db.create_queries(queries=queries)
    if only_show_queries:
        logger.debug(db.queries)
        return
    db.migrar_datos(double_query=double_query)
    if logs:
        db.log_errors()


class MysqlConnector:
    def __init__(self, host, db, username, password, port):
        self.host = host
        self.db = db
        self.username = username
        self.password = password
        self.port = port
        self.errors = []
        self.migration = []
        self.cnx = None
        self.queries = []

    def connect(self) -> None:
        """
            Establishes a connection to a MySQL database using the specified connection parameters. The function expects to receive the following parameters:
            - username: the username for connecting to the database
            - password: the corresponding password for the user
            - host: the IP address or domain name of the database server
            - db: the name of the database you wish to connect to.
            - port: the port number of the database server

            The function handles exceptions in case the connection fails by raising a RuntimeError with an explanatory error message.
        """
        try:
            self.cnx = mysql.connector.connect(user=self.username, password=self.password, host=self.host,
                                               database=self.db, port=self.port)
        except mysql.connector.errors.DatabaseError as e:
            error = "Could not connect to the db because: " + str(e)
            raise RuntimeError(error)

    def close_db(self) -> None:
        """
            Closes the connection to the database that was previously established in the `connect_db` function.
        """
        self.cnx.close()

    def premigrar(self, migration) -> None:
        """
            Prepares a list of structures for migration.
            :param migration: an instance of the Migration class that contains a list of structures.
            :type migration: Migration
        """
        self.migration = migration.structs

    def build_queries_for_struct(self, structure: Structure, queries: tuple) -> tuple:
        """
            Creates SQL queries from a structure and a list of queries.
            :param structure: A structure object.
            :param queries: List of unformatted SQL queries.
            :return: A tuple of formatted SQL queries.
        """
        data = vars(structure)
        return tuple(query.format(**data) for query in queries)

    def create_queries(self, queries: tuple) -> None:
        """
            Creates SQL queries for each structure in the `migration` list.
            :param queries: List of unformatted SQL queries.
            :return: None
        """
        for estructura in self.migration:
            self.queries.append(self.build_queries_for_struct(estructura, queries))

    def migrar_datos(self, double_query: bool) -> None:
        """
            Executes the data migration queries stored in the `queries` list.
            If multiple queries will execute all
            double_query is only for use the returned id of the first query to execute the second one
            If any of the queries fail, it is added to the `errors` list.
        """
        logger.debug("EXECUTING QUERIES")
        cursor = self.cnx.cursor(buffered=False)
        try:
            total_queries = len(self.queries)
            for query in self.queries:
                try:
                    id = None  # Restart id
                    cursor.execute(query[0])
                    id = cursor.lastrowid
                    if (id is not None and len(query) > 1 and double_query) and id != 0:
                        cursor.execute(query[1].format(id=id))
                    if id is not None and len(query) > 1 and not double_query:
                        for single_query in query:
                            cursor.execute(single_query)

                except mysql.connector.Error as error:
                    # If the query fails, add it to the errors list
                    self.errors.append(("QUERY: " + str(query), "ERROR: " + str(error)))
            self.cnx.commit()
        except mysql.connector.Error as error:
            raise
        finally:
            cursor.close()
        logger.debug("FINISHING EXECUTION")

    def log_errors(self) -> None:
        """
            Log Errors
        """
        for error in self.errors:
            logger.error(str(error))
