import csv

from db_loader.structs.struct_strategy import StrategyExtractor


class CsvReader:
    def __init__(self, csv_path: str, strategy: StrategyExtractor, encoding: str, delimiter: str = ',', quote_char: str = '"',
                 new_line: str = ''):
        self.structs = []
        self.csv_path = csv_path
        self.strategy = strategy
        self.delimiter = delimiter
        self.quote_char = quote_char
        self.new_line = new_line
        self.encoding = encoding
        self.lector = None

    def empty_structs(self) -> None:
        """
        Empty the structs list
        """
        self.structs = []

    def point_to_starter_row(self, starter_row: int) -> None:
        """
        Point the reader to the starter row
        :type starter_row: int
        :param starter_row:
        :return:
        """
        for _ in range(starter_row):
            next(self.lector)

    def generate_structs_from_csv(self, indexes: dict, starter_row: int = 0) -> None:
        """
        Generate the structs from the csv
        :type indexes: dict
        :type starter_row: int
        :param indexes:
        :param starter_row:
        :return:
        """
        self.empty_structs()

        with open(self.csv_path, newline=self.new_line, encoding=self.encoding) as self.csv_path:
            # Open the csv file
            self.lector = csv.reader(self.csv_path, delimiter=self.delimiter, quotechar=self.quote_char)
            # Point the reader to the starter row
            self.point_to_starter_row(starter_row)
            # Initialize the struct data
            struct_data = []

            for row in self.lector:
                # Extract the data from the csv
                data = self.strategy.perform_extractor(indexes=indexes, data=struct_data, row=row)
                if data is not None and data:
                    # Append the data to the struct data
                    struct_data.append(data)

            self.structs = self.strategy.generate_struct(struct_data=struct_data)
