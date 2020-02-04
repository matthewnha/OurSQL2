from table import Table
from config import *

class Database():

    def __init__(self):
        self.tables = []
        pass

    def open(self, path):
        pass

    def close(self):
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key):
        if num_columns > PageRangeMaxBasePages:
            raise Exception('Exceeds maximum number of columns')
        
        table = Table(name, num_columns, key)
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        pass
