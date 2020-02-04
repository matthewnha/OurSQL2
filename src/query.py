from table import Table, Record
from index import Index
import time

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    """

    def __init__(self, table):
        self.table = table
        pass

    """
    # internal Method
    # Read a record with specified RID
    """

    def delete(self, key):
        pass

    """
    # Insert a record with specified columns
    """

    # INDIRECTION_COLUMN = 0
    # RID_COLUMN = 1
    # TIMESTAMP_COLUMN = 2
    # SCHEMA_ENCODING_COLUMN = 3

    def insert(self, *columns):
        keyCol = self.table.key

        data = columns[1:keyCol]
        if keyCol < self.table.num_columns - 1:
            data += columns[keyCol+1:]
        ok = self.table.createRow(columns[keyCol], data)

        return ok

    """
    # Read a record with specified key
    # :param key: the key value to select records based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    """

    def select(self, key, query_columns):
        pass

    """
    # Update a record with specified key and columns
    """

    def update(self, key, *columns):
        pass

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        pass