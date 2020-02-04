from table import Table, Record
from index import Index


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

    def insert(self, *columns):
        schema_encoding = '0' * self.table.num_columns
        pass

    """
    # Read a record with specified key
    # :param key: the key value to select records based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    """

# should probably be implemented by table
    def select(self, key, query_columns):
        rid = table.key_index[key]
        record = table.page_directory[rid]
        indirection_key = record[INDIRECTION]
        values = [None]*sum(query_columns)

        for n in range(0,record[SCHEMA_ENCODING_COLUNM]):
            if record[SCHEMA_ENCODING_COLUNM][n] == 0:
                values[n] = record[n + OFFSET]
                query_columns[n] = 0

        while indirection_key != key or sum(query_columns) != 0:

            for n in range(0,len(query_columns)):

                if query_columns[n] == 1 and record[n] != None:
                    values[n] = record[n + OFFSET]
                    query_columns[n] = 0


                indirection_key = table.page_directory[indirection_key][INDIRECTION]

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
