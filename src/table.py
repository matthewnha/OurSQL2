from page import *
from pagerange import PageRange
from index import Index
from time import time
from config import *
from math import ceil

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.key_index = {} # key -> base record PID
        self.index = Index(self)
        self.num_rows = 0
        pass

    def createRow(self, key, columns_data):
        schema_encoding = [0 for _ in range(self.num_columns)]

        num_page_ranges = ceil(self.num_rows / CellsPerPage)
        num_pages_in_last = self.num_rows % CellsPerPage

        if num_pages_in_last == 0:
            new_page_range = PageRange()

            # Indirection TODO

            # RID TODO

            # Timestamp TODO

            # Schema Encoding
            col_page_idx = new_page_range.createBasePage()
            col_page = new_page_range.getPage(col_page_idx)
            col_page.write(bytes(schema_encoding))

            # Key

            # User Data
            for i in range(self.num_columns):
                col_page_idx = new_page_range.createBasePage()
                col_page = new_page_range.getPage(col_page_idx)
                col_page.write([columns_data[i]])

                bytesOut = col_page.read(0)
                print(int.from_bytes(bytesOut, 'little'))
            
        else:
            pass

    def __merge(self):
        pass
 
