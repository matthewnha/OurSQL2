from page import *
from pagerange import PageRange
from index import Index
from time import time
from config import *
from math import ceil
from util import *

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
START_USER_DATA_COLUMN = 4

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
        self.page_ranges = []
        self.page_directory = {}
        self.key_index = {} # key -> base record PID
        self.index = Index(self)
        self.num_rows = 0
        self.last_rid = -1
        pass

    def createRow(self, key, columns_data):
        schema_encoding = [0 for _ in range(self.num_columns)]

        num_page_ranges = ceil(self.num_rows / CellsPerPage)
        num_pages_in_last = self.num_rows % CellsPerPage

        if num_pages_in_last == 0:
            new_page_range = PageRange()
            self.page_ranges.append(new_page_range)

            # Order of createBasePages are strict!
            indirection_page = new_page_range.createBasePage()[1]
            rid_page = new_page_range.createBasePage()[1]
            time_page = new_page_range.createBasePage()[1]
            schema_page = new_page_range.createBasePage()[1]

            key_page = new_page_range.createBasePage()[1]
            column_pages = [new_page_range.createBasePage()[1] for _ in range(self.num_columns - 1)]
            
        else: # last page range has space
            page_range = self.page_ranges[num_page_ranges - 1] # type: PageRange # last page range 
            indirection_page = page_range.getPage(INDIRECTION_COLUMN)
            rid_page = page_range.getPage(RID_COLUMN)
            time_page = page_range.getPage(TIMESTAMP_COLUMN)
            schema_page = page_range.getPage(SCHEMA_ENCODING_COLUMN)
            key_page = page_range.getPage(START_USER_DATA_COLUMN)
            column_pages = [page_range.getPage(START_USER_DATA_COLUMN+i) for i in range(self.num_columns - 1)]

        # RID
        self.last_rid += 1
        rid = self.last_rid
        rid_in_bytes = int_to_bytes(rid)
        num_records_in_page = rid_page.write(rid_in_bytes)

        # Indirection
        indirection_page.write(rid_in_bytes)

        # Timestamp
        bytes_to_write = b'\x00'
        time_page.write(bytes_to_write)

        # Schema Encoding
        bytes_to_write = bytes(schema_encoding)
        schema_page.write(bytes_to_write)

        # Key
        bytes_to_write = int_to_bytes(key)
        key_page.write(bytes_to_write)

        # User Data
        for i in range(len(column_pages)):
            column_page = column_pages[i]
            bytes_to_write = int_to_bytes(columns_data[i])
            column_page.write(bytes_to_write)
        
        self.num_rows += 1
        page_range_idx = ceil(self.num_rows / CellsPerPage) - 1
        page_idx = self.num_rows % CellsPerPage - 1
        cell_idx = num_records_in_page - 1

        self.page_directory[rid] = (cell_idx, page_idx, page_range_idx)
        self.key_index[key] = rid

    def select(self, key, query_columns):
        rid = self.key_index[key]
        pid = self.page_directory[rid]
        cell_idx, page_idx, page_range_idx = pid

        page_range = self.page_ranges[page_range_idx] # type: PageRange

        resp = []

        for i, do_query in enumerate(query_columns):
            if (do_query):
                page = page_range.getPage(START_USER_DATA_COLUMN + i) # type: Page
                data = page.read(cell_idx)
                resp.append(int_from_bytes(data))

        return resp


    def __merge(self):
        pass

