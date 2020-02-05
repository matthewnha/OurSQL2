from page import *
from pagerange import PageRange
from index import Index
from time import time
from config import *
from math import ceil, floor
from util import *

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
START_USER_DATA_COLUMN = 4

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key_col= key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key_col):
        self.name = name
        self.key_col = key_col
        self.num_columns = num_columns
        self.num_sys_columns = 4
        self.num_total_cols = self.num_sys_columns + self.num_columns
        self.num_rows = 0

        self.page_ranges = []
        self.page_directory = {}
        self.key_index = {} # key -> base record PID
        self.index = Index(self)

        self.prev_rid = 0
        pass

    def get_page(self, pid): # type: Page
        cell_idx, page_idx, page_range_idx = pid
        page_range = self.page_ranges[page_range_idx] # type: PageRange
        page = page_range.get_page(page_idx) # type: Page

        return page

    def get_open_base_page(self, col_idx):
        # how many pages for this column exists
        num_col_pages = ceil(self.num_rows / CELLS_PER_PAGE)

        # index of the last used page in respect to all pages across all page ranges
        prev_outer_page_idx = col_idx + max(0, num_col_pages - 1) * self.num_total_cols

        # index of last used page range
        prev_page_range_idx = floor(prev_outer_page_idx / PAGE_RANGE_MAX_BASE_PAGES)

        # index of last used page in respect to the specific page range
        prev_inner_page_idx = get_inner_index_from_outer_index(prev_outer_page_idx, PAGE_RANGE_MAX_BASE_PAGES)

        # index of cell within page
        mod = self.num_rows % CELLS_PER_PAGE
        max_cell_index = CELLS_PER_PAGE - 1
        prev_cell_idx = max_cell_index if (0 == mod) else (mod - 1)

        if max_cell_index == prev_cell_idx: # last page was full
            # Go to next col page

            # New cell's page index in respect to all pages
            outer_page_idx = col_idx if 0 == self.num_rows else prev_outer_page_idx + self.num_total_cols

            # New cell's page range index
            page_range_idx = floor(outer_page_idx / PAGE_RANGE_MAX_BASE_PAGES)
            
            try:
                page_range = self.page_ranges[page_range_idx] # type: PageRange
            except IndexError:
                page_range = PageRange()
                self.page_ranges.append(page_range)

            # New cell's page index in respect to pages in page range
            inner_page_idx = get_inner_index_from_outer_index(outer_page_idx, PAGE_RANGE_MAX_BASE_PAGES)

            cell_idx = 0
            created_inner_page_idx, page = page_range.create_base_page()

            if created_inner_page_idx != inner_page_idx:
                raise Exception('Created inner page index is not the same as the expected inner page index',
                    page.get_num_records(),
                    created_inner_page_idx, cell_idx, inner_page_idx, page_range_idx, outer_page_idx)

        else: # there's space in the last used page
            outer_page_idx = prev_outer_page_idx
            page_range_idx = prev_page_range_idx
            inner_page_idx = prev_inner_page_idx
            cell_idx = prev_cell_idx + 1
            
            page_range = self.page_ranges[page_range_idx] # type: PageRange

            page = page_range.get_page(inner_page_idx)
            if (None == page):
                raise Exception('No page returned', cell_idx, inner_page_idx, page_range_idx, outer_page_idx, self.num_rows, col_idx)
            
        pid = [cell_idx, inner_page_idx, page_range_idx]  
        return (pid, page)

    def create_row(self, columns_data):

        # ORDER OF THESE LINES MATTER
        indirection_pid, indirection_page = self.get_open_base_page(INDIRECTION_COLUMN)
        rid_pid, rid_page = self.get_open_base_page(RID_COLUMN)
        time_pid, time_page = self.get_open_base_page(TIMESTAMP_COLUMN)
        schema_pid, schema_page = self.get_open_base_page(SCHEMA_ENCODING_COLUMN)
        column_pids_and_pages = [self.get_open_base_page(START_USER_DATA_COLUMN + i) for i in range(self.num_columns)]

        # RID
        self.prev_rid += 1
        rid = self.prev_rid
        rid_in_bytes = int_to_bytes(rid)
        num_records_in_page = rid_page.write(rid_in_bytes)

        # Indirection
        indirection_page.write(rid_in_bytes)

        # Timestamp
        bytes_to_write = b'\x00'
        time_page.write(bytes_to_write)

        # Schema Encoding
        schema_encoding = [0 for _ in range(self.num_columns)]
        bytes_to_write = bytes(schema_encoding)
        print('bytes_to_write', bytes_to_write)
        schema_page.write(bytes_to_write)

        # User Data
        for i, col_pid_and_page in enumerate(column_pids_and_pages):
            col_pid, col_page = col_pid_and_page
            bytes_to_write = int_to_bytes(columns_data[i])
            col_page.write(bytes_to_write)
        
        key = columns_data[self.key_col]
        sys_cols = [indirection_pid, rid_pid, time_pid, schema_pid]
        data_cols = [pid for pid, _ in column_pids_and_pages]
        record = Record(rid, key, sys_cols + data_cols)
        self.page_directory[rid] = record
        self.key_index[key] = rid
        self.num_rows += 1

    def update_row(self, key, update_data):
         # todo: traverse tail records
        base_rid = self.key_index[key]
        base_record = self.page_directory[base_rid] # type: Record
        schema_encoding = [0 if new_col_val == None else 1 for new_col_val in update_data]

        if 0 == sum(schema_encoding):
            return False

        # Get base record indirection
        base_indir_page_pid = base_record.columns[INDIRECTION_COLUMN]
        base_indir_page = self.get_page(base_indir_page_pid) # type: Page
        base_indir_cell_idx,_,_ = base_indir_page_pid
        prev_update_rid_bytes = base_indir_page.read(base_indir_cell_idx)

        # Base record encoding
        base_enc_page_pid = base_record.columns[SCHEMA_ENCODING_COLUMN]
        base_enc_page = self.get_page(base_enc_page_pid) # type: Page
        base_enc_cell_idx,_,_ = base_enc_page_pid

        self.prev_rid += 1
        new_rid = self.prev_rid

        # Meta columns

        # Get tail pages for meta info
        _,_,page_range_idx = base_record.columns[INDIRECTION_COLUMN]
        page_range = self.page_ranges[page_range_idx] # type: PageRange
        indirection_pid, indirection_page = page_range.get_open_tail_page()

        _,_,page_range_idx = base_record.columns[RID_COLUMN]
        page_range = self.page_ranges[page_range_idx] # type: PageRange
        rid_pid, rid_page = page_range.get_open_tail_page()

        _,_,page_range_idx = base_record.columns[TIMESTAMP_COLUMN]
        page_range = self.page_ranges[page_range_idx] # type: PageRange
        time_pid, time_page = page_range.get_open_tail_page()

        _,_,page_range_idx = base_record.columns[SCHEMA_ENCODING_COLUMN]
        page_range = self.page_ranges[page_range_idx] # type: PageRange
        schema_pid, schema_page = page_range.get_open_tail_page()

        # RID
        self.prev_rid += 1
        rid = self.prev_rid
        rid_in_bytes = int_to_bytes(rid)
        num_records_in_page = rid_page.write(rid_in_bytes)

        # Indirection
        indirection_page.write(prev_update_rid_bytes)

        # Timestamp todo: all timestamps
        bytes_to_write = b'\x00'
        time_page.write(bytes_to_write)

        # Schema Encoding
        bytes_to_write = bytes(schema_encoding)
        schema_page.write(bytes_to_write)

        meta_columns = [indirection_pid, rid_pid, time_pid, schema_pid]

        # Data Columns
        data_columns = []
        for i, pid in enumerate(update_data):
            if 0 == schema_encoding[i]:
                data_columns.append(None)
                continue

            col_idx = START_USER_DATA_COLUMN + i
            _,_,page_range_idx = base_record.columns[col_idx]
            page_range = self.page_ranges[page_range_idx] # type: PageRange

            # Get/make open tail page from the respective og page range
            inner_page_idx, tail_page = page_range.get_open_tail_page()

            bytes_to_write = int_to_bytes(update_data[i])
            num_records = tail_page.write(bytes_to_write)
            cell_idx = num_records - 1

            pid = [cell_idx, inner_page_idx, page_range_idx]  
            data_columns.append(pid)

        tail_record = Record(new_rid, key, meta_columns + data_columns)

        # Update base record indirection and schema
        new_rid_bytes = int_to_bytes(rid)
        base_indir_page.writeToCell(new_rid_bytes, base_indir_cell_idx)

        base_schema_enc_bytes = base_enc_page.read(base_enc_cell_idx)
        schema_enc_str = parse_schema_enc_from_bytes(base_schema_enc_bytes)
        base_schema_enc = int(schema_enc_str, 2)
        tail_schema_enc = int("".join(str(x) for x in schema_encoding), 2)
        new_base_enc = int(bin(base_schema_enc | tail_schema_enc), 2)

        list_schema_enc = []
        mask = 0b1
        for i in range(len(update_data)):
            list_schema_enc.insert(0, 0 if int(bin(new_base_enc & mask), 2) == 0 else 1)
            mask = mask << 1

        read = base_enc_page.read(base_enc_cell_idx)
        print(read)
        bytes_to_write = bytes(list_schema_enc)
        base_enc_page.writeToCell(bytes_to_write, base_enc_cell_idx)
        print(bytes_to_write)

        read = base_enc_page.read(base_enc_cell_idx)[0: self.num_columns]
        print(read)
        print(parse_schema_enc_from_bytes(read))

        # print('enc', base_schema_enc, tail_schema_enc, new_base_enc_binary, new_base_enc)

        return True

    def select(self, key, query_columns):
        # todo: traverse tail records
        rid = self.key_index[key]
        record = self.page_directory[rid]
        

        resp = []

        for i, pid in enumerate(record.columns[START_USER_DATA_COLUMN:]):
            if query_columns[i] == 0:
                continue
            page = self.get_page(pid)
            data = page.read(pid[0])
            resp.append(int_from_bytes(data))

        return resp

    def __merge(self):
        pass

