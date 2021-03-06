import time
import threading
from math import ceil, floor

from page import *
from pagerange import PageRange
from index import Index
from config import *
from util import *

# from diskmanager import DiskManager
from bufferpool import BufferPool


class MetaRecord:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

    def copy(self):
        copy = MetaRecord(self.rid, self.key, self.columns)
        return copy

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
    def __init__(self, name, num_columns, key_col, disk):
        
        self.name = name
        self.key_col = key_col
        self.num_columns = num_columns
        self.num_sys_columns = 4 # Don't export
        self.num_total_cols = self.num_sys_columns + self.num_columns # Don't export
        self.num_rows = 0

        self.page_ranges = []
        self.page_directory = {}
        self._rw_locks = {} # only base records for now # Don't export
        self._del_locks = {} # Don't export

        self.merge_lock = threading.Lock() # Don't export

        self.prev_rid = 0
        self.prev_tid = 2**64 - 1

        self.bp = BufferPool(self, disk)
        self.key_index = {} # key -> base MetaRecord PID # Don't export
        self.index = Index(self) # Don't export

        pass

    def rw_locks(self, rid):
        if rid not in self._rw_locks:
            self._rw_locks[rid] = threading.Lock()

        return self._rw_locks[rid]

    def del_locks(self, rid):
        if rid not in self._del_locks:
            self._del_locks[rid] = threading.Lock()

        return self._del_locks[rid]

    def get_page(self, pid): # type: Page
        # cell_idx, page_idx, page_range_idx = pid
        # page_range = self.page_ranges[page_range_idx] # type: PageRange
        # page = page_range.get_page(page_idx) # type: Page

        page = self.bp.get_page(pid)
        return page

    def get_page_range(self,page_range_idx):
        return self.page_ranges[page_range_idx]

    def read_pid(self, pid): # type: Page
        page = self.get_page(pid) # type: Page
        read = page.read(pid[0])
        return read

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
        base_page_is_new = False

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
                index = len(self.page_ranges)
                self.page_ranges.append(page_range)
                self.bp.write_new_page_range(page_range, index)

            # New cell's page index in respect to pages in page range
            inner_page_idx = get_inner_index_from_outer_index(outer_page_idx, PAGE_RANGE_MAX_BASE_PAGES)

            cell_idx = 0
            created_inner_page_idx, page = page_range.create_base_page()

            # print("Created new base page")
            if created_inner_page_idx != inner_page_idx:
                raise Exception('Created inner page index is not the same as the expected inner page index',
                    page.get_num_records(),
                    created_inner_page_idx, cell_idx, inner_page_idx, page_range_idx, outer_page_idx)
            
            # base_page_is_new = True

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

        # if base_page_is_new or not page.is_loaded:
        # print("Trying to add", pid, "to bufferpool")
        self.bp.add_page(pid,page)

        return (pid, page)

    def create_row(self, columns_data):
        key = columns_data[self.key_col]

        if key in self.key_index:
            raise Exception('Key already exists')

        with self.merge_lock:
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
            millisec = int(round(time.time()*1000))
            bytes_to_write = int_to_bytes(millisec)
            cell_dex = time_page.write(bytes_to_write)

            # Schema Encoding
            schema_encoding = 0
            bytes_to_write = int_to_bytes(schema_encoding)
            schema_page.write(bytes_to_write)

            # User Data
            for i, col_pid_and_page in enumerate(column_pids_and_pages):
                col_pid, col_page = col_pid_and_page
                bytes_to_write = int_to_bytes(columns_data[i])
                col_page.write(bytes_to_write)

            sys_cols = [indirection_pid, rid_pid, time_pid, schema_pid]
            data_cols = [pid for pid, _ in column_pids_and_pages]
            record = MetaRecord(rid, key, sys_cols + data_cols)
            self.page_directory[rid] = record
            self._rw_locks[rid] = threading.Lock()
            self._del_locks[rid] = threading.Lock()
            self.key_index[key] = rid
            self.num_rows += 1

            return True

    def update_row(self, key, update_data):
        base_rid = self.key_index[key]

        lock_attempts = 0
        while(1):

            # Start acquire lock ===========

            lock_attempts += 1
            acquire_resp = acquire_all([self.merge_lock, self.rw_locks(base_rid)])
            if acquire_resp is False:
                continue

            locks = acquire_resp
            
            # Acquired lock ===========

            base_record = self.page_directory[base_rid] # type: MetaRecord

            tail_schema_encoding = 0

            for i,value in enumerate(update_data[::-1]):
                if value is None:
                    tail_schema_encoding += 0
                else:
                    tail_schema_encoding += 2**i

            if 0 == tail_schema_encoding:

                # Release locks and return
                release_all(locks)
                # self.rw_locks[base_rid].release()
                # self.merge_lock.release()
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

            # Meta columns

            # Get tail pages for base_record.columns[INDIRECTION_COLUMN]
            _,_,page_range_idx = base_record.columns[RID_COLUMN]
            page_range = self.page_ranges[page_range_idx] # type: PageRange
            ind_inner_idx, indirection_page = page_range.get_open_tail_page()
            indirection_pid = [None, ind_inner_idx, page_range_idx]
            self.bp.add_page(indirection_pid,indirection_page)

            # write indirection
            num_records_in_page = indirection_page.write(prev_update_rid_bytes)
            ind_cell_idx = num_records_in_page - 1
            indirection_pid[0] = ind_cell_idx
            

            _,_,page_range_idx = base_record.columns[RID_COLUMN]
            page_range = self.page_ranges[page_range_idx] # type: PageRange
            rid_inner_idx, rid_page = page_range.get_open_tail_page()
            rid_pid = [None, rid_inner_idx, page_range_idx]
            self.bp.add_page(rid_pid,rid_page)
            # write rid
            self.prev_tid -= 1
            new_rid = self.prev_tid
            rid_in_bytes = int_to_bytes(new_rid)
            num_records_in_page = rid_page.write(rid_in_bytes)
            rid_cell_idx = num_records_in_page - 1
            rid_pid[0] = rid_cell_idx
            

            _,_,page_range_idx = base_record.columns[TIMESTAMP_COLUMN]
            page_range = self.page_ranges[page_range_idx] # type: PageRange
            time_inner_idx, time_page = page_range.get_open_tail_page()
            time_pid = [None, time_inner_idx, page_range_idx]
            self.bp.add_page(time_pid,time_page)
            # write Timestamp todo: all timestamps
            millisec = int(round(time.time()*1000))
            bytes_to_write = int_to_bytes(millisec)
            num_records_in_page = time_page.write(bytes_to_write)
            time_cell_idx = num_records_in_page - 1
            time_pid[0] = time_cell_idx
            

            _,_,page_range_idx = base_record.columns[SCHEMA_ENCODING_COLUMN]
            page_range = self.page_ranges[page_range_idx] # type: PageRange
            schema_inner_idx, schema_page = page_range.get_open_tail_page()
            schema_pid = [None, schema_inner_idx, page_range_idx]
            self.bp.add_page(schema_pid,schema_page)

            # write encoding
            bytes_to_write = int_to_bytes(tail_schema_encoding)
            num_records_in_page = schema_page.write(bytes_to_write)
            schema_cell_idx = num_records_in_page - 1
            schema_pid[0] = schema_cell_idx
            

            meta_columns = [indirection_pid, rid_pid, time_pid, schema_pid]

            # Data Columns
            data_columns = []
            tail_schema_encoding_binary = bin(tail_schema_encoding)[2:].zfill(self.num_columns)
            for i, pid in enumerate(update_data):
                if '0' == tail_schema_encoding_binary[i]:
                    data_columns.append(None)
                    continue

                col_idx = START_USER_DATA_COLUMN + i
                _,_,page_range_idx = base_record.columns[col_idx]
                page_range = self.page_ranges[page_range_idx] # type: PageRange

                # Get/make open tail page from the respective og page range
                inner_page_idx, tail_page = page_range.get_open_tail_page()

                pid = [None, inner_page_idx, page_range_idx]
                self.bp.add_page(pid, tail_page)
                bytes_to_write = int_to_bytes(update_data[i])
                num_records = tail_page.write(bytes_to_write)
                cell_idx = num_records - 1

                pid[0] = cell_idx
                data_columns.append(pid)

            tail_record = MetaRecord(new_rid, key, meta_columns + data_columns)
            self.page_directory[new_rid] = tail_record
            # Update base record indirection and schema
            new_rid_bytes = int_to_bytes(new_rid)

            if not base_indir_page.is_loaded:
                base_indir_page = self.get_page(base_indir_page_pid)

            base_indir_page.write_to_cell(new_rid_bytes, base_indir_cell_idx)

            if not base_enc_page.is_loaded:
                base_enc_page = self.get_page(base_enc_page_pid)

            base_schema_enc_bytes = base_enc_page.read(base_enc_cell_idx)
            base_schema_enc_int = int_from_bytes(base_schema_enc_bytes)
            new_base_enc = base_schema_enc_int | tail_schema_encoding
            bytes_to_write = int_to_bytes(new_base_enc)
            base_enc_page.write_to_cell(bytes_to_write, base_enc_cell_idx)
            
            # Release locks and return
            release_all(locks)
            return True

    def select(self, key, query_columns):

        try:
            self.key_index[key]
        except KeyError: 
            raise Exception("Not a valid key.")
            return []

        if 0 == self.key_index[key]:
            # raise Exception("Key has been deleted.")
            return []

        collapsed = self.collapse_row(key, query_columns)

        record = Record(None, key, collapsed)
        return [record]

    def collapse_row(self, key, query_columns):
        resp = [None for _ in query_columns]
        rid = self.key_index[key]
        need = query_columns.copy()

        lock_attempts = 0
        while(1):

            # Start acquire lock ===========

            lock_attempts += 1
            acquire_resp = acquire_all([self.merge_lock, self.rw_locks(rid)])
            if acquire_resp is False:
                continue

            locks = acquire_resp
            
            # Acquired lock ===========

            # Reading base record
            base_record = self.page_directory[rid] # type: MetaRecord
            base_enc_pid = base_record.columns[SCHEMA_ENCODING_COLUMN]
            base_enc_bytes = self.read_pid(base_enc_pid)
            base_enc_binary = bin(int_from_bytes(base_enc_bytes))[2:].zfill(self.num_columns)
            # print(base_enc_pid, "Looking for data using schema",base_enc_binary)
            tps_all = resp.copy()

            for data_col_idx, is_dirty in enumerate(base_enc_binary):
                
                if need[data_col_idx] == 0:
                    continue

                col_pid = base_record.columns[START_USER_DATA_COLUMN + data_col_idx]
                tps = self.get_page(col_pid).read_tps()
                tps_all[data_col_idx] = tps

                data = self.read_pid(col_pid)
                resp[data_col_idx] = int_from_bytes(data)

                if is_dirty == '0':
                    need[data_col_idx] = 0

            # get RID of next tail record
            curr_indir_pid = base_record.columns[INDIRECTION_COLUMN]
            next_rid = int_from_bytes(self.read_pid(curr_indir_pid))
            # read tail records
            while sum(need) != 0 and next_rid < tps: #  todo: or indirection > tps or more?
                curr_record = self.page_directory[next_rid]
                curr_enc_pid = curr_record.columns[SCHEMA_ENCODING_COLUMN]
                curr_enc_bytes = self.read_pid(curr_enc_pid)
                curr_enc = int_from_bytes(curr_enc_bytes)
                curr_enc_binary = bin(curr_enc)[2:].zfill(self.num_columns)

                for data_col_idx, is_updated in enumerate(curr_enc_binary):
                    if need[data_col_idx] == 0:
                        continue

                    if is_updated == '0':
                        continue
                    
                    if next_rid >= tps_all[data_col_idx]:
                        need[data_col_idx] = 0
                        continue

                    # print('LOOKED AT TAIL')

                    col_pid = curr_record.columns[START_USER_DATA_COLUMN + data_col_idx]
                    data = self.read_pid(col_pid)
                    data = int_from_bytes(data)
                    resp[data_col_idx] = data
                    need[data_col_idx] = 0

                if sum(need) != 0:
                    curr_indir_pid = curr_record.columns[INDIRECTION_COLUMN]
                    next_rid = int_from_bytes(self.read_pid(curr_indir_pid))

                    if next_rid == rid: # if next rid is base
                        raise Exception("Came back to original, didn't get all we needed")

            # Release locks and return
            release_all(locks)
            return resp

    def delete_record(self, key):

        try:
            base_rid = self.key_index[key]
        except KeyError:
            raise Exception("Not a valid key")

        if 0 == base_rid:
            raise Exception("Key has been deleted.")

        lock_attempts = 0
        while(1):

            # Start acquire lock ===========

            lock_attempts += 1
            acquire_resp = acquire_all([self.merge_lock, self.rw_locks(base_rid), self.del_locks(base_rid)]) # todo: double check del locks
            if acquire_resp is False:
                continue

            locks = acquire_resp

            # Acquired lock ===========

            base_record = self.page_directory[base_rid]  # type: MetaRecord
            base_rid_page = self.get_page(base_record.columns[RID_COLUMN])
            base_rid_cell_inx,_,_ = base_record.columns[RID_COLUMN]

            base_rid_page.write_to_cell(int_to_bytes(0),base_rid_cell_inx)
            del self.key_index[key]
            if 0 in self.page_directory:
                base_record.rid = 0
                self.page_directory[0].append(base_record)
            else:
                self.page_directory[0] = [base_record]

            base_indir_page_pid = base_record.columns[INDIRECTION_COLUMN]
            new_tail_rid = self.read_pid(base_indir_page_pid)
            new_tail_rid = int_from_bytes(new_tail_rid)


            while True:            
                new_tail_record = self.page_directory[new_tail_rid]
                new_tail_rid_page = self.get_page(new_tail_record.columns[RID_COLUMN]) # type: Page
                new_tail_rid_cell_inx,_,_ = new_tail_record.columns[RID_COLUMN]

                new_tail_rid_page.write_to_cell(int_to_bytes(0),new_tail_rid_cell_inx)
                del self.page_directory[new_tail_rid]
                self.page_directory[0].append(new_tail_record)
                if(base_rid == new_tail_rid):
                    break
                else:
                    new_tail_indir_page_pid = new_tail_record.columns[INDIRECTION_COLUMN]
                    new_tail_rid = self.read_pid(new_tail_indir_page_pid)
                    new_tail_rid = int_from_bytes(new_tail_rid)

            # Release locks and return
            release_all(locks)
            return True

        del self._del_locks[base_rid]
        del self._rw_locks[base_rid]


    def sum_records(self, start_range, end_range, aggregate_column_index):
        #col_idx = aggregate_column_index + START_USER_DATA_COLUMN
        query_columns = [0]*self.num_columns
        query_columns [aggregate_column_index] = 1

        sum = 0

        if start_range <= end_range:
            curr_key = start_range
            end = end_range
        else:
            curr_key = end_range
            end = start_range

        while curr_key != (end+1):            
            try:
                curr_rid = self.key_index[curr_key]
            except KeyError:
                curr_key += 1
                continue
            if curr_rid == 0:
                curr_key += 1 
                continue
            value = self.collapse_row(curr_key,query_columns)[aggregate_column_index]
            sum += value
            curr_key += 1

        return sum

    def __merge(self):
        pass

