from table import *
from config import *
from page import Page
from util import *

class MergeJob:

    def __init__(self, table):
        self.copied_metarecords = {} # key to metarecord
        self.copied_base_pages = {} # (inner, pr) to page
        self.copied_prev_rid = None
        self.min_tid = RESERVED_TID
        self.table = table # type: Table

    def copy_data(self):
        self.copied_prev_rid = self.table.prev_rid
        copied_metarecords = {}
        copied_base_pages = {}
        for rid in range(1, self.copied_prev_rid+1):
            if rid not in self.table.page_directory:
                continue

            current_record = self.table.page_directory[rid].copy()
        
            for pid in current_record.columns:
                cell_idx, inner_idx, range_idx = pid

                if (inner_idx, range_idx) not in copied_base_pages:
                    copied_base_pages[(inner_idx, range_idx)] = self.table.get_page(pid)
                
            copied_metarecords[rid] = current_record

        return [copied_metarecords, copied_base_pages]
        

    def read_copied_by_pid(self, pid):
        cell_idx, inner_idx, range_idx = pid
        page = self.copied_base_pages[(inner_idx, range_idx)] # type: Page
        bytes_read = page.read(cell_idx)
        return bytes_read

    def read_copied_by_pid_as_int(self, pid):
        return int_from_bytes(self.read_copied_by_pid(pid))

    def write_to_copied_by_pid(self, pid, data):
        '''
        write to copied page

        pid: full address
        data: data as number
        '''

        cell_idx, inner_idx, range_idx = pid
        page = self.copied_base_pages[(inner_idx, range_idx)] # type: Page
        bytes_to_write = int_to_bytes(data)
        page.write_to_cell(bytes_to_write, cell_idx)
        self.copied_base_pages[(inner_idx, range_idx)] = page
        # print('')

    def collapse_record(self, rid):
        base_record = self.copied_metarecords[rid] # type: MetaRecord

        resp = [None for _ in range(self.table.num_columns)]
        need = [1 for _ in range(self.table.num_columns)]

        base_enc_pid = base_record.columns[SCHEMA_ENCODING_COLUMN]
        base_enc = self.read_copied_by_pid_as_int(base_enc_pid)
        base_enc_binary = bin(base_enc)[2:].zfill(self.table.num_columns)

        tps_all = resp.copy()

        for data_col_idx, is_dirty in enumerate(base_enc_binary):
            if need[data_col_idx] == 0:
                continue
            col_pid = base_record.columns[START_USER_DATA_COLUMN + data_col_idx]
            tps = self.table.get_page(col_pid).read_tps()
            tps_all[data_col_idx] = tps

            data = self.read_copied_by_pid(col_pid)
            resp[data_col_idx] = int_from_bytes(data)

            if is_dirty == '0':
                need[data_col_idx] = 0

        # get RID of next tail record
        curr_indir_pid = base_record.columns[INDIRECTION_COLUMN]
        next_rid = self.read_copied_by_pid_as_int(curr_indir_pid)

        # read tail records
        while sum(need) != 0: #  todo: or indirection > tps or more?
            self.min_tid = min(self.min_tid, next_rid)
            curr_record = self.table.page_directory[next_rid]

            curr_enc_pid = curr_record.columns[SCHEMA_ENCODING_COLUMN]
            curr_enc_bytes = self.table.read_pid(curr_enc_pid)
            curr_enc = int_from_bytes(curr_enc_bytes)
            curr_enc_binary = bin(curr_enc)[2:].zfill(self.table.num_columns)

            for data_col_idx, is_updated in enumerate(curr_enc_binary):
                if need[data_col_idx] == 0:
                    continue

                if is_updated == '0':
                    continue
                
                if next_rid >= tps_all[data_col_idx]:
                    need[data_col_idx] = 0
                    continue

                col_pid = curr_record.columns[START_USER_DATA_COLUMN + data_col_idx]
                data = self.table.read_pid(col_pid)
                data = int_from_bytes(data)
                resp[data_col_idx] = data
                need[data_col_idx] = 0

            if sum(need) != 0:
                curr_indir_pid = curr_record.columns[INDIRECTION_COLUMN]
                next_rid = int_from_bytes(self.table.read_pid(curr_indir_pid))

                if next_rid == rid: # if next rid is base
                    raise Exception("Came back to original, didn't get all we needed")

        # print('\tresp', resp)
        return [base_record, resp]

    def write_collapsed_pages(self, base_record, data_cols):
        
        for i, data in enumerate(data_cols):
            pid = base_record.columns[START_USER_DATA_COLUMN + i]
            self.write_to_copied_by_pid(pid, data)

    def write_tps_to_all(self):

        for page in self.copied_base_pages.values():
            page.write_tps(self.min_tid)

    def load_into_table(self, merged_record):
        og_record = self.table.page_directory[merged_record.rid]
        merged_data_cols = merged_record.columns[START_USER_DATA_COLUMN:]

        for i, pid in enumerate(merged_data_cols):
            cell_idx, inner_idx, range_idx = pid
            new_page = self.copied_base_pages[(inner_idx, range_idx)]
            og_page = self.table.get_page(pid)
            og_page.data = new_page.data
            # print('')

        # og_metacolumns  = og_record.columns[0:START_USER_DATA_COLUMN]
        # merged_record.columns = og_metacolumns + merged_data_cols
        # self.table.page_directory[merged_record.rid] = merged_record

    def run(self):
        
        with self.table.merge_lock:
            self.copied_metarecords, self.copied_base_pages = self.copy_data()

        for rid in range(1, self.copied_prev_rid+1):

            if rid not in self.table.page_directory:
                continue

            # Lock record from deletion
            self.table.del_locks[rid].acquire()
            if rid not in self.table.page_directory: # If we just acquired the del lock, we might've acquired it after a delete
                self.table.del_locks[rid].release()
                # print('Got lock but the base record was deleted')
                continue

            # Collapse
            base_record, data_cols = self.collapse_record(rid)
            self.write_collapsed_pages(base_record, data_cols)

            # Release lock
            self.table.del_locks[rid].release()

        self.write_tps_to_all()

        for rid in range(1, self.copied_prev_rid+1):

            if rid not in self.copied_metarecords:
                continue

            # Lock record from rw/deletion
            while(1):
                acquire_resp = acquire_all([self.table.rw_locks[rid], self.table.del_locks[rid]])
                if acquire_resp is False:
                    continue
                locks = acquire_resp

                if rid not in self.table.page_directory: # If we just acquired the del lock, we might've acquired it after a delete
                    release_all(locks)
                    break

                # Write back
                metarecord = self.copied_metarecords[rid]
                self.load_into_table(metarecord)

                # Release lock
                release_all(locks)
                break