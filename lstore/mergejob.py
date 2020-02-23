from table import *
from config import *
from page import Page
from util import *
import logging
import threading
import time

class MergeJob:

    def __init__(self, table):
        self.copied_metarecords = {}
        self.copied_base_pages = {}
        self.copied_prev_rid = None
        self.min_tid = 2**64
        self.table = table # type: Table

    def copy_data(self):
        self.copied_prev_rid = self.table.prev_rid
        copied_metarecords = {}
        copied_base_pages = {}
        for rid in range(1, self.copied_prev_rid+1):
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

    def collapse_record(self, rid):
        base_record = self.copied_metarecords[rid] # type: MetaRecord
        USER_COLS_START = START_USER_DATA_COLUMN
        # for pid in base_record.columns[USER_COLS_START:]:
        #     pass


        resp = [None for _ in range(self.table.num_columns)]
        need = [1 for _ in range(self.table.num_columns)]

        base_enc_pid = base_record.columns[SCHEMA_ENCODING_COLUMN]
        base_enc_bytes = self.read_copied_by_pid(base_enc_pid)
        base_enc_binary = bin(int_from_bytes(base_enc_bytes))[2:].zfill(self.table.num_columns)

        for data_col_idx, is_dirty in enumerate(base_enc_binary):
            if is_dirty == '1' or need[data_col_idx] == 0:
                continue
            col_pid = base_record.columns[START_USER_DATA_COLUMN + data_col_idx]
            data = self.read_copied_by_pid(col_pid)
            resp[data_col_idx] = int_from_bytes(data)
            need[data_col_idx] = 0


        # read base record
        curr_indir_pid = base_record.columns[INDIRECTION_COLUMN]
        next_rid = self.read_copied_by_pid(curr_indir_pid)
        next_rid = int_from_bytes(next_rid)
        curr_record = self.table.page_directory[next_rid] # Reading tails from real data in table now

        curr_enc_pid = curr_record.columns[SCHEMA_ENCODING_COLUMN]
        curr_enc_bytes = self.table.read_pid(curr_enc_pid)
        curr_enc = int_from_bytes(curr_enc_bytes)
        curr_enc_binary = bin(curr_enc)[2:].zfill(self.table.num_columns)

        for data_col_idx, is_updated in enumerate(curr_enc_binary):
            #print("still needs",need,"curr schema",curr_enc_binary,"dex", data_col_idx, "at", is_updated)
            if is_updated == '0' or need[data_col_idx] == 0:
                continue
            col_pid = curr_record.columns[START_USER_DATA_COLUMN + data_col_idx]
            data = self.table.read_pid(col_pid)
            data = int_from_bytes(data)
            resp[data_col_idx] = data
            need[data_col_idx] = 0

        # read tail records
        while sum(need) != 0: #  todo: or indirection > tps or more?
            curr_indir_pid = curr_record.columns[INDIRECTION_COLUMN]
            next_rid = self.table.read_pid(curr_indir_pid)
            next_rid = int_from_bytes(next_rid)
            self.min_tid = min(self.min_tid, next_rid)
            curr_record = self.table.page_directory[next_rid]

            curr_enc_pid = curr_record.columns[SCHEMA_ENCODING_COLUMN]
            curr_enc_bytes = self.table.read_pid(curr_enc_pid)
            curr_enc = int_from_bytes(curr_enc_bytes)
            curr_enc_binary = bin(curr_enc)[2:].zfill(self.table.num_columns)
 
            for data_col_idx, is_updated in enumerate(curr_enc_binary):
                #print("still needs",need,"curr schema",curr_enc_binary,"dex", data_col_idx, "at", is_updated)
                if is_updated == '0' or need[data_col_idx] == 0:
                    continue
                col_pid = curr_record.columns[START_USER_DATA_COLUMN + data_col_idx]
                data = self.table.read_pid(col_pid)
                data = int_from_bytes(data)
                resp[data_col_idx] = data
                need[data_col_idx] = 0

        return [base_record, resp]

    def write_collapsed_pages(self, base_record, data_cols):
        
        for i, data in enumerate(data_cols):
            pid = base_record.columns[START_USER_DATA_COLUMN + 1]
            print('')
            self.write_to_copied_by_pid(pid, data)
            print('')
        pass

    def write_tps_to_all(self):

        for page in self.copied_base_pages.values():
            print('')
            page.write_tps(self.min_tid)
            print('')

        pass
            
            

    def run(self):
        
        # todo: lock
        self.copied_metarecords, self.copied_base_pages = self.copy_data()

        for rid in range(1, self.copied_prev_rid+1):
            base_record, data_cols = self.collapse_record(rid)
            self.write_collapsed_pages(base_record, data_cols)

        self.write_tps_to_all()
        # todo: lock and write to 

        pass