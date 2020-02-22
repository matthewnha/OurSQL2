from table import Table
from config import *


class MergeJob:

    def __init__(self, table):
        self.base_pages = {}
        self.table = table # type: Table

    def copy_base_pages(self,table):
        pass

    def collapse_record(self, rid):
        base_record = self.table.page_directory[rid] # type: MetaRecord
        USER_COLS_START = self.table.START_USER_DATA_COLUMN
        for pid in base_record.columns[USER_COLS_START:]:
            pass


        resp = [None for _ in range(self.table.num_columns)]
        resp = [1 for _ in range(self.table.num_columns)]

        base_enc_pid = base_record.columns[SCHEMA_ENCODING_COLUMN]
        base_enc_bytes = self.table.read_pid(base_enc_pid)
        base_enc_binary = bin(int_from_bytes(base_enc_bytes))[2:].zfill(self.num_columns)

        for data_col_idx, is_dirty in enumerate(base_enc_binary):
            if is_dirty == '1' or need[data_col_idx] == 0:
                continue
            col_pid = base_record.columns[START_USER_DATA_COLUMN + data_col_idx]
            data = self.read_pid(col_pid)
            resp[data_col_idx] = int_from_bytes(data)
            need[data_col_idx] = 0

        curr_record = base_record

        while indirection != base_rid or indirection > tsp:
            curr_indir_pid = curr_record.columns[INDIRECTION_COLUMN]
            next_rid = self.read_pid(curr_indir_pid)
            next_rid = int_from_bytes(next_rid)
            curr_record = self.page_directory[next_rid]

            curr_enc_pid = curr_record.columns[SCHEMA_ENCODING_COLUMN]
            curr_enc_bytes = self.read_pid(curr_enc_pid)
            curr_enc = int_from_bytes(curr_enc_bytes)
            curr_enc_binary = bin(curr_enc)[2:].zfill(self.num_columns)
            #print(next_rid, "next", curr_enc_pid, "schema", curr_enc_binary, "order")

            for data_col_idx, is_updated in enumerate(curr_enc_binary):
                #print("still needs",need,"curr schema",curr_enc_binary,"dex", data_col_idx, "at", is_updated)
                if is_updated == '0' or need[data_col_idx] == 0:
                    continue
                col_pid = curr_record.columns[START_USER_DATA_COLUMN + data_col_idx]
                data = self.read_pid(col_pid)
                data = int_from_bytes(data)
                resp[data_col_idx] = data
                need[data_col_idx] = 0

        return resp
            

    def run(self):


        for rid in range(self.table.prev_rid):
            collapse_record(rid)
            

        pass