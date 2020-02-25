from config import *
from util import *
from table import Table
from db import Database
from pagerange import PageRange
from page import Page

PAGE_OFFSET = 1
PAGE_RANGE_OFFSET = 2

class DiskManager:

    def __init__(self, my_database = db(), dir = "."):
        self.files = []
        self.database = my_database
        self.page_ranges = []
        self.separator = int_to_bytes(int_from_bytes('NewTable'.encode('utf-8')))
        self.database_folder = dir + '/'

    def write_to_disk(self, path, page_pid):
        pass

    def load_from_disk(self, path, page_pid):
        pass

    def open_db(self, database):
        pass

    def close_db(self, database):
        pass

    def purge(self):
        pass

    def write_db_directory(self):
        binary_file = open(self.database_folder + "Database_Directory", 'w+b')

        data = bytearray()
        for key, value in self.my_database.tables.items():
            data += int_to_bytes(len(key.encode('utf-8'))) + key.encode('utf-8')

            data += int_to_bytes(value.key_col)
            
            data += int_to_bytes(value.num_columns)

            for pagerange in value.page_ranges:
                data += int_to_bytes(pagerange.pagerange_id)

            data += self.separator

# Give table by name
    def write_table_meta(self, table):
        try:  
            write_table = self.table[table]
        except KeyError:
            return False

        try:
            binary_file = open(self.database_folder + write_table.name + "_meta", 'w+b')
        except FileNotFoundError:
            return False
        data = bytearray()
        data += int_to_bytes(table.prev_rid) # rid
        data += int_to_bytes(table.prev_tid) # tid
        # key is the rid and value are the metarecords
        # columns point to the location in data
        for key, value in self.table.page_directory.items(): 
            data += int_to_bytes(key)

            for column in value.column:
                for dex in column:
                    data += int_to_bytes(dex)
        
        binary_file.write(data)
        binary_file.close()

        return True

    def write_page_range(self, pagerange, table_folder):
        try:
            binary_file = open(self.database_folder + "/" + table_folder + "/" + "pagerange_" + str(pagerange.pagerange_id), "w+b")
        except FileNotFoundError:
            return False
        data = bytearray()

        data += int_to_bytes(pagerange.base_page_count) # base page count first
        data += int_to_bytes(pagerange.tail_page_count) # tail page count second

        base_string = int_to_bytes(int_from_bytes("basedata".encode('utf-8')))

        data += base_string
        for page in pagerange.base_pages:
            if page != None:
                data += int_to_bytes(page.num_records)
                data += page.data
            

        tail_string = int_to_bytes(int_from_bytes("taildata".encode('utf-8')))

        data += tail_string

        for page in pagerange.tail_pages:
            data += int_to_bytes(page.num_records)
            data += page.data

        binary_file.write(data)
        binary_file.close()

    # def write_page_range_meta(self, pagerange):
    #     pass
    def write_page(self, pagerange, page, inner_page_idx, base_or_tail, table_folder):
        if page.data == None:
            raise Exception("Page not loaded")

        binary_file = open(self.database_folder + "/" + table_folder + "/" + "pagerange_" + str(pagerange.pagerange_id), "r+b")

        data = bytearray()

        if base_or_tail == 'base':
            if inner_page_idx < pagerange.base_page_count:
                page_location = PAGE_RANGE_OFFSET * CELL_SIZE_BYTES + inner_page_idx * (PAGE_SIZE + PAGE_OFFSET + 1) - CELL_SIZE_BYTES
            else:
                return False

        elif base_or_tail == 'tail': 
            if inner_page_idx < pagerange.tail_page_count: 
                page_location = PAGE_RANGE_OFFSET * CELL_SIZE_BYTES + (pagerange.base_page_count + inner_page_idx) * (PAGE_SIZE + PAGE_OFFSET + 2) - CELL_SIZE_BYTES
            else:
                return False
        
        binary_file.seek(page_location)
        binary_file.write(int_to_bytes(page.num_records))
        binary_file.write(page.data)
        binary_file.close()

    def import_from_disk(self):
        pass

    def import_table(self, table):
        pass

    def import_table_meta(self, table):
        pass

    def import_page(self, pagerange, inner_page_idx, base_or_tail, table_folder, page = Page(True)):
        try:
            binary_file = open(self.database_folder + "/" + table_folder + "/" + "pagerange_" + pagerange.pagerange_id, 'r+b')
        except FileNotFoundError:
            return False

        data = bytearray()
        
        if base_or_tail == 'base':
            if inner_page_idx < pagerange.base_page_count:
                page_location = PAGE_RANGE_OFFSET * CELL_SIZE_BYTES + inner_page_idx * (PAGE_SIZE + PAGE_OFFSET + 1) - CELL_SIZE_BYTES
        elif base_or_tail == 'tail': 
            if inner_page_idx < pagerange.tail_page_count: 
                page_location = PAGE_RANGE_OFFSET * CELL_SIZE_BYTES + (pagerange.base_page_count + inner_page_idx) * (PAGE_SIZE + PAGE_OFFSET + 2) - CELL_SIZE_BYTES
        
        binary_file.seek(page_location)
        page.num_records = binary_file.read(CELL_SIZE_BYTES)
        
        data += binary_file.read(PAGE_SIZE)
        page.load(data)

        return page

    def import_page_ranges(self, pagerange_num, pagerange = PageRange()):

        try:
            binary_file = open(self.database_folder + "pagerange_" + pagerange_num, 'r+b')
        except FileNotFoundError:
            return False

        current = 0
        binary_file.seek(0,0)

        pagerange.pagerange_id = pagerange_num
        pagerange.base_page_count = int_from_bytes(binary_file.read(CELL_SIZE_BYTES))
        pagerange.tail_page_count = int_from_bytes(binary_file.read(CELL_SIZE_BYTES))

        try: 
            base_string = binary_file.read(CELL_SIZE_BYTES).decode('utf-8')
            print(base_string)
        
        if base_string == 'basedata':
            print("Success")
        else:
            del pagerange
            return None
        try: 
            current = CELL_SIZE_BYTES * (PAGE_RANGE_OFFSET + 1) + CELL_SIZE_BYTES * (PAGE_OFFSET*2) + PAGE_SIZE * pagerange.base_page_count
            binary_file.seek(current)
            tail_string = binary_file.read(CELL_SIZE_BYTES).decode('utf-8')
            print(tail_string)

        if tail_string == 'taildata':
            print("TSuccess")
        else:
            del pagerange
            return None

        return pagerange
        # current += CELL_SIZE_BYTES * (PAGE_RANGE_OFFSET + 1)
        # not sure to import whole page range

    # def import_page_range_meta(self):
    #     pass
