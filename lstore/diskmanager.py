from config import *
from util import *
from table import Table, MetaRecord
from db import Database
from pagerange import PageRange
from page import Page

PAGE_OFFSET = 1
PAGE_RANGE_OFFSET = 2
NUMBER_OF_DEXS = 3

class DiskManager:

    def __init__(self, my_database = db(), dir = "."):
        self.files = []
        self.database = my_database
        self.page_ranges = []
        self.separator = int_to_bytes(int_from_bytes('NewTable'.encode('utf-8')))
        self.database_folder = dir + '/'

    def write_to_disk(self, table_name, path, page_pid):
        _, page_idx, page_range_idx = page_pid
        try:
            current_table = self.my_database.tables[table_name] # type: Table
        except KeyError:
            return False
        
        page = current_table.get_page(page_pid) # type: Page
        pagerange = current_table.get_page_range(page_range_idx)
        
        if page_idx >= PAGE_RANGE_MAX_BASE_PAGES:
            type_of_page = 'tail'
        else:
            type_of_page = 'base'

        self.write_page(pagerange, page, page_idx, type_of_page, table_name)
        pass

    def load_from_disk(self, path, page_pid):
        pass

    def open_db(self):
        database_directory_file = open(self.database_folder + "Database_Directory", 'r+b')

        num_of_tables = int_from_bytes(database_directory_file.read(CELL_SIZE_BYTES))

        for i in range(num_of_tables):
            table_name_len = int_to_bytes(database_directory_file.read(CELL_SIZE_BYTES))
            table_name = database_directory_file.read(table_name_len)
            key_col = database_directory_file.read(CELL_SIZE_BYTES)
            num_columns = database_directory_file.read(CELL_SIZE_BYTES)

            new_table = Table(table_name,num_columns,key_col)
            self.import_table(new_table)

            num_page_ranges = database_directory_file.read(CELL_SIZE_BYTES)

            for i in range(num_page_ranges):
                pagerange_id = database_directory_file.read(CELL_SIZE_BYTES)

                self.import_page_ranges(pagerange_id, table_name)

                print(database_directory_file.read(CELL_SIZE_BYTES).decode('utf-8'))


    def write_db_directory(self):
        binary_file = open(self.database_folder + "Database_Directory", 'w+b')

        data = bytearray()

        data += int_to_bytes(len(self.my_database.tables))

        for name, table in self.my_database.tables.items():
            data += int_to_bytes(len(name.encode('utf-8'))) + name.encode('utf-8')

            data += int_to_bytes(table.key_col)
            
            data += int_to_bytes(table.num_columns)

            data += int_from_bytes(len(table.page_ranges))
            for pagerange in table.page_ranges:
                data += int_to_bytes(pagerange.pagerange_id)

                self.import_page_ranges()

            data += self.separator

    def close_db(self, database):
        self.write_db_directory()


        for table_name, table in self.my_dictionary.tables.items():
            self.write_table_meta(table_name)

            for pagerange in table.page_ranges:
                self.write_page_range(pagerange,table_name)
        pass

    def purge(self):
        pass


    def import_table(self, table):
        binary_file = open(self.database_folder + table.name + "_meta", 'r+b')

        data = bytearray(binary_file.read())

        current = 0
        table.prev_rid = int_from_bytes(data[current : current + CELL_SIZE_BYTES - 1])

        current += CELL_SIZE_BYTES
        table.prev_tid = int_from_bytes(data[current : current + CELL_SIZE_BYTES - 1])

        current += CELL_SIZE_BYTES
        page_directory_size = int_from_bytes(data[current : current + CELL_SIZE_BYTES - 1])

        for i in range(page_directory_size):
            current += CELL_SIZE_BYTES
            key = int_from_bytes(data[current : current + CELL_SIZE_BYTES - 1])

            columns = []
            for i in range(table.num_total_columns):
                column = []

                for i in range(NUMBER_OF_DEXS):
                    current += CELL_SIZE_BYTES
                    column.append(int_from_bytes(data[current : current + CELL_SIZE_BYTES - 1]))

                columns.append(column)
            value = MetaRecord(key,columns[table.key_col],columns)

            table.page_directory[key] = value
        
        return table

        pass
# Give table by name
    def write_table_meta(self, table_name):
        try:  
            table = self.my_dictionary.table[table]
        except KeyError:
            return False

        try:
            binary_file = open(self.database_folder + table.name + "_meta", 'w+b')
        except FileNotFoundError:
            return False
        data = bytearray()
        data += int_to_bytes(table.prev_rid) # rid
        data += int_to_bytes(table.prev_tid) # tid
        data += int_to_bytes(len(table.page_directory))
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

    def import_page_ranges(self, pagerange_num, table_folder, pagerange = PageRange()):

        try:
            binary_file = open(self.database_folder + "/" + table_folder + "/" + "pagerange_" + str(pagerange_num), "r+b")
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
