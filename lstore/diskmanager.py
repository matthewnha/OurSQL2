from config import *
from util import *
from page_rw_utils import *
from table import MetaRecord, Table
from pagerange import PageRange
from page import Page
import os

PAGE_OFFSET = 1
PAGE_RANGE_OFFSET = 2
NUMBER_OF_DEXS = 3

class DiskManager:

    def __init__(self,  dir = "database_files"):
        self.files = []
        self.my_database = None # type : db
        self.page_ranges = []

        if(dir[-1] != '/'):
            dir += '/'
            
        self.database_folder = dir



    def set_path(self, path):
        access_rights = 0o755
        if(path[-1] != '/'):
            path += '/'
        path_list = path.split('/')
        if path_list[0] == '~':
            path_list[0] = os.getenv('HOME')
        self.database_folder = '/'.join(path_list)

        
    def make_table_folder(self, table_name):
        path = self.database_folder + table_name
        access_rights = 0o755
        try:
            os.mkdir(path, access_rights)
        except OSError:
            print ("Creation of the directory %s failed" % path)
        else:
            print ("Successfully created the directory %s" % path)


    # Todo
    def write_to_disk(self, table_name, page_pid):
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

    
    # Todo
    def load_from_disk(self, path, page_pid):
        pass


    def open_db(self):
        try:
            os.mkdir(self.database_folder)
        except OSError:
            print ("Creation of the directory %s failed" % self.database_folder)
        else:
            print ("Successfully created the directory %s" % self.database_folder)

        try: 
            database_directory_file = open(self.database_folder + "Database_Directory", 'r+b')
        except FileNotFoundError:
            return False

        num_of_tables = int_from_bytes(database_directory_file.read(CELL_SIZE_BYTES))

        for i in range(num_of_tables):
            table_name_len = int_from_bytes(database_directory_file.read(CELL_SIZE_BYTES))
            table_name = database_directory_file.read(table_name_len).decode('utf-8')
            key_col = int_from_bytes(database_directory_file.read(CELL_SIZE_BYTES))
            num_columns = int_from_bytes(database_directory_file.read(CELL_SIZE_BYTES))

            new_table = Table(table_name,num_columns,key_col)
            self.import_table(new_table) # type : Table

            num_page_ranges = int_from_bytes(database_directory_file.read(CELL_SIZE_BYTES))
            new_table.page_ranges = [None]*num_page_ranges

            for i in range(num_page_ranges):
                new_table.page_ranges[i] = self.import_page_ranges(i, table_name)

            self.my_database.tables[table_name] = new_table
            print(database_directory_file.read(CELL_SIZE_BYTES).decode('utf-8'))
        
        database_directory_file.close()

        return True


    def write_db_directory(self):
        binary_file = open(self.database_folder + "Database_Directory", 'w+b')

        data = bytearray()

        data += int_to_bytes(len(self.my_database.tables))
        self.separator = int_to_bytes(int_from_bytes('NewTable'.encode('utf-8')))

        for name, table in self.my_database.tables.items():
            data += int_to_bytes(len(name.encode('utf-8'))) + name.encode('utf-8')
            data += int_to_bytes(table.key_col)
            data += int_to_bytes(table.num_columns)
            data += int_to_bytes(len(table.page_ranges))
            data += self.separator

        binary_file.write(data)
        binary_file.close()

    # Todo
    def close_db(self):
        self.write_db_directory()


        for table_name, table in self.my_database.tables.items():
            self.write_table_meta(table_name)

            for i, pagerange in enumerate(table.page_ranges):
                self.write_page_range(pagerange, i, table_name)
        del self.my_database
        
    # Todo
    def purge_table(self, table_name):
        pass

    # Todo
    def purge(self):
        pass

    """
            --- Meta_file format ---

    Every piece of info is 8 bytes.
    Outline of file.
    - Prev_rid
    - Prev_tid
    - Page_Directory Length

    - MetaRecords
        - BaseRecords
            - Rid
            - Key
            - Columns

        - TailRecords
            - Rid
            - Key
            - Schema
            - Columns
    """

    def import_table(self, table):
        try:
            meta_file = open(self.database_folder + table.name + '/' + table.name + '_meta', 'r+b')
        except FileNotFoundError:
            return False

        table.prev_rid = int_from_bytes(meta_file.read(CELL_SIZE_BYTES))
        table.prev_tid = int_from_bytes(meta_file.read(CELL_SIZE_BYTES))
        page_directory_size = int_from_bytes(meta_file.read(CELL_SIZE_BYTES))


        tail_flag = False

        for i in range(page_directory_size):

            # Read rid and key
            rid = int_from_bytes(meta_file.read(CELL_SIZE_BYTES))
            key = int_from_bytes(meta_file.read(CELL_SIZE_BYTES))

            if rid > table.prev_rid:
                tail_flag = True

            columns = [None for _ in range(table.num_total_cols)]
            
            # print('tail flag', tail_flag)
            # Read columns of base record

            if not tail_flag:
                for i in range(table.num_total_cols):          
                    column = []

                    for j in range(NUMBER_OF_DEXS):
                        column.append(int_from_bytes(meta_file.read(CELL_SIZE_BYTES)))

                    columns[i] = column

            # Read columns of tail record
            else:

                # Tells which columnbs to skip
                schema = int_from_bytes(meta_file.read(CELL_SIZE_BYTES))
                schema_encoding = bin(schema)[2:].zfill(table.num_columns)[::-1]

                for i in range(table.num_total_cols):
                            
                    if i >= START_USER_DATA_COLUMN and schema_encoding[i - START_USER_DATA_COLUMN] == '0':
                        continue
                    else:
                        column = []
                        for j in range(NUMBER_OF_DEXS):
                            column.append(int_from_bytes(meta_file.read(CELL_SIZE_BYTES)))

                        columns[i] = column

            metarecord = MetaRecord(rid,key,columns)

            if(not tail_flag):
                table.key_index[key] = rid

            
            table.page_directory[rid] = metarecord

        meta_file.close()
        return table


    # Give table by name
    def write_table_meta(self, table_name):
        try:  
            table = self.my_database.tables[table_name]
        except KeyError:
            return False

        try:
            binary_file = open(self.database_folder + table_name + '/' + table_name + "_meta", 'w+b')
        except FileNotFoundError:
            self.make_table_folder(table_name)
            binary_file = open(self.database_folder + table_name + '/' + table_name + "_meta", 'w+b')

        data = bytearray()

        #
        # Write some globals
        #
        data += int_to_bytes(table.prev_rid) # rid
        data += int_to_bytes(table.prev_tid) # tid
        data += int_to_bytes(len(table.page_directory))

        # key is the rid and value are the metarecords
        # columns point to the location in data

        num_of_rids = len(table.page_directory)
        rids = list(table.page_directory.keys()) # type : dict
        metarecords = list(table.page_directory.values())

        tail_flag = False
        counter = 0

        #
        # Write all Meta records
        #
        while counter < num_of_rids:

            current_rid = rids[counter]
            current_record = metarecords[counter]

            if current_rid > table.prev_rid: # First tail rid
                tail_flag = True

            # Write rid and key
            data += int_to_bytes(current_rid)
            data += int_to_bytes(current_record.key)

            # Write columns of base record
            if not tail_flag:

                for column in current_record.columns:

                    for dex in column:
                        data += int_to_bytes(dex)

            # Write columns of tail record
            else:
                the_columns = bytearray()
                schema = 0

                for i, column in enumerate(current_record.columns):

                    if column == None:
                        continue
                    else:
                        if i >= START_USER_DATA_COLUMN:
                            schema += 2**(i - START_USER_DATA_COLUMN)
                        for dex in column:
                            the_columns += int_to_bytes(dex)

                # Tells import_table which columns to ignore
                schema = int_to_bytes(schema)


                data += schema + the_columns

            counter += 1
        
        binary_file.write(data)
        binary_file.close()

        return True

    # Todo
    def write_page_range(self, pagerange, pagerange_num, table_name):
        # print("Here")
        try:
            binary_file = open(self.database_folder + "/" + table_name + "/" + "pagerange_" + str(pagerange_num), "w+b")
        except FileNotFoundError:
            return False

        data = encode_pagerange(pagerange)

        # data = bytearray()

        # data += int_to_bytes(pagerange.base_page_count) # base page count first
        # data += int_to_bytes(pagerange.tail_page_count) # tail page count second

        # base_string = int_to_bytes(int_from_bytes("basedata".encode('utf-8')))

        # data += base_string
        # for page in pagerange.base_pages:
        #     if page != None:
        #         data += int_to_bytes(page.num_records)
        #         data += page.data
            

        # tail_string = int_to_bytes(int_from_bytes("taildata".encode('utf-8')))

        # data += tail_string

        # for page in pagerange.tail_pages:
        #     if page != None:
        #         data += int_to_bytes(page.num_records)
        #         data += page.data

        binary_file.write(data)
        binary_file.close()

        return True

    # Todo
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

    # Todo
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
        binary_file.close()

        return page

    #Todo
    def import_page_ranges(self, pagerange_num, table_folder):

        try:
            binary_file = open(self.database_folder + "/" + table_folder + "/" + "pagerange_" + str(pagerange_num), "r+b")
        except FileNotFoundError:
            return False

        return decode_pagerange(binary_file.read())

        # current = 0
        # binary_file.seek(0,0)

        # pagerange.pagerange_id = pagerange_num
        # pagerange.base_page_count = int_from_bytes(binary_file.read(CELL_SIZE_BYTES))
        # pagerange.tail_page_count = int_from_bytes(binary_file.read(CELL_SIZE_BYTES))

        # try: 
        #     base_string = binary_file.read(CELL_SIZE_BYTES).decode('utf-8')
        #     print(base_string)
        # except:
        #     print("UGG")

        # if base_string == 'basedata':
        #     print("Success")
        # else:
        #     del pagerange
        #     return None
        # try: 
        #     current = CELL_SIZE_BYTES * (PAGE_RANGE_OFFSET + 1) + CELL_SIZE_BYTES * (PAGE_OFFSET*2) + PAGE_SIZE * pagerange.base_page_count
        #     binary_file.seek(current)
        #     tail_string = binary_file.read(CELL_SIZE_BYTES).decode('utf-8')
        #     print(tail_string)
        # except:
        #     print("UGG")

        # if tail_string == 'taildata':
        #     print("TSuccess")
        # else:
        #     del pagerange
        #     return None

