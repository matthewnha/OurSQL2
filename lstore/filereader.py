from util import *
from config import *
from table import Table, MetaRecord

PAGE_OFFSET = 1
PAGE_RANGE_OFFSET = 2
NUMBER_OF_DEXS = 3

def read_files():
    file = open('./database_files/Database_Directory', 'r+b')

    length = len(file.read())

    file.seek(0,0)
    print(int_from_bytes(file.read(CELL_SIZE_BYTES)))
    i = 0
    while (i < length - CELL_SIZE_BYTES):
        if i == 0:
            name_len = int_from_bytes(file.read(CELL_SIZE_BYTES))
            name = bytearray(file.read(name_len)).decode('utf-8')
            print(name_len, "Length", name, "Name")

            key_col = int_from_bytes(file.read(CELL_SIZE_BYTES))
            num_columns = int_from_bytes(file.read(CELL_SIZE_BYTES))
            i += CELL_SIZE_BYTES * 6
            print("The key is", key_col, "columns", num_columns)
            print("Number of page ranges is",int_from_bytes(file.read(CELL_SIZE_BYTES)))
        else:
            print("PageRange ID",int_from_bytes(file.read(CELL_SIZE_BYTES)))
            i += CELL_SIZE_BYTES

    print(file.read(CELL_SIZE_BYTES).decode('utf-8'))

    table = Table('Grades',5,0)

    # Import table function

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
    meta_file = open('./database_files/' + name + '_meta', 'r+b')
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

                columns.append(column)

        # Read columns of tail record
        else:
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

        table.page_directory[rid] = metarecord

    meta_file.close()

    return table









