from config import *
from util import *

class Page:

    def __init__(self, is_importing = False):
        self.num_records = 0
        # self.indexes = None

        if is_importing:
            self.data = None
            self.is_loaded = False
            self.is_dirty = False
        else:
            self.data = bytearray(PAGE_SIZE)
            self.is_loaded = True
            self.write_tps(RESERVED_TID)
            self.is_dirty = True

    @property
    def is_loaded(self):
        return self.__is_loaded

    @is_loaded.setter
    def is_loaded(self, is_loaded):
        self.__is_loaded = is_loaded
        
    # def set_pid(self,indexes):
    #     self.indexes = indexes

    def load(self, data, num_records):
        if self.data == None:
            self.data = data
            self.num_records = num_records
        self.is_loaded = True

        return self

    
    def has_capacity(self):
        return self.num_records < (CELLS_PER_PAGE)

    def get_num_records(self):
        return self.num_records

    def write(self, value):
        '''
            Writes bytes to the page

            value: Must be bytes of the specified CELLS_PER_PAGE size
        '''
        if not self.has_capacity():
            raise Exception('page is full')


        start = (self.num_records + 1) * CELL_SIZE_BYTES
        end = start + CELL_SIZE_BYTES
        if len(value) != CELL_SIZE_BYTES:
            value = int.from_bytes(value,'little')
            value = value.to_bytes(CELL_SIZE_BYTES,'little')
        self.data[start:end] = value
        self.num_records += 1
        self.is_dirty = True
        return self.num_records

    def write_to_cell(self, value, cell_idx):
        '''
            Writes bytes to the page at specific cell
            Only write to cells that you KNOW have been written to!

            value: Must be bytes of the specified CELLS_PER_PAGE size
        '''

        start = (cell_idx + 1) * CELL_SIZE_BYTES
        end = start + CELL_SIZE_BYTES
        if len(value) != CELL_SIZE_BYTES:
            value = int.from_bytes(value,'little')
            value = value.to_bytes(CELL_SIZE_BYTES,'little')
        self.data[start:end] = value
        self.is_dirty = True

        return self.num_records

    def write_tps(self, tid):
        bytes_to_write = tid.to_bytes(CELL_SIZE_BYTES,'little')
        start = 0
        end = start + CELL_SIZE_BYTES
        self.data[start:end] = bytes_to_write
        self.is_dirty = True

    def read_tps(self) -> int:
        return int_from_bytes(bytes(self.data[0:CELL_SIZE_BYTES]))


    def read(self, cellIndex):
        '''
            Reads bytes from page, returning a bytearray
        '''

        if cellIndex > CELLS_PER_PAGE - 1:
            raise Exception('cellIndex exceeds page size')

        start = (cellIndex + 1) * CELL_SIZE_BYTES
        end = start + CELL_SIZE_BYTES
        return bytes(self.data[start:end])

    def copy(self):
        copy = Page()
        copy.data = self.data.copy()
        copy.num_records = self.num_records
        return copy

