from config import *
from util import *

class Page:

    def __init__(self, is_importing = False):
        self.num_records = 0
        if is_importing:
            self.data = None
        else:
            self.data = bytearray(4096)
        self.cellSize = (4096 // BLOCKS_PER_PAGE)
        self.is_dirty = False

    def load(self, data):
        if self.data == None:
            self.data = data
        pass
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

        start = (self.num_records + 1) * self.cellSize
        end = start + self.cellSize
        if len(value) != self.cellSize:
            value = int.from_bytes(value,'little')
            value = value.to_bytes(self.cellSize,'little')
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

        start = (cell_idx + 1) * self.cellSize
        end = start + self.cellSize
        if len(value) != self.cellSize:
            value = int.from_bytes(value,'little')
            value = value.to_bytes(self.cellSize,'little')
        self.data[start:end] = value
        self.is_dirty = True

        return self.num_records

    def write_tps(self, tid):
        bytes_to_write = tid.to_bytes(self.cellSize,'little')
        start = 0
        end = start + self.cellSize
        self.data[start:end] = bytes_to_write
        self.is_dirty = True

    def read_tps(self) -> int:
        return int_from_bytes(bytes(self.data[0:self.cellSize]))


    def read(self, cellIndex):
        '''
            Reads bytes from page, returning a bytearray
        '''

        if cellIndex > CELLS_PER_PAGE - 1:
            raise Exception('cellIndex exceeds page size')

        start = (cellIndex + 1) * self.cellSize
        end = start + self.cellSize
        return bytes(self.data[start:end])

    def copy(self):
        copy = Page()
        copy.data = self.data.copy()
        copy.num_records = self.num_records
        return copy

