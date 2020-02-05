from config import *
class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)
        self.cellSize = 4096 // CELLS_PER_PAGE

    def has_capacity(self):
        print('has_capacity', self.num_records, CELLS_PER_PAGE)
        return self.num_records < CELLS_PER_PAGE

    def get_num_records(self):
        return self.num_records

    def write(self, value):
        '''
            Writes bytes to the page

            value: Must be bytes of the specified CELLS_PER_PAGE size
        '''
        if not self.has_capacity():
            raise Exception('page is full')

        start = self.num_records * self.cellSize
        end = start + self.cellSize
        self.data[start:end] = value
        self.num_records += 1
        return self.num_records

    def writeToCell(self, value, cell_idx):
        '''
            Writes bytes to the page at specific cell
            Only write to cells that you KNOW have been written to!

            value: Must be bytes of the specified CELLS_PER_PAGE size
        '''

        start = cell_idx * self.cellSize
        end = start + self.cellSize
        self.data[start:end] = value

        return self.num_records

    def read(self, cellIndex):
        '''
            Reads bytes from page, returning a bytearray
        '''

        if cellIndex > CELLS_PER_PAGE - 1:
            raise Exception('cellIndex exceeds page size')

        start = cellIndex * self.cellSize
        end = start + self.cellSize
        return bytes(self.data[start:end])

