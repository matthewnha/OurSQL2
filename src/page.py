from config import *

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)
        self.cellSize = round(4096 / CellsPerPage)

    def has_capacity(self):
        return self.num_records < CellsPerPage

    def write(self, value):
        '''
            Writes bytes to the page

            value: Must be bytes of the specified CellsPerPage size
        '''

        if not self.has_capacity():
            raise Exception('page is full')

        start = self.num_records * self.cellSize
        end = start + self.cellSize
        self.data[start:end] = value

        self.num_records += 1
        return self.num_records

    def read(self, cellIndex):
        '''
            Reads bytes from page, returning a bytearray
        '''

        if cellIndex > CellsPerPage - 1:
            raise Exception('cellIndex exceeds page size')

        start = cellIndex * self.cellSize
        end = start + self.cellSize
        return bytes(self.data[start:end])

