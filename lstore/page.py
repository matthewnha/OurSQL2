from config import *
from util import *
from time import process_time

import threading

class Page:

    def __init__(self, is_importing = False):
        self.num_records = 0
        self.num_records_lock = threading.RLock()
        self.latch = threading.Lock()
        self.write_latch = threading.Lock()
        self.disk_latch = threading.Lock()
        self.pop_latch = threading.Lock()
        self.page_lock = threading.Lock()

        if is_importing:
            self._data = None
            self.is_loaded = False
            self.is_dirty = False
        else:
            self._data = bytearray(PAGE_SIZE)
            self.is_loaded = True
            self.write_tps(RESERVED_TID)
            self.is_dirty = True

    @property
    def is_loaded(self):
        return self.__is_loaded

    @is_loaded.setter
    def is_loaded(self, is_loaded):
        self.__is_loaded = is_loaded

    @property
    def is_dirty(self):
        return self.__is_loaded

    @is_dirty.setter
    def is_dirty(self, is_loaded):
        self.__is_loaded = is_loaded

    def load(self, data, num_records=None, is_dirty=None, force=False):
        with self.write_latch:
            if self.is_loaded and not force:
                return self

            self.is_loaded = True
            self._data = data

            if num_records is not None:
                self.num_records = num_records

            if is_dirty is not None:
                self.is_dirty = is_dirty

            return self

    def unload(self):
        with lock_inst.page_lock:
            with self.write_latch:
                self._data = None
                self.is_loaded = False
    
    def has_capacity(self):
        with lock_inst.page_lock:
            with self.num_records_lock:
                return self.num_records < (CELLS_PER_PAGE)

    def get_num_records(self):
        with lock_inst.page_lock:

            with self.num_records_lock:
                return self.num_records

    def write(self, value):
        '''
            Writes bytes to the page

            value: Must be bytes of the specified CELLS_PER_PAGE size
        '''
        with lock_inst.page_lock:

            with self.num_records_lock:
                if not self.has_capacity():
                    raise Exception('page is full')

                start = (self.num_records + 1) * CELL_SIZE_BYTES
                self.num_records += 1
                record_num = self.num_records # the number of this particular record (index+1)

            end = start + CELL_SIZE_BYTES
            if len(value) != CELL_SIZE_BYTES:
                value = int.from_bytes(value, 'little')
                value = value.to_bytes(CELL_SIZE_BYTES, 'little')

            with self.write_latch:
                self._data[start:end] = value
                self.is_dirty = True
            return record_num

    def write_to_cell(self, value, cell_idx, increment=False):
        '''
            Writes bytes to the page at specific cell
            Only write to cells that you KNOW have been written to!

            value: Must be bytes of the specified CELLS_PER_PAGE size
        '''
        with lock_inst.page_lock:
            if increment:
                with self.num_records_lock:
                    self.num_records += 1

            start = (cell_idx + 1) * CELL_SIZE_BYTES
            end = start + CELL_SIZE_BYTES
            if len(value) != CELL_SIZE_BYTES:
                value = int.from_bytes(value,'little')
                value = value.to_bytes(CELL_SIZE_BYTES,'little')
            with self.write_latch:
                self._data[start:end] = value
                self.is_dirty = True

            return self.num_records

    def write_tps(self, tid):
        with lock_inst.page_lock:
            bytes_to_write = tid.to_bytes(CELL_SIZE_BYTES,'little')
            start = 0
            end = start + CELL_SIZE_BYTES

            with self.write_latch:
                self._data[start:end] = bytes_to_write
                self.is_dirty = True

    def read_tps(self) -> int:
        return int_from_bytes(bytes(self._data[0:CELL_SIZE_BYTES]))

    def read(self, cellIndex):
        '''
            Reads bytes from page, returning a bytearray
        '''
        with lock_inst.page_lock:
            if cellIndex > CELLS_PER_PAGE - 1:
                raise Exception('cellIndex exceeds page size')

            start = (cellIndex + 1) * CELL_SIZE_BYTES
            end = start + CELL_SIZE_BYTES
            return bytes(self._data[start:end])

    def copy(self):
        with lock_inst.page_lock:
            with self.write_latch:
                copy = Page()
                copy._data = self._data.copy()
                copy.num_records = self.num_records
                return copy

