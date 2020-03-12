from page import Page
from pagerange import PageRange
from config import *
from latch import *
from collections import defaultdict

import logging
import threading

class BufferPool:

    def __init__(self, table, disk):
        self.max_pages = MAX_POOL_PAGES
        self.disk = disk # type : DiskManager
        self.num_pool_pages = 0
        self.pages = [] # todo: just pop random for now, later organize
        # self.pins = {}
        self.merge_pins = defaultdict(int)
        self.pins = defaultdict(int)
        self.page_index = {}
        self.loaded_off_pool = []

        # self.least_recently_used = []
        self.table = table # type: Table

        self.pop_lock = threading.Lock() # i love hip hop
        self.pages_lock = threading.Lock()
        self.num_pool_pages_lock = threading.Lock()

    def get_page(self, pid, pin=False):

        _, page_idx, page_range_idx = pid
        page_range = self.table.page_ranges[page_range_idx] # type: PageRange
        page = page_range.get_page(page_idx) # type: Page
        page_key = (pid[1],pid[2])
        
        if pin:
            self.pin(page_key)
 
        if not page.is_loaded:
            self._load_from_disk(pid, page)

        if (page_key, page) not in self.pages:

            if self.num_pool_pages >= MAX_POOL_PAGES:
                self._pop_page()

            self.num_pool_pages += 1


            self.pages.append((page_key, page))
        
        return page

    def add_page(self, pid, page, pin=False):
        if self.num_pool_pages >= MAX_POOL_PAGES:
            self._pop_page()
        
        page_key = (pid[1], pid[2])
        
        if pin:
            self.pin(page_key)
        
        with self.pages_lock:
            if (page_key, page) in self.pages:
                index2 = self.pages.index((page_key, page))
                value = self.pages.pop(index2)
                self.pages.append(value)

            elif page.is_loaded:
                self.pages.append((page_key, page))
                with self.num_pool_pages_lock:
                        self.num_pool_pages += 1

            else:
                self.get_page(pid)

    def _load_from_disk(self, pid, page):    
        page_key = (pid[1],pid[2])
        self.disk.import_page(page, page_key, self.table, self.table.name)

        return page

    def _pop_page(self) -> Page:
        '''
        Pops page and decides via ? method (LRU/MRU)
        '''

        with self.pages_lock:
            if self.num_pool_pages < MAX_POOL_PAGES:
                return

            logging.debug("{}: {}".format(threading.get_ident(), "bp._pop_page"))

            i = 0

            page_key, page_to_pop = self.pages[i]
            pages_to_remove = []
            num_pages_to_remove = self.num_pool_pages//4

            while i < self.num_pool_pages and len(pages_to_remove) < num_pages_to_remove:

                page_key, page_to_pop = self.pages[i]

                if self.pins[page_key] <= 0:
                    pages_to_remove.append(self.pages.pop(i))
                    self.num_pool_pages -= 1

                if i >= self.num_pool_pages:
                    i = 0
                else:
                    i += 1

            for page in pages_to_remove:

                page_key, page_to_pop = page

                # if self.table.merging == 0 and (page_key not in self.pins or self.pins[page_key] == 0):
                if self.table.merging == 0 and self.pins[page_key] == 0:
                    if page_to_pop.is_dirty:
                        self._write_to_disk(page_key, page_to_pop)

                    page_to_pop.unload()
                else:
                    self.loaded_off_pool.append((page_key, page_to_pop))
    
    def write_new_page_range(self, page_range, num):
        self.disk.write_page_range(page_range, num, self.table.name)

    def _write_to_disk(self, page_key, page):
        self.disk.write_page(page, page_key, self.table, self.table.name)
        page.is_dirty = False

    def drop_page(self, page_key):
        try:
            index = self.page_index[page_key]
        except KeyError:
            logging.error("Not in pool")

        if self.pins[page_key] == 0:
            # del self.pages[page_key] Need to do this
            del self.pins[page_key]
            self.num_pool_pages -= 1
        else:
            raise Exception("Cannot drop, Page is Pinned")

    def pin(self, page_key):
    

        if page_key in self.pins:
            self.pins[page_key] += 1
        else:
            self.pins[page_key] = 1

        logging.debug("{}: {} {} {}".format(threading.get_ident(), "bp.pin", page_key, self.pins[page_key]))

    def unpin(self, page_key):
        if self.pins[page_key] != 0:
            self.pins[page_key] -= 1

        logging.debug("{}: {} {} {}".format(threading.get_ident(), "bp.unpin", page_key, self.pins[page_key]))

    def pin_merge(self, page_key):
    

        if page_key in self.merge_pins:
            self.merge_pins[page_key] += 1
        else:
            self.merge_pins[page_key] = 1

        logging.debug("{}: {} {} {}".format(threading.get_ident(), "bp.pin_merge", page_key, self.merge_pins[page_key]))

    def unpin_merge(self, page_key):
        if self.merge_pins[page_key] != 0:
            self.merge_pins[page_key] -= 1

        logging.debug("{}: {} {} {}".format(threading.get_ident(), "bp.unpin_merge", page_key, self.merge_pins[page_key]))

    def flush_unpooled(self):
        while self.loaded_off_pool:
            page_key, page_to_pop = self.loaded_off_pool.pop()

            if self.pins[page_key] <= 0:
                continue

            with WriteLatch(page_to_pop.latch):
                if page_to_pop.is_dirty:
                    self._write_to_disk(page_key, page_to_pop)

                page_to_pop.unload()

                logging.debug("{}: {} {} {} {}".format(threading.get_ident(), "bp.flush_unpooled unloaded item", page_key, self.pins[page_key], self.pins[page_key]))

            