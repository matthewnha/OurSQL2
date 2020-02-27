from page import Page
from pagerange import PageRange
from config import *

class BufferPool:

    def __init__(self, table, disk):
        self.max_pages = MAX_PAGES
        self.disk = disk
        self.num_pool_pages = 0
        self.pages = [] # todo: just pop random for now, later organize
        self.pins = {}
        self.least_recently_used = None
        self.table = table # type: Table

    def pop_page(self) -> Page:
        '''
        Pops page and decides via ? method (LRU/MRU)
        '''

        # todo: choose a page to pop. pops oldest page for now, get page pid
        page_to_pop = self.pages.pop()
        if (page_to_pop.is_dirty):
            self.write_to_disk(pid, page_to_pop)
        page_to_pop.data = None
        page_to_pop.is_loaded = False

        pass
    
    def write_to_disk(self, pid, page):
        success = self.disk.write_page(pid, self.table, table.name)
        page.is_dirty = False

        return success

    def load_from_disk(self, pid, page):
        if self.num_pool_pages < MAX_POOL_PAGES:
            self.pop_page() # Choose page to remove from pool

        # todo: get pid
        success = self.disk.import_page(pid, self.table, table.name)

        if not success:
            raise Exception("Page didn't exist on disk")

        self.pages.append(page)

    def handle(self, page, pid, page_func, *args):
        if not page.is_loaded:
            self.load_from_disk(page)

        return page_func(*args)

    # def get_page(self, page_key):
    #     self.num_pages += 1
    #     pass

    def drop_page(self, page_key):
        if self.pins[page_key] == 0 and len(self.pages) != 0:
            del self.pages[page_key]
            del self.pins[page_key]
            self.num_pages -= 1
        else:
            raise Exception("Cannot drop, Page is Pinned")

    # def retrieve_page(self, page_location):
    #     pass
    
    # def return_page(self, page_key):
    #     pass

    def pin(self, page_key):
        self.pins[page_key] += 1
        pass

    def is_dirty(self, page_key):
        return self.pages[page_key].is_dirty

    def remove_pin(self, page_key):
        if self.pins[page_key] != 0:
            self.pins[page_key] += 1
        pass