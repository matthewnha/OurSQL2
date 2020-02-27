from page import Page
from pagerange import PageRange
from config import *

class BufferPool:

    def __init__(self, table):
        self.max_pages = MAX_PAGES
        self.num_pool_pages = 0
        self.pages = [] # todo: just pop random for now, later organize
        self.pins = {}
        self.least_recently_used = None
        self.table = table # type: Table

    def pop_page(self) -> Page:
        '''
        Pops page and decides via ? method (LRU/MRU)
        '''

        # todo: choose a page to pop. pops oldest page for now
        page_to_pop = self.pages.pop()

        page_to_pop.data = None
        page_to_pop.is_loaded = False

        pass

    def load_from_disk(self, page):
        if self.num_pool_pages < MAX_POOL_PAGES:
            self.pop_page() # Choose page to remove from pool

        pid = None # todo: get pid
        success = self.table.disk.load_page_from_disk(pid)

        if not success:
            raise Exception("Page didn't exist on disk")

        self.pages.append(page)

    def handle(self, page, page_func, *args):
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