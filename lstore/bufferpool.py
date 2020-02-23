from page import Page
from pagerange import PageRange
from config import *

class buffferpool:


    def __init__(self):
        self.max_pages = MAX_PAGES
        self.num_pages = 0
        self.pages = {}
        self.pins = {}
        self.paths = {}
        pass

    def load_from_disk(self, page):
        page_to_pop = self.pop_page()
        page_to_pop.data = None
        page_to_pop
        pass


    # pool.handle(page.read, 0)
    def handle(self, page, page_func, *args):
        if not page_func.loaded:
            load_from_disk(page)

        return page_func(*args)

    # def get_page(self, page_key):
    #     self.num_pages += 1
    #     pass

    def pop_page(self):
        pass
    
    # def retrieve_page(self, page_location):
    #     pass
    
    # def return_page(self, page_key):
    #     pass

    def pin(self, page_key):
        pass

    def is_dirty(self, page_key):
        pass

    def unpin(self, page_key):
        pass