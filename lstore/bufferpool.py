from page import Page
from pagerange import PageRange
from config import *

class buffferpool:


    def __init__(self):
        self.max_pages = MAX_PAGES
        self.num_pages = 0
        self.pages = {}
        self.page_loc = {}
        self.files = ''
        pass

    def get_page(self, page_key):
        self.num_pages += 1
        pass

    def return_page(self, page_key):
        pass

    def pin(self, page_key):
        pass

    def is_dirty(self, page_key):
        pass

    def unpin(self, page_key):
        pass