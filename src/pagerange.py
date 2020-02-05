from config import *
from page import Page

# PageRange:
#   Pages = list of size 12
#   baseRecordCount = 0
#   tailRecords = 0
#   createRecord()
  
class PageRange:

    def __init__(self):
        self.pages = [None for _ in range(PAGE_RANGE_MAX_SIZE)]
        self.base_page_count = 0
        self.tail_page_count = 0

    def has_open_base_pages(self):
        return self.base_page_count < PAGE_RANGE_MAX_BASE_PAGES

    def create_base_page(self):
        '''
            Creates base page and returns (page index, page instance)
        '''

        if self.base_page_count >= PAGE_RANGE_MAX_BASE_PAGES:
            return None

        page_index = self.base_page_count
        new_page = Page()
        self.pages[page_index] = new_page

        self.base_page_count += 1

        return (page_index, new_page)

    def create_tail_page(self):
        if self.base_page_count >= PAGE_RANGE_MAX_BASE_PAGES:
            return None

        page_index = self.tail_page_count
        new_page = Page()
        self.pages[page_index] = new_page

        self.tail_page_count += 1

        return (page_index, new_page)

    def get_page(self, page_index):
        return self.pages[page_index]