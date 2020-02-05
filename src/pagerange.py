from config import *
from page import Page

# PageRange:
#   Pages = list of size 12
#   baseRecordCount = 0
#   tailRecords = 0
#   createRecord()
  
class PageRange:

    def __init__(self):
        self.base_pages = [None for _ in range(PAGE_RANGE_MAX_BASE_PAGES)]
        self.tail_pages = []
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

        inner_page_index = self.base_page_count
        new_page = Page()
        self.base_pages[inner_page_index] = new_page

        self.base_page_count += 1

        return (inner_page_index, new_page)

    def create_tail_page(self):
        '''
            Returns (inner_page_index, new_page)
        '''
        inner_page_index = self.tail_page_count + PAGE_RANGE_MAX_BASE_PAGES
        new_page = Page()
        self.tail_pages.append(new_page)
        self.tail_page_count += 1

        return (inner_page_index, new_page)

    def get_latest_tail(self):
        '''
            Returns (inner_page_index, latest_tail_page)
        '''
        if self.tail_page_count == 0:
            return None

        last_tail_idx = self.tail_page_count - 1
        inner_idx = PAGE_RANGE_MAX_BASE_PAGES + last_tail_idx
        return (inner_idx, self.tail_pages[last_tail_idx])

    def get_open_tail_page(self):
        '''
            Returns (inner_page_index, tail_page)
        '''
        if self.tail_page_count == 0:
            inner_idx, tail_page = self.create_tail_page()
        else:
            inner_idx, tail_page = self.get_latest_tail()
            is_open = tail_page.has_capacity()
            print('isopen', is_open)
            if not is_open:
                inner_idx, tail_page = self.create_tail_page()

        return (inner_idx, tail_page)

    def get_page(self, inner_page_index):
        if inner_page_index < PAGE_RANGE_MAX_BASE_PAGES:
            return self.base_pages[inner_page_index]

        last_tp_index = inner_page_index - PAGE_RANGE_MAX_BASE_PAGES
        return self.tail_pages[last_tp_index]