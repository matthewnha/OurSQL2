from page import Page
from pagerange import PageRange
from config import *
from util import *

class BufferPool:

    def __init__(self, table, disk):
        self.max_pages = MAX_PAGES
        self.disk = disk
        self.num_pool_pages = 0
        self.pages = [] # todo: just pop random for now, later organize
        self.pins = {}
        self.least_recently_used = None
        self.table = table # type: Table

    def add_page(self, pid, page):
        if len(pid) > 2:
            raise Exception('Only pass elements 1 and 2 of pid')

        if self.num_pool_pages >= MAX_POOL_PAGES:
            self.pop_page() # Choose page to remove from pool

        if page.is_loaded:
            self.pages.append(pid)
            self.num_pool_pages += 1
        else:
            self.load_from_disk(pid, page)

    def pop_page(self) -> Page:
        '''
        Pops page and decides via ? method (LRU/MRU)
        '''

        # todo: choose a page to pop. pops oldest page for now, get page pid
        i = 0
        pid = self.pages[i]
        page_to_pop = self.table.get_page(pid)

        while(encode_pid(pid) in self.pins and self.pins[encode_pid(pid)] > 0):
            pid = self.pages[i]
            page_to_pop = self.table.get_page(pid)
            i += 1

        if page_to_pop.is_dirty:
            self.write_to_disk(pid, page_to_pop)

        page_to_pop.data = None
        page_to_pop.is_loaded = False
        self.pins[encode_pid(pid)] = 0
        self.num_pool_pages -= 1
        self.pages.pop(i)
    
    def write_to_disk(self, pid, page):
        success = self.disk.write_page(pid, page, self.table, self.table.name)
        page.is_dirty = False

        return success

    def get_page(self, pid):
        if len(pid) != 2:
            raise Exception('Get page only accepts pids with inner idx and range idx')

        page_idx, page_range_idx = pid
        page_range = self.table.page_ranges[page_range_idx] # type: PageRange
        page = self.table.page_range.get_page(page_idx) # type: Page

        if not page.is_loaded:
            self.load_from_disk(pid,page)
        
        return page


    def load_from_disk(self, pid, page):
        if len(pid) != 2:
            raise Exception('Get page only accepts pids with inner idx and range idx')

        if self.num_pool_pages >= MAX_POOL_PAGES:
            # print('Buffer full, must pop a page')
            self.pop_page() # Choose page to remove from pool

        success = self.disk.import_page(pid, page, self.table, self.table.name)

        if not success:
            raise Exception("Page didn't exist on disk")

        self.add_page(pid, page)

        return page

    def handle(self, pid, page, page_func, *args):
        if not page.is_loaded:
            if len(pid) > 2:
                pid = pid[1:]

            self.load_from_disk(pid, page)

        return page_func(*args)

    def drop_page(self, index):
        if self.pins[index] == 0 and len(self.pages) > index:
            del self.pages[index]
            del self.pins[index]
            self.num_pool_pages -= 1
        else:
            raise Exception("Cannot drop, Page is Pinned")

    def pin(self, page_key):
        self.pins[page_key] += 1
        pass

    def is_dirty(self, page_key):
        return self.pages[page_key].is_dirty

    def remove_pin(self, page_key):
        if self.pins[page_key] != 0:
            self.pins[page_key] += 1
        pass

#     def get_page(self, pid) -> Page: # type: Page
#         cell_idx, page_idx, page_range_idx = pid
#         page_range = self.page_ranges[page_range_idx] # type: PageRange
#         page = page_range.get_page(page_idx) # type: Page

#         return page

#     def get_page_range(self,page_range_idx):
#         return self.page_ranges[page_range_idx]

#     def read_pid(self, pid): # type: Page
#         cell_idx, page_idx, page_range_idx = pid
#         page_range = self.page_ranges[page_range_idx] # type: PageRange
#         page = page_range.get_page(page_idx) # type: Page
#         read = page.read(cell_idx)

#         return read

#     def get_open_base_page(self, col_idx):
#         # how many pages for this column exists
#         num_col_pages = ceil(self.num_rows / CELLS_PER_PAGE)

#         # index of the last used page in respect to all pages across all page ranges
#         prev_outer_page_idx = col_idx + max(0, num_col_pages - 1) * self.num_total_cols

#         # index of last used page range
#         prev_page_range_idx = floor(prev_outer_page_idx / PAGE_RANGE_MAX_BASE_PAGES)

#         # index of last used page in respect to the specific page range
#         prev_inner_page_idx = get_inner_index_from_outer_index(prev_outer_page_idx, PAGE_RANGE_MAX_BASE_PAGES)

#         # index of cell within page
#         mod = self.num_rows % CELLS_PER_PAGE
#         max_cell_index = CELLS_PER_PAGE - 1
#         prev_cell_idx = max_cell_index if (0 == mod) else (mod - 1)

#         if max_cell_index == prev_cell_idx: # last page was full
#             # Go to next col page

#             # New cell's page index in respect to all pages
#             outer_page_idx = col_idx if 0 == self.num_rows else prev_outer_page_idx + self.num_total_cols

#             # New cell's page range index
#             page_range_idx = floor(outer_page_idx / PAGE_RANGE_MAX_BASE_PAGES)
            
#             try:
#                 page_range = self.page_ranges[page_range_idx] # type: PageRange
#             except IndexError:
#                 page_range = PageRange()
#                 self.page_ranges.append(page_range)

#             # New cell's page index in respect to pages in page range
#             inner_page_idx = get_inner_index_from_outer_index(outer_page_idx, PAGE_RANGE_MAX_BASE_PAGES)

#             cell_idx = 0
#             created_inner_page_idx, page = page_range.create_base_page()

#             if created_inner_page_idx != inner_page_idx:
#                 raise Exception('Created inner page index is not the same as the expected inner page index',
#                     page.get_num_records(),
#                     created_inner_page_idx, cell_idx, inner_page_idx, page_range_idx, outer_page_idx)

#         else: # there's space in the last used page
#             outer_page_idx = prev_outer_page_idx
#             page_range_idx = prev_page_range_idx
#             inner_page_idx = prev_inner_page_idx
#             cell_idx = prev_cell_idx + 1
            
#             page_range = self.page_ranges[page_range_idx] # type: PageRange

#             page = page_range.get_page(inner_page_idx)
#             if (None == page):
#                 raise Exception('No page returned', cell_idx, inner_page_idx, page_range_idx, outer_page_idx, self.num_rows, col_idx)
            
#         pid = [cell_idx, inner_page_idx, page_range_idx]  
#         return (pid, page)
# +