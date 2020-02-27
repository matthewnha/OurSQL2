from page import Page
from pagerange import PageRange
from config import *

class BufferPool:

    def __init__(self, table, disk):
        self.max_pages = MAX_POOL_PAGES
        self.disk = disk # type : DiskManager
        self.num_pool_pages = 0
        self.pages = [] # todo: just pop random for now, later organize
        self.pins = {}
        self.page_index = {}
        # self.least_recently_used = []
        self.table = table # type: Table

    def add_page(self, pid, page):
        if self.num_pool_pages >= MAX_POOL_PAGES:
            self.pop_page() # Choose page to remove from pool
        
        page_key = (pid[1],pid[2])
        index = self.num_pool_pages
        if (page_key, page) in self.pages:
            index2 = self.pages.index((page_key, page))
            value = self.pages.pop(index2)
            self.pages.append(value)
            # print(pid, "Page already in bufferpool, moved from", index2, "to", index - 1)

        elif page.is_loaded:
            self.pages.append((page_key, page))
            # self.page_index[page_key] = index
            self.pins[page_key] = 0
            self.num_pool_pages += 1

        elif page.data == None:
            self.load_from_disk(pid, page)

        # print(pid, "Added page to bufferpool at index", index)

    # def move_to_back_and_pop(self, idx):
    #     print("At idx",idx,"Key is", self.pages[idx][0]) 
    #     print(len(self.pages), "Compared 1 to", self.num_pool_pages)
    #     temp = self.pages[self.num_pool_pages - 1]
    #     self.pages[self.num_pool_pages - 1] = self.pages[idx]
    #     self.pages[idx] = temp
    #     print("At idx",idx,"Key is", self.pages[idx][0])

    #     popped_page = self.pages.pop()
        
    #     self.num_pool_pages += -1
    #     print(len(self.pages), "Compared 2 to", self.num_pool_pages)

    #     temp = self.pages[self.num_pool_pages - 1]
    #     self.pages[self.num_pool_pages - 1] = self.pages[idx]
    #     self.pages[idx] = temp
    #     print("At idx",idx,"Key is", self.pages[idx][0])

    #     self.page_index[self.pages[idx][0]] = idx
    #     self.page_index[self.pages[self.num_pool_pages - 1][0]] = self.num_pool_pages - 1

    #     return popped_page


    def pop_page(self) -> Page:
        '''
        Pops page and decides via ? method (LRU/MRU)
        '''

        # todo: choose a page to pop. pops oldest page for now, get page pid
        i = 0
        page_key, page_to_pop = self.pages[i]

        # print(self.num_pool_pages, "num of pages")
        # print("Least used page pid is", tuple(page_key))
        while self.pins[page_key] > 0 and i < self.num_pool_pages:

            page_key, page_to_pop = self.pages[i]
            i += 1

            if i >= self.num_pool_pages:
                i = 0

        # if i != self.num_pool_pages - 1:
        #     page_key, page_to_pop = self.move_to_back_and_pop(i)
        # else:
        page_key, page_to_pop = self.pages.pop(i)
        self.num_pool_pages += -1

        if page_to_pop.is_dirty:
            self.write_to_disk(page_key, page_to_pop)

        page_to_pop.data = None
        page_to_pop.is_loaded = False

        del self.pins[page_key]
        # del self.page_index[page_key]


        # print("Bufferpool was full, page removed", page_key, "at" , i)
    
    def write_new_page_range(self, page_range, num):
        # print("Writing new pagerange file, pagerange:", num, " in table",self.table.name)
        self.disk.write_page_range(page_range, num, self.table.name)

    def write_to_disk(self, page_key, page):
        self.disk.write_page(page, page_key, self.table, self.table.name)
        page.is_dirty = False

        # print("Wrote to disk")
        # return success

    def get_page(self, pid):

        cell_idx, page_idx, page_range_idx = pid
        page_range = self.table.page_ranges[page_range_idx] # type: PageRange
        page = page_range.get_page(page_idx) # type: Page

        if not page.is_loaded:
            self.load_from_disk(pid, page)
        
        # print("Got page" , pid)
        return page


    def load_from_disk(self, pid, page):
        if self.num_pool_pages >= MAX_POOL_PAGES:
            self.pop_page()

            
        page_key = (pid[1],pid[2])
        # if self.num_pool_pages >= MAX_POOL_PAGES:
        #     self.pop_page() # Choose page to remove from pool

        # todo: get pid
        self.disk.import_page(page, page_key, self.table, self.table.name)


        # if not success:
        #     raise Exception("Page didn't exist on disk")
        page_key = (pid[1],pid[2])
        index = self.num_pool_pages
        self.pages.append((page_key, page))
        # self.page_index[page_key] = index
        self.pins[page_key] = 0
        self.num_pool_pages += 1

        # print("loaded page and added to bufferpool")
        # return page

    def handle(self, page, pid, page_func, *args):
        if not page.is_loaded:
            self.load_from_disk(pid, page)

        return page_func(*args)

    def drop_page(self, page_key):
        try:
            index = self.page_index[page_key]
        except KeyError:
            print("Not in pool")

        if self.pins[page_key] == 0:
            # del self.pages[page_key] Need to do this
            del self.pins[page_key]
            self.num_pool_pages -= 1
        else:
            raise Exception("Cannot drop, Page is Pinned")

    def pin(self, page_key):
        self.pins[page_key] += 1
        pass

    def is_dirty(self, page_key):
        # index = self.page_index[page_key]
        # return self.pages[index][1].is_dirty
        pass

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