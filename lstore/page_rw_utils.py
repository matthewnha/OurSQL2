from page import Page
from pagerange import PageRange
from util import *

def encode_pagerange(pr):

    #
    # Write Meta
    #
    BYTES_base_page_count = int_to_bytes(pr.base_page_count)
    BYTES_tail_page_count = int_to_bytes(pr.tail_page_count)

    #
    # Write Base Pages
    #

    BYTES_base_pages = b''
    for i in range(pr.base_page_count):
        page = pr.base_pages[i]
        BYTES_base_pages += encode_page(page)  # 8 + 4096

    #
    # Write Tail Pages
    #

    BYTES_tail_pages = b''
    for i in range(pr.tail_page_count):
        page = pr.tail_pages[i]
        BYTES_tail_pages += encode_page(page)

    write = b''
    write += BYTES_base_page_count  # 8 bytes
    write += BYTES_tail_page_count  # 8 bytes
    write += BYTES_base_pages      # num_bp * (8+4096)
    write += BYTES_tail_pages      # num_tp * (8+4096)

    return write

def encode_page(page: Page):
    if page == None:
        num_records = 0
        data = bytearray(4096)
    else:
        num_records = page.num_records
        data = page.data

    out = b''
    out += int_to_bytes(num_records)  # 8
    out += bytes(data)               # 4096

    return out

def decode_pagerange(BYTES_pr) -> PageRange:

    pr = PageRange()

    #
    # Read Meta
    #

    BYTES_base_page_count = BYTES_pr[0:8]  # 8 bytes
    BYTES_tail_page_count = BYTES_pr[8:16]  # 8 bytes

    pr.base_page_count = int_from_bytes(BYTES_base_page_count)
    pr.tail_page_count = int_from_bytes(BYTES_tail_page_count)

    #
    # Read Base Pages
    #

    bytes_page_size = 8 + 4096
    offset_end_base = 16

    for i in range(pr.base_page_count):
        start = 16 + (i * bytes_page_size)
        end = start + bytes_page_size
        offset_end_base = end  # so we know where the tails start
        BYTES_page = BYTES_pr[start:end]

        page = decode_page(BYTES_page)
        pr.base_pages[i] = page

    #
    # Read Tail Pages
    #

    for i in range(pr.tail_page_count):
        start = offset_end_base + (i * bytes_page_size)
        end = start + bytes_page_size
        BYTES_page = BYTES_pr[start:end]

        page = decode_page(BYTES_page)
        pr.tail_pages.append(page)

    return pr

def decode_page(BYTES_page) -> Page:
    page = Page(True)
    page.num_records = int_from_bytes(BYTES_page[0:8])
    page.data = BYTES_page[8:]
    return page

def fetch_pr_bytes(pr_idx):
    pass

def extract_page_data_from_pr(pr_bytes, inner_idx):

    max_base_pages = 16
    offset_start_bp = 16
    bytes_page_size = 8 + 4096
    
    if inner_idx < 16: # Getting base page
        pass
    else:
        pass