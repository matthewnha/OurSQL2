# Global Setting for the Database
# PageSize, StartRID, etc..
from math import floor

BYTE_ORDER = 'little'
PAGE_RANGE_MAX_BASE_PAGES = 16 # max columns
CELLS_PER_PAGE = 512
CELL_SIZE_BYTES = 4096 // CELLS_PER_PAGE