# Global Setting for the Database
# PageSize, StartRID, etc..
from math import floor

# Page/pagerange data
BYTE_ORDER = 'little'
PAGE_RANGE_MAX_BASE_PAGES = 16 # max columns
BLOCKS_PER_PAGE = 512
CELLS_PER_PAGE = BLOCKS_PER_PAGE - 1
PAGE_SIZE = 4096
CELL_SIZE_BYTES = 4096 // BLOCKS_PER_PAGE

# Meta columns
INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
START_USER_DATA_COLUMN = 4

RESERVED_TID = 2**64 - 1

# Bufferpool
MAX_POOL_PAGES = 250

# Encoding
SIZE_ENCODED_PAGE = 8 + PAGE_SIZE

PR_META_OFFSETS = [
    0, # BP_NUM
    8, # TP_NUM
    16, # END
]