from page import Page
from config import *

page = Page()

bytesToWrite = bytes([100])
if len(bytesToWrite) > CellSizeInBytes:
    raise Exception('Attempted to write bytes that exceeds specified cell size')

page.write(bytesToWrite)
page.write(bytesToWrite)
page.write(bytesToWrite)
page.write(bytesToWrite)

bytes = page.read(0)
print(int.from_bytes(bytes, byteorder="little"))