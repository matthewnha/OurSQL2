from config import *
from page import Page

# PageRange:
#   Pages = list of size 12
#   baseRecordCount = 0
#   tailRecords = 0
#   createRecord()
  
class PageRange:

    def __init__(self):
        self.pages = [None for _ in range(PageRangeMaxSize)]
        self.basePageCount = 0
        self.tailPageCount = 0

    def createBasePage(self):
        if self.basePageCount >= PageRangeMaxBasePages:
            return None

        pageIndex = self.basePageCount
        newPage = Page()
        self.pages[pageIndex] = newPage

        self.basePageCount += 1

        return (pageIndex, newPage)

    def createTailPage(self):
        if self.basePageCount >= PageRangeMaxBasePages:
            return None

        pageIndex = self.tailPageCount
        newPage = Page()
        self.pages[pageIndex] = newPage

        self.tailPageCount += 1

        return (pageIndex, newPage)

    def getPage(self, pageIndex):
        return self.pages[pageIndex]