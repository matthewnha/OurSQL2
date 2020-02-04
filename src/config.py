# Global Setting for the Database
# PageSize, StartRID, etc..

ByteOrder = 'little'
PageRangeMaxBasePages = 50 # max columns
PageRangeMaxSize = 200 # 150 tail pages possible
CellsPerPage = 500
CellSizeInBytes = round(4096 / CellsPerPage)