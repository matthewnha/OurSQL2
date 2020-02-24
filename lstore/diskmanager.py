


class DiskManager:

    def __init__(self):
        self.files = []
        self.tables = {}
        self.page_ranges = []
        self.metafile = ""
        pass

    def write_to_disk(self, path, page_pid):
        pass

    def load_from_disk(self, path, page_pid):
        pass

    def open_db(self, ):
        pass

    def close_db(self, ):
        pass

    def purge(self):
        pass

    def write_table_meta(self, table):
        pass

    def write_page_range(self, page_range,num):
        pass

    def write_page_range_meta(self, page_range,num):
        pass

    def import_from_disk(self):
        pass

    def import_page(self):
        pass

    def import_page_ranges(self):
        pass

    def import_table(self, table):
        pass

    def import_table_meta(self):
        pass

    def import_page_range_meta(self):
        pass
