from table import Table
from config import *
from diskmanager import DiskManager

class Database():

    def __init__(self):
        self.tables = {}
        self.my_manager = DiskManager("database_files")
        self.my_manager.my_database = self
        pass

    def open(self, path):
        DiskManager.open_db()
        pass

    def close(self):
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def create_table(self, name, num_columns, key):
        table = Table(name, num_columns, key)
        self.tables[name] = table
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        pass
    
    """
     # Returns table with the passed name
    """

    def get_table(self, name):
        pass
