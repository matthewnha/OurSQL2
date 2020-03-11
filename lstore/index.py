from tree import *
from table import Table

"""
A data strucutre holding indices for various columns of a table. Key column should be indexed by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns
        self.table = table # type : Table

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column_idx, value):
        tree = self.indices[column_idx]
        if tree is None:
            raise Exception("This column is not indexed")

        return tree.get_rid(value)

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def is_indexed(self, column):
        return not (self.indices[column] == None)

    def locate_range(self, begin, end, column):
        tree = self.indices[column_idx]
        if tree is None:
            raise Exception("This column is not indexed")
        return tree.bulk_search(begin,end)

    def sum_range(self, begin, end, column):
        tree = self.indices[column_idx]
        if tree is None:
            raise Exception("This column is not indexed")
        return tree.sum_range(begin, end, column)

    def insert(self, val, rid, column_idx):
        tree = self.indices[column_idx]
        if tree is None:
            raise Exception("This column is not indexed")
        tree.insert(val, rid)

    def remove(self, column_idx, key, rid):
        tree = self.indices[column_idx]

        if tree is None:
            raise Exception("This column is not indexed")

        tree.remove(key, rid)

    def update_index(self, column_idx, key, new_key, rid):
        tree = self.indices[column_idx]

        if tree is None:
            raise Exception("This column is not indexed")

        tree.remove(key,rid)
        tree.insert(new_key,rid)

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        if column_number >= self.table.num_columns:
            print("Out of range")
            return None

        self.indices[column_number] = BPlusTree(16)

        table_keys = self.table.key_index.keys()
        table_rids = self.table.key_index.values()

        table_col = [None for a in self.table.num_columns]
        table_col[column_number] = 1

        for i in range(len(self.table.key_index)):
            value = table.select(table_keys[i],table_col)[0].column[column_number]
            self.indices[column_number].insert(value,table_rids[i])


    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.indices[column_number] = None

    def write_index(self):
        pass

    def read_index(self):
        pass
