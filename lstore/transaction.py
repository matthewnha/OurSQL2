from table import Table, Record
from index import Index
import time
import threading
import util
from lock_manager import *


gathering_lock = threading.Lock()
x_queries = ["insert", "update", "delete", "increment"]
s_queries = ["select", "sum"]

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.timestamp = time.time()
        self.locks = []

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, *args):
        print('query name', query.__name__)
        query_name = query.__name__

        r = Resource() # todo: get proper resource

        if query_name in x_queries:
            self.locks.append(r.x_lock)
        elif query_name in s_queries:
            self.locks.append(r.s_lock)
        else:
            raise Exception("Unknown query")

        self.queries.append((query, args))

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
        return self.commit()
    
    def abort(self):
        #TODO: do roll-back and any other necessary operations
        return False

    def commit(self):
        # TODO: commit to database
        return True

