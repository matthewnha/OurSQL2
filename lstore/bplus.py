class Node:
    
    def __init__(self):
        self.max
        self.left = None
        self.right = None

        self.index = None
        self.rid = None
        self.is_leaf = False
  

class BplusTree:

    def __init__(self, column):
        self.num_leaves = 0
        self.root = None
