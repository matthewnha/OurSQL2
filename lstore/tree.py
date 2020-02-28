
#basic tree

class Node(object):
    
    def __init__(self, nodeObjects):
        self.nodeObjects = nodeObjects
        self.isLeaf = True
        self.keys = []
        self.rids = []

    def add(self, key, rid):
        if not self.keys: #if keys is empty (root)
            self.keys.append(key)
            self.rids.append([rid])
            return None
        
        for i, item in enumerate(self.keys): #check for existing key in node
            if key == item:
                self.rids[i].append(rid) #if node exists add rid to rids[]
                break
            elif key < item: #if key is beginning of node and key doesnt already exist
                self.keys = self.keys[:i] + [key] + self.keys[i:] 
                self.rids = self.rids[:i] + [[rid]] + self.rids[i:]
                break
            elif i + 1 == len(self.keys): # check for end of node
                self.keys.append(key) #add key to end of node
                self.rids.append([rid])
                break
    
    def remove(self, key, rid):
        for i, item in enumerate(self.keys):
            print("keys: ", self.keys[i])
            if key == item:
                for j, itemRID in enumerate(self.rids[i]):
                   # print("RID: ", self.rids[i][j])
                    if rid == itemRID:
                        print("found RID")
                        del(self.rids[i][j]) #perform the delete
                        if len(self.rids[i]) == 0: #if key is empty, remove key
                            del(self.keys[i])
                            self.rids.remove([])
                            #self.rids = self.rids[:i-1] + self.rids[i-1:]
                            print("removed key")
                        break

                    else:
                        print("RID not found")
            else:
                print("Key not found")
            
    def split(self):
    #split and send to child nodes
        #left and right inherit number of objects
        left = Node(self.nodeObjects) 
        right = Node(self.nodeObjects)
        mid = self.nodeObjects/2

        left.keys = self.keys[:mid]
        left.rids = self.rids[:mid]

        right.keys = self.keys[mid:]
        right.rids = self.rids[mid:]

        self.keys = [right.keys[0]]
        self.rids = [left, right]
        self.isLeaf = False

    def isFull(self):
        len(self.keys) == self.nodeObjects

    def getKeys(self, counter = 0):
        print(counter)
        print(str(self.keys))

        if not self.isLeaf:
            for item in self.rids:
                item.getKeys(counter+1)


class BPlustTree(object):
    def __init__(self, nodeObjects = 4):
        self.root = Node(nodeObjects)

    def _find(self, node, key): #return index of key and its rids
        for i, item in enumerate(node.keys):
            if key < item:
                return node.rids[i], i
        
        return node.rids[i+1], i+1

    def _merge(self, parent, child, index):
        parent.rids.pop(index)
        pivot = child.keys[0]

        for i, item in enumerate(parent.keys):
            if pivot < item:
                parent.keys = parent.keys[:i] + [pivot] + parent.keys[i:]
                parent.rids = parent.rids[:i] + child.rids + parent.rids[i:]
                break
            
            elif i+1 == len(parent.keys):
                parent.keys+= [pivot]
                parent.rids += child.rids
                break

        
    def insert(self, key, rid):
        parent = None
        child = self.root
        
        while not child.isLeaf:
            parent = child
            child, index = self._find(child,key)

        child.add(key, rid)
        print("test")
        if child.isFull():
            child.split()
            print("split happened")
            if parent and not parent.isFull():
                self._merge(parent, child, index)

    def remove(self, key, rid):
        self.root.remove(key, rid)

    def getRID(self, key):
        child = self.root
        while not child.isLeaf:
            child, index = self._find(child, key)

        for i, item in enumerate(child.keys):
            if key == item:
                return child.rids[i]

        return None
    

    def getKeys(self):
        self.root.getKeys()


def demo_node():
    print("Creating node")
    node = Node(12)
    print("number of objects in nodes: ", node.nodeObjects)
    print("Add key a")
    node.add('a', 'alpha')


    print("check if node is full")
    node.isFull()
    node.getKeys()

    print("Adding keys b c d")
    node.add('b', 'bravo')
    node.add('c', 'charlie')
    node.add('d', 'delta')
    
    print("Check if node is full")
    #node.isFull()
    node.getKeys()
    print(node.isFull())
    #print(len(node.keys))
   # if (len(node.keys) >= 2):
    #    print("node full")

def demo_tree():
    print("Create tree")

    bplustree = BPlustTree(4)
    print("add one item to tree")
    bplustree.insert('a', 'alpha')
    bplustree.getKeys()
    bplustree.insert('b', 'bravo')
    bplustree.insert('c', 'charlie')
    bplustree.insert('d', 'delta')
    bplustree.getKeys()

    bplustree.insert('e', 'echo')
    bplustree.getKeys()
    print(bplustree.getRID('e'))

    bplustree.insert('f', 'foxtrot')
    bplustree.insert('g', 'golf')
    bplustree.insert('h', 'hotel')
    bplustree.getKeys()

    bplustree.insert('h', 'honey')
    bplustree.getKeys()
    print(bplustree.getRID('h'))


def demo_treeNum():
    bplustree = BPlustTree(4)
    print("Start demo")
    while True:
        command = input("Enter a command: ")

        if command == 'exit':
            break
            
        elif command == 'insert':
            insertvalue = input("Enter key value: ")
            insertRID = input("Enter corresponding RID: ")
            bplustree.insert(insertvalue, insertRID)
            print("inserted")

        elif command == 'remove':
            insertvalue = input("Enter key value: ")
            insertRID = input("Enter corresponding RID: ")
            bplustree.remove(insertvalue, insertRID)
            print(bplustree.getRID(insertvalue))

        elif command == 'find':
            readRID = input("Enter key value to display RIDs: ")
            print(bplustree.getRID(readRID))
        
        elif command == 'keys':
            bplustree.getKeys()
        
        else:
            print("Not valid input")


        print("\n___________________________________________\n")
    
    


if __name__ == "__main__":
   # demo_node()
    #demo_tree()
    demo_treeNum()








