from random import choice, randint, sample, seed
#basic tree

class Node:
    
    def __init__(self, nodeObjects):
        self.nodeObjects = nodeObjects
        self.isLeaf = True
        self.keys = []
        self.rids = []
    @property
    def isLeaf(self):
        return self.__isLeaf

    @isLeaf.setter
    def isLeaf(self, is_Leaf):
        self.__isLeaf = is_Leaf

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
        # if self.isLeaf == True:
        #     for i, item in enumerate(self.keys):
        #         if key == item:
        #             if rid == itemRID:
        #                 print("found RID")
        #                 del(self.rids[i][j]) #perform the delete
        #             if len(self.rids[i]) == 0: #if key is empty, remove key
        #                 del(self.keys[i])
        #                 self.rids.remove([])
        #                 #self.rids = self.rids[:i-1] + self.rids[i-1:]
        #                 print("removed key")
        #                 break
        #             else:
        #                 print("RID not found")
        #     print("keys: ", self.keys[i])
        #            # print("RID: ", self.rids[i][j])
        pass      
            
    def split(self):
    #split and send to child nodes
        #left and right inherit number of objects
        left = Node(self.nodeObjects) 
        right = Node(self.nodeObjects)
        mid = self.nodeObjects//2

        left.keys = self.keys[:mid]
        left.rids = self.rids[:mid]

        right.keys = self.keys[mid:]
        right.rids = self.rids[mid:]

        self.keys = [right.keys[0]]
        self.rids = [left, right]

        self.isLeaf = False

    def isFull(self):
        return len(self.keys) == self.nodeObjects

    def key_helper(self, bool, ret=""):
        if bool:
            for val in bool:
                if val:
                    ret += "|  "
                else:
                    ret += "   "
            
            if bool[-1]:
                ret += "|--" 
            else:
                ret += "`--"
        
        ret += str(self.keys) + "\n"

        if not self.isLeaf:

            for val in self.rids:

                if val != self.rids[-1]:
                    bool.append(True)

                else:
                    bool.append(False) 

                if isinstance(val,Node):
                    ret = val.key_helper(bool, ret) 
                    bool.pop()

        return ret





    def getKeys(self):
        bool = []
        to_print = self.key_helper(bool)
        return to_print
            


class BPlusTree:
    def __init__(self, nodeObjects = 4):
        self.root = Node(nodeObjects)

    def _find(self, node, key): #return index of key and its rids
        for i, item in enumerate(node.keys):
            if key < item:
                return node.rids[i], i
        
        return node.rids[i+1], i+1

    def find(self, node, key): #return index of key and its rids
        return self._find(node, key)

    def _merge(self, parent, child, index):
        parent.rids.pop(index)
        pivot = child.keys[0]
        parent.add(pivot,child.rids)
        # for i, item in enumerate(parent.keys):
        #     if pivot < item:
        #         parent.keys = parent.keys[:i] + [pivot] + parent.keys[i:]
        #         parent.rids = parent.rids[:i] + child.rids + parent.rids[i:]
        #         break
            
        #     elif i+1 == len(parent.keys):
        #         parent.keys+= [pivot]
        #         parent.rids += child.rids
        #         break

        
    def insert(self, key, rid):
        child = self.root
        parent = None
        while not child.isLeaf:
            parent = child
            child, index = self._find(child,key)

        child.add(key, rid)
        print("test")
        if child.isFull():
            child.split()
            print("split happened")
            if parent and not parent.isFull():
                print("At merge")
                self._merge(parent, child, index)


    def remove(self, key, rid):
        parents = []
        prev_node = self.root
        child = self.root

        while not child.isLeaf:

            child, index = self._find(child, key)
            parents.append((prev_node, index))
            prev_node = child

        for i, item in enumerate(child.keys):
            if key == item:
                for j, itemRID in enumerate(child.rids[i]):
                    if rid == itemRID:
                        print("found RID")
                        del(child.rids[i][j]) #perform the delete
                    if len(child.rids[i]) == 0: #if key is empty, remove key
                        del(child.keys[i])
                        child.rids.remove([])
                        if(len(child.keys) < child.nodeObjects/2):
                            self.balance(parents, child)
                        #self.rids = self.rids[:i-1] + self.rids[i-1:]
                    print("removed key")
                    break
                else:
                    print("RID: ", child.rids[i][j])

    def balance(self, parents, child):

        if parents:
            parent, index = parents.pop()
        else:
            return None

        sibling_index = index
        to_the_left = False
        if index < len(parent.keys)-1:
            sibling_index += 1
        else:
            sibling_index += -1
            to_the_left = True
        
        sibling = parent.rids[sibling_index]
        
        if len(sibling.keys) <= sibling.nodeObjects/2 + 1:
            for i, key in enumerate(sibling.keys):
                child.add(key, sibling.keys.pop(i))

            if to_the_left:
                del parent.keys[sibling_index]
            else:
                del parent.keys[index]

            del parent.rids[sibling_index]
            del sibling

            if len(parent.keys) < parent.nodeObjects//2:
                self.balance(parents, parent)
        else: 
            i = 0
            while i < len(sibling.keys) and len(child.keys) < child.nodeObjects/2:

                current_key = sibling.keys.pop(i)
                child.add(key, sibling.rids.pop(i))
            
            parent.keys[index] = current_key
    
                    




    def getRID(self, key):
        child = self.root
        while not child.isLeaf:
            child, index = self._find(child, key)

        for i, item in enumerate(child.keys):
            if key == item:
                return child.rids[i]

        return None
    

    def getKeys(self):
        return self.root.getKeys()


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
    seed(12345)
    bplustree = BPlustTree(4)
    print("Start demo")
    keys = []

    for i in range(64):
        key = randint(0, 9000)
        while key in keys:
            key = randint(0, 9000)

        bplustree.insert(key,i)
        print(key, "Inserted at", i)
        keys.append((key,i))

    print(bplustree.getKeys())

    deleted_keys = sample(keys, 2)
    for key in deleted_keys:
        bplustree.remove(key[0],key[1])
    
    print(bplustree.getKeys())

    while True:

        command = input("Enter a command: ")

        if command == 'exit':
            break
            
        elif command == 'insert':
            insertvalue = input("Enter key value: ")
            insertRID = input("Enter corresponding RID: ")
            while(1):
                try: 
                    insertvalue = int(insertvalue)
                    insertRID = int(insertRID)
                    break
                except ValueError:
                    insertvalue = input("Enter key as interger: ")
                    insertRID = input("Enter RID as interger: ")

            bplustree.insert(insertvalue, insertRID)
            print("inserted")

        elif command == 'remove':
            insertvalue = input("Enter key value: ")
            insertRID = input("Enter corresponding RID: ")
            while(1):
                try: 
                    insertvalue = int(insertvalue)
                    insertRID = int(insertRID)
                    break
                except ValueError:
                    insertvalue = input("Enter key as interger: ")
                    insertRID = input("Enter RID as interger: ")
            bplustree.remove(insertvalue, insertRID)
            print(bplustree.getRID(insertvalue))

        elif command == 'find':
            readRID = input("Enter key value to display RIDs: ")
            while(1):
                try: 
                    readRID = int(readRID)
                    break
                except ValueError:
                    readRID = input("Enter key value as integar to display RIDs: ")
            print(bplustree.getRID(readRID))
        
        elif command == 'keys':
            print(bplustree.getKeys())
        
        else:
            print("Not valid input")


        print("\n___________________________________________\n")
    
    


if __name__ == "__main__":
   # demo_node()
    #demo_tree()
    demo_treeNum()








