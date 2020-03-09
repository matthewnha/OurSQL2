from random import choice, randint, sample, seed
#basic tree

class Node():
    
    def __init__(self, nodeObjects):
        self.nodeObjects = nodeObjects
        self.isLeaf = True
        self.left = None
        self.right = None
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
        
        if self.isLeaf:
            rid = [rid]
            
        for i, item in enumerate(self.keys): #check for existing key in node
            if key == item:
                self.rids[i].append(rid) #if node exists add rid to rids[]
                break
            elif key < item: #if key is beginning of node and key doesnt already exist
                self.keys = self.keys[:i] + [key] + self.keys[i:] 
                self.rids = self.rids[:i] + [rid] + self.rids[i:]
                break
            elif i + 1 == len(self.keys): # check for end of node
                self.keys.append(key) #add key to end of node
                self.rids.append(rid)
                break
        
    
    def remove(self, key, rid):

        for i, item in enumerate(self.keys):
            if key == item:
                for j, itemRID in enumerate(self.rids[i]):
                    if rid == itemRID:
                        del(self.rids[i][j]) #perform the delete

                        if len(self.rids[i]) == 0: #if key is empty, remove key
                            del(self.keys[i])
                            self.rids.remove([]) 
                        
                        break
                break
        pass      

    def update_keys(self):
        if self.isLeaf:
            print("HMMMMM")
            return None
        for i in range(1,len(self.rids)):
            if i > len(self.keys):
                self.keys.append(self.rids[i].keys[0])
            else:
                if self.keys[i-1] != self.rids[i].keys[0]:
                    self.keys[i-1] = self.rids[i].keys[0]

    def split(self):
    #split and send to child nodes
        #left and right inherit number of objects
        left = Node(self.nodeObjects) 
        right = Node(self.nodeObjects)
        mid = (self.nodeObjects + 1)//2

        left.keys = self.keys[:mid]
        right.keys = self.keys[mid:]

        if self.isLeaf == False:
            left.isLeaf = False
            right.isLeaf = False
            if self.nodeObjects % 2 == 0:
                left.rids = self.rids[:mid +1]
                right.rids = self.rids[mid + 1:]
                self.keys = [right.keys.pop(0)]
            else:
                left.rids = self.rids[:mid]
                right.rids = self.rids[mid:]
                self.keys = [left.keys[-1]]

                
        else:        
            left.rids = self.rids[:mid]
            right.rids = self.rids[mid:]
            self.keys = [right.keys[0]]


        self.rids = [left, right]

        self.isLeaf = False

    def isFull(self):
        return len(self.keys) == self.nodeObjects + 1

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
            



class BPlusTree(object):
    def __init__(self, nodeObjects = 4):
        self.root = Node(nodeObjects)

    def _find(self, node, key): #return index of key and its rids
        
        for i, item in enumerate(node.keys):
            if key < item:
                return node.rids[i], i
                
        if i < len(node.rids) - 1:
            return node.rids[i+1], i+1
        else:
            return None, i


    def find(self, node, key): #return index of key and its rids
        return self._find(node, key)

    def _merge(self, parents, child):

        if parents:
            parent, index = parents.pop()
        else:
            return None

        parent.rids.pop(index)
        pivot = child.keys[0]
        
        for i, item in enumerate(parent.keys):

            if pivot < item:
                parent.keys = parent.keys[:i] + [pivot] + parent.keys[i:]
                left = parent.rids[:i]
                right = parent.rids[i:]

                for rid in child.rids: 
                    left.append(rid)
                parent.rids = left + right
                break
            
            elif i+1 == len(parent.keys):

                parent.keys.append(pivot)

                for rid in child.rids:
                    parent.rids.append(rid)
                break
        
        if parent.isFull():

            parent.split()
            if parents and not parents[-1][0].isFull():
                # print("At merge")
                self._merge(parents, parent)

            else:
                print(self.getKeys())
                if parent == self.root:
                    print("At root")
         
        
    def insert(self, key, rid):

        child = self.root
        prev_node = self.root
        parents = []

        while not child == None and not child.isLeaf:

            child, index = self._find(child, key)
            parents.append((prev_node, index))
            prev_node = child

        if child == None:
            parent = parents[-1][0]
            child = Node(parent.nodeObjects)
            child.add(key,rid)
            parent.rids.append(child)
        else:
            child.add(key, rid)

        # print("test")
        if child.isFull():
            child.split()
            # print("split happened")
            if parents and not parents[-1][0].isFull():
                # print("At merge")
                self._merge(parents, child)



    def remove(self, key, rid):
        parents = []
        prev_node = self.root
        child = self.root

        while not child == None and not child.isLeaf:

            child, index = self._find(child, key)
            parents.append((prev_node, index))
            prev_node = child

        if child != None:
            child.remove(key,rid)
            
            if(len(child.keys) < child.nodeObjects/2):
                # print(self.getKeys())
                self._balance(parents, child)
                # print(self.getKeys())

        return None
                        # print("RID: ", child.rids[i][j])

    def _balance(self, parents, child):

        if parents:
            parent, index = parents.pop()
        else:
            print("EHHHHHHH")
            return None

        sibling_index = index
        to_the_left = False

        if index == len(parent.keys):
            index += -1
            sibling_index += -1
            to_the_left = True
        else:
            if parent.nodeObjects % 2 == 0:
                sibling_index += 1
            elif len(parent.keys) == len(parent.rids) and index == len(parent.keys) - 1:
                index += -1
                sibling_index += -1
                to_the_left = True
            else:
                sibling_index += 1
                
        
        sibling = parent.rids[sibling_index]

        is_Leafs = False
        if sibling.isLeaf and child.isLeaf:
            is_Leafs = True
        
        if len(sibling.keys) <= (sibling.nodeObjects + 1)/2:
            
            i = 0
            while sibling.keys or sibling.rids:
                if is_Leafs:
                    child.add(sibling.keys.pop(0), sibling.rids.pop(0)[0])
                else:
                    if sibling.keys:
                        child.add(sibling.keys.pop(0), sibling.rids.pop(0))
                    else:
                        if to_the_left:
                            child.rids.insert(i,sibling.rids.pop(0))
                        else:
                            child.rids.append(sibling.rids.pop(0))
                i += 1

            if not child.isLeaf and child.rids[0].isLeaf:
                child.update_keys()

            del parent.rids[sibling_index]
            
            if to_the_left:
                del parent.keys[sibling_index]
            else:
                del parent.keys[index]

            if parent.rids[0].isLeaf:
                parent.update_keys()

            del sibling

            if parent != self.root and len(parent.keys) < parent.nodeObjects/2 :
                self._balance(parents, parent)
            elif parent == self.root and not parent.keys:
                print("At root")
                print(self.getKeys())
                if not parent.keys:
                    self.root = parent.rids[0]
                    print(self.getKeys())

        else:
            i = 0
            while i < len(sibling.keys) and len(child.keys) < child.nodeObjects/2:
                if to_the_left:
                    current_key = sibling.keys.pop()
                    if is_Leafs:
                        child.add(current_key, sibling.rids.pop()[0])
                    else:
                        child.add(current_key, sibling.rids.pop())
                else:
                    current_key = sibling.keys.pop(i)
                    if is_Leafs:
                        child.add(current_key, sibling.rids.pop(i)[0])
                    else:
                        child.add(current_key, sibling.rids.pop(i))

            if parent.rids[0].isLeaf:
                parent.update_keys()
            else:
                if to_the_left:
                    parent.keys[index] = child.keys[0]
                else:
                    parent.keys[index] = child.keys[-1]

            if not child.isLeaf and child.rids[0].isLeaf:
                child.update_keys()
            
                      

    

    def getRID(self, key):
        child = self.root

        while not child == None and not child.isLeaf:
            child, index = self._find(child, key)

        if child != None:
            for i, item in enumerate(child.keys):
                if key == item:
                    return child.rids[i]

        return None
    

    def getKeys(self):
        return self.root.getKeys()


def demo_treeNum():
    # seed(123245)
    bplustree = BPlustTree(5)
    print("Start demo")
    keys = []

    for i in range(16):
        key = randint(0, 9000)
        while key in keys:
            key = randint(0, 9000)

        bplustree.insert(key,i)
        print(key, "Inserted at", i)
        keys.append((key,i))

    print(bplustree.getKeys())

    deleted_keys = sample(keys, 2)
    
    for key in deleted_keys:
        print(key)
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
    

def demo_tree():
    seed(12345)
    bplustree = BPlusTree(9)
    print("Start demo")
    keys = []

    for i in range(200):
        key = randint(0, 9000)
        while key in keys:
            key = randint(0, 9000)

        bplustree.insert(key,i)
        print(key, "Inserted at", i)
        keys.append((key,i))

    print(bplustree.getKeys())

    for key in keys:
        rid = bplustree.getRID(key[0])

        if rid[0] == key[1]:
            pass
            # print("Successful find")
        else:
            print("Error of key", key[0],"Rid is",key[1],"should be",rid[0])  


    deleted_keys = sample(keys, 100)
    for i, key in enumerate(deleted_keys):
        print(key)
        # if (i > 35 and i < 40):
        #     print(bplustree.getKeys())

        bplustree.remove(key[0],key[1])
        # if (i > 35 and i < 40):
        #     print(bplustree.getKeys())
    
    print(bplustree.getKeys())

    for key in keys:
        rid = bplustree.getRID(key[0])
        if rid == None:
            if key in deleted_keys:
                print("Key successful deleted")
            else: 
                print("Key should be here", key)
        elif rid[0] == key[1]:
            if key in deleted_keys:
                print("Key should be deleted", key)
                print(bplustree.getKeys())
            # print("Successful find")
        else:
            print("Error of key", key[0],"Rid is",rid,"should be",key[1])  

if __name__ == "__main__":
   # demo_node()
    demo_tree()
    #demo_treeNum()








