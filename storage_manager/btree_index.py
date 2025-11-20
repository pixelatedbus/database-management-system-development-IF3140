import os
import pickle

class BPlusTreeIndex: 
    def __init__(self, table_name : str, column_name : str, order=5):
        self.index = BPlusTree(order)
        self.table_name = table_name
        self.column_name = column_name

    def insert(self, key: str, record_id: int):
        # masukin key-value pair ke index
        self.index.insert(key, record_id)

    def search(self, key):
        # cari record_id yang match dengan key
        return self.index.search(key)
    
    def save(self, filepath: str):
        # save index ke binary file pake pickle
        dir_path = os.path.dirname(filepath)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)
        with open(filepath, 'wb') as f:
            pickle.dump(self.index, f)

    def load(self, filepath: str):
        # load index dari binary file
        # kalo file ga exist, bikin index kosong
        if not os.path.exists(filepath):
            self.index = BPlusTree(self.order)
            return
        with open(filepath, 'rb') as f:
            self.index = pickle.load(f)

class BPlusTreeNode:
    def __init__(self, order, leaf=False):
        self.order = order 
        self.leaf = leaf
        self.keys = [] # List of keys
        self.children = [] # List of children nodes or values for leaf nodes

class BPlusTree: 
    def __init__(self, order=5):
        self.root = BPlusTreeNode(order, leaf=True)
        self.order = order # Maximum number of children per internal node
    

    def search(self, key): # TODO: implement
        node = self._find_leaf(self.root, key)
        for i, k in enumerate(node.keys):
            if k == key:
                return node.children[i]
        return None

    def search_range(self, start_key, end_key): # TODO: implement
        node = self._find_leaf(self.root, start_key)
        results = []
        while node:
            for i, key in enumerate(node.keys):
                if start_key <= key <= end_key:
                    results.append(node.children[i])
                elif key > end_key:
                    return results
            if len(node.children) > len(node.keys):
                node = node.children[-1]
            else:
                break
        return results

    def insert(self, key: str, value: int):
        """
        param:
        key: str - The key to insert into the B+ tree.
        value: int - The value associated with the key."""
        node = self._find_leaf(self.root, key) # Locate the appropriate leaf node
        insert_pos = 0 # Find the position to insert the new key

        while insert_pos < len(node.keys) and node.keys[insert_pos] < key:
            insert_pos += 1
        node.keys.insert(insert_pos, key)
        node.children.insert(insert_pos, value)

        if len(node.keys) > self.order - 1:
            self._split_leaf(node)

    def _find_leaf(self, node, key):
        """
        Helper method to find the leaf node where a key should be located.
        param:
        node: BPlusTreeNode - The current node being examined.
        key: str - The key to locate.
        return: BPlusTreeNode - The46 leaf node where the key should be located.
         """
        if node.leaf:
            return node
        for i, item in enumerate(node.keys):
            if key < item:
                return self._find_leaf(node.children[i], key)
            
        return self._find_leaf(node.children[-1], key)
    
    def _split_leaf(self, node):
        mid = len(node.keys) // 2
        new_leaf = BPlusTreeNode(self.order, leaf=True)
        new_leaf.keys = node.keys[mid:]
        new_leaf.children = node.children[mid:]

        node.keys = node.keys[:mid]
        node.children = node.children[:mid]

        new_leaf.children.append(node.children[-1] if len(node.children) > len(node.keys) else None)
        node.children[-1] = new_leaf

        if node == self.root:
            new_root = BPlusTreeNode(self.order)
            new_root.keys = [new_leaf.keys[0]]
            new_root.children = [node, new_leaf]
            self.root = new_root
        else:
            self._insert_into_parent(node, new_leaf.keys[0], new_leaf)

    def _insert_into_parent(self, node, key, new_node):
        parent = self._find_parent(self.root, node)
        if not parent:
            new_root = BPlusTreeNode(self.order)
            new_root.keys = [key]
            new_root.children = [node, new_node]
            self.root = new_root
            return

        idx = parent.children.index(node)
        parent.keys.insert(idx, key)
        parent.children.insert(idx + 1, new_node)

        if len(parent.keys) > self.order - 1: # Internal node overflow
            self._split_internal(parent)
    
    def _split_internal(self, node): 
        mid = len(node.keys) // 2
        new_internal = BPlusTreeNode(self.order)
        new_internal.keys = node.keys[mid + 1:]
        new_internal.children = node.children[mid + 1:]
        up_key = node.keys[mid]
        node.keys = node.keys[:mid]
        node.children = node.children[:mid + 1]

        if node == self.root:
            new_root = BPlusTreeNode(self.order)
            new_root.keys = [up_key]
            new_root.children = [node, new_internal]
            self.root = new_root
        else:
            self._insert_into_parent(node, up_key, new_internal)

    def _find_parent(self, current, child):
        if current.leaf or current.children[0].leaf:
            return None
        for c in current.children:
            if c == child:
                return current
            res = self._find_parent(c, child)
            if res:
                return res
        return None