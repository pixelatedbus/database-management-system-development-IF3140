import os
import pickle

class BPlusTreeIndex:
    def __init__(self, table_name : str, column_name : str, order=5):
        self.order = order
        self.index = BPlusTree(order)
        self.table_name = table_name
        self.column_name = column_name

    def insert(self, key, record_id: int):
        # masukin key-value pair ke index
        # key bisa any type (int, float, str) biar comparison work correctly
        self.index.insert(key, record_id)

    def search(self, key):
        # cari record_id yang match dengan key (untuk '=')
        # return list buat konsistensi dengan HashIndex
        result = self.index.search(key)
        return [result] if result is not None else []

    def search_range(self, start_key, end_key):
        # cari record_ids dalam range [start_key, end_key] inclusive
        return self.index.search_range(start_key, end_key)

    def search_by_operation(self, operation: str, operand):
        # search berdasarkan operation type
        # support: '=', '<', '<=', '>', '>='

        if operation == '=':
            return self.search(operand)
        elif operation == '<':
            # semua key < operand
            return self._scan_less_than(operand)
        elif operation == '<=':
            # semua key <= operand
            return self._scan_less_equal(operand)
        elif operation == '>':
            # semua key > operand
            return self._scan_greater_than(operand)
        elif operation == '>=':
            # semua key >= operand
            return self._scan_greater_equal(operand)
        else:
            return []

    def _scan_less_than(self, target_key):
        # scan semua keys < target_key
        # traverse dari leftmost leaf sampai ketemu target
        results = []
        node = self._get_leftmost_leaf()

        while node:
            for i, key in enumerate(node.keys):
                if key < target_key:
                    results.append(node.children[i])
                else:
                    return results

            # next leaf via linked list
            node = node.next

        return results

    def _scan_less_equal(self, target_key):
        # scan semua keys <= target_key
        results = []
        node = self._get_leftmost_leaf()

        while node:
            for i, key in enumerate(node.keys):
                if key <= target_key:
                    results.append(node.children[i])
                else:
                    return results

            # next leaf
            node = node.next

        return results

    def _scan_greater_than(self, target_key):
        # scan semua keys > target_key
        # find first key > target, then scan ke kanan
        results = []
        node = self._get_leftmost_leaf()

        started = False
        while node:
            for i, key in enumerate(node.keys):
                if key > target_key:
                    started = True
                    results.append(node.children[i])
                elif started:
                    # udah lewat range
                    pass

            # next leaf
            node = node.next

        return results

    def _scan_greater_equal(self, target_key):
        # scan semua keys >= target_key
        results = []
        node = self._get_leftmost_leaf()

        while node:
            for i, key in enumerate(node.keys):
                if key >= target_key:
                    results.append(node.children[i])

            # next leaf
            node = node.next

        return results

    def _get_leftmost_leaf(self):
        # dapetin leftmost leaf node buat scanning
        node = self.index.root
        while not node.leaf:
            node = node.children[0]
        return node

    def get_height(self) -> int:
        """Get tinggi B+ tree."""
        return self.index.get_height()

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
        self.next = None # Pointer to next leaf (for leaf nodes only)

class BPlusTree: 
    def __init__(self, order=5):
        self.root = BPlusTreeNode(order, leaf=True)
        self.order = order # Maximum number of children per internal node
    

    def search(self, key): 
        """Cari record_id berdasarkan key."""
        node = self._find_leaf(self.root, key)
        for i, k in enumerate(node.keys):
            if k == key:
                return node.children[i]
        return None

    def search_range(self, start_key, end_key):
        """Cari semua record_ids dalam range [start_key, end_key] menggunakan linked list."""
        # cari node yang berisi start_key
        node = self._find_leaf(self.root, start_key)
        results = []
        
        # traverse leaf nodes menggunakan linked list
        while node:
            for i, key in enumerate(node.keys):
                if start_key <= key <= end_key:
                    results.append(node.children[i])
                elif key > end_key:
                    return results
            
            # pindah ke next leaf
            node = node.next
            
        return results

    def get_height(self) -> int:
        """
        Hitung tinggi B+ tree (jumlah level dari root ke leaf).
        Tinggi 1 = cuma root (yang juga leaf).
        """
        if self.root is None:
            return 0
        
        height = 1
        node = self.root
        
        # traverse dari root ke leaf untuk hitung tinggi
        while not node.leaf:
            height += 1
            node = node.children[0]  # ambil leftmost child
        
        return height

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
        Helper method untuk cari leaf node dimana key seharusnya berada.
        
        param:
        node: BPlusTreeNode - The current node being examined.
        key: str - The key to locate.
        return: BPlusTreeNode - The leaf node where the key should be located.
        """
        if node.leaf:
            return node
        for i, item in enumerate(node.keys):
            if key < item:
                return self._find_leaf(node.children[i], key)
            
        return self._find_leaf(node.children[-1], key)
    
    def _split_leaf(self, node):
        """
        Split leaf node yang udah penuh.
        Bagi keys dan values jadi 2 bagian, setup linked list pointer.
        """
        mid = len(node.keys) // 2
        new_leaf = BPlusTreeNode(self.order, leaf=True)
        
        # split keys and children (values)
        new_leaf.keys = node.keys[mid:]
        new_leaf.children = node.children[mid:]
        
        node.keys = node.keys[:mid]
        node.children = node.children[:mid]
        
        # setup linked list pointer: node -> new_leaf -> node.next
        new_leaf.next = node.next
        node.next = new_leaf

        if node == self.root:
            # Kalo root yang split, bikin root baru
            new_root = BPlusTreeNode(self.order)
            new_root.keys = [new_leaf.keys[0]]
            new_root.children = [node, new_leaf]
            self.root = new_root
        else:
            # Push up smallest key dari new_leaf ke parent
            self._insert_into_parent(node, new_leaf.keys[0], new_leaf)

    def _insert_into_parent(self, node, key, new_node):
        """
        Insert key dan new_node ke parent dari node.
        Kalo parent juga overflow, recursively split.
        """
        parent = self._find_parent(self.root, node)
        if not parent:
            # Node gaada parent, bikin root baru
            new_root = BPlusTreeNode(self.order)
            new_root.keys = [key]
            new_root.children = [node, new_node]
            self.root = new_root
            return

        # Insert key dan new_node ke parent
        idx = parent.children.index(node)
        parent.keys.insert(idx, key)
        parent.children.insert(idx + 1, new_node)

        if len(parent.keys) > self.order - 1: 
            # Internal node overflow, perlu split
            self._split_internal(parent)
    
    def _split_internal(self, node):
        """
        Split internal node yang udah penuh.
        Push up middle key ke parent (beda dari leaf yang copy up).
        """
        mid = len(node.keys) // 2
        new_internal = BPlusTreeNode(self.order)
        new_internal.keys = node.keys[mid + 1:]
        new_internal.children = node.children[mid + 1:]
        up_key = node.keys[mid]
        node.keys = node.keys[:mid]
        node.children = node.children[:mid + 1]

        if node == self.root:
            # Root yang split, bikin root baru
            new_root = BPlusTreeNode(self.order)
            new_root.keys = [up_key]
            new_root.children = [node, new_internal]
            self.root = new_root
        else:
            # Push up ke parent
            self._insert_into_parent(node, up_key, new_internal)

    def _find_parent(self, current, child):
        """
        Cari parent node dari child node.
        Return None kalo child adalah root atau tidak ditemukan.
        """
        # Base case: root atau leaf tidak punya children
        if current.leaf:
            return None
        
        # Check apakah child ada di children current node
        if child in current.children:
            return current
        
        # Recursively search di subtrees (cuma internal nodes)
        for c in current.children:
            if not c.leaf:  # hanya search di internal nodes
                res = self._find_parent(c, child)
                if res:
                    return res
        
        return None