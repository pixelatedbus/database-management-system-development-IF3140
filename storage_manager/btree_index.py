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

    def delete(self, key, record_id: int):
        # delete key-value pair dari index
        self.index.delete(key, record_id)

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
            if len(node.children) > len(node.keys):
                node = node.children[-1]
            else:
                break

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
            if len(node.children) > len(node.keys):
                node = node.children[-1]
            else:
                break

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
            if len(node.children) > len(node.keys):
                node = node.children[-1]
            else:
                break

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
            if len(node.children) > len(node.keys):
                node = node.children[-1]
            else:
                break

        return results

    def _get_leftmost_leaf(self):
        # dapetin leftmost leaf node buat scanning
        node = self.index.root
        while not node.leaf:
            node = node.children[0]
        return node

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

    # ========== B+ Tree Deletion Operations ==========
    def delete(self, key):
        """
        Step by step:
        1. Find the leaf node containing the key.
        2. Remove the key and its associated data pointer from the leaf node.
        3. If the leaf node has enough keys, we're done.
        4. If the leaf node has too few keys, try to borrow a key from a sibling node.
        5. If borrowing is not possible, merge with a sibling node.
        """
        leaf = self._find_leaf_node(self.root, key)
        if not self._delete_from_leaf(leaf, key):
            return  # Key not found; nothing to delete
        if len(leaf.keys) < (self.order - 1) // 2:
            self._handle_underflow(leaf)

    # ========== B+ Tree Delete Helper ==========
    def _delete_from_leaf(self, leaf, key):
        if key in leaf.keys:
            index = leaf.keys.index(key)
            leaf.keys.pop(index)
            leaf.children.pop(index)
            return True
        return False
    
    def _find_sibling(self, parent, node):
        for i, child in enumerate(parent.children):
            if child == node:
                left_sibling = parent.children[i - 1] if i > 0 else None
                right_sibling = parent.children[i + 1] if i < len(parent.children) - 1 else None
                return left_sibling, right_sibling
        return None, None
    
    def _borrow_from_sibling(self, parent, node, sibling, is_left_sibling):
        if is_left_sibling:
            borrowed_key = sibling.keys.pop(-1)
            borrowed_child = sibling.children.pop(-1)
            node.keys.insert(0, borrowed_key)
            node.children.insert(0, borrowed_child)
            parent_key_index = parent.children.index(node) - 1
            parent.keys[parent_key_index] = node.keys[0]
        else:
            borrowed_key = sibling.keys.pop(0)
            borrowed_child = sibling.children.pop(0)
            node.keys.append(borrowed_key)
            node.children.append(borrowed_child)
            parent_key_index = parent.children.index(node)
            parent.keys[parent_key_index] = sibling.keys[0]

    def _merge_with_sibling(self, parent, node, sibling, is_left_sibling):
        if is_left_sibling:
            sibling.keys.extend(node.keys)
            sibling.children.extend(node.children)
            parent_key_index = parent.children.index(node) - 1
            parent.keys.pop(parent_key_index)
            parent.children.remove(node)
        else:
            node.keys.extend(sibling.keys)
            node.children.extend(sibling.children)
            parent_key_index = parent.children.index(sibling)
            parent.keys.pop(parent_key_index - 1)
            parent.children.remove(sibling)

    def _handle_underflow(self, node):
        parent = self._find_parent(self.root, node)
        if not parent:
            if len(node.keys) == 0 and not node.is_leaf:
                self.root = node.children[0]
            return
        left_sibling, right_sibling = self._find_sibling(parent, node)
        if left_sibling and len(left_sibling.keys) > (self.order - 1) // 2:
            self._borrow_from_sibling(parent, node, left_sibling, True)
        elif right_sibling and len(right_sibling.keys) > (self.order - 1) // 2:
            self._borrow_from_sibling(parent, node, right_sibling, False)
        else:
            if left_sibling:
                self._merge_with_sibling(parent, node, left_sibling, True)
            elif right_sibling:
                self._merge_with_sibling(parent, node, right_sibling, False)
        