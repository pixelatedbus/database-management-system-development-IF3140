import os
import pickle

class BPlusTreeIndex:
    """
    B+ Tree Index wrapper untuk table indexing.
    Mendukung operations: insert, delete, search, range queries.
    """
    def __init__(self, table_name: str, column_name: str, order=5):
        self.order = order
        self.index = BPlusTree(order)
        self.table_name = table_name
        self.column_name = column_name

    def insert(self, key, record_id: int):
        """
        Insert key-value pair ke index.

        Args:
            key: Key untuk indexing (any comparable type)
            record_id: ID record dalam storage
        """
        self.index.insert(key, record_id)

    def delete(self, key, record_id: int):
        """
        Delete key-value pair dari index.

        Args:
            key: Key yang akan dihapus
            record_id: Record ID yang akan dihapus

        Returns:
            bool: True jika berhasil, False jika tidak ditemukan
        """
        return self.index.delete(key, record_id)

    def search(self, key):
        """
        Cari record_id yang match dengan key.

        Args:
            key: Key yang dicari

        Returns:
            list: List of record_ids (untuk konsistensi dengan HashIndex)
        """
        results = self.index.search(key)
        return results if results else []

    def search_range(self, start_key, end_key):
        """
        Cari record_ids dalam range [start_key, end_key] inclusive.

        Args:
            start_key: Batas bawah range
            end_key: Batas atas range

        Returns:
            list: List of record_ids dalam range
        """
        return self.index.search_range(start_key, end_key)

    def search_by_operation(self, operation: str, operand):
        """
        Search berdasarkan operation type.

        Args:
            operation: Operation type ('=', '<', '<=', '>', '>=')
            operand: Value untuk comparison

        Returns:
            list: List of record_ids yang memenuhi condition
        """
        if operation == '=':
            return self.search(operand)
        elif operation == '<':
            return self._scan_less_than(operand)
        elif operation == '<=':
            return self._scan_less_equal(operand)
        elif operation == '>':
            return self._scan_greater_than(operand)
        elif operation == '>=':
            return self._scan_greater_equal(operand)
        else:
            return []

    def _scan_less_than(self, target_key):
        """Scan semua keys < target_key."""
        results = []
        node = self._get_leftmost_leaf()

        while node:
            for i, key in enumerate(node.keys):
                if key < target_key:
                    # Support multiple values per key
                    if isinstance(node.children[i], list):
                        results.extend(node.children[i])
                    else:
                        results.append(node.children[i])
                else:
                    return results
            node = node.next

        return results

    def _scan_less_equal(self, target_key):
        """Scan semua keys <= target_key."""
        results = []
        node = self._get_leftmost_leaf()

        while node:
            for i, key in enumerate(node.keys):
                if key <= target_key:
                    if isinstance(node.children[i], list):
                        results.extend(node.children[i])
                    else:
                        results.append(node.children[i])
                else:
                    return results
            node = node.next

        return results

    def _scan_greater_than(self, target_key):
        """Scan semua keys > target_key."""
        results = []
        node = self._get_leftmost_leaf()

        while node:
            for i, key in enumerate(node.keys):
                if key > target_key:
                    if isinstance(node.children[i], list):
                        results.extend(node.children[i])
                    else:
                        results.append(node.children[i])
            node = node.next

        return results

    def _scan_greater_equal(self, target_key):
        """Scan semua keys >= target_key."""
        results = []
        node = self._get_leftmost_leaf()

        while node:
            for i, key in enumerate(node.keys):
                if key >= target_key:
                    if isinstance(node.children[i], list):
                        results.extend(node.children[i])
                    else:
                        results.append(node.children[i])
            node = node.next

        return results

    def _get_leftmost_leaf(self):
        """Get leftmost leaf node untuk scanning."""
        node = self.index.root
        while not node.leaf:
            node = node.children[0]
        return node

    def get_height(self) -> int:
        """
        Get height dari B+ tree.

        Returns:
            int: Height dari tree (root = 1, empty tree = 0)
        """
        if not self.index.root:
            return 0

        height = 1
        node = self.index.root
        while not node.leaf:
            height += 1
            node = node.children[0]

        return height

    def save(self, filepath: str):
        """
        Save index ke binary file menggunakan pickle.

        Args:
            filepath: Path untuk save file
        """
        dir_path = os.path.dirname(filepath)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)
        with open(filepath, 'wb') as f:
            pickle.dump(self.index, f)

    def load(self, filepath: str):
        """
        Load index dari binary file.

        Args:
            filepath: Path file yang akan di-load
        """
        if not os.path.exists(filepath):
            self.index = BPlusTree(self.order)
            return
        with open(filepath, 'rb') as f:
            self.index = pickle.load(f)


class BPlusTreeNode:
    """Node dalam B+ Tree."""
    def __init__(self, order, leaf=False):
        self.order = order
        self.leaf = leaf
        self.keys = []  # List of keys
        self.children = []  # List of children nodes atau values untuk leaf nodes
        self.next = None  # Pointer ke next leaf (untuk leaf nodes only)
        self.parent = None  # Pointer ke parent node (untuk efficient traversal)


class BPlusTree:
    """
    B+ Tree implementation dengan full support untuk:
    - Insert dengan automatic splitting
    - Delete dengan underflow handling dan rebalancing
    - Point search dan range search
    - Duplicate key handling
    """
    def __init__(self, order=5):
        self.root = BPlusTreeNode(order, leaf=True)
        self.order = order
        self.min_keys = (order - 1) // 2  # Minimum keys untuk internal nodes

    def search(self, key):
        """
        Cari record_ids berdasarkan key.
        Support duplicate keys.

        Args:
            key: Key yang dicari

        Returns:
            list: List of record_ids yang match dengan key
        """
        node = self._find_leaf(self.root, key)
        results = []

        for i, k in enumerate(node.keys):
            if k == key:
                # Support multiple values per key
                if isinstance(node.children[i], list):
                    results.extend(node.children[i])
                else:
                    results.append(node.children[i])

        return results

    def search_range(self, start_key, end_key):
        """
        Cari semua record_ids dalam range [start_key, end_key] inclusive.
        Menggunakan linked list untuk efficient traversal.

        Args:
            start_key: Batas bawah range
            end_key: Batas atas range

        Returns:
            list: Record_ids dalam range
        """
        node = self._find_leaf(self.root, start_key)
        results = []

        while node:
            for i, key in enumerate(node.keys):
                if start_key <= key <= end_key:
                    if isinstance(node.children[i], list):
                        results.extend(node.children[i])
                    else:
                        results.append(node.children[i])
                elif key > end_key:
                    return results

            node = node.next

        return results

    def insert(self, key, value: int):
        """
        Insert key-value pair ke B+ tree.
        Support duplicate keys dengan storing multiple values.

        Args:
            key: Key untuk insert (any comparable type)
            value: Record ID untuk insert
        """
        node = self._find_leaf(self.root, key)

        # Cari position untuk insert atau existing key
        insert_pos = 0
        while insert_pos < len(node.keys) and node.keys[insert_pos] < key:
            insert_pos += 1

        # Check if key already exists (untuk duplicate handling)
        if insert_pos < len(node.keys) and node.keys[insert_pos] == key:
            # Key exists, add value ke existing list
            if isinstance(node.children[insert_pos], list):
                node.children[insert_pos].append(value)
            else:
                # Convert ke list untuk support multiple values
                node.children[insert_pos] = [node.children[insert_pos], value]
        else:
            # New key, insert normally
            node.keys.insert(insert_pos, key)
            node.children.insert(insert_pos, value)

            # Check overflow
            if len(node.keys) > self.order - 1:
                self._split_leaf(node)

    def delete(self, key, value: int):
        """
        Delete entry dengan key dan value tertentu dari B+ tree.
        Handle underflow dan rebalancing dengan borrowing atau merging.

        Args:
            key: Key yang akan dihapus
            value: Value yang akan dihapus

        Returns:
            bool: True jika berhasil, False jika tidak ditemukan
        """
        node = self._find_leaf(self.root, key)

        # Cari key di leaf
        for i, k in enumerate(node.keys):
            if k == key:
                # Handle multiple values per key
                if isinstance(node.children[i], list):
                    if value in node.children[i]:
                        node.children[i].remove(value)
                        # Jika masih ada values lain, don't delete key
                        if len(node.children[i]) > 0:
                            return True
                        # Jika list kosong, delete key
                        node.keys.pop(i)
                        node.children.pop(i)
                    else:
                        return False
                elif node.children[i] == value:
                    node.keys.pop(i)
                    node.children.pop(i)
                else:
                    continue

                # Handle underflow (kecuali root)
                if node != self.root and len(node.keys) < self.min_keys:
                    self._handle_underflow(node)

                return True

        return False

    def _handle_underflow(self, node):
        """
        Handle underflow di node dengan borrowing dari sibling atau merging.

        Args:
            node: Node yang underflow
        """
        parent = node.parent
        if not parent:
            # Root underflow - allowed
            return

        # Cari index node di parent
        node_index = parent.children.index(node)

        # Try borrow from left sibling
        if node_index > 0:
            left_sibling = parent.children[node_index - 1]
            if len(left_sibling.keys) > self.min_keys:
                self._borrow_from_left(node, left_sibling, parent, node_index)
                return

        # Try borrow from right sibling
        if node_index < len(parent.children) - 1:
            right_sibling = parent.children[node_index + 1]
            if len(right_sibling.keys) > self.min_keys:
                self._borrow_from_right(node, right_sibling, parent, node_index)
                return

        # Cannot borrow, must merge
        if node_index > 0:
            # Merge with left sibling
            left_sibling = parent.children[node_index - 1]
            self._merge_nodes(left_sibling, node, parent, node_index - 1)
        else:
            # Merge with right sibling
            right_sibling = parent.children[node_index + 1]
            self._merge_nodes(node, right_sibling, parent, node_index)

    def _borrow_from_left(self, node, left_sibling, parent, node_index):
        """Borrow key dari left sibling."""
        if node.leaf:
            # Borrow dari leaf
            borrowed_key = left_sibling.keys.pop()
            borrowed_value = left_sibling.children.pop()

            node.keys.insert(0, borrowed_key)
            node.children.insert(0, borrowed_value)

            # Update parent key
            parent.keys[node_index - 1] = node.keys[0]
        else:
            # Borrow dari internal node
            parent_key = parent.keys[node_index - 1]
            borrowed_key = left_sibling.keys.pop()
            borrowed_child = left_sibling.children.pop()

            node.keys.insert(0, parent_key)
            node.children.insert(0, borrowed_child)
            borrowed_child.parent = node

            parent.keys[node_index - 1] = borrowed_key

    def _borrow_from_right(self, node, right_sibling, parent, node_index):
        """Borrow key dari right sibling."""
        if node.leaf:
            # Borrow dari leaf
            borrowed_key = right_sibling.keys.pop(0)
            borrowed_value = right_sibling.children.pop(0)

            node.keys.append(borrowed_key)
            node.children.append(borrowed_value)

            # Update parent key
            parent.keys[node_index] = right_sibling.keys[0]
        else:
            # Borrow dari internal node
            parent_key = parent.keys[node_index]
            borrowed_key = right_sibling.keys.pop(0)
            borrowed_child = right_sibling.children.pop(0)

            node.keys.append(parent_key)
            node.children.append(borrowed_child)
            borrowed_child.parent = node

            parent.keys[node_index] = borrowed_key

    def _merge_nodes(self, left_node, right_node, parent, left_index):
        """
        Merge right_node ke left_node.

        Args:
            left_node: Node sebelah kiri
            right_node: Node sebelah kanan
            parent: Parent dari kedua nodes
            left_index: Index left_node di parent
        """
        if left_node.leaf:
            # Merge leaf nodes
            left_node.keys.extend(right_node.keys)
            left_node.children.extend(right_node.children)
            left_node.next = right_node.next
        else:
            # Merge internal nodes
            # Include parent key yang memisahkan kedua nodes
            separator_key = parent.keys[left_index]
            left_node.keys.append(separator_key)
            left_node.keys.extend(right_node.keys)
            left_node.children.extend(right_node.children)

            # Update parent pointers
            for child in right_node.children:
                child.parent = left_node

        # Remove separator key dari parent
        parent.keys.pop(left_index)
        parent.children.pop(left_index + 1)

        # Handle parent underflow
        if parent == self.root:
            if len(parent.keys) == 0:
                # Root kosong, promote left_node jadi root
                self.root = left_node
                left_node.parent = None
        elif len(parent.keys) < self.min_keys:
            self._handle_underflow(parent)

    def _find_leaf(self, node, key):
        """
        Find leaf node where key should be located.

        Args:
            node: Current node
            key: Key yang dicari

        Returns:
            BPlusTreeNode: Leaf node untuk key
        """
        if node.leaf:
            return node

        for i, item in enumerate(node.keys):
            if key < item:
                return self._find_leaf(node.children[i], key)

        return self._find_leaf(node.children[-1], key)

    def _split_leaf(self, node):
        """
        Split leaf node yang overflow.

        Args:
            node: Leaf node yang akan di-split
        """
        mid = len(node.keys) // 2
        new_leaf = BPlusTreeNode(self.order, leaf=True)

        # Split keys and children
        new_leaf.keys = node.keys[mid:]
        new_leaf.children = node.children[mid:]

        node.keys = node.keys[:mid]
        node.children = node.children[:mid]

        # Setup linked list: node -> new_leaf -> node.next
        new_leaf.next = node.next
        node.next = new_leaf

        # Set parent
        new_leaf.parent = node.parent

        if node == self.root:
            # Root split, create new root
            new_root = BPlusTreeNode(self.order)
            new_root.keys = [new_leaf.keys[0]]
            new_root.children = [node, new_leaf]
            node.parent = new_root
            new_leaf.parent = new_root
            self.root = new_root
        else:
            # Push up smallest key dari new_leaf ke parent
            self._insert_into_parent(node, new_leaf.keys[0], new_leaf)

    def _insert_into_parent(self, left_node, key, right_node):
        """
        Insert key ke parent dan link ke new child node.

        Args:
            left_node: Node sebelah kiri
            key: Key yang akan di-push up
            right_node: Node baru sebelah kanan
        """
        parent = left_node.parent

        if not parent:
            # Create new root
            new_root = BPlusTreeNode(self.order)
            new_root.keys = [key]
            new_root.children = [left_node, right_node]
            left_node.parent = new_root
            right_node.parent = new_root
            self.root = new_root
            return

        # Find position untuk insert
        idx = parent.children.index(left_node)
        parent.keys.insert(idx, key)
        parent.children.insert(idx + 1, right_node)
        right_node.parent = parent

        # Check overflow di parent
        if len(parent.keys) > self.order - 1:
            self._split_internal(parent)

    def _split_internal(self, node):
        """
        Split internal node yang overflow.

        Args:
            node: Internal node yang akan di-split
        """
        mid = len(node.keys) // 2
        new_internal = BPlusTreeNode(self.order)

        # Split keys and children
        new_internal.keys = node.keys[mid + 1:]
        new_internal.children = node.children[mid + 1:]
        up_key = node.keys[mid]

        node.keys = node.keys[:mid]
        node.children = node.children[:mid + 1]

        # Update parent pointers
        for child in new_internal.children:
            child.parent = new_internal

        new_internal.parent = node.parent

        if node == self.root:
            # Root split
            new_root = BPlusTreeNode(self.order)
            new_root.keys = [up_key]
            new_root.children = [node, new_internal]
            node.parent = new_root
            new_internal.parent = new_root
            self.root = new_root
        else:
            # Push up ke parent
            self._insert_into_parent(node, up_key, new_internal)

    def print_tree(self, node=None, level=0):
        """
        Print tree structure untuk debugging.

        Args:
            node: Node yang akan di-print (default: root)
            level: Level depth untuk indentation
        """
        if node is None:
            node = self.root

        print("  " * level + f"Level {level}: {node.keys} {'(leaf)' if node.leaf else '(internal)'}")

        if not node.leaf:
            for child in node.children:
                self.print_tree(child, level + 1)