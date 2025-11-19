from __future__ import annotations

class QueryTree:
    def __init__(self, type: str, val: str = "", parent: QueryTree | None = None):
        self.type: str = type
        self.val: str = val
        self.childs: list[QueryTree] = []
        self.parent: QueryTree | None = parent
    def add_child(self, child_node: QueryTree):
        child_node.parent = self
        self.childs.append(child_node)
    def __repr__(self) -> str:
        return f"QueryTree(type='{self.type}', val='{self.val}')"
    
    def tree(self, prefix: str = "", is_last: bool = True) -> str:
        node_str = f"{self.type}"
        if self.val:
            node_str += f"(\"{self.val}\")"
        
        result = prefix
        if prefix:
            result += "└── " if is_last else "├── "
        result += node_str + "\n"
        # children
        for i, child in enumerate(self.childs):
            is_last_child = (i == len(self.childs) - 1)
            new_prefix = prefix + ("    " if is_last else "│   ")
            result += child.tree(new_prefix, is_last_child)
        return result

    def clone(self, deep: bool = False) -> QueryTree:
        """
        Clone node, dengan children jika True, tanpa children jika False
        """
        cloned_node = QueryTree(self.type, self.val)
        for child in self.childs:
            cloned_child = child.clone(deep) if deep else child
            cloned_node.add_child(cloned_child)
        return cloned_node
    
    def replace_child(self, old_child: QueryTree, new_child: QueryTree) -> bool:
        """
        Ganti child dengan node baru
        """
        index = self.childs.index(old_child)
        self.childs[index] = new_child
        new_child.parent = self
        old_child.parent = None
        return True
    
    def remove_child(self, child_node: QueryTree) -> bool:
        """
        Menghapus child
        """
        self.childs.remove(child_node)
        child_node.parent = None
        return True
    
    def traverse_preorder(self, visitor: callable) -> None:
        """
        Traversal pre-order, call visitor untuk setiap node (parent dulu)
        """
        visitor(self)
        for child in self.childs:
            child.traverse_preorder(visitor)
    
    def traverse_postorder(self, visitor: callable) -> None:
        """
        Traversal post-order, call visitor untuk setiap node (children dulu)
        """
        for child in self.childs:
            child.traverse_postorder(visitor)
        visitor(self)
    
    def find_nodes(self, predicate: callable) -> list[QueryTree]:
        """
        Mencari semua node yang memenuhi predicate
        """
        result = []
        if predicate(self):
            result.append(self)
        for child in self.childs:
            result.extend(child.find_nodes(predicate))
        return result
    
    def find_nodes_by_type(self, node_type: str) -> list[QueryTree]:
        """
        Mencari semua node berdasarkan tipe
        """
        return self.find_nodes(lambda node: node.type == node_type)
    
    def find_first_node(self, predicate: callable) -> QueryTree | None:
        """
        Mencari node pertama yang memenuhi predicate
        """
        if predicate(self):
            return self
        for child in self.childs:
            result = child.find_first_node(predicate)
            if result is not None:
                return result
        return None
    
    def get_parent(self) -> QueryTree | None:
        """
        Mengembalikan parent dari node
        """
        return self.parent
    
    def get_children(self) -> list[QueryTree]:
        """
        Mengembalikan salinan list children
        """
        return self.childs.copy()
    
    def get_child(self, index: int) -> QueryTree | None:
        """
        Mengembalikan child pada index tertentu
        """
        if 0 <= index < len(self.childs):
            return self.childs[index]
        return None
    
    def insert_between_child(self, child: QueryTree, new_node: QueryTree) -> bool:
        """
        Insert new_node antara parent dan child
        """
        index = self.childs.index(child)
        self.childs[index] = new_node
        new_node.parent = self
        new_node.add_child(child)
        child.parent = new_node
        return True
    
    def remove_node_keep_children(self) -> None:
        """
        Menghapus node tapi mempertahankan children-nya
        """
        if self.parent is None:
            raise ValueError("Tidak ada parent")
        
        parent = self.parent
        index = parent.childs.index(self)
        
        # Hapus node dari parent
        parent.childs.pop(index)
        
        # Tambah children ke parent di posisi yang sama
        for child in reversed(self.childs):
            parent.childs.insert(index, child)
            child.parent = parent
        
        # Clear children dari node yang dihapus
        self.childs = []
        self.parent = None
        return True
    
    ### Node checking ###
    def is_node_type(self, type: str) -> bool:
        """
        Cek apakah tipe node adalah type
        """
        return self.type == type

    def is_node_value(self, value: str) -> bool:
        """
        Cek apakah node memiliki value tertentu
        """
        return self.val == value