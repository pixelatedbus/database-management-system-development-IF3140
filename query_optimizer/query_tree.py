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
        
        for i, child in enumerate(self.childs):
            is_last_child = (i == len(self.childs) - 1)
            new_prefix = prefix + ("    " if is_last else "│   ")
            result += child.tree(new_prefix, is_last_child)
        return result
    
    def is_node_type(self, type: str) -> bool:
        """Cek apakah type node sesuai."""
        return self.type == type

    def is_node_value(self, value: str) -> bool:
        """Cek apakah value node sesuai."""
        return self.val == value

    def get_child(self, index: int) -> QueryTree | None:
        """Ambil child ke-i."""
        if 0 <= index < len(self.childs):
            return self.childs[index]
        return None

    def clone(self) -> QueryTree:
        """Deep clone query tree."""
        new_node = QueryTree(self.type, self.val)
        for child in self.childs:
            new_node.add_child(child.clone())
        return new_node
