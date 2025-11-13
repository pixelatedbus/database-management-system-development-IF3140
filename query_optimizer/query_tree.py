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
