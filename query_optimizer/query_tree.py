from __future__ import annotations

# Operator types:
# UNARY operators (1 child)
# BINARY operators (2 children)
# LEAF nodes (0 children)
# SPECIAL: UPDATE, INSERT, DELETE, BEGIN_TRANSACTION, COMMIT

UNARY_OPERATORS = {
    "PROJECT",    # π - projection (SELECT columns)
    "FILTER",     # σ - selection (WHERE condition)
    "SORT",       # ORDER BY
}

BINARY_OPERATORS = {
    "JOIN",       # ⋈ - join (butuh 2 relasi)
}

LEAF_NODES = {
    "RELATION",   # Base table/relation
    "LIMIT",      # LIMIT value
}

SPECIAL_OPERATORS = {
    "UPDATE",     # DML operation
    "INSERT",     # DML operation
    "DELETE",     # DML operation
    "BEGIN_TRANSACTION",  # Transaction start
    "COMMIT",             # Transaction commit
}

VALID_JOIN_TYPES = {"NATURAL", "ON"}

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
