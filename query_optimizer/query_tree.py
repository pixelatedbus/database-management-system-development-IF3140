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

    def clone(self) -> QueryTree:
        cloned_node = QueryTree(self.type, self.val)
        for child in self.childs:
            cloned_child = child.clone()
            cloned_node.add_child(cloned_child)
        return cloned_node
    
    def remove_child(self, child_node: QueryTree) -> None:
        self.childs.remove(child_node)
        child_node.parent = None

    def replace_node(self, new_node: QueryTree) -> None:
        if self.parent is not None:
            index = self.parent.childs.index(self)
            self.parent.childs[index] = new_node
            new_node.parent = self.parent
            self.parent = None
        # else: do nothing
    
    def get_children(self) -> list[QueryTree]:
        return self.childs.copy()
    
    def traverse_preorder(self) -> list[QueryTree]:
        nodes = [self]
        for child in self.childs:
            nodes.extend(child.traverse_preorder())
        return nodes
    
    def traverse_postorder(self) -> list[QueryTree]:
        nodes = []
        for child in self.childs:
            nodes.extend(child.traverse_postorder())
        nodes.append(self)
        return nodes
    
    def find_nodes_by_type(self, type: str) -> list[QueryTree]:
        return [node for node in self.traverse_preorder() if node.type == type]