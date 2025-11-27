from __future__ import annotations

class QueryTree:
    def __init__(self, type: str, val: str, children: list[QueryTree], parent: QueryTree = None):
        self.type = type
        self.val = val
        self.children = children
        self.parent = parent
        
class ParsedQuery:
    def __init__(self, query_tree: QueryTree, query: str):
        self.query_tree = query_tree
        self.query = query