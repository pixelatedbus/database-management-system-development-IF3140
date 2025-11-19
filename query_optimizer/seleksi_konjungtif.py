"""
Seleksi Konjungtif
"""

from query_optimizer.optimization_engine import ParsedQuery
from query_optimizer.query_tree import QueryTree


def seleksi_konjungtif(query: ParsedQuery) -> ParsedQuery:
    """Transformasi FILTER(AND) menjadi cascaded filters."""
    transformed_tree = seleksi_konjungtif_rec(query.query_tree)
    return ParsedQuery(transformed_tree, query.query)


def seleksi_konjungtif_rec(node: QueryTree) -> QueryTree:
    """Rekursif transformasi seleksi konjungtif."""
    if node is None:
        return None
    
    for i in range(len(node.childs)):
        node.childs[i] = seleksi_konjungtif_rec(node.childs[i])
    
    if node.is_node_type("FILTER") and node.is_node_value("AND"):
        return transform_and_filter(node)
    
    return node


def transform_and_filter(and_node: QueryTree) -> QueryTree:
    """Transformasi FILTER(AND) menjadi chain of cascaded filters."""
    num_children = len(and_node.childs)
    if num_children < 3:
        return and_node
    
    first_child = and_node.get_child(0)
    if first_child.is_node_type("FILTER"):
        return and_node
    
    source = first_child
    conditions = and_node.childs[1:]
    current = source
    
    for i in range(len(conditions) - 1, -1, -1):
        condition = conditions[i]
        
        if len(condition.childs) > 0:
            if condition.is_node_type("FILTER") and condition.val.startswith(("WHERE", "IN", "EXIST")):
                new_filter = QueryTree("FILTER", condition.val)
                new_filter.add_child(current)
            else:
                new_filter = update_filter_source(condition, current)
        else:
            new_filter = QueryTree("FILTER", condition.val)
            new_filter.add_child(current)
        
        current = new_filter
    
    return current


def update_filter_source(filter_node: QueryTree, new_source: QueryTree) -> QueryTree:
    """Update source dari filter node."""
    new_node = QueryTree(filter_node.type, filter_node.val)
    
    if filter_node.val in {"AND", "OR"} and len(filter_node.childs) > 0:
        first_child = filter_node.get_child(0)
        if not first_child.is_node_type("FILTER"):
            new_node.add_child(new_source)
            for i in range(1, len(filter_node.childs)):
                new_node.add_child(filter_node.childs[i])
        else:
            new_node.add_child(new_source)
            for child in filter_node.childs:
                new_node.add_child(child)
    else:
        new_node.add_child(new_source)
        for i in range(1, len(filter_node.childs)):
            new_node.add_child(filter_node.childs[i])
    
    return new_node


def is_conjunctive_filter(node: QueryTree) -> bool:
    """Cek apakah node adalah FILTER(AND)."""
    return node.is_node_type("FILTER") and node.is_node_value("AND")


def can_transform(node: QueryTree) -> bool:
    """Cek apakah FILTER(AND) bisa ditransformasi."""
    if not is_conjunctive_filter(node):
        return False
    
    if len(node.childs) < 3:
        return False
    
    first_child = node.get_child(0)
    if first_child.is_node_type("FILTER"):
        return False
    
    for i in range(1, len(node.childs)):
        child = node.get_child(i)
        if not child.is_node_type("FILTER"):
            return False
    
    return True

def cascade_filters(query: ParsedQuery, filter_order: list[int] | None = None) -> ParsedQuery:
    """Cascade AND filters dengan control urutan."""
    def cascade_rec(node: QueryTree) -> QueryTree:
        if node is None:
            return None
        
        for i in range(len(node.childs)):
            node.childs[i] = cascade_rec(node.childs[i])
        
        if is_conjunctive_filter(node) and len(node.childs) >= 3:
            first_child = node.get_child(0)
            if not first_child.is_node_type("FILTER"):
                return cascade_and_with_order(node, filter_order)
        
        return node
    
    transformed_tree = cascade_rec(query.query_tree)
    return ParsedQuery(transformed_tree, query.query)


def cascade_and_with_order(and_node: QueryTree, order: list[int] | None = None) -> QueryTree:
    """Cascade AND filter dengan urutan spesifik."""
    first_child = and_node.get_child(0)
    source = first_child
    conditions = and_node.childs[1:]
    
    if order is None:
        order = list(range(len(conditions)))[::-1]
    else:
        if len(order) != len(conditions):
            order = list(range(len(conditions)))[::-1]
    
    current = source
    for idx in order:
        if 0 <= idx < len(conditions):
            condition = conditions[idx]
            new_filter = QueryTree("FILTER", condition.val)
            new_filter.add_child(current)
            current = new_filter
    
    return current


def uncascade_filters(query: ParsedQuery) -> ParsedQuery:
    """Convert cascaded filters kembali ke bentuk AND."""
    def uncascade_rec(node: QueryTree) -> QueryTree:
        if node is None:
            return None
        
        for i in range(len(node.childs)):
            node.childs[i] = uncascade_rec(node.childs[i])
        
        if node.is_node_type("FILTER") and len(node.childs) == 1:
            filters = []
            current = node
            source = None
            
            while current is not None and current.is_node_type("FILTER"):
                if len(current.childs) == 1:
                    filters.append(current)
                    current = current.get_child(0)
                else:
                    break
            
            if len(filters) >= 2 and current is not None:
                source = current
                and_node = QueryTree("FILTER", "AND")
                and_node.add_child(source)
                
                for f in filters:
                    condition = QueryTree("FILTER", f.val)
                    and_node.add_child(condition)
                
                return and_node
        
        return node
    
    transformed_tree = uncascade_rec(query.query_tree)
    return ParsedQuery(transformed_tree, query.query)


def clone_tree(node: QueryTree) -> QueryTree:
    """Deep clone query tree."""
    if node is None:
        return None
    return node.clone(deep=True)
