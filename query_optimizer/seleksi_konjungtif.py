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
    
    # Transform OPERATOR_S(AND) menjadi cascaded filters
    if node.is_node_type("OPERATOR_S") and node.is_node_value("AND"):
        return transform_and_filter(node)
    
    return node


def transform_and_filter(and_node: QueryTree) -> QueryTree:
    """Transformasi OPERATOR_S(AND) menjadi chain of cascaded filters."""
    num_children = len(and_node.childs)
    if num_children < 3:
        return and_node
    
    # OPERATOR_S structure: child[0] = source, child[1+] = conditions
    source = and_node.get_child(0)
    conditions = and_node.childs[1:]
    current = source
    
    # Build cascaded filters from bottom to top (reverse order)
    for i in range(len(conditions) - 1, -1, -1):
        condition = conditions[i]
        
        # Jika condition adalah FILTER leaf (0 children), buat FILTER baru dengan current sebagai child
        if len(condition.childs) == 0:
            new_filter = QueryTree("FILTER", condition.val)
            new_filter.add_child(current)
        # Jika condition adalah OPERATOR/OPERATOR_S, update source-nya
        elif condition.type in {"OPERATOR", "OPERATOR_S"}:
            new_filter = update_filter_source(condition, current)
        # Jika condition adalah FILTER dengan children, clone dan update source
        else:
            new_filter = QueryTree("FILTER", condition.val)
            new_filter.add_child(current)
            # Copy children lainnya jika ada (untuk IN/EXIST pattern)
            for j in range(1, len(condition.childs)):
                new_filter.add_child(condition.childs[j])
        
        current = new_filter
    
    return current


def update_filter_source(operator_node: QueryTree, new_source: QueryTree) -> QueryTree:
    """Update source dari OPERATOR_S node."""
    new_node = QueryTree(operator_node.type, operator_node.val)
    
    # OPERATOR_S: replace child[0] dengan new_source, keep child[1+]
    if operator_node.type == "OPERATOR_S":
        new_node.add_child(new_source)
        for i in range(1, len(operator_node.childs)):
            new_node.add_child(operator_node.childs[i])
    # OPERATOR: convert to OPERATOR_S dengan new_source
    elif operator_node.type == "OPERATOR":
        new_node = QueryTree("OPERATOR_S", operator_node.val)
        new_node.add_child(new_source)
        for child in operator_node.childs:
            new_node.add_child(child)
    # FILTER: keep as is dengan new_source
    else:
        new_node.add_child(new_source)
        for i in range(1, len(operator_node.childs)):
            new_node.add_child(operator_node.childs[i])
    
    return new_node


def is_conjunctive_filter(node: QueryTree) -> bool:
    """Cek apakah node adalah OPERATOR_S(AND) dengan >= 3 children."""
    return node.is_node_type("OPERATOR_S") and node.is_node_value("AND") and len(node.childs) >= 3


def can_transform(node: QueryTree) -> bool:
    """Cek apakah OPERATOR_S(AND) bisa ditransformasi."""
    if not is_conjunctive_filter(node):
        return False
    
    if len(node.childs) < 3:
        return False
    
    # First child harus bukan FILTER/OPERATOR (harus source tree)
    first_child = node.get_child(0)
    if first_child.type in {"FILTER", "OPERATOR", "OPERATOR_S"}:
        return False
    
    # Remaining children harus FILTER/OPERATOR
    for i in range(1, len(node.childs)):
        child = node.get_child(i)
        if child.type not in {"FILTER", "OPERATOR", "OPERATOR_S"}:
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
    """Cascade OPERATOR_S(AND) dengan urutan spesifik."""
    source = and_node.get_child(0)
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
            # Create FILTER with condition value
            new_filter = QueryTree("FILTER", condition.val)
            new_filter.add_child(current)
            current = new_filter
    
    return current


def uncascade_filters(query: ParsedQuery) -> ParsedQuery:
    """Convert cascaded filters kembali ke bentuk OPERATOR_S(AND)."""
    def uncascade_rec(node: QueryTree) -> QueryTree:
        if node is None:
            return None
        
        for i in range(len(node.childs)):
            node.childs[i] = uncascade_rec(node.childs[i])
        
        # Detect cascaded filters: FILTER with 1 child
        if node.is_node_type("FILTER") and len(node.childs) == 1:
            filters = []
            current = node
            source = None
            
            # Collect all cascaded filters
            while current is not None and current.is_node_type("FILTER"):
                if len(current.childs) == 1:
                    filters.append(current)
                    current = current.get_child(0)
                else:
                    break
            
            # If we have 2+ filters and a valid source, create OPERATOR_S(AND)
            if len(filters) >= 2 and current is not None:
                source = current
                and_node = QueryTree("OPERATOR_S", "AND")
                and_node.add_child(source)
                
                # Add conditions as FILTER leaves (0 children)
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
