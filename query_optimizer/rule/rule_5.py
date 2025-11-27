from query_optimizer.optimization_engine import ParsedQuery
from query_optimizer.query_tree import QueryTree
import random
from typing import Any

def join_komutatif(query: ParsedQuery, join_orders: dict[int, bool] = None) -> ParsedQuery:
    """
    Mengubah urutan tabel pada operasi JOIN.
    Rule: JOIN(A, B) ≡ JOIN(B, A)
    
    Args:
        query: Parsed query to transform
        join_orders: Dict[join_id, should_swap]
                    True = swap children (B, A)
                    False = keep original (A, B)
    """
    if join_orders is None:
        # Default: swap all joins
        transformed_tree = join_komutatif_rec(query.query_tree, None)
    else:
        transformed_tree = join_komutatif_rec(query.query_tree, join_orders)
    return ParsedQuery(transformed_tree, query.query)

def join_komutatif_rec(node: QueryTree, join_orders: dict[int, bool] = None) -> QueryTree:
    if node is None:
        return None
    
    for i in range(len(node.childs)):
        node.childs[i] = join_komutatif_rec(node.childs[i], join_orders)
        if node.childs[i]:
            node.childs[i].parent = node
            
    if node.is_node_type("JOIN"):
        if join_orders is None:
            node.childs[0], node.childs[1] = node.childs[1], node.childs[0]
        else:
            should_swap = join_orders.get(node.id, False)
            if should_swap:
                node.childs[0], node.childs[1] = node.childs[1], node.childs[0]
        
    return node

def find_join_nodes(query: ParsedQuery) -> dict[int, Any]:
    """
    Find all JOIN nodes in the query tree.
    
    Returns:
        Dict[node.id, metadata]
        metadata = {
            'left_child': node,
            'right_child': node,
            'join_type': str (INNER/NATURAL/CROSS)
        }
    """
    result = {}
    _find_join_nodes_rec(query.query_tree, result)
    return result

def _find_join_nodes_rec(node: QueryTree, result: dict[int, Any]):
    """Recursive helper to find JOIN nodes."""
    if node is None:
        return
    
    if node.is_node_type("JOIN"):
        result[node.id] = {
            'left_child': node.childs[0] if len(node.childs) > 0 else None,
            'right_child': node.childs[1] if len(node.childs) > 1 else None,
            'join_type': node.val or 'INNER'
        }
    
    for child in node.childs:
        _find_join_nodes_rec(child, result)

def generate_join_child_params(metadata: dict) -> bool:
    """Generate random parameter: True = swap, False = keep."""
    return random.choice([True, False])

def copy_join_child_params(params: bool) -> bool:
    """Copy join child params (bool is immutable, just return it)."""
    return params

def mutate_join_child_params(params: bool) -> bool:
    """Mutate by flipping boolean."""
    return not params

def validate_join_child_params(params: bool) -> bool:
    """Validate that params is a boolean."""
    return isinstance(params, bool)