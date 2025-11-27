"""
Seleksi Komutatif

Transformasi berdasarkan sifat komutatif operator AND:
- A AND B = B AND A
- Input: OPERATOR(AND) dengan N kondisi
- Output: OPERATOR(AND) dengan kondisi yang di-reorder

Perbedaan dengan Rule 1:
- Rule 1 (Seleksi Konjungtif): Cascading (pemecahan FILTER dengan AND)
- Rule 2 (Seleksi Komutatif): Reordering (mengubah urutan kondisi dalam AND)

Contoh transformasi:
Input:
OPERATOR("AND")
├── condition_a [selectivity=0.5]
├── condition_b [selectivity=0.1]  <- lebih selektif
└── condition_c [selectivity=0.8]

Output (optimal: kondisi paling selektif duluan):
OPERATOR("AND")
├── condition_b [selectivity=0.1]  <- most selective first
├── condition_a [selectivity=0.5]
└── condition_c [selectivity=0.8]

Note: Rule ini hanya mengubah URUTAN kondisi dalam OPERATOR(AND),
tidak mengubah struktur tree seperti Rule 1.
"""

from query_optimizer.optimization_engine import ParsedQuery
from query_optimizer.query_tree import QueryTree
import random


def analyze_and_operators_for_reorder(query: ParsedQuery) -> dict[int, int]:
    """
    Analisa query untuk mencari semua OPERATOR(AND) yang bisa di-reorder.
    
    Returns:
        Dict mapping operator_id -> jumlah kondisi
    """
    operators = {}
    
    def analyze_rec(node: QueryTree):
        if node is None:
            return
        
        # Cek apakah ini OPERATOR(AND) dengan >= 2 kondisi
        if node.is_node_type("OPERATOR") and node.is_node_value("AND"):
            num_conditions = len(node.childs)
            if num_conditions >= 2:
                operators[node.id] = num_conditions
        
        # Rekursif ke semua children
        for child in node.childs:
            analyze_rec(child)
    
    analyze_rec(query.query_tree)
    return operators


def generate_random_rule_2_params(num_conditions: int) -> list[int]:
    """
    Generate random permutation untuk reorder kondisi.
    
    Args:
        num_conditions: Jumlah kondisi dalam OPERATOR(AND)
    
    Returns:
        List of indices representing new order
        Example: [2, 0, 1] means condition_2, condition_0, condition_1
    """
    indices = list(range(num_conditions))
    random.shuffle(indices)
    return indices


def copy_rule_2_params(rule_2_params: list[int]) -> list[int]:
    """Deep copy reorder parameters."""
    return rule_2_params.copy()


def count_conditions_in_rule_2_params(rule_2_params: list[int]) -> int:
    """Count total number of conditions in reorder params."""
    return len(rule_2_params)


def mutate_rule_2_params(params: list[int]) -> list[int]:
    """
    Mutate rule 2 params - swap 2 positions atau reverse subsequence.
    
    Mutation strategies:
    - swap: Tukar 2 posisi random
    - reverse_subseq: Reverse subsequence random
    - rotate: Rotate semua elemen
    """
    if not params or len(params) < 2:
        return params
    
    mutated = params.copy()
    mutation_type = random.choice(['swap', 'reverse_subseq', 'rotate'])
    
    if mutation_type == 'swap':
        # Swap 2 random positions
        idx1, idx2 = random.sample(range(len(mutated)), 2)
        mutated[idx1], mutated[idx2] = mutated[idx2], mutated[idx1]
    
    elif mutation_type == 'reverse_subseq':
        # Reverse a random subsequence
        if len(mutated) >= 2:
            start = random.randint(0, len(mutated) - 2)
            end = random.randint(start + 1, len(mutated))
            mutated[start:end] = reversed(mutated[start:end])
    
    elif mutation_type == 'rotate':
        # Rotate all elements by random amount
        k = random.randint(1, len(mutated) - 1)
        mutated = mutated[k:] + mutated[:k]
    
    return mutated


def reorder_and_conditions(
    query: ParsedQuery, 
    operator_orders: dict[int, list[int]] | None = None
) -> ParsedQuery:
    """
    Reorder kondisi dalam OPERATOR(AND) berdasarkan order yang diberikan.
    
    Args:
        query: Query yang akan ditransformasi
        operator_orders: Dict mapping operator_id -> urutan kondisi baru
                        Example: {42: [2, 0, 1]} means reorder to [cond2, cond0, cond1]
                        
                        Note: If operator_orders is None or empty, returns query unchanged.
                        If an AND operator's ID is not found, it's reordered anyway using
                        the first available order (to handle cloned trees with new IDs).
    
    Returns:
        ParsedQuery dengan kondisi yang sudah di-reorder
    """
    if not operator_orders:
        return query
    
    # Get list of all orders (for fallback when ID doesn't match)
    orders_list = list(operator_orders.values())
    order_index = 0
    
    def reorder_rec(node: QueryTree) -> QueryTree:
        nonlocal order_index
        if node is None:
            return None
        
        # Process children first (bottom-up)
        for i in range(len(node.childs)):
            node.childs[i] = reorder_rec(node.childs[i])
        
        # Check if this is OPERATOR(AND) that needs reordering
        if node.is_node_type("OPERATOR") and node.is_node_value("AND"):
            operator_id = node.id
            order = None
            
            # Try to get order by ID first
            if operator_id in operator_orders:
                order = operator_orders[operator_id]
            # Fallback: use next available order (for cloned trees)
            elif order_index < len(orders_list):
                order = orders_list[order_index]
                order_index += 1
            
            # Apply reordering if we have a valid order
            if order and len(order) == len(node.childs):
                # Reorder children
                original_childs = node.childs.copy()
                node.childs = [clone_tree(original_childs[i]) for i in order]
        
        return node
    
    transformed_tree = reorder_rec(query.query_tree)
    return ParsedQuery(transformed_tree, query.query)


def validate_rule_2_params(params: list[int], num_conditions: int) -> bool:
    """
    Validate rule 2 params.
    
    Checks:
    - All indices 0..num_conditions-1 are present
    - No duplicates
    - Correct length
    """
    if not params or len(params) != num_conditions:
        return False
    
    # Check all indices present and no duplicates
    return set(params) == set(range(num_conditions))


def clone_tree(node: QueryTree) -> QueryTree:
    """Deep clone query tree."""
    if node is None:
        return None
    return node.clone(deep=True)
