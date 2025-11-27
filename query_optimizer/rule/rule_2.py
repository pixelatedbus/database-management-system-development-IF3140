"""
Seleksi Komutatif

OPERATOR("AND")
├── condition_a
├── condition_b
└── condition_c

Contoh transformasi: [2, 0, 1]
OPERATOR("AND")
├── condition_c
├── condition_a
└── condition_b

kontrol parameter:
- operator_orders: dict[int, list[int]], contoh: {operator_node.id: [2,0,1]}
"""

from query_optimizer.optimization_engine import ParsedQuery
from query_optimizer.query_tree import QueryTree
import random


def analyze_and_operators_for_reorder(query: ParsedQuery) -> dict[int, int]:
    operators = {}
    
    def analyze_rec(node: QueryTree):
        if node is None:
            return
        
        if node.is_node_type("OPERATOR") and node.is_node_value("AND"):
            num_conditions = len(node.childs)
            if num_conditions >= 2:
                operators[node.id] = num_conditions
        
        for child in node.childs:
            analyze_rec(child)
    
    analyze_rec(query.query_tree)
    return operators


def generate_random_rule_2_params(num_conditions: int) -> list[int]:
    indices = list(range(num_conditions))
    random.shuffle(indices)
    return indices


def copy_rule_2_params(rule_2_params: list[int]) -> list[int]:
    return rule_2_params.copy()


def count_conditions_in_rule_2_params(rule_2_params: list[int]) -> int:
    return len(rule_2_params)


def mutate_rule_2_params(params: list[int]) -> list[int]:
    if not params or len(params) < 2:
        return params
    
    mutated = params.copy()
    mutation_type = random.choice(['swap', 'reverse_subseq', 'rotate'])
    
    if mutation_type == 'swap':
        idx1, idx2 = random.sample(range(len(mutated)), 2)
        mutated[idx1], mutated[idx2] = mutated[idx2], mutated[idx1]
    
    elif mutation_type == 'reverse_subseq':
        if len(mutated) >= 2:
            start = random.randint(0, len(mutated) - 2)
            end = random.randint(start + 1, len(mutated))
            mutated[start:end] = reversed(mutated[start:end])
    
    elif mutation_type == 'rotate':
        k = random.randint(1, len(mutated) - 1)
        mutated = mutated[k:] + mutated[:k]
    
    return mutated


def reorder_and_conditions(
    query: ParsedQuery, 
    operator_orders: dict[int, list[int]] | None = None
) -> ParsedQuery:
    if not operator_orders:
        return query
    
    orders_list = list(operator_orders.values())
    order_index = 0
    
    def reorder_rec(node: QueryTree) -> QueryTree:
        nonlocal order_index
        if node is None:
            return None
        
        for i in range(len(node.childs)):
            node.childs[i] = reorder_rec(node.childs[i])

        if node.is_node_type("OPERATOR") and node.is_node_value("AND"):
            operator_id = node.id
            order = None

            if operator_id in operator_orders:
                order = operator_orders[operator_id]
            elif order_index < len(orders_list):
                order = orders_list[order_index]
                order_index += 1

            if order and len(order) == len(node.childs):
                original_childs = node.childs.copy()
                node.childs = [original_childs[i] for i in order]

        return node
    
    transformed_tree = reorder_rec(query.query_tree)
    return ParsedQuery(transformed_tree, query.query)


def validate_rule_2_params(params: list[int], num_conditions: int) -> bool:
    if not params or len(params) != num_conditions:
        return False
    
    return set(params) == set(range(num_conditions))
