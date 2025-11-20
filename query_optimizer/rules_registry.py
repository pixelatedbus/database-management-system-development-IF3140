"""
Registry untuk Rules Optimasi Query
"""

from query_optimizer.optimization_engine import ParsedQuery
from query_optimizer.query_tree import QueryTree
from query_optimizer.rule_1 import (
    seleksi_konjungtif,
    cascade_filters,
    clone_tree
)
import random


def rule_seleksi_konjungtif(query: ParsedQuery) -> ParsedQuery:
    """Rule 1: Transformasi AND menjadi cascaded filters."""
    return seleksi_konjungtif(query)

def rule_seleksi_komutatif(query: ParsedQuery) -> ParsedQuery:
    """
    Rule 2: Tukar urutan filter berurutan.
    σ(c1)(σ(c2)(R)) ≡ σ(c2)(σ(c1)(R))
    """
    cloned = clone_tree(query.query_tree)
    cloned_query = ParsedQuery(cloned, query.query)
    
    def swap_filters(node):
        if node is None:
            return node
        
        if (node.is_node_type("FILTER") and 
            len(node.childs) == 1 and 
            node.childs[0].is_node_type("FILTER")):
            
            if random.random() < 0.5:
                node.val, node.childs[0].val = node.childs[0].val, node.val
        
        for child in node.childs:
            swap_filters(child)
        
        return node
    
    result_tree = swap_filters(cloned)
    return ParsedQuery(result_tree, query.query)


ALL_RULES = [
    ("seleksi_konjungtif", rule_seleksi_konjungtif),
    ("seleksi_komutatif", rule_seleksi_komutatif),
]


def get_all_rules():
    """Ambil daftar semua rules yang tersedia."""
    return ALL_RULES


def get_rule_by_name(name: str):
    """Ambil fungsi rule berdasarkan nama."""
    for rule_name, rule_func in ALL_RULES:
        if rule_name == name:
            return rule_func
    return None


def apply_random_rule(query: ParsedQuery) -> tuple[ParsedQuery, str]:
    """Terapkan rule acak ke query."""
    rule_name, rule_func = random.choice(ALL_RULES)
    try:
        transformed = rule_func(query)
        return transformed, rule_name
    except Exception as e:
        return query, f"{rule_name}_failed"


def apply_random_rules(query: ParsedQuery, num_rules: int = 3) -> tuple[ParsedQuery, list[str]]:
    """Terapkan beberapa rule acak ke query."""
    current_query = query
    applied_rules = []
    
    for _ in range(num_rules):
        current_query, rule_name = apply_random_rule(current_query)
        applied_rules.append(rule_name)
    
    return current_query, applied_rules
