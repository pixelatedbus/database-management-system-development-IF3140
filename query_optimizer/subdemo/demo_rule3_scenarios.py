"""
Demo scenarios for Rule 3 - Projection Elimination
"""

from query_optimizer.query_tree import QueryTree
from query_optimizer.optimization_engine import ParsedQuery, OptimizationEngine
from query_optimizer.rule_3 import seleksi_proyeksi


def print_separator(title):
    """Print section separator"""
    print("\n" + "="*70)
    print(f"  {title}")
    print("="*70)


def count_projects(node):
    """Count PROJECT nodes in tree"""
    if node is None:
        return 0
    count = 1 if node.type == "PROJECT" else 0
    for child in node.childs:
        count += count_projects(child)
    return count


def scenario_1_nested_projections():
    """Scenario 3.1: Basic nested projection elimination"""
    print("\n")
    print_separator("SCENARIO 3.1: Nested Projection Elimination")
    
    print("Query: SELECT id, name FROM (SELECT * FROM users)")
    
    relation = QueryTree("RELATION", "users")
    
    inner_project = QueryTree("PROJECT", "*")
    inner_project.add_child(relation)
    
    col1 = QueryTree("COLUMN_REF", "")
    col1_name = QueryTree("COLUMN_NAME", "")
    col1_id = QueryTree("IDENTIFIER", "id")
    col1_name.add_child(col1_id)
    col1.add_child(col1_name)
    
    col2 = QueryTree("COLUMN_REF", "")
    col2_name = QueryTree("COLUMN_NAME", "")
    col2_id = QueryTree("IDENTIFIER", "name")
    col2_name.add_child(col2_id)
    col2.add_child(col2_name)
    
    outer_project = QueryTree("PROJECT", "")
    outer_project.add_child(col1)
    outer_project.add_child(col2)
    outer_project.add_child(inner_project)
    
    query = ParsedQuery(outer_project, "SELECT id, name FROM (SELECT * FROM users)")
    
    print("\nOriginal Query Tree (nested projections):")
    print(query.query_tree.tree())
    original_count = count_projects(query.query_tree)
    print(f"\nPROJECT nodes: {original_count}")
    
    print("\n" + "-"*70)
    print("Applying Rule 3")
    
    optimized = seleksi_proyeksi(query)
    
    print("\nOptimized Query Tree:")
    print(optimized.query_tree.tree())
    optimized_count = count_projects(optimized.query_tree)
    print(f"\nPROJECT nodes: {optimized_count} (eliminated {original_count - optimized_count})")


def scenario_2_triple_nested():
    """Scenario 3.2: Triple nested projections"""
    print("\n")
    print_separator("SCENARIO 3.2: Triple Nested Projections")
    
    print("Query: SELECT id FROM (SELECT id, name FROM (SELECT * FROM users))")
    
    relation = QueryTree("RELATION", "users")
    
    inner1 = QueryTree("PROJECT", "*")
    inner1.add_child(relation)
    
    col_id1 = QueryTree("COLUMN_REF", "")
    col_name1 = QueryTree("COLUMN_NAME", "")
    id1 = QueryTree("IDENTIFIER", "id")
    col_name1.add_child(id1)
    col_id1.add_child(col_name1)
    
    col_name_node = QueryTree("COLUMN_REF", "")
    col_name2 = QueryTree("COLUMN_NAME", "")
    name1 = QueryTree("IDENTIFIER", "name")
    col_name2.add_child(name1)
    col_name_node.add_child(col_name2)
    
    inner2 = QueryTree("PROJECT", "")
    inner2.add_child(col_id1)
    inner2.add_child(col_name_node)
    inner2.add_child(inner1)
    
    col_id2 = QueryTree("COLUMN_REF", "")
    col_name3 = QueryTree("COLUMN_NAME", "")
    id2 = QueryTree("IDENTIFIER", "id")
    col_name3.add_child(id2)
    col_id2.add_child(col_name3)
    
    outer = QueryTree("PROJECT", "")
    outer.add_child(col_id2)
    outer.add_child(inner2)
    
    query = ParsedQuery(outer, "SELECT id FROM (SELECT id, name FROM (SELECT * FROM users))")
    
    print("\nOriginal Query Tree (triple nested):")
    print(query.query_tree.tree())
    original_count = count_projects(query.query_tree)
    print(f"\nPROJECT nodes: {original_count}")
    
    print("\n" + "-"*70)
    print("Applying Rule 3 recursively")
    
    optimized = seleksi_proyeksi(query)
    
    print("\nOptimized Query Tree:")
    print(optimized.query_tree.tree())
    optimized_count = count_projects(optimized.query_tree)
    print(f"\nPROJECT nodes: {optimized_count} (eliminated {original_count - optimized_count})")

