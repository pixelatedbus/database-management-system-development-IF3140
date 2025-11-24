"""
Demo scenarios for Rule 7 - Filter Pushdown over Join
"""

from query_optimizer.query_tree import QueryTree
from query_optimizer.optimization_engine import ParsedQuery, OptimizationEngine
from query_optimizer.rule_7 import apply_pushdown, find_patterns


def print_separator(title):
    """Print section separator"""
    print("\n" + "="*70)
    print(f"  {title}")
    print("="*70)


def make_column_ref(col_name, table_name=None):
    """Helper to create COLUMN_REF node"""
    col_ref = QueryTree("COLUMN_REF", "")
    col_name_node = QueryTree("COLUMN_NAME", "")
    identifier = QueryTree("IDENTIFIER", col_name)
    col_name_node.add_child(identifier)
    col_ref.add_child(col_name_node)
    
    if table_name:
        table_node = QueryTree("TABLE_NAME", "")
        table_id = QueryTree("IDENTIFIER", table_name)
        table_node.add_child(table_id)
        col_ref.add_child(table_node)
    
    return col_ref


def count_filters(node):
    """Count FILTER nodes in tree"""
    if node is None:
        return 0
    count = 1 if node.type == "FILTER" else 0
    for child in node.childs:
        count += count_filters(child)
    return count


def scenario_1_single_condition():
    """Scenario 7.1: Pushdown single filter condition"""
    print("\n")
    print_separator("SCENARIO 7.1: Single Condition Pushdown")
    
    print("Concept: FILTER(JOIN(R, S), cond_R) → JOIN(FILTER(R, cond_R), S)")
    print("Query: SELECT * FROM users JOIN profiles WHERE users.age > 18")
    
    # Build JOIN
    rel1 = QueryTree("RELATION", "users")
    rel2 = QueryTree("RELATION", "profiles")
    
    join_left = make_column_ref("id", "users")
    join_right = make_column_ref("user_id", "profiles")
    
    join_cond = QueryTree("COMPARISON", "=")
    join_cond.add_child(join_left)
    join_cond.add_child(join_right)
    
    join = QueryTree("JOIN", "INNER")
    join.add_child(rel1)
    join.add_child(rel2)
    join.add_child(join_cond)
    
    # Filter: users.age > 18
    age_col = make_column_ref("age", "users")
    age_val = QueryTree("LITERAL_NUMBER", "18")
    age_comp = QueryTree("COMPARISON", ">")
    age_comp.add_child(age_col)
    age_comp.add_child(age_val)
    
    filter_node = QueryTree("FILTER", "")
    filter_node.add_child(join)
    filter_node.add_child(age_comp)
    
    query = ParsedQuery(filter_node, "SELECT * FROM users JOIN profiles ON users.id = profiles.user_id WHERE users.age > 18")
    
    engine = OptimizationEngine()
    
    print("\nOriginal Query Tree:")
    print(query.query_tree.tree())
    original_filters = count_filters(query.query_tree)
    print(f"\nFILTER nodes: {original_filters}")
    cost_original = engine.get_cost(query)
    print(f"Cost: {cost_original:.2f}")
    
    print("\n" + "-"*70)
    print("Applying Rule 7: Push filter down to users relation")
    
    optimized = apply_pushdown(query)
    
    print("\nOptimized Query Tree:")
    print(optimized.query_tree.tree())
    optimized_filters = count_filters(optimized.query_tree)
    print(f"\nFILTER nodes: {optimized_filters}")
    cost_optimized = engine.get_cost(optimized)
    print(f"Cost: {cost_optimized:.2f}")
    
    print(f"\n✓ Filter pushed down! Reduces rows before join")
    
    print("\nKey Point: Filter applied earlier reduces intermediate result size")


def scenario_2_multiple_conditions():
    """Scenario 7.2: Pushdown multiple filter conditions"""
    print("\n")
    print_separator("SCENARIO 7.2: Multiple Conditions Pushdown")
    
    print("Concept: FILTER(JOIN, cond_R AND cond_S) → JOIN(FILTER(R, cond_R), FILTER(S, cond_S))")
    print("Query: SELECT * FROM users JOIN profiles WHERE users.age > 18 AND profiles.verified = 'true'")
    
    # Build JOIN
    rel1 = QueryTree("RELATION", "users")
    rel2 = QueryTree("RELATION", "profiles")
    
    join_left = make_column_ref("id", "users")
    join_right = make_column_ref("user_id", "profiles")
    
    join_cond = QueryTree("COMPARISON", "=")
    join_cond.add_child(join_left)
    join_cond.add_child(join_right)
    
    join = QueryTree("JOIN", "INNER")
    join.add_child(rel1)
    join.add_child(rel2)
    join.add_child(join_cond)
    
    # Filter: users.age > 18
    age_col = make_column_ref("age", "users")
    age_val = QueryTree("LITERAL_NUMBER", "18")
    age_comp = QueryTree("COMPARISON", ">")
    age_comp.add_child(age_col)
    age_comp.add_child(age_val)
    
    # Filter: profiles.verified = 'true'
    verified_col = make_column_ref("verified", "profiles")
    verified_val = QueryTree("LITERAL_STRING", "true")
    verified_comp = QueryTree("COMPARISON", "=")
    verified_comp.add_child(verified_col)
    verified_comp.add_child(verified_val)
    
    # AND operator
    and_op = QueryTree("OPERATOR", "AND")
    and_op.add_child(age_comp)
    and_op.add_child(verified_comp)
    
    filter_node = QueryTree("FILTER", "")
    filter_node.add_child(join)
    filter_node.add_child(and_op)
    
    query = ParsedQuery(filter_node, "SELECT * FROM users JOIN profiles ON users.id = profiles.user_id WHERE users.age > 18 AND profiles.verified = 'true'")
    
    engine = OptimizationEngine()
    
    print("\nOriginal Query Tree:")
    print(query.query_tree.tree())
    original_filters = count_filters(query.query_tree)
    print(f"\nFILTER nodes: {original_filters}")
    cost_original = engine.get_cost(query)
    print(f"Cost: {cost_original:.2f}")
    
    print("\n" + "-"*70)
    print("Applying Rule 7: Split and push both filters down")
    
    optimized = apply_pushdown(query)
    
    print("\nOptimized Query Tree:")
    print(optimized.query_tree.tree())
    optimized_filters = count_filters(optimized.query_tree)
    print(f"\nFILTER nodes: {optimized_filters}")
    cost_optimized = engine.get_cost(optimized)
    print(f"Cost: {cost_optimized:.2f}")
    
    print(f"\n✓ Both filters pushed down to respective relations!")
    
    print("\nKey Point: Each filter pushed to its relevant relation")
    print("Result: Both sides of join have fewer rows")
