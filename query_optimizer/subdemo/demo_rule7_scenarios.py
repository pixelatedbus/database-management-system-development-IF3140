from query_optimizer.query_tree import QueryTree
from query_optimizer.optimization_engine import OptimizationEngine
from query_optimizer.rule.rule_7 import apply_pushdown


def print_separator(title):
    print("\n" + "="*70)
    print(f"  {title}")
    print("="*70)


def make_column_ref(col_name, table_name=None):
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
    if node is None:
        return 0
    count = 1 if node.type == "FILTER" else 0
    for child in node.childs:
        count += count_filters(child)
    return count


def scenario_1_single_condition():
    print("\n")
    print_separator("SCENARIO 7.1: Single Condition Pushdown")
    
    sql = "SELECT * FROM users JOIN profiles ON users.id = profiles.user_id WHERE users.age > 18"
    print(f"Query: {sql}")
    
    engine = OptimizationEngine()
    query = engine.parse_query(sql)
    
    print("\nOriginal Query Tree:")
    print(query.query_tree.tree())
    original_filters = count_filters(query.query_tree)
    print(f"\nFILTER nodes: {original_filters}")
    cost_original = engine.get_cost(query)
    print(f"Cost: {cost_original:.2f}")
    
    print("\n" + "-"*70)
    print("Applying Rule 7")
    
    optimized = apply_pushdown(query)
    
    print("\nOptimized Query Tree:")
    print(optimized.query_tree.tree())
    optimized_filters = count_filters(optimized.query_tree)
    print(f"\nFILTER nodes: {optimized_filters}")
    cost_optimized = engine.get_cost(optimized)
    print(f"Cost: {cost_optimized:.2f} (improvement: {cost_original - cost_optimized:.2f})")


def scenario_2_multiple_conditions():
    print("\n")
    print_separator("SCENARIO 7.2: Multiple Conditions Pushdown")
    
    sql = "SELECT * FROM users JOIN profiles ON users.id = profiles.user_id WHERE users.age > 18 AND profiles.verified = 'true'"
    print(f"Query: {sql}")
    
    engine = OptimizationEngine()
    query = engine.parse_query(sql)
    
    print("\nOriginal Query Tree:")
    print(query.query_tree.tree())
    original_filters = count_filters(query.query_tree)
    print(f"\nFILTER nodes: {original_filters}")
    cost_original = engine.get_cost(query)
    print(f"Cost: {cost_original:.2f}")
    
    print("\n" + "-"*70)
    print("Applying Rule 7")
    
    optimized = apply_pushdown(query)
    
    print("\nOptimized Query Tree:")
    print(optimized.query_tree.tree())
    optimized_filters = count_filters(optimized.query_tree)
    print(f"\nFILTER nodes: {optimized_filters}")
    cost_optimized = engine.get_cost(optimized)
    print(f"Cost: {cost_optimized:.2f} (improvement: {cost_original - cost_optimized:.2f})")

