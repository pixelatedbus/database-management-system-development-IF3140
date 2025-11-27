from query_optimizer.query_tree import QueryTree
from query_optimizer.optimization_engine import OptimizationEngine
from query_optimizer.rule.rule_8 import push_projection_over_joins


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


def count_projects(node):
    if node is None:
        return 0
    count = 1 if node.type == "PROJECT" else 0
    for child in node.childs:
        count += count_projects(child)
    return count


def scenario_1_basic_pushdown():
    print("\n")
    print_separator("SCENARIO 8.1: Basic Projection Pushdown")
    
    sql = "SELECT users.name, profiles.bio FROM users JOIN profiles ON users.id = profiles.user_id"
    print(f"Query: {sql}")
    
    engine = OptimizationEngine()
    query = engine.parse_query(sql)
    
    print("\nOriginal Query Tree:")
    print(query.query_tree.tree(show_id=True))
    original_projects = count_projects(query.query_tree)
    print(f"\nPROJECT nodes: {original_projects}")
    cost_original = engine.get_cost(query)
    print(f"Cost: {cost_original:.2f}")
    
    print("\n" + "-"*70)
    print("Applying Rule 8")
    
    optimized = push_projection_over_joins(query)
    
    print("\nOptimized Query Tree:")
    print(optimized.query_tree.tree(show_id=True))
    optimized_projects = count_projects(optimized.query_tree)
    print(f"\nPROJECT nodes: {optimized_projects} (+{optimized_projects - original_projects})")
    cost_optimized = engine.get_cost(optimized)
    print(f"Cost: {cost_optimized:.2f} (improvement: {cost_original - cost_optimized:.2f})")


def scenario_2_selective_projection():
    print("\n")
    print_separator("SCENARIO 8.2: Selective Projection")
    
    sql = "SELECT users.email FROM users JOIN profiles ON users.id = profiles.user_id"
    print(f"Query: {sql}")
    
    engine = OptimizationEngine()
    query = engine.parse_query(sql)
    
    print("\nOriginal Query Tree:")
    print(query.query_tree.tree(show_id=True))
    cost_original = engine.get_cost(query)
    print(f"Cost: {cost_original:.2f}")
    
    print("\n" + "-"*70)
    print("Applying Rule 8")
    
    optimized = push_projection_over_joins(query)
    
    print("\nOptimized Query Tree:")
    print(optimized.query_tree.tree(show_id=True))
    cost_optimized = engine.get_cost(optimized)
    print(f"Cost: {cost_optimized:.2f} (improvement: {cost_original - cost_optimized:.2f})")
