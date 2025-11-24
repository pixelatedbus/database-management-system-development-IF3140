"""
Demo scenarios for Rule 8 - Projection over Join
"""

from query_optimizer.query_tree import QueryTree
from query_optimizer.optimization_engine import ParsedQuery, OptimizationEngine
from query_optimizer.rule_8 import push_projection_over_joins, analyze_projection_over_join


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


def count_projects(node):
    """Count PROJECT nodes in tree"""
    if node is None:
        return 0
    count = 1 if node.type == "PROJECT" else 0
    for child in node.childs:
        count += count_projects(child)
    return count


def scenario_1_basic_pushdown():
    """Scenario 8.1: Basic projection pushdown"""
    print("\n")
    print_separator("SCENARIO 8.1: Basic Projection Pushdown")
    
    print("Concept: PROJECT(cols, JOIN(R, S)) → PROJECT(cols, JOIN(PROJECT(R_cols), PROJECT(S_cols)))")
    print("Query: SELECT users.name, profiles.bio FROM users JOIN profiles")
    
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
    
    # Projected columns: users.name, profiles.bio
    col1 = make_column_ref("name", "users")
    col2 = make_column_ref("bio", "profiles")
    
    project = QueryTree("PROJECT", "")
    project.add_child(col1)
    project.add_child(col2)
    project.add_child(join)
    
    query = ParsedQuery(project, "SELECT users.name, profiles.bio FROM users JOIN profiles ON users.id = profiles.user_id")
    
    engine = OptimizationEngine()
    
    print("\nOriginal Query Tree:")
    print(query.query_tree.tree())
    original_projects = count_projects(query.query_tree)
    print(f"\nPROJECT nodes: {original_projects}")
    cost_original = engine.get_cost(query)
    print(f"Cost: {cost_original:.2f}")
    
    print("\n" + "-"*70)
    print("Applying Rule 8: Push projections to join children")
    
    optimized = push_projection_over_joins(query)
    
    print("\nOptimized Query Tree:")
    print(optimized.query_tree.tree())
    optimized_projects = count_projects(optimized.query_tree)
    print(f"\nPROJECT nodes: {optimized_projects}")
    cost_optimized = engine.get_cost(optimized)
    print(f"Cost: {cost_optimized:.2f}")
    
    print(f"\n✓ Projections pushed down! (+{optimized_projects - original_projects} PROJECT nodes)")
    
    print("\nKey Point: Each relation projects only needed columns + join keys")
    print("Benefit: Reduced tuple width before join")


def scenario_2_selective_projection():
    """Scenario 8.2: Selective projection with many columns"""
    print("\n")
    print_separator("SCENARIO 8.2: Selective Projection")
    
    print("Concept: Project only 2 columns from tables with many columns")
    print("Significant reduction in data volume")
    
    # Build JOIN (same as scenario 1)
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
    
    # Project only email from users
    col1 = make_column_ref("email", "users")
    
    project = QueryTree("PROJECT", "")
    project.add_child(col1)
    project.add_child(join)
    
    query = ParsedQuery(project, "SELECT users.email FROM users JOIN profiles ON users.id = profiles.user_id")
    
    engine = OptimizationEngine()
    
    print("\nOriginal Query Tree:")
    print(query.query_tree.tree())
    print("\nWithout pushdown: JOIN processes ALL columns from both tables")
    cost_original = engine.get_cost(query)
    print(f"Cost: {cost_original:.2f}")
    
    print("\n" + "-"*70)
    print("Applying Rule 8: Push projection down")
    
    optimized = push_projection_over_joins(query)
    
    print("\nOptimized Query Tree:")
    print(optimized.query_tree.tree())
    print("\nWith pushdown: Each table projects only needed columns")
    print("  - users: id (join key) + email (output)")
    print("  - profiles: user_id (join key) only")
    cost_optimized = engine.get_cost(optimized)
    print(f"Cost: {cost_optimized:.2f}")
    
    if cost_optimized < cost_original:
        print(f"\n✓ Significant improvement: {cost_original - cost_optimized:.2f}")
    
    print("\nKey Point: More selective projection = bigger benefit")
    print("Especially important for wide tables with many columns")
