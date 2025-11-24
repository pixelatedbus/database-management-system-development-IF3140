"""
Demo scenarios for Rule 2 - Reorder AND Conditions (Seleksi Komutatif)
"""

from query_optimizer.query_tree import QueryTree
from query_optimizer.optimization_engine import ParsedQuery, OptimizationEngine
from query_optimizer.rule_2 import analyze_and_operators_for_reorder, reorder_and_conditions


def print_separator(title):
    """Print section separator"""
    print("\n" + "="*70)
    print(f"  {title}")
    print("="*70)


def scenario_1_original_order():
    """Scenario 2.1: Original order baseline"""
    print("\n")
    print_separator("SCENARIO 2.1: Original Order - Baseline")
    
    sql = "SELECT * FROM users WHERE age > 25 AND status = 'active' AND city = 'Jakarta'"
    print(f"Query: {sql}")
    
    engine = OptimizationEngine()
    query = engine.parse_query(sql)
    
    print("\nOriginal Query Tree:")
    print(query.query_tree.tree(show_id=True))
    cost = engine.get_cost(query)
    print(f"Cost: {cost:.2f}")


def scenario_2_reversed_order():
    """Scenario 2.2: Reversed order"""
    print("\n")
    print_separator("SCENARIO 2.2: Reversed Order")
    
    sql = "SELECT * FROM users WHERE age > 25 AND status = 'active' AND city = 'Jakarta'"
    
    engine = OptimizationEngine()
    query = engine.parse_query(sql)
    and_operator = query.query_tree.childs[0].childs[1]
    
    print("\nOriginal Query Tree:")
    print(query.query_tree.tree(show_id=True))
    cost_original = engine.get_cost(query)
    print(f"Original cost: {cost_original:.2f}")
    
    print("\n" + "-"*70)
    print("Transformation: Reversed order [2, 1, 0]")
    
    operator_orders = {and_operator.id: [2, 1, 0]}
    transformed = reorder_and_conditions(query, operator_orders)
    
    print("\nReordered Query Tree:")
    print(transformed.query_tree.tree(show_id=True))
    cost = engine.get_cost(transformed)
    print(f"Cost: {cost:.2f} (change: {cost - cost_original:+.2f})")


def scenario_3_optimal_order():
    """Scenario 2.3: Finding optimal order"""
    print("\n")
    print_separator("SCENARIO 2.3: Finding Optimal Order")
    
    sql = "SELECT * FROM users WHERE age > 25 AND status = 'active' AND city = 'Jakarta'"
    
    engine = OptimizationEngine()
    query = engine.parse_query(sql)
    and_operator = query.query_tree.childs[0].childs[1]
    
    print("\nOriginal Query Tree:")
    print(query.query_tree.tree(show_id=True))
    cost_original = engine.get_cost(query)
    print(f"Original cost: {cost_original:.2f}")
    
    import itertools
    permutations = list(itertools.permutations([0, 1, 2]))
    
    print("\n" + "-"*70)
    print("Testing all 6 permutations:")
    
    best_order = None
    best_cost = float('inf')
    
    for perm in permutations:
        operator_orders = {and_operator.id: list(perm)}
        transformed = reorder_and_conditions(query, operator_orders)
        cost = engine.get_cost(transformed)
        
        status = ""
        if cost < best_cost:
            best_cost = cost
            best_order = list(perm)
            status = " ← BEST"
        
        print(f"  {list(perm)}: {cost:.2f}{status}")
    
    print(f"\nOptimal: {best_order} with cost {best_cost:.2f} (improvement: {cost_original - best_cost:.2f})")
