"""
Demo scenarios for Rule 1 - Cascade Filters (Seleksi Konjungtif)
"""

from query_optimizer.optimization_engine import OptimizationEngine
from query_optimizer.rule.rule_1 import cascade_filters, uncascade_filters


def print_separator(title):
    """Print section separator"""
    print("\n" + "="*70)
    print(f"  {title}")
    print("="*70)


def scenario_1_full_cascade():
    """Scenario 1.1: Full cascade (all single filters)"""
    print("\n")
    print_separator("SCENARIO 1.1: Full Cascade - All Single Filters")
    
    sql = "SELECT * FROM users WHERE age > 25 AND status = 'active' AND city = 'Jakarta'"
    print(f"Query: {sql}")
    
    engine = OptimizationEngine()
    query = engine.parse_query(sql)
    and_operator = query.query_tree.childs[0].childs[1]
    
    print("\nOriginal Query Tree:")
    print(query.query_tree.tree(show_id=True))
    cost_original = engine.get_cost(query)
    print(f"Cost: {cost_original:.2f}")
    
    print("\n" + "-"*70)
    print("Transformation: Full cascade [0, 1, 2] - three separate FILTERs")
    
    operator_orders = {and_operator.id: [0, 1, 2]}
    transformed = cascade_filters(query, operator_orders)
    
    print("\nTransformed Query Tree:")
    print(transformed.query_tree.tree(show_id=True))
    cost = engine.get_cost(transformed)
    print(f"Cost: {cost:.2f} (change: {cost - cost_original:+.2f})")


def scenario_2_no_cascade():
    """Scenario 1.2: No cascade (keep all grouped)"""
    print("\n")
    print_separator("SCENARIO 1.2: No Cascade - Keep All Grouped")
    
    sql = "SELECT * FROM users WHERE age > 25 AND status = 'active' AND city = 'Jakarta'"
    
    engine = OptimizationEngine()
    query = engine.parse_query(sql)
    and_operator = query.query_tree.childs[0].childs[1]
    
    print("\nOriginal Query Tree:")
    print(query.query_tree.tree(show_id=True))
    cost_original = engine.get_cost(query)
    print(f"Cost: {cost_original:.2f}")
    
    print("\n" + "-"*70)
    print("Transformation: No cascade [[0, 1, 2]] - single FILTER with AND")
    
    operator_orders = {and_operator.id: [[0, 1, 2]]}
    transformed = cascade_filters(query, operator_orders)
    
    print("\nTransformed Query Tree:")
    print(transformed.query_tree.tree(show_id=True))
    cost = engine.get_cost(transformed)
    print(f"Cost: {cost:.2f} (no change)")


def scenario_3_mixed_cascade():
    """Scenario 1.3: Mixed cascade (some single, some grouped)"""
    print("\n")
    print_separator("SCENARIO 1.3: Mixed Cascade")
    
    sql = "SELECT * FROM users WHERE age > 25 AND status = 'active' AND city = 'Jakarta'"
    
    engine = OptimizationEngine()
    query = engine.parse_query(sql)
    and_operator = query.query_tree.childs[0].childs[1]
    
    print("\nOriginal Query Tree:")
    print(query.query_tree.tree(show_id=True))
    cost_original = engine.get_cost(query)
    print(f"Cost: {cost_original:.2f}")
    
    print("\n" + "-"*70)
    print("Transformation: Mixed [2, [0, 1]] - FILTER(c2) → FILTER(AND(c0, c1))")
    
    operator_orders = {and_operator.id: [2, [0, 1]]}
    transformed = cascade_filters(query, operator_orders)
    
    print("\nTransformed Query Tree:")
    print(transformed.query_tree.tree(show_id=True))
    cost = engine.get_cost(transformed)
    print(f"Cost: {cost:.2f} (change: {cost - cost_original:+.2f})")


def scenario_4_uncascade():
    """Scenario 1.4: Reverse transformation (uncascade)"""
    print("\n")
    print_separator("SCENARIO 1.4: Reverse Transformation - Uncascade")
    
    sql = "SELECT * FROM users WHERE age > 25 AND status = 'active' AND city = 'Jakarta'"
    
    engine = OptimizationEngine()
    query = engine.parse_query(sql)
    and_operator = query.query_tree.childs[0].childs[1]
    
    print("\nOriginal (AND operator):")
    print(query.query_tree.tree(show_id=True))
    cost_original = engine.get_cost(query)
    print(f"Cost: {cost_original:.2f}")
    
    print("\n" + "-"*70)
    print("Step 1: Cascade [0, 1, 2]")
    operator_orders = {and_operator.id: [0, 1, 2]}
    cascaded = cascade_filters(query, operator_orders)
    
    print("\nCascaded:")
    print(cascaded.query_tree.tree(show_id=True))
    cost_cascaded = engine.get_cost(cascaded)
    print(f"Cost: {cost_cascaded:.2f}")
    
    print("\n" + "-"*70)
    print("Step 2: Uncascade")
    uncascaded = uncascade_filters(cascaded)
    
    print("\nUncascaded (back to AND):")
    print(uncascaded.query_tree.tree(show_id=True))
    cost_uncascaded = engine.get_cost(uncascaded)
    print(f"Cost: {cost_uncascaded:.2f}")
