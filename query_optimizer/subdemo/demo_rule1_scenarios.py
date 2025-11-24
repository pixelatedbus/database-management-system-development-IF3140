"""
Demo scenarios for Rule 1 - Cascade Filters (Seleksi Konjungtif)
"""

from query_optimizer.query_tree import QueryTree
from query_optimizer.optimization_engine import ParsedQuery, OptimizationEngine
from query_optimizer.rule_1 import cascade_filters, analyze_and_operators, uncascade_filters


def print_separator(title):
    """Print section separator"""
    print("\n" + "="*70)
    print(f"  {title}")
    print("="*70)


def scenario_1_full_cascade():
    """Scenario 1.1: Full cascade (all single filters)"""
    print("\n")
    print_separator("SCENARIO 1.1: Full Cascade - All Single Filters")
    
    print("Concept: Convert AND(c0, c1, c2) → FILTER(c0) → FILTER(c1) → FILTER(c2)")
    print("Query: SELECT * FROM users WHERE age > 25 AND status = 'active' AND city = 'Jakarta'")
    
    relation = QueryTree("RELATION", "users")
    
    comp1 = QueryTree("COMPARISON", ">")  # age > 25
    comp2 = QueryTree("COMPARISON", "=")  # status = 'active'
    comp3 = QueryTree("COMPARISON", "=")  # city = 'Jakarta'
    
    and_operator = QueryTree("OPERATOR", "AND")
    and_operator.add_child(comp1)
    and_operator.add_child(comp2)
    and_operator.add_child(comp3)
    
    filter_node = QueryTree("FILTER")
    filter_node.add_child(relation)
    filter_node.add_child(and_operator)
    
    query = ParsedQuery(filter_node, "SELECT * FROM users WHERE age > 25 AND status = 'active' AND city = 'Jakarta'")
    
    engine = OptimizationEngine()
    
    print("\nOriginal Query Tree:")
    print(query.query_tree.tree(show_id=True))
    cost_original = engine.get_cost(query)
    print(f"Cost: {cost_original:.2f}")
    
    # Full cascade [0, 1, 2]
    print("\n" + "-"*70)
    print("Applying full cascade: [0, 1, 2]")
    print("Result: Three separate FILTER nodes in sequence")
    
    operator_orders = {and_operator.id: [0, 1, 2]}
    transformed = cascade_filters(query, operator_orders)
    
    print("\nTransformed Query Tree:")
    print(transformed.query_tree.tree(show_id=True))
    cost = engine.get_cost(transformed)
    print(f"Cost: {cost:.2f}")
    
    if cost < cost_original:
        print(f"✓ Better! Improvement: {cost_original - cost:.2f}")
    else:
        print(f"Cost change: {cost - cost_original:.2f}")
    
    print("\nKey Point: Each condition becomes separate FILTER node")
    print("Benefit: Most selective condition can be evaluated first")


def scenario_2_no_cascade():
    """Scenario 1.2: No cascade (keep all grouped)"""
    print("\n")
    print_separator("SCENARIO 1.2: No Cascade - Keep All Grouped")
    
    print("Concept: Keep AND(c0, c1, c2) as single operator (no transformation)")
    
    relation = QueryTree("RELATION", "users")
    
    comp1 = QueryTree("COMPARISON", ">")
    comp2 = QueryTree("COMPARISON", "=")
    comp3 = QueryTree("COMPARISON", "=")
    
    and_operator = QueryTree("OPERATOR", "AND")
    and_operator.add_child(comp1)
    and_operator.add_child(comp2)
    and_operator.add_child(comp3)
    
    filter_node = QueryTree("FILTER")
    filter_node.add_child(relation)
    filter_node.add_child(and_operator)
    
    query = ParsedQuery(filter_node, "SELECT * FROM users WHERE age > 25 AND status = 'active' AND city = 'Jakarta'")
    
    engine = OptimizationEngine()
    
    print("\nOriginal Query Tree:")
    print(query.query_tree.tree(show_id=True))
    cost_original = engine.get_cost(query)
    print(f"Cost: {cost_original:.2f}")
    
    # No cascade [[0, 1, 2]]
    print("\n" + "-"*70)
    print("Applying no cascade: [[0, 1, 2]]")
    print("Result: Single FILTER with AND operator (unchanged)")
    
    operator_orders = {and_operator.id: [[0, 1, 2]]}
    transformed = cascade_filters(query, operator_orders)
    
    print("\nTransformed Query Tree:")
    print(transformed.query_tree.tree(show_id=True))
    cost = engine.get_cost(transformed)
    print(f"Cost: {cost:.2f}")
    
    print("\nKey Point: No structural change, all conditions stay in AND")


def scenario_3_mixed_cascade():
    """Scenario 1.3: Mixed cascade (some single, some grouped)"""
    print("\n")
    print_separator("SCENARIO 1.3: Mixed Cascade")
    
    print("Concept: Mix of single FILTER and grouped AND")
    print("Example: [2, [0, 1]] → FILTER(c2) → FILTER(AND(c0, c1))")
    
    relation = QueryTree("RELATION", "users")
    
    comp1 = QueryTree("COMPARISON", ">")
    comp2 = QueryTree("COMPARISON", "=")
    comp3 = QueryTree("COMPARISON", "=")
    
    and_operator = QueryTree("OPERATOR", "AND")
    and_operator.add_child(comp1)
    and_operator.add_child(comp2)
    and_operator.add_child(comp3)
    
    filter_node = QueryTree("FILTER")
    filter_node.add_child(relation)
    filter_node.add_child(and_operator)
    
    query = ParsedQuery(filter_node, "SELECT * FROM users WHERE age > 25 AND status = 'active' AND city = 'Jakarta'")
    
    engine = OptimizationEngine()
    
    print("\nOriginal Query Tree:")
    print(query.query_tree.tree(show_id=True))
    cost_original = engine.get_cost(query)
    print(f"Cost: {cost_original:.2f}")
    
    # Mixed cascade [2, [0, 1]]
    print("\n" + "-"*70)
    print("Applying mixed cascade: [2, [0, 1]]")
    print("Result: c2 as single FILTER, then c0 and c1 grouped in AND")
    
    operator_orders = {and_operator.id: [2, [0, 1]]}
    transformed = cascade_filters(query, operator_orders)
    
    print("\nTransformed Query Tree:")
    print(transformed.query_tree.tree(show_id=True))
    cost = engine.get_cost(transformed)
    print(f"Cost: {cost:.2f}")
    
    if cost < cost_original:
        print(f"✓ Better! Improvement: {cost_original - cost:.2f}")
    else:
        print(f"Cost change: {cost - cost_original:.2f}")
    
    print("\nKey Point: Flexibility to evaluate most selective condition first,")
    print("then group remaining conditions together")


def scenario_4_uncascade():
    """Scenario 1.4: Reverse transformation (uncascade)"""
    print("\n")
    print_separator("SCENARIO 1.4: Reverse Transformation - Uncascade")
    
    print("Concept: Convert cascaded filters back to AND operator")
    
    relation = QueryTree("RELATION", "users")
    
    comp1 = QueryTree("COMPARISON", ">")
    comp2 = QueryTree("COMPARISON", "=")
    comp3 = QueryTree("COMPARISON", "=")
    
    and_operator = QueryTree("OPERATOR", "AND")
    and_operator.add_child(comp1)
    and_operator.add_child(comp2)
    and_operator.add_child(comp3)
    
    filter_node = QueryTree("FILTER")
    filter_node.add_child(relation)
    filter_node.add_child(and_operator)
    
    query = ParsedQuery(filter_node, "SELECT * FROM users WHERE age > 25 AND status = 'active' AND city = 'Jakarta'")
    
    engine = OptimizationEngine()
    
    print("\nStarting with original (AND operator):")
    print(query.query_tree.tree(show_id=True))
    cost_original = engine.get_cost(query)
    print(f"Cost: {cost_original:.2f}")
    
    # Cascade
    print("\n" + "-"*70)
    print("Step 1: Cascade filters [0, 1, 2]")
    operator_orders = {and_operator.id: [0, 1, 2]}
    cascaded = cascade_filters(query, operator_orders)
    
    print("\nCascaded Query Tree:")
    print(cascaded.query_tree.tree(show_id=True))
    cost_cascaded = engine.get_cost(cascaded)
    print(f"Cost: {cost_cascaded:.2f}")
    
    # Uncascade
    print("\n" + "-"*70)
    print("Step 2: Uncascade (reverse transformation)")
    uncascaded = uncascade_filters(cascaded)
    
    print("\nUncascaded Query Tree (back to AND):")
    print(uncascaded.query_tree.tree(show_id=True))
    cost_uncascaded = engine.get_cost(uncascaded)
    print(f"Cost: {cost_uncascaded:.2f}")
    
    print("\nTransformation flow:")
    print("  1. Original:  FILTER(AND(c0, c1, c2))")
    print("  2. Cascade:   FILTER(c0) → FILTER(c1) → FILTER(c2)")
    print("  3. Uncascade: FILTER(AND(c0, c1, c2))  [back to step 1]")
    
    print("\nKey Point: Bidirectional transformation preserves semantics")
