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
    
    print("Concept: Original order [0, 1, 2] as baseline for comparison")
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
    cost = engine.get_cost(query)
    print(f"Cost: {cost:.2f}")
    
    print("\nKey Point: Baseline cost for comparing other orderings")


def scenario_2_reversed_order():
    """Scenario 2.2: Reversed order"""
    print("\n")
    print_separator("SCENARIO 2.2: Reversed Order")
    
    print("Concept: Reverse order [2, 1, 0] - evaluate in opposite sequence")
    
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
    print(f"Original cost: {cost_original:.2f}")
    
    # Reverse order
    print("\n" + "-"*70)
    print("Applying reversed order: [2, 1, 0]")
    print("Order: c2 (city = Jakarta) → c1 (status = active) → c0 (age > 25)")
    
    operator_orders = {and_operator.id: [2, 1, 0]}
    transformed = reorder_and_conditions(query, operator_orders)
    
    print("\nReordered Query Tree:")
    print(transformed.query_tree.tree(show_id=True))
    cost = engine.get_cost(transformed)
    print(f"Cost: {cost:.2f}")
    
    if cost < cost_original:
        print(f"✓ Better! Improvement: {cost_original - cost:.2f}")
    else:
        print(f"Cost change: {cost - cost_original:.2f}")
    
    print("\nKey Point: Order matters when selectivity differs")


def scenario_3_optimal_order():
    """Scenario 2.3: Finding optimal order"""
    print("\n")
    print_separator("SCENARIO 2.3: Finding Optimal Order")
    
    print("Concept: Test all permutations to find best order")
    
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
    print(f"Original cost: {cost_original:.2f}")
    
    # Test all permutations
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
            status = " ← NEW BEST"
        
        print(f"  Order {list(perm)}: Cost {cost:.2f}{status}")
    
    print("\n" + "-"*70)
    print(f"Optimal order: {best_order}")
    print(f"Best cost: {best_cost:.2f}")
    print(f"Improvement over original: {cost_original - best_cost:.2f}")
    
    print("\nKey Point: Exhaustive search finds optimal order")
    print("Note: For larger queries, use Genetic Algorithm (Demo 11)")
