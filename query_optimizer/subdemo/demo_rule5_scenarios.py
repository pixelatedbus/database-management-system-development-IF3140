"""
Separate scenarios for Demo Rule 5 (Join Commutativity) to keep demo.py cleaner
"""

from query_optimizer.optimization_engine import OptimizationEngine
from query_optimizer.rule import rule_5


def print_separator(title):
    """Print section separator"""
    print("\n" + "="*70)
    print(f"  {title}")
    print("="*70)


def scenario_1_basic_swap():
    """Scenario 5.1: Basic JOIN(A, B) → JOIN(B, A)"""
    print("\n")
    print_separator("SCENARIO 5.1: Basic JOIN Commutativity")
    
    print("Concept: JOIN(A, B) ≡ JOIN(B, A)")
    print("Rule: We can swap the order of tables in a JOIN")
    print("Query: SELECT * FROM users u INNER JOIN orders o ON u.id = o.user_id")
    
    engine = OptimizationEngine()
    sql = "SELECT * FROM users u INNER JOIN orders o ON u.id = o.user_id"
    parsed = engine.parse_query(sql)
    
    print("\nOriginal Query Tree:")
    print(parsed.query_tree.tree(show_id=True))
    cost_original = engine.get_cost(parsed)
    print(f"Cost: {cost_original:.2f}")
    
    # Find JOIN nodes
    join_nodes = rule_5.find_join_nodes(parsed)
    print(f"\nFound {len(join_nodes)} JOIN node(s)")
    for join_id, metadata in join_nodes.items():
        print(f"  JOIN {join_id}: {metadata['join_type']}")
        print(f"    Left: {metadata['left_child'].type if metadata['left_child'] else 'None'}")
        print(f"    Right: {metadata['right_child'].type if metadata['right_child'] else 'None'}")
    
    print("\n" + "-"*70)
    print("Option 1: Keep original order (A, B)")
    join_orders_no_swap = {join_id: False for join_id in join_nodes.keys()}
    no_swap = rule_5.join_komutatif(parsed, join_orders_no_swap)
    print(no_swap.query_tree.tree(show_id=True))
    cost_no_swap = engine.get_cost(no_swap)
    print(f"Cost: {cost_no_swap:.2f}")
    
    print("\n" + "-"*70)
    print("Option 2: Swap to (B, A)")
    join_orders_swap = {join_id: True for join_id in join_nodes.keys()}
    swapped = rule_5.join_komutatif(parsed, join_orders_swap)
    print(swapped.query_tree.tree(show_id=True))
    cost_swapped = engine.get_cost(swapped)
    print(f"Cost: {cost_swapped:.2f}")
    
    print("\n" + "-"*70)
    print(f"Cost Comparison: Original={cost_no_swap:.2f}, Swapped={cost_swapped:.2f}")
    if cost_swapped < cost_no_swap:
        print(f"✓ Swapping is better! Savings: {cost_no_swap - cost_swapped:.2f}")
    elif cost_swapped > cost_no_swap:
        print(f"✗ Swapping is worse. Increase: {cost_swapped - cost_no_swap:.2f}")
    else:
        print("= Same cost (symmetric tables)")
    
    print("\nKey Point: join_child_params = {join_id: bool} - True swaps, False keeps original")
    
    return swapped, no_swap


def scenario_2_multiple_joins():
    """Scenario 5.2: Multiple JOINs - Selective Swapping"""
    print("\n")
    print_separator("SCENARIO 5.2: Multiple JOINs - Selective Swapping")
    
    print("Concept: With multiple JOINs, we can swap each independently")
    print("Query: SELECT * FROM users u JOIN orders o ON u.id = o.user_id JOIN products p ON o.product_id = p.id")
    
    engine = OptimizationEngine()
    sql = """
    SELECT * FROM users u 
    INNER JOIN orders o ON u.id = o.user_id
    INNER JOIN products p ON o.product_id = p.id
    """
    parsed = engine.parse_query(sql)
    
    print("\nOriginal Query Tree (nested JOINs):")
    print(parsed.query_tree.tree(show_id=True))
    cost_original = engine.get_cost(parsed)
    print(f"Cost: {cost_original:.2f}")
    
    # Find JOIN nodes
    join_nodes = rule_5.find_join_nodes(parsed)
    print(f"\nFound {len(join_nodes)} JOIN node(s)")
    join_ids = list(join_nodes.keys())
    
    print("\n" + "-"*70)
    print("Option 1: No swaps (original order)")
    no_swap = {join_id: False for join_id in join_ids}
    result1 = rule_5.join_komutatif(parsed, no_swap)
    cost1 = engine.get_cost(result1)
    print(f"Cost: {cost1:.2f}")
    
    print("\n" + "-"*70)
    print("Option 2: Swap first JOIN only")
    swap_first = {join_ids[0]: True, join_ids[1]: False}
    result2 = rule_5.join_komutatif(parsed, swap_first)
    cost2 = engine.get_cost(result2)
    print(result2.query_tree.tree(show_id=True))
    print(f"Cost: {cost2:.2f}")
    
    print("\n" + "-"*70)
    print("Option 3: Swap second JOIN only")
    swap_second = {join_ids[0]: False, join_ids[1]: True}
    result3 = rule_5.join_komutatif(parsed, swap_second)
    cost3 = engine.get_cost(result3)
    print(result3.query_tree.tree(show_id=True))
    print(f"Cost: {cost3:.2f}")
    
    print("\n" + "-"*70)
    print("Option 4: Swap both JOINs")
    swap_both = {join_id: True for join_id in join_ids}
    result4 = rule_5.join_komutatif(parsed, swap_both)
    cost4 = engine.get_cost(result4)
    print(result4.query_tree.tree(show_id=True))
    print(f"Cost: {cost4:.2f}")
    
    print("\n" + "-"*70)
    print(f"Cost Summary:")
    print(f"  No swap: {cost1:.2f}")
    print(f"  Swap 1st: {cost2:.2f}")
    print(f"  Swap 2nd: {cost3:.2f}")
    print(f"  Swap both: {cost4:.2f}")
    
    costs = [cost1, cost2, cost3, cost4]
    best_cost = min(costs)
    best_option = ["No swap", "Swap 1st", "Swap 2nd", "Swap both"][costs.index(best_cost)]
    print(f"\n✓ Best option: {best_option} (cost: {best_cost:.2f})")
    
    return result4


def scenario_3_natural_join():
    """Scenario 5.3: NATURAL JOIN Commutativity"""
    print("\n")
    print_separator("SCENARIO 5.3: NATURAL JOIN Commutativity")
    
    print("Concept: NATURAL JOIN is also commutative")
    print("Query: SELECT * FROM employees e NATURAL JOIN payroll p")
    
    engine = OptimizationEngine()
    sql = "SELECT * FROM employees e NATURAL JOIN payroll p"
    parsed = engine.parse_query(sql)
    
    print("\nOriginal Query Tree:")
    print(parsed.query_tree.tree(show_id=True))
    cost_original = engine.get_cost(parsed)
    print(f"Cost: {cost_original:.2f}")
    
    # Find JOIN nodes
    join_nodes = rule_5.find_join_nodes(parsed)
    print(f"\nFound {len(join_nodes)} NATURAL JOIN node(s)")
    
    print("\n" + "-"*70)
    print("After swapping:")
    join_orders = {join_id: True for join_id in join_nodes.keys()}
    swapped = rule_5.join_komutatif(parsed, join_orders)
    print(swapped.query_tree.tree(show_id=True))
    cost_swapped = engine.get_cost(swapped)
    print(f"Cost: {cost_swapped:.2f}")
    
    print("\n" + "-"*70)
    print(f"Cost Comparison: Original={cost_original:.2f}, Swapped={cost_swapped:.2f}")
    print("Note: NATURAL JOIN semantics remain the same after swap")
    
    return swapped


def scenario_4_bidirectional():
    """Scenario 5.4: Bidirectional Transformation"""
    print("\n")
    print_separator("SCENARIO 5.4: Bidirectional Transformation")
    
    print("Concept: Swap twice returns to original")
    print("JOIN(A, B) → JOIN(B, A) → JOIN(A, B)")
    print("Query: SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id")
    
    engine = OptimizationEngine()
    sql = "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id"
    parsed = engine.parse_query(sql)
    
    print("\nOriginal:")
    print(parsed.query_tree.tree(show_id=True))
    cost_original = engine.get_cost(parsed)
    print(f"Cost: {cost_original:.2f}")
    
    # Find JOIN nodes
    join_nodes = rule_5.find_join_nodes(parsed)
    join_orders_swap = {join_id: True for join_id in join_nodes.keys()}
    
    print("\n" + "-"*70)
    print("After first swap:")
    swapped_once = rule_5.join_komutatif(parsed, join_orders_swap)
    print(swapped_once.query_tree.tree(show_id=True))
    cost_swapped = engine.get_cost(swapped_once)
    print(f"Cost: {cost_swapped:.2f}")
    
    print("\n" + "-"*70)
    print("After second swap (back to original):")
    swapped_twice = rule_5.join_komutatif(swapped_once, join_orders_swap)
    print(swapped_twice.query_tree.tree(show_id=True))
    cost_final = engine.get_cost(swapped_twice)
    print(f"Cost: {cost_final:.2f}")
    
    print("\n" + "-"*70)
    print(f"Cost: {cost_original:.2f} → {cost_swapped:.2f} → {cost_final:.2f}")
    print(f"Original cost == Final cost: {abs(cost_original - cost_final) < 0.01}")
    
    return swapped_twice


def scenario_5_ga_exploration():
    """Scenario 5.5: Genetic Algorithm Exploration"""
    print("\n")
    print_separator("SCENARIO 5.5: GA Explores Join Orders")
    
    print("Concept: GA can explore different join orders to find optimal")
    print("Query: SELECT * FROM users u JOIN orders o ON u.id = o.user_id JOIN products p ON o.product_id = p.id")
    
    from query_optimizer.genetic_optimizer import GeneticOptimizer
    
    engine = OptimizationEngine()
    sql = """
    SELECT * FROM users u 
    INNER JOIN orders o ON u.id = o.user_id
    INNER JOIN products p ON o.product_id = p.id
    """
    parsed = engine.parse_query(sql)
    
    print("\nOriginal Query:")
    print(parsed.query_tree.tree(show_id=True))
    cost_original = engine.get_cost(parsed)
    print(f"Original Cost: {cost_original:.2f}")
    
    print("\n" + "-"*70)
    print("Running Genetic Algorithm (10 generations)...")
    ga = GeneticOptimizer(
        population_size=20,
        generations=10,
        mutation_rate=0.2,
        crossover_rate=0.8
    )
    
    optimized = ga.optimize(parsed)
    
    print("\nOptimized Query:")
    print(optimized.query_tree.tree(show_id=True))
    cost_optimized = engine.get_cost(optimized)
    print(f"Optimized Cost: {cost_optimized:.2f}")
    
    print("\n" + "-"*70)
    print(f"Cost Improvement: {cost_original:.2f} → {cost_optimized:.2f}")
    if cost_optimized < cost_original:
        improvement = ((cost_original - cost_optimized) / cost_original) * 100
        print(f"✓ Improved by {improvement:.1f}%")
    
    # Show best parameters
    if ga.best_individual and 'join_child_params' in ga.best_individual.operation_params:
        print("\nBest JOIN child parameters:")
        for join_id, swap in ga.best_individual.operation_params['join_child_params'].items():
            print(f"  JOIN {join_id}: {'SWAP' if swap else 'KEEP'}")
    
    return optimized


def run_all_scenarios():
    """Run all Rule 5 scenarios"""
    print("\n" + "="*70)
    print("  DEMO: RULE 5 - JOIN COMMUTATIVITY")
    print("  JOIN(A, B) ≡ JOIN(B, A)")
    print("="*70)
    
    scenario_1_basic_swap()
    scenario_2_multiple_joins()
    scenario_3_natural_join()
    scenario_4_bidirectional()
    scenario_5_ga_exploration()
    
    print("\n" + "="*70)
    print("  DEMO COMPLETE")
    print("="*70)
    print("\nKey Takeaways:")
    print("1. JOIN operations are commutative: JOIN(A,B) ≡ JOIN(B,A)")
    print("2. Different orders may have different costs (based on table sizes)")
    print("3. GA can explore all possible join orders to find the optimal")
    print("4. Works with INNER, NATURAL, and CROSS JOINs")
    print("5. Can swap multiple JOINs independently for best performance")


if __name__ == "__main__":
    run_all_scenarios()
