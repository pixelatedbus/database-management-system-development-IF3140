from query_optimizer.optimization_engine import OptimizationEngine
from query_optimizer.rule import rule_6


def print_separator(title):
    print("\n" + "="*70)
    print(f"  {title}")
    print("="*70)


def scenario_1_basic_reassociation():
    print("\n")
    print_separator("SCENARIO 6.1: Basic JOIN Associativity")

    print("Concept: (A JOIN B) JOIN C = A JOIN (B JOIN C)")
    print("Rule: We can change the nesting structure of JOINs")
    print("Query: SELECT * FROM employees e JOIN payroll p ON e.id = p.employee_id JOIN accounts a ON p.id = a.payroll_id")

    engine = OptimizationEngine()
    sql = """
    SELECT * FROM employees e
    INNER JOIN payroll p ON e.id = p.employee_id
    INNER JOIN accounts a ON p.id = a.payroll_id
    """
    parsed = engine.parse_query(sql)

    print("\nOriginal Query Tree (Left-Associate: ((E JOIN P) JOIN A)):")
    cost_original = engine.get_cost(parsed)
    print(f"Cost: {cost_original:.2f}")

    patterns = rule_6.find_patterns(parsed)
    print(f"\nFound {len(patterns)} nested JOIN pattern(s)")
    for join_id, metadata in patterns.items():
        print(f"  JOIN {join_id}:")
        print(f"    Inner JOIN ID: {metadata['inner_join_id']}")
        print(f"    Outer condition: {metadata['outer_condition']}")
        print(f"    Inner condition: {metadata['inner_condition']}")

    print("\n" + "-"*70)
    print("Option 1: Keep left-associate ((E JOIN P) JOIN A)")
    decisions_left = {join_id: 'left' for join_id in patterns.keys()}
    left_assoc = rule_6.apply_associativity(parsed, decisions_left)
    cost_left = engine.get_cost(left_assoc)
    print(f"Cost: {cost_left:.2f}")

    print("\n" + "-"*70)
    print("Option 2: Right-associate to (E JOIN (P JOIN A))")
    decisions_right = {join_id: 'right' for join_id in patterns.keys()}
    right_assoc = rule_6.apply_associativity(parsed, decisions_right)
    cost_right = engine.get_cost(right_assoc)
    print(f"Cost: {cost_right:.2f}")

    print("\n" + "-"*70)
    print("Option 3: No change (keep original)")
    decisions_none = {join_id: 'none' for join_id in patterns.keys()}
    no_change = rule_6.apply_associativity(parsed, decisions_none)
    cost_none = engine.get_cost(no_change)
    print(f"Cost: {cost_none:.2f}")

    print("\n" + "-"*70)
    print(f"Cost Comparison:")
    print(f"  Left-associate:  {cost_left:.2f}")
    print(f"  Right-associate: {cost_right:.2f}")
    print(f"  No change:       {cost_none:.2f}")

    best = min([('left', cost_left), ('right', cost_right), ('none', cost_none)], key=lambda x: x[1])
    print(f"\nBest strategy: {best[0]} with cost {best[1]:.2f}")

    print("\nKey Point: join_associativity_params = {join_id: 'left'|'right'|'none'}")
    print("  'left': Shift joins to left - ((A JOIN B) JOIN C)")
    print("  'right': Shift joins to right - (A JOIN (B JOIN C))")
    print("  'none': Keep current structure")

    return right_assoc, left_assoc, no_change


def scenario_2_semantic_validation():
    print("\n")
    print_separator("SCENARIO 6.2: Semantic Validation")

    print("Concept: Reassociation requires semantic check")
    print("Rule: Outer join condition must only reference inner tables")
    print("Query: Three-way join with conditions")

    engine = OptimizationEngine()
    sql = """
    SELECT * FROM users u
    INNER JOIN orders o ON u.id = o.user_id
    INNER JOIN products p ON o.product_id = p.id
    """
    parsed = engine.parse_query(sql)

    print("\nOriginal Query Tree:")
    cost_original = engine.get_cost(parsed)
    print(f"Cost: {cost_original:.2f}")

    patterns = rule_6.find_patterns(parsed)
    print(f"\nFound {len(patterns)} nested JOIN pattern(s)")

    if patterns:
        print("\n" + "-"*70)
        print("Attempting right-associate...")
        decisions = {join_id: 'right' for join_id in patterns.keys()}
        result = rule_6.apply_associativity(parsed, decisions)
        cost_result = engine.get_cost(result)
        print(f"Cost: {cost_result:.2f}")

        print("\nAnalysis:")
        print("Outer condition references orders and products")
        print("Can safely reassociate the nested joins")
        print("Result: U JOIN (O JOIN P)")
    else:
        print("\nNo nested JOIN patterns found for this query structure")

    return result if patterns else parsed


def scenario_3_performance_impact():
    print("\n")
    print_separator("SCENARIO 6.3: Performance Impact Analysis")

    print("Concept: Join order affects intermediate result size")
    print("Scenario: Large users table, medium orders, small products")

    engine = OptimizationEngine()
    sql = """
    SELECT * FROM users u
    INNER JOIN orders o ON u.id = o.user_id
    INNER JOIN products p ON o.product_id = p.id
    """
    parsed = engine.parse_query(sql)

    print("\nAssume table sizes:")
    print("  - users: 10,000 rows")
    print("  - orders: 5,000 rows")
    print("  - products: 500 rows")

    patterns = rule_6.find_patterns(parsed)

    print("\n" + "-"*70)
    print("Strategy 1: Left-Associate ((U JOIN O) JOIN P)")
    print("  Step 1: U JOIN O -> 5,000 rows")
    print("  Step 2: (U JOIN O) JOIN P -> 5,000 x 500 comparisons")
    decisions_left = {join_id: 'left' for join_id in patterns.keys()}
    left_result = rule_6.apply_associativity(parsed, decisions_left)
    cost_left = engine.get_cost(left_result)
    print(f"  Estimated cost: {cost_left:.2f}")

    print("\n" + "-"*70)
    print("Strategy 2: Right-Associate (U JOIN (O JOIN P))")
    print("  Step 1: O JOIN P -> intermediate result")
    print("  Step 2: U JOIN (O JOIN P) -> different comparisons")
    decisions_right = {join_id: 'right' for join_id in patterns.keys()}
    right_result = rule_6.apply_associativity(parsed, decisions_right)
    cost_right = engine.get_cost(right_result)
    print(f"  Estimated cost: {cost_right:.2f}")

    print("\n" + "-"*70)
    print("Analysis:")
    if cost_left < cost_right:
        print(f"Left-associate is better by {cost_right - cost_left:.2f}")
        print("  Reason: Smaller intermediate result (10K vs 500)")
    else:
        print(f"Right-associate is better by {cost_left - cost_right:.2f}")
        print("  Reason: Depends on join selectivity")

    print("\nKey Insight: Optimal associativity depends on:")
    print("  - Table sizes")
    print("  - Join selectivity")
    print("  - Available indexes")
    print("  -> This is why GA exploration is valuable!")

    return left_result, right_result


def scenario_4_complex_nested():
    print("\n")
    print_separator("SCENARIO 6.4: Complex Nested JOINs")

    print("Concept: Multiple nested JOINs with selective reassociation")
    print("Query: Four-way join")

    engine = OptimizationEngine()
    sql = """
    SELECT * FROM users u
    INNER JOIN orders o ON u.id = o.user_id
    INNER JOIN products p ON o.product_id = p.id
    INNER JOIN logs l ON u.id = l.user_id
    """
    parsed = engine.parse_query(sql)

    print("\nOriginal Query Tree (Left-Associate: (((U JOIN O) JOIN P) JOIN L)):")
    cost_original = engine.get_cost(parsed)
    print(f"Cost: {cost_original:.2f}")

    patterns = rule_6.find_patterns(parsed)
    print(f"\nFound {len(patterns)} nested JOIN pattern(s)")

    if len(patterns) >= 2:
        join_ids = list(patterns.keys())

        print("\n" + "-"*70)
        print("Strategy 1: Right-associate outer, keep inner left")
        decisions1 = {join_ids[0]: 'right', join_ids[1]: 'left'}
        result1 = rule_6.apply_associativity(parsed, decisions1)
        cost1 = engine.get_cost(result1)
        print(f"Cost: {cost1:.2f}")

        print("\n" + "-"*70)
        print("Strategy 2: Right-associate both")
        decisions2 = {join_id: 'right' for join_id in join_ids}
        result2 = rule_6.apply_associativity(parsed, decisions2)
        cost2 = engine.get_cost(result2)
        print(f"Cost: {cost2:.2f}")

        print("\n" + "-"*70)
        print("Strategy 3: Keep both left (original)")
        decisions3 = {join_id: 'left' for join_id in join_ids}
        result3 = rule_6.apply_associativity(parsed, decisions3)
        cost3 = engine.get_cost(result3)
        print(f"Cost: {cost3:.2f}")

        print("\n" + "-"*70)
        print(f"Cost Comparison:")
        print(f"  Strategy 1: {cost1:.2f}")
        print(f"  Strategy 2: {cost2:.2f}")
        print(f"  Strategy 3: {cost3:.2f}")

        best = min([('1', cost1), ('2', cost2), ('3', cost3)], key=lambda x: x[1])
        print(f"\nBest strategy: Strategy {best[0]} with cost {best[1]:.2f}")

        return result1, result2, result3
    else:
        print("\nInsufficient nested JOINs for complex scenario")
        return parsed, parsed, parsed


def run_all_scenarios():
    print("\n" + "="*70)
    print("  RULE 6 DEMO: JOIN ASSOCIATIVITY")
    print("="*70)

    scenario_1_basic_reassociation()
    scenario_2_semantic_validation()
    scenario_3_performance_impact()
    scenario_4_complex_nested()

    print("\n" + "="*70)
    print("  DEMO COMPLETE")
    print("="*70)
    print("\nKey Takeaways:")
    print("1. Join associativity changes join tree structure")
    print("2. Semantic validation ensures correctness")
    print("3. Different associations have different costs")
    print("4. GA explores to find optimal join order")
    print("5. Parameters: {join_id: 'left'|'right'|'none'}")


if __name__ == "__main__":
    run_all_scenarios()
