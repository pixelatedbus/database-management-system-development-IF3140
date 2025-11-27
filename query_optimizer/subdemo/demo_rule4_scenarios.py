"""
Separate scenarios for Demo Rule 4 to keep demo.py cleaner
"""

from query_optimizer.optimization_engine import OptimizationEngine
from query_optimizer import rule_4


def print_separator(title):
    """Print section separator"""
    print("\n" + "="*70)
    print(f"  {title}")
    print("="*70)


def scenario_1_basic_cross_join():
    """Scenario 4.1: Basic FILTER over CROSS JOIN"""
    print("\n")
    print_separator("SCENARIO 4.1: Basic FILTER over CROSS JOIN")
    
    print("Concept: FILTER(JOIN(CROSS), condition) -> JOIN(INNER, condition)")
    print("Query: SELECT * FROM employees e, payroll p WHERE e.id = p.employee_id")
    
    engine = OptimizationEngine()
    sql = "SELECT * FROM employees e, payroll p WHERE e.id = p.employee_id"
    parsed = engine.parse_query(sql)
    
    print("\nOriginal Query Tree:")
    print(parsed.query_tree.tree(show_id=True))
    
    patterns = rule_4.find_patterns(parsed)
    print(f"\nFound {len(patterns)} pattern(s)")
    for join_id, metadata in patterns.items():
        print(f"  JOIN {join_id}: {len(metadata['filter_conditions'])} filter condition(s)")
    
    print("\n" + "-"*70)
    print("Option 1: Keep FILTER separate")
    join_params_separate = {join_id: [] for join_id in patterns.keys()}
    separate_query = rule_4.apply_merge(parsed, join_params_separate)
    print(separate_query.query_tree.tree(show_id=True))
    cost_separate = engine.get_cost(separate_query)
    print(f"Cost: {cost_separate:.2f}")
    
    print("\n" + "-"*70)
    print("Option 2: Merge all conditions")
    join_params_merge = {join_id: metadata['filter_conditions'] for join_id, metadata in patterns.items()}
    merged_query = rule_4.apply_merge(parsed, join_params_merge)
    print(merged_query.query_tree.tree(show_id=True))
    cost_merge = engine.get_cost(merged_query)
    print(f"Cost: {cost_merge:.2f}")
    
    print("\n" + "-"*70)
    print(f"Cost Comparison: Separate={cost_separate:.2f}, Merge={cost_merge:.2f}")
    if cost_merge < cost_separate:
        print(f"✓ Merging is better! Savings: {cost_separate - cost_merge:.2f}")
    
    print("\nKey Point: join_params = {join_id: [condition_ids]} - [] keeps FILTER separate")
    
    return merged_query, separate_query


def scenario_2_filter_over_inner():
    """Scenario 4.2: Additional FILTER over INNER JOIN"""
    print("\n")
    print_separator("SCENARIO 4.2: FILTER over INNER JOIN")
    
    print("Concept: Can we add FILTER condition to existing INNER JOIN?")
    print("Query: SELECT * FROM employees e INNER JOIN payroll p ON e.department_id = p.department_id WHERE e.salary > 5000")
    
    engine = OptimizationEngine()
    sql = "SELECT * FROM employees e INNER JOIN payroll p ON e.department_id = p.department_id WHERE e.salary > 5000"
    parsed = engine.parse_query(sql)
    
    print("\nOriginal Query Tree:")
    print(parsed.query_tree.tree(show_id=True))
    
    patterns = rule_4.find_patterns(parsed)
    print(f"\nFound {len(patterns)} pattern(s)")
    for join_id, metadata in patterns.items():
        print(f"  JOIN {join_id}: {len(metadata['filter_conditions'])} filter condition(s), {len(metadata['existing_conditions'])} existing condition(s)")
    
    print("\n" + "-"*70)
    print("Option 1: Keep FILTER separate")
    join_params_sep = {join_id: [] for join_id in patterns.keys()}
    separate = rule_4.apply_merge(parsed, join_params_sep)
    print(separate.query_tree.tree(show_id=True))
    cost_sep = engine.get_cost(separate)
    print(f"Cost: {cost_sep:.2f}")
    
    print("\n" + "-"*70)
    print("Option 2: Merge all conditions")
    join_params_merge = {join_id: metadata['filter_conditions'] for join_id, metadata in patterns.items()}
    merged = rule_4.apply_merge(parsed, join_params_merge)
    print(merged.query_tree.tree(show_id=True))
    cost_merge = engine.get_cost(merged)
    print(f"Cost: {cost_merge:.2f}")
    
    print("\n" + "-"*70)
    print(f"Cost Comparison: Separate={cost_sep:.2f}, Merge={cost_merge:.2f}")
    if cost_merge < cost_sep:
        print(f"✓ Merging is better! Savings: {cost_sep - cost_merge:.2f}")
    
    return merged, separate


def scenario_3_undo_merge():
    """Scenario 4.3: Reverse Transformation (Undo Merge)"""
    print("\n")
    print_separator("SCENARIO 4.3: Reverse Transformation (Undo Merge)")
    
    print("Concept: Can we convert INNER JOIN back to CROSS JOIN + FILTER?")
    print("Answer: Yes, using undo_merge()")
    print("Query: SELECT * FROM employees e INNER JOIN payroll p ON e.id = p.employee_id")
    
    engine = OptimizationEngine()
    sql = "SELECT * FROM employees e INNER JOIN payroll p ON e.id = p.employee_id"
    parsed = engine.parse_query(sql)
    
    print("\nStarting with INNER JOIN:")
    print(parsed.query_tree.tree(show_id=True))
    cost_inner = engine.get_cost(parsed)
    print(f"Cost: {cost_inner:.2f}")
    
    print("\n" + "-"*70)
    print("After undo_merge():")
    undone = rule_4.undo_merge(parsed)
    print(undone.query_tree.tree(show_id=True))
    cost_undone = engine.get_cost(undone)
    print(f"Cost: {cost_undone:.2f}")
    print(f"\nCost increase: {cost_undone - cost_inner:.2f}")
    
    return undone


def scenario_4_nested_filters():
    """Scenario 4.4: Nested FILTERs from Undo (Advanced)"""
    print("\n")
    print_separator("SCENARIO 4.4: Nested FILTERs from Undo (Advanced)")
    
    print("Concept: What happens when INNER JOIN with AND conditions is undone?")
    print("Result: FILTER with AND conditions over CROSS JOIN")
    print("Query: SELECT * FROM employees e INNER JOIN payroll p ON e.department_id = p.department_id AND e.salary > 5000")
    
    engine = OptimizationEngine()
    sql = "SELECT * FROM employees e INNER JOIN payroll p ON e.department_id = p.department_id AND e.salary > 5000"
    parsed = engine.parse_query(sql)
    
    print("\nStarting with INNER JOIN (multiple AND conditions):")
    print(parsed.query_tree.tree(show_id=True))
    cost_inner = engine.get_cost(parsed)
    print(f"Cost: {cost_inner:.2f}")
    
    def count_filters(node):
        if node is None:
            return 0
        count = 1 if node.type == "FILTER" else 0
        for child in node.childs:
            count += count_filters(child)
        return count
    
    num_before = count_filters(parsed.query_tree)
    
    print("\n" + "-"*70)
    print("After undo_merge():")
    undone = rule_4.undo_merge(parsed)
    print(undone.query_tree.tree(show_id=True))
    cost_undone = engine.get_cost(undone)
    print(f"Cost: {cost_undone:.2f}")
    
    num_after = count_filters(undone.query_tree)
    
    print("\n" + "-"*70)
    print(f"FILTER nodes: {num_before} -> {num_after}")
    print(f"Cost: {cost_inner:.2f} -> {cost_undone:.2f}")
    if cost_undone > cost_inner:
        print(f"Cost increase: {cost_undone - cost_inner:.2f} (CROSS produces more rows)")
    
    return undone


def scenario_5_merge_into_merged():
    """Scenario 4.5: Merge FILTER into Already-Merged INNER JOIN"""
    print("\n")
    print_separator("SCENARIO 4.5: Merge FILTER into Already-Merged INNER JOIN")
    
    print("Concept: FILTER over INNER JOIN (already has condition)")
    print("Question: Can we merge additional FILTER into existing JOIN condition?")
    print("Answer: Yes! Combine with AND operator")
    print("Query: SELECT * FROM employees e INNER JOIN accounts a ON e.id = a.employee_id WHERE e.salary > 7000")
    
    engine = OptimizationEngine()
    sql = "SELECT * FROM employees e INNER JOIN accounts a ON e.id = a.employee_id WHERE e.salary > 7000"
    parsed = engine.parse_query(sql)
    
    print("\nOriginal Query Tree:")
    print(parsed.query_tree.tree(show_id=True))
    cost_original = engine.get_cost(parsed)
    print(f"\nCost: {cost_original:.2f}")
    
    patterns = rule_4.find_patterns(parsed)
    print(f"\nFound {len(patterns)} pattern(s) to merge")
    for join_id, metadata in patterns.items():
        print(f"  JOIN {join_id}: {len(metadata['filter_conditions'])} filter condition(s), {len(metadata['existing_conditions'])} existing condition(s)")
    
    print("\n" + "-"*70)
    print("Option 1: Keep FILTER separate")
    join_params_sep = {join_id: [] for join_id in patterns.keys()}
    separate = rule_4.apply_merge(parsed, join_params_sep)
    print(separate.query_tree.tree(show_id=True))
    cost_sep = engine.get_cost(separate)
    print(f"Cost: {cost_sep:.2f}")
    
    print("\n" + "-"*70)
    print("Option 2: Merge all conditions")
    join_params_merge = {join_id: metadata['filter_conditions'] for join_id, metadata in patterns.items()}
    merged = rule_4.apply_merge(parsed, join_params_merge)
    print(merged.query_tree.tree(show_id=True))
    cost_merge = engine.get_cost(merged)
    print(f"Cost: {cost_merge:.2f}")
    
    print("\n" + "-"*70)
    print(f"Cost Comparison: Separate={cost_sep:.2f}, Merge={cost_merge:.2f}")
    if cost_merge < cost_sep:
        print(f"✓ Merging is better! Savings: {cost_sep - cost_merge:.2f}")
    
    print("\n" + "="*70)
    print("  UNDO TRANSFORMATION: Two Possible Results")
    print("="*70)
    
    print("\nStarting from MERGED state:")
    print(merged.query_tree.tree(show_id=True))
    
    print("\n" + "-"*70)
    print("After undo_merge():")
    undone = rule_4.undo_merge(merged)
    print(undone.query_tree.tree(show_id=True))
    cost_undone = engine.get_cost(undone)
    print(f"Cost: {cost_undone:.2f}")
    
    print("\n" + "="*70)
    print("  COST SUMMARY")
    print("="*70)
    print(f"Separate: {cost_sep:.2f} | Merged: {cost_merge:.2f} | Undone: {cost_undone:.2f}")
    
    print("\n" + "="*70)
    print("  PARTIAL MERGE DEMO")
    print("="*70)
    
    partial_patterns = rule_4.find_patterns(undone)
    if partial_patterns:
        for join_id, metadata in partial_patterns.items():
            filter_conds = metadata['filter_conditions']
            if len(filter_conds) >= 2:
                partial_params = {join_id: [filter_conds[0]]}
                partial = rule_4.apply_merge(undone, partial_params)
                
                print(f"\nMerge condition {filter_conds[0]}, keep {filter_conds[1]} in FILTER:")
                print(partial.query_tree.tree(show_id=True))
                cost_partial = engine.get_cost(partial)
                print(f"Cost: {cost_partial:.2f}")
                print(f"\nAll merged: {cost_merge:.2f} | Partial: {cost_partial:.2f} | None: {cost_undone:.2f}")
                break
    
    return merged, undone
