"""
Separate scenarios for Demo Rule 4 to keep demo.py cleaner
"""

from query_optimizer.optimization_engine import OptimizationEngine
from query_optimizer import rule_4
from query_optimizer.query_tree import QueryTree
from query_optimizer.optimization_engine import ParsedQuery


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
    print("Building: FILTER(e.id = p.employee_id) -> JOIN(CROSS, employees, payroll)")
    
    # Build CROSS JOIN structure
    rel1 = QueryTree("RELATION", "employees")
    alias1 = QueryTree("ALIAS", "e")
    alias1.add_child(rel1)
    
    rel2 = QueryTree("RELATION", "payroll")
    alias2 = QueryTree("ALIAS", "p")
    alias2.add_child(rel2)
    
    join = QueryTree("JOIN", "CROSS")
    join.add_child(alias1)
    join.add_child(alias2)
    
    # Build condition: e.id = p.employee_id
    left_col = make_column_ref("id", "e")
    right_col = make_column_ref("employee_id", "p")
    
    condition = QueryTree("COMPARISON", "=")
    condition.add_child(left_col)
    condition.add_child(right_col)
    
    # Build FILTER node
    filter_node = QueryTree("FILTER", "")
    filter_node.add_child(join)
    filter_node.add_child(condition)
    
    # Build PROJECT node
    project = QueryTree("PROJECT", "*")
    project.add_child(filter_node)
    
    parsed = ParsedQuery(project, "SELECT * FROM employees e, payroll p WHERE e.id = p.employee_id")
    
    engine = OptimizationEngine()
    
    print("\nOriginal Query Tree:")
    print(parsed.query_tree.tree(show_id=True))
    
    # Analyze patterns
    patterns = rule_4.find_patterns(parsed)
    print(f"\nFound {len(patterns)} pattern(s)")
    for join_id, metadata in patterns.items():
        print(f"  JOIN {join_id}: {len(metadata['filter_conditions'])} filter condition(s)")
    
    # Option 1: Keep separate
    print("\n" + "-"*70)
    print("Option 1: Keep FILTER separate (merge no conditions = [])")
    print("Structure: PROJECT -> FILTER -> JOIN(CROSS)")
    join_params_separate = {join_id: [] for join_id in patterns.keys()}  # Empty list = no merge
    separate_query = rule_4.apply_merge(parsed, join_params_separate)
    print("\nTransformed Query Tree:")
    print(separate_query.query_tree.tree(show_id=True))
    cost_separate = engine.get_cost(separate_query)
    print(f"Cost: {cost_separate:.2f}")
    
    # Option 2: Merge
    print("\n" + "-"*70)
    print("Option 2: Merge FILTER into JOIN (merge all conditions)")
    print("Structure: PROJECT -> JOIN(INNER, condition)")
    join_params_merge = {join_id: metadata['filter_conditions'] for join_id, metadata in patterns.items()}
    merged_query = rule_4.apply_merge(parsed, join_params_merge)
    print("\nTransformed Query Tree:")
    print(merged_query.query_tree.tree(show_id=True))
    cost_merge = engine.get_cost(merged_query)
    print(f"Cost: {cost_merge:.2f}")
    
    print("\n" + "-"*70)
    print(f"Cost Comparison: Separate={cost_separate:.2f}, Merge={cost_merge:.2f}")
    if cost_merge < cost_separate:
        print(f"✓ Merging is better! Savings: {cost_separate - cost_merge:.2f}")
    
    print("\nKey Point: CROSS JOIN + FILTER can be merged into INNER JOIN")
    print("Format: join_params = {join_id: [condition_ids_to_merge]}")
    print("  - Empty list [] = keep FILTER separate")
    print("  - [cond_ids] = merge those conditions into JOIN")
    
    return merged_query, separate_query


def scenario_2_filter_over_inner():
    """Scenario 4.2: Additional FILTER over INNER JOIN"""
    print("\n")
    print_separator("SCENARIO 4.2: FILTER over INNER JOIN")
    
    print("Concept: Can we add FILTER condition to existing INNER JOIN?")
    print("Building: FILTER(salary > 5000) -> JOIN(INNER, dept_id = dept_id)")
    
    # Build INNER JOIN with condition
    rel1 = QueryTree("RELATION", "employees")
    alias1 = QueryTree("ALIAS", "e")
    alias1.add_child(rel1)
    
    rel2 = QueryTree("RELATION", "payroll")
    alias2 = QueryTree("ALIAS", "p")
    alias2.add_child(rel2)
    
    # JOIN condition: e.department_id = p.department_id
    dept_left = make_column_ref("department_id", "e")
    dept_right = make_column_ref("department_id", "p")
    join_cond = QueryTree("COMPARISON", "=")
    join_cond.add_child(dept_left)
    join_cond.add_child(dept_right)
    
    inner_join = QueryTree("JOIN", "INNER")
    inner_join.add_child(alias1)
    inner_join.add_child(alias2)
    inner_join.add_child(join_cond)
    
    # Additional FILTER: e.salary > 5000
    salary_col = make_column_ref("salary", "e")
    salary_val = QueryTree("LITERAL_NUMBER", "5000")
    filter_cond = QueryTree("COMPARISON", ">")
    filter_cond.add_child(salary_col)
    filter_cond.add_child(salary_val)
    
    filter_over_inner = QueryTree("FILTER", "")
    filter_over_inner.add_child(inner_join)
    filter_over_inner.add_child(filter_cond)
    
    project = QueryTree("PROJECT", "*")
    project.add_child(filter_over_inner)
    
    parsed = ParsedQuery(project, "SELECT * FROM employees e INNER JOIN payroll p ON e.department_id = p.department_id WHERE e.salary > 5000")
    
    engine = OptimizationEngine()
    
    print("\nOriginal Query Tree:")
    print(parsed.query_tree.tree(show_id=True))
    
    patterns = rule_4.find_patterns(parsed)
    print(f"\nFound {len(patterns)} pattern(s)")
    for join_id, metadata in patterns.items():
        print(f"  JOIN {join_id}: {len(metadata['filter_conditions'])} filter condition(s), {len(metadata['existing_conditions'])} existing condition(s)")
    
    # Option 1: Keep separate
    print("\n" + "-"*70)
    print("Option 1: Keep FILTER separate (merge no conditions = [])")
    join_params_sep = {join_id: [] for join_id in patterns.keys()}  # Empty list = no merge
    separate = rule_4.apply_merge(parsed, join_params_sep)
    print("\nTransformed Query Tree:")
    print(separate.query_tree.tree(show_id=True))
    cost_sep = engine.get_cost(separate)
    print(f"Cost: {cost_sep:.2f}")
    
    # Option 2: Merge
    print("\n" + "-"*70)
    print("Option 2: Merge FILTER into JOIN (merge all conditions)")
    print("Result: JOIN(INNER, dept_id = dept_id AND salary > 5000)")
    join_params_merge = {join_id: metadata['filter_conditions'] for join_id, metadata in patterns.items()}
    merged = rule_4.apply_merge(parsed, join_params_merge)
    print("\nTransformed Query Tree:")
    print(merged.query_tree.tree(show_id=True))
    cost_merge = engine.get_cost(merged)
    print(f"Cost: {cost_merge:.2f}")
    
    print("\n" + "-"*70)
    print(f"Cost Comparison: Separate={cost_sep:.2f}, Merge={cost_merge:.2f}")
    if cost_merge < cost_sep:
        print(f"✓ Merging is better! Savings: {cost_sep - cost_merge:.2f}")
    
    print("\nKey Point: Multiple conditions combined with AND operator in JOIN")
    
    return merged, separate


def scenario_3_undo_merge():
    """Scenario 4.3: Reverse Transformation (Undo Merge)"""
    print("\n")
    print_separator("SCENARIO 4.3: Reverse Transformation (Undo Merge)")
    
    print("Concept: Can we convert INNER JOIN back to CROSS JOIN + FILTER?")
    print("Answer: Yes, using undo_merge()")
    
    # Build INNER JOIN (already merged)
    rel1 = QueryTree("RELATION", "employees")
    alias1 = QueryTree("ALIAS", "e")
    alias1.add_child(rel1)
    
    rel2 = QueryTree("RELATION", "payroll")
    alias2 = QueryTree("ALIAS", "p")
    alias2.add_child(rel2)
    
    # Condition: e.id = p.employee_id
    left_col = make_column_ref("id", "e")
    right_col = make_column_ref("employee_id", "p")
    condition = QueryTree("COMPARISON", "=")
    condition.add_child(left_col)
    condition.add_child(right_col)
    
    inner_join = QueryTree("JOIN", "INNER")
    inner_join.add_child(alias1)
    inner_join.add_child(alias2)
    inner_join.add_child(condition)
    
    project = QueryTree("PROJECT", "*")
    project.add_child(inner_join)
    
    parsed = ParsedQuery(project, "SELECT * FROM employees e INNER JOIN payroll p ON e.id = p.employee_id")
    
    engine = OptimizationEngine()
    
    print("\nStarting with INNER JOIN:")
    print(parsed.query_tree.tree(show_id=True))
    cost_inner = engine.get_cost(parsed)
    print(f"Cost: {cost_inner:.2f}")
    
    # Apply undo_merge
    print("\n" + "-"*70)
    print("Applying undo_merge()...")
    undone = rule_4.undo_merge(parsed)
    
    print("\nAfter undo (back to CROSS JOIN + FILTER):")
    print(undone.query_tree.tree(show_id=True))
    cost_undone = engine.get_cost(undone)
    print(f"Cost: {cost_undone:.2f}")
    
    print("\n" + "-"*70)
    print("Transformation flow:")
    print("  1. Start:  JOIN(INNER, condition)")
    print("  2. Undo:   FILTER -> JOIN(CROSS)")
    print(f"\nCost: {cost_inner:.2f} -> {cost_undone:.2f} (increase: {cost_undone - cost_inner:.2f})")
    
    print("\nKey Point: Bidirectional transformation preserves semantics")
    print("  - apply_merge(join_id: [cond_ids]):  Merge conditions into JOIN")
    print("  - apply_merge(join_id: []):          Keep conditions in FILTER")
    print("  - undo_merge():                      INNER -> CROSS + FILTER")
    
    return undone


def scenario_4_nested_filters():
    """Scenario 4.4: Nested FILTERs from Undo (Advanced)"""
    print("\n")
    print_separator("SCENARIO 4.4: Nested FILTERs from Undo (Advanced)")
    
    print("Concept: What happens when INNER JOIN with AND conditions is undone?")
    print("Result: FILTER with AND conditions over CROSS JOIN")
    
    # Build INNER JOIN with AND conditions
    rel1 = QueryTree("RELATION", "employees")
    alias1 = QueryTree("ALIAS", "e")
    alias1.add_child(rel1)
    
    rel2 = QueryTree("RELATION", "payroll")
    alias2 = QueryTree("ALIAS", "p")
    alias2.add_child(rel2)
    
    # Condition 1: e.department_id = p.department_id
    dept_left = make_column_ref("department_id", "e")
    dept_right = make_column_ref("department_id", "p")
    cond1 = QueryTree("COMPARISON", "=")
    cond1.add_child(dept_left)
    cond1.add_child(dept_right)
    
    # Condition 2: e.salary > 5000
    salary_col = make_column_ref("salary", "e")
    salary_val = QueryTree("LITERAL_NUMBER", "5000")
    cond2 = QueryTree("COMPARISON", ">")
    cond2.add_child(salary_col)
    cond2.add_child(salary_val)
    
    # AND operator
    and_op = QueryTree("OPERATOR", "AND")
    and_op.add_child(cond1)
    and_op.add_child(cond2)
    
    inner_join = QueryTree("JOIN", "INNER")
    inner_join.add_child(alias1)
    inner_join.add_child(alias2)
    inner_join.add_child(and_op)
    
    project = QueryTree("PROJECT", "*")
    project.add_child(inner_join)
    
    parsed = ParsedQuery(project, "SELECT * FROM employees e INNER JOIN payroll p ON e.department_id = p.department_id AND e.salary > 5000")
    
    engine = OptimizationEngine()
    
    print("\nStarting with INNER JOIN (multiple AND conditions):")
    print(parsed.query_tree.tree(show_id=True))
    cost_inner = engine.get_cost(parsed)
    print(f"Cost: {cost_inner:.2f}")
    
    # Count FILTER nodes before
    def count_filters(node):
        if node is None:
            return 0
        count = 1 if node.type == "FILTER" else 0
        for child in node.childs:
            count += count_filters(child)
        return count
    
    num_before = count_filters(parsed.query_tree)
    
    # Apply undo_merge
    print("\n" + "-"*70)
    print("Applying undo_merge()...")
    undone = rule_4.undo_merge(parsed)
    
    print("\nAfter undo (FILTER with AND over CROSS JOIN):")
    print(undone.query_tree.tree(show_id=True))
    cost_undone = engine.get_cost(undone)
    print(f"Cost: {cost_undone:.2f}")
    
    num_after = count_filters(undone.query_tree)
    
    print("\n" + "-"*70)
    print("Structure Analysis:")
    print(f"  FILTER nodes before undo: {num_before}")
    print(f"  FILTER nodes after undo:  {num_after}")
    
    print("\nBEFORE undo:")
    print("  PROJECT(*)")
    print("  └── JOIN(INNER)")
    print("      ├── employees")
    print("      ├── payroll")
    print("      └── AND(dept_id = dept_id, salary > 5000)")
    
    print("\nAFTER undo:")
    print("  PROJECT(*)")
    print("  └── FILTER")
    print("      ├── JOIN(CROSS)")
    print("      │   ├── employees")
    print("      │   └── payroll")
    print("      └── AND(dept_id = dept_id, salary > 5000)")
    
    print("\n" + "-"*70)
    print(f"Cost Impact: {cost_inner:.2f} -> {cost_undone:.2f}")
    if cost_undone > cost_inner:
        print(f"Cost increase: {cost_undone - cost_inner:.2f}")
        print("Reason: CROSS JOIN produces more rows than INNER JOIN")
        print("Recommendation: Keep as INNER JOIN")
    
    print("\nKey Point: AND conditions stay together in FILTER after undo")
    print("  - All conditions move from JOIN to FILTER as single AND operator")
    print("  - JOIN becomes CROSS (no condition)")
    print("  - Typically less efficient, but allows filter reordering (Rule 1, 2)")
    
    return undone


def scenario_5_merge_into_merged():
    """Scenario 4.5: Merge FILTER into Already-Merged INNER JOIN"""
    print("\n")
    print_separator("SCENARIO 4.5: Merge FILTER into Already-Merged INNER JOIN")
    
    print("Concept: FILTER over INNER JOIN (already has condition)")
    print("Question: Can we merge additional FILTER into existing JOIN condition?")
    print("Answer: Yes! Combine with AND operator")
    
    # Build INNER JOIN with existing condition
    rel1 = QueryTree("RELATION", "employees")
    alias1 = QueryTree("ALIAS", "e")
    alias1.add_child(rel1)
    
    rel2 = QueryTree("RELATION", "departments")
    alias2 = QueryTree("ALIAS", "d")
    alias2.add_child(rel2)
    
    # Existing JOIN condition: e.department_id = d.id
    dept_left = make_column_ref("department_id", "e")
    dept_right = make_column_ref("id", "d")
    join_cond = QueryTree("COMPARISON", "=")
    join_cond.add_child(dept_left)
    join_cond.add_child(dept_right)
    
    inner_join = QueryTree("JOIN", "INNER")
    inner_join.add_child(alias1)
    inner_join.add_child(alias2)
    inner_join.add_child(join_cond)
    
    # Additional FILTER: e.salary > 7000
    salary_col = make_column_ref("salary", "e")
    salary_val = QueryTree("LITERAL_NUMBER", "7000")
    filter_cond = QueryTree("COMPARISON", ">")
    filter_cond.add_child(salary_col)
    filter_cond.add_child(salary_val)
    
    filter_over_inner = QueryTree("FILTER", "")
    filter_over_inner.add_child(inner_join)
    filter_over_inner.add_child(filter_cond)
    
    project = QueryTree("PROJECT", "*")
    project.add_child(filter_over_inner)
    
    parsed = ParsedQuery(project, "SELECT * FROM employees e INNER JOIN departments d ON e.department_id = d.id WHERE e.salary > 7000")
    
    engine = OptimizationEngine()
    
    print("\nOriginal Query Tree:")
    print("  PROJECT(*)")
    print("  └── FILTER(salary > 7000)")
    print("      └── JOIN(INNER, department_id = id)")
    print("          ├── employees (e)")
    print("          └── departments (d)")
    print()
    print(parsed.query_tree.tree(show_id=True))
    cost_original = engine.get_cost(parsed)
    print(f"\nCost: {cost_original:.2f}")
    
    patterns = rule_4.find_patterns(parsed)
    print(f"\nFound {len(patterns)} pattern(s) to merge")
    for join_id, metadata in patterns.items():
        print(f"  JOIN {join_id}: {len(metadata['filter_conditions'])} filter condition(s), {len(metadata['existing_conditions'])} existing condition(s)")
    
    # Option 1: Keep FILTER separate
    print("\n" + "-"*70)
    print("Option 1: Keep FILTER separate (merge no conditions = [])")
    print("Structure: FILTER -> JOIN(INNER, original condition)")
    join_params_sep = {join_id: [] for join_id in patterns.keys()}  # Empty list = no merge
    separate = rule_4.apply_merge(parsed, join_params_sep)
    print("\nTransformed Query Tree:")
    print(separate.query_tree.tree(show_id=True))
    cost_sep = engine.get_cost(separate)
    print(f"Cost: {cost_sep:.2f}")
    
    # Option 2: Merge FILTER into JOIN
    print("\n" + "-"*70)
    print("Option 2: Merge FILTER into JOIN (merge all conditions)")
    print("Result: JOIN(INNER, department_id = id AND salary > 7000)")
    join_params_merge = {join_id: metadata['filter_conditions'] for join_id, metadata in patterns.items()}
    merged = rule_4.apply_merge(parsed, join_params_merge)
    print("\nTransformed Query Tree:")
    print("  PROJECT(*)")
    print("  └── JOIN(INNER, AND condition)")
    print("      ├── employees (e)")
    print("      ├── departments (d)")
    print("      └── AND")
    print("          ├── department_id = id")
    print("          └── salary > 7000")
    print()
    print(merged.query_tree.tree(show_id=True))
    cost_merge = engine.get_cost(merged)
    print(f"Cost: {cost_merge:.2f}")
    
    print("\n" + "-"*70)
    print(f"Cost Comparison: Separate={cost_sep:.2f}, Merge={cost_merge:.2f}")
    if cost_merge < cost_sep:
        print(f"✓ Merging is better! Savings: {cost_sep - cost_merge:.2f}")
    
    # Now demonstrate undo with two different results
    print("\n" + "="*70)
    print("  UNDO TRANSFORMATION: Two Possible Results")
    print("="*70)
    
    # First undo: from merged state
    print("\nStarting from MERGED state:")
    print(merged.query_tree.tree(show_id=True))
    
    print("\n" + "-"*70)
    print("Applying undo_merge()...")
    undone = rule_4.undo_merge(merged)
    
    print("\nResult: FILTER with AND operator over CROSS JOIN")
    print("  PROJECT(*)")
    print("  └── FILTER")
    print("      ├── JOIN(CROSS)")
    print("      │   ├── employees (e)")
    print("      │   └── departments (d)")
    print("      └── AND")
    print("          ├── department_id = id")
    print("          └── salary > 7000")
    print()
    print(undone.query_tree.tree(show_id=True))
    cost_undone = engine.get_cost(undone)
    print(f"Cost: {cost_undone:.2f}")
    
    # Show alternative: stair-like FILTERs (conceptual)
    print("\n" + "-"*70)
    print("Alternative (if undo applied twice or cascade used):")
    print("Result: Nested/Stair-like FILTERs")
    print("  PROJECT(*)")
    print("  └── FILTER(salary > 7000)")
    print("      └── FILTER(department_id = id)")
    print("          └── JOIN(CROSS)")
    print("              ├── employees (e)")
    print("              └── departments (d)")
    print("\nNote: This 'stair' structure could be created by:")
    print("  1. Undo merge twice (if pattern allows)")
    print("  2. Apply Rule 1 uncascade after first undo")
    print("  3. Sequential filter application instead of AND combination")
    
    print("\n" + "="*70)
    print("  KEY INSIGHTS")
    print("="*70)
    print("\n1. MERGE (apply_merge with True):")
    print("   - Combines FILTER condition with existing JOIN condition")
    print("   - Uses AND operator to join conditions")
    print("   - Structure: JOIN(INNER, original_cond AND filter_cond)")
    
    print("\n2. UNDO (undo_merge):")
    print("   - Moves all JOIN conditions back to FILTER")
    print("   - JOIN becomes CROSS (no condition)")
    print("   - Keeps conditions combined with AND in single FILTER")
    
    print("\n3. STAIR-LIKE FILTERS (alternative structure):")
    print("   - Each condition in separate FILTER node")
    print("   - Nested structure: FILTER -> FILTER -> JOIN(CROSS)")
    print("   - Allows independent filter ordering (Rule 1, 2)")
    print("   - Can be created via uncascade or sequential application")
    
    print("\n4. COST IMPLICATIONS:")
    print(f"   - Separate FILTER + INNER: {cost_sep:.2f}")
    print(f"   - Merged (AND in JOIN):    {cost_merge:.2f}")
    print(f"   - Undone (FILTER + CROSS): {cost_undone:.2f}")
    print("   - Best: Merged < Separate < Undone (typically)")
    
    print("\n5. TRANSFORMATION PATHS:")
    print("   Path A (AND operator):")
    print("     FILTER -> JOIN(INNER, c1)")
    print("        => merge => JOIN(INNER, c1 AND c2)")
    print("        => undo  => FILTER(c1 AND c2) -> JOIN(CROSS)")
    print("\n   Path B (Stair/Cascade):")
    print("     FILTER -> JOIN(INNER, c1)")
    print("        => undo => FILTER(c1) -> JOIN(CROSS)")
    print("        => add  => FILTER(c2) -> FILTER(c1) -> JOIN(CROSS)")
    
    # Bonus: Demonstrasi partial merge using undone query (has 2 conditions in FILTER)
    print("\n" + "="*70)
    print("  BONUS: PARTIAL MERGE (New Feature!)")
    print("="*70)
    print("\nWith new format, we can merge SOME conditions, not all!")
    print("Let's use the undone query which has 2 conditions in FILTER:")
    
    # Find patterns in undone (FILTER with AND over CROSS)
    partial_patterns = rule_4.find_patterns(undone)
    if partial_patterns:
        for join_id, metadata in partial_patterns.items():
            filter_conds = metadata['filter_conditions']
            if len(filter_conds) >= 2:
                # Merge only first condition
                partial_params = {join_id: [filter_conds[0]]}  # Only merge first condition
                partial = rule_4.apply_merge(undone, partial_params)
                
                print(f"\nOriginal (undone): FILTER(cond1 AND cond2) -> JOIN(CROSS)")
                print(f"Partial merge: Merge only first condition {filter_conds[0]}")
                print(f"Keep in FILTER: condition {filter_conds[1]}")
                print("\nResult structure:")
                print("  PROJECT(*)")
                print("  └── FILTER (remaining condition)")
                print("      └── JOIN(INNER, merged condition)")
                print("\nTree:")
                print(partial.query_tree.tree(show_id=True))
                cost_partial = engine.get_cost(partial)
                print(f"\nCost: {cost_partial:.2f}")
                
                print("\n" + "-"*70)
                print("Flexibility: Choose which conditions to merge!")
                print(f"  - All:     {join_id}: {filter_conds} (all conditions)")
                print(f"  - Partial: {join_id}: {partial_params[join_id]} (only some conditions)")
                print(f"  - None:    {join_id}: [] (keep all in FILTER)")
                print("\nCost comparison:")
                print(f"  - All merged:     {cost_merge:.2f}")
                print(f"  - Partial merged: {cost_partial:.2f}")
                print(f"  - None merged:    {cost_undone:.2f}")
                break
    else:
        print("\nNote: Current query structure doesn't have multiple filter conditions")
        print("Partial merge is most useful when you have multiple conditions to choose from!")
    
    return merged, undone
