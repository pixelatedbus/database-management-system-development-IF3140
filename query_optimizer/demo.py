"""
Demo Program untuk Query Optimizer
"""


def print_separator(title=""):
    """Print section separator"""
    print("\n" + "="*70)
    if title:
        print(f"  {title}")
        print("="*70)


def print_query_tree(query_tree, indent=0):
    """Print query tree structure recursively"""
    if query_tree is None:
        return
    
    prefix = "  " * indent
    node_info = f"{query_tree.type}"
    
    if query_tree.val:
        node_info += f" [{query_tree.val}]"
    
    print(f"{prefix}{node_info}")
    
    for child in query_tree.childs:
        print_query_tree(child, indent + 1)

def print_help():
    """Print help message for demo usage"""
    print_separator("QUERY OPTIMIZER DEMO HELP")
    print("Usage:")
    print("  python -m query_optimizer.demo 1  # Run demo parse")
    print("  python -m query_optimizer.demo 2  # Run demo optimized (compare with/without GA)")
    print("  python -m query_optimizer.demo 3  # Run demo Rule 3 (projection elimination)")
    print("  python -m query_optimizer.demo 4  # Run demo Rule 7 (filter pushdown)")
    print("  python -m query_optimizer.demo 5  # Run demo Rule 8 (projection over join)")
    print("  python -m query_optimizer.demo 6  # Run demo Rule 1 (cascade filters)")
    print("  python -m query_optimizer.demo 7  # Run demo Rule 4 (push selection into joins)")
    print("  python -m query_optimizer.demo 8  # Run demo genetic with all rules (1, 2, 4)")
    print("  python -m query_optimizer.demo 9  # Run demo Rule 2 (reorder AND conditions)")
    print("  python -m query_optimizer.demo 10 # Run all demos sequentially")
    print_separator()

def demo_parse():
    """Demo: parse several example SQL queries and print their query trees."""
    print_separator("DEMO PARSE")
    from query_optimizer.optimization_engine import OptimizationEngine

    engine = OptimizationEngine()

    examples = [
        "SELECT id, name FROM users",
        "SELECT * FROM orders WHERE amount > 1000",
        "SELECT a.id, b.name FROM a JOIN b ON a.id = b.a_id WHERE a.x > 5"
    ]

    for sql in examples:
        print_separator(f"Parsing: {sql}")
        try:
            parsed = engine.parse_query(sql)
            print("Original SQL:", sql)
            print("Query Tree:")
            print(parsed.query_tree.tree())
        except Exception as e:
            print(f"Error parsing '{sql}': {e}")

    return None

def demo_optimized():
    """Demo: run optimization on a sample query and compare non-GA and GA results."""
    print_separator("DEMO OPTIMIZER")
    from query_optimizer.optimization_engine import OptimizationEngine

    engine = OptimizationEngine()

    sql = "SELECT * FROM orders WHERE amount > 1000 * 2 AND status = 'pending'"
    print_separator(f"Parsing and optimizing: {sql}")
    parsed = engine.parse_query(sql)

    print("Original Query Tree:")
    print(parsed.query_tree.tree())

    print_separator("Optimize without Genetic Algorithm (rules only)")
    optimized_no_ga = engine.optimize_query(parsed, use_genetic=False)
    print(optimized_no_ga.query_tree.tree())
    print(f"Estimated cost (no GA): {engine.get_cost(optimized_no_ga)}")

    print_separator("Optimize with Genetic Algorithm (small run)")
    optimized_ga = engine.optimize_query(parsed, use_genetic=True, population_size=10, generations=5)
    print(optimized_ga.query_tree.tree())
    print(f"Estimated cost (with GA): {engine.get_cost(optimized_ga)}")

    return optimized_ga

def demo_rule_3():
    """Demo 3: Rule 3 - Projection Elimination"""
    print_separator("DEMO 3: Rule 3 - Projection Elimination")
    
    from query_optimizer.query_tree import QueryTree
    from query_optimizer.optimization_engine import ParsedQuery
    from query_optimizer.rule_3 import seleksi_proyeksi
    
    print_separator("Creating query with nested projections (redundant)")
    
    # Build query tree manually with nested projections
    # PROJECT(id, name)
    # └── PROJECT(*)
    #     └── RELATION("users")
    
    relation = QueryTree("RELATION", "users")
    
    # Inner projection: SELECT *
    inner_project = QueryTree("PROJECT", "*")
    inner_project.add_child(relation)
    
    # Outer projection: SELECT id, name
    col1 = QueryTree("COLUMN_REF", "")
    col1_name = QueryTree("COLUMN_NAME", "")
    col1_id = QueryTree("IDENTIFIER", "id")
    col1_name.add_child(col1_id)
    col1.add_child(col1_name)
    
    col2 = QueryTree("COLUMN_REF", "")
    col2_name = QueryTree("COLUMN_NAME", "")
    col2_id = QueryTree("IDENTIFIER", "name")
    col2_name.add_child(col2_id)
    col2.add_child(col2_name)
    
    outer_project = QueryTree("PROJECT", "")
    outer_project.add_child(col1)
    outer_project.add_child(col2)
    outer_project.add_child(inner_project)
    
    query = ParsedQuery(outer_project, "SELECT id, name FROM (SELECT * FROM users)")
    
    print("\nOriginal Query Tree (nested projections):")
    print(query.query_tree.tree())
    
    # Count PROJECT nodes
    def count_projects(node):
        if node is None:
            return 0
        count = 1 if node.type == "PROJECT" else 0
        for child in node.childs:
            count += count_projects(child)
        return count
    
    original_count = count_projects(query.query_tree)
    print(f"\nPROJECT nodes before: {original_count}")
    
    print_separator("Applying Rule 3: Projection Elimination")
    print("Rule: PROJECT_1(PROJECT_2(Source)) ≡ PROJECT_1(Source)")
    
    optimized = seleksi_proyeksi(query)
    
    print("\nOptimized Query Tree (projection eliminated):")
    print(optimized.query_tree.tree())
    
    optimized_count = count_projects(optimized.query_tree)
    print(f"\nPROJECT nodes after: {optimized_count}")
    
    if optimized_count < original_count:
        print(f"Successfully eliminated {original_count - optimized_count} nested projection(s)!")
    
    print_separator("DEMO 3 COMPLETED")
    
    return optimized

def demo_rule_7():
    """Demo 3.5: Rule 7 - Filter Pushdown over Join"""
    print_separator("DEMO 3.5: Rule 7 - Filter Pushdown over Join")
    
    from query_optimizer.query_tree import QueryTree
    from query_optimizer.optimization_engine import ParsedQuery
    from query_optimizer.rule_7 import apply_pushdown, find_patterns, is_pushable
    
    print_separator("Creating FILTER -> JOIN pattern")
    
    # Build query: SELECT * FROM users JOIN profiles WHERE users.age > 18 AND profiles.verified = 'true'
    # FILTER
    # └── JOIN
    #     ├── RELATION("users")
    #     ├── RELATION("profiles")
    #     └── COMPARISON("=") [users.id = profiles.user_id]
    # └── OPERATOR("AND")
    #     ├── COMPARISON(">") [users.age > 18]
    #     └── COMPARISON("=") [profiles.verified = 'true']
    
    # Helper to create COLUMN_REF
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
    
    # Build JOIN
    rel1 = QueryTree("RELATION", "users")
    rel2 = QueryTree("RELATION", "profiles")
    
    # Join condition: users.id = profiles.user_id
    join_left = make_column_ref("id", "users")
    join_right = make_column_ref("user_id", "profiles")
    
    join_cond = QueryTree("COMPARISON", "=")
    join_cond.add_child(join_left)
    join_cond.add_child(join_right)
    
    join = QueryTree("JOIN", "INNER")
    join.add_child(rel1)
    join.add_child(rel2)
    join.add_child(join_cond)
    
    # Filter conditions
    # users.age > 18
    age_col = make_column_ref("age", "users")
    age_val = QueryTree("LITERAL_NUMBER", "18")
    age_comp = QueryTree("COMPARISON", ">")
    age_comp.add_child(age_col)
    age_comp.add_child(age_val)
    
    # profiles.verified = 'true'
    verified_col = make_column_ref("verified", "profiles")
    verified_val = QueryTree("LITERAL_STRING", "true")
    verified_comp = QueryTree("COMPARISON", "=")
    verified_comp.add_child(verified_col)
    verified_comp.add_child(verified_val)
    
    # AND operator
    and_op = QueryTree("OPERATOR", "AND")
    and_op.add_child(age_comp)
    and_op.add_child(verified_comp)
    
    # FILTER node
    filter_node = QueryTree("FILTER", "")
    filter_node.add_child(join)
    filter_node.add_child(and_op)
    
    query = ParsedQuery(filter_node, "SELECT * FROM users JOIN profiles ON users.id = profiles.user_id WHERE users.age > 18 AND profiles.verified = 'true'")
    
    print("\nOriginal Query Tree (FILTER -> JOIN):")
    print(query.query_tree.tree())
    
    # Find patterns
    print_separator("Finding FILTER -> JOIN patterns")
    patterns = find_patterns(query)
    print(f"\nFound {len(patterns)} pattern(s):")
    for filter_id, pattern_info in patterns.items():
        print(f"\nPattern for FILTER node ID {filter_id}:")
        print(f"  - JOIN node ID: {pattern_info['join_id']}")
        print(f"  - Number of conditions: {pattern_info['num_conditions']}")
        print(f"  - Has AND operator: {pattern_info['has_and']}")
        print(f"\n  All conditions in this pattern are eligible for pushdown optimization")
    
    print_separator("Applying Rule 7: Filter Pushdown")
    print("Rule: FILTER(JOIN(R, S), cond) -> JOIN(FILTER(R, cond_R), FILTER(S, cond_S))")
    print("Strategy: Push filters closer to data source to reduce join size")
    
    optimized = apply_pushdown(query)
    
    print("\nOptimized Query Tree (filters pushed down):")
    print(optimized.query_tree.tree())
    
    # Count FILTER nodes
    def count_filters(node):
        if node is None:
            return 0
        count = 1 if node.type == "FILTER" else 0
        for child in node.childs:
            count += count_filters(child)
        return count
    
    original_filters = count_filters(query.query_tree)
    optimized_filters = count_filters(optimized.query_tree)
    
    print(f"\nFILTER nodes before: {original_filters}")
    print(f"FILTER nodes after: {optimized_filters}")
    
    if optimized_filters > original_filters:
        print(f"Successfully pushed down filters! (+{optimized_filters - original_filters} FILTER nodes closer to source)")
    
    print("\nBenefit: Reduced data volume before join operation, improving query performance")
    
    print_separator("DEMO 3.5 COMPLETED")
    
    return optimized

def demo_rule_8():
    """Demo 3.7: Rule 8 - Projection over Join"""
    print_separator("DEMO 3.7: Rule 8 - Projection over Join")
    
    from query_optimizer.query_tree import QueryTree
    from query_optimizer.optimization_engine import ParsedQuery
    from query_optimizer.rule_8 import push_projection_over_joins, analyze_projection_over_join
    
    print_separator("Creating PROJECT -> JOIN pattern")
    
    # Build query: SELECT users.name, profiles.bio FROM users JOIN profiles ON users.id = profiles.user_id
    # PROJECT
    # └── JOIN
    #     ├── RELATION("users")
    #     ├── RELATION("profiles")
    #     └── COMPARISON("=") [users.id = profiles.user_id]
    
    # Helper to create COLUMN_REF
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
    
    # Build JOIN
    rel1 = QueryTree("RELATION", "users")
    rel2 = QueryTree("RELATION", "profiles")
    
    # Join condition: users.id = profiles.user_id
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
    
    # PROJECT node
    project = QueryTree("PROJECT", "")
    project.add_child(col1)
    project.add_child(col2)
    project.add_child(join)
    
    query = ParsedQuery(project, "SELECT users.name, profiles.bio FROM users JOIN profiles ON users.id = profiles.user_id")
    
    print("\nOriginal Query Tree (PROJECT -> JOIN):")
    print(query.query_tree.tree())
    
    # Analyze patterns
    print_separator("Analyzing PROJECT -> JOIN pattern")
    opportunities = analyze_projection_over_join(query)
    print(f"\nFound {len(opportunities)} optimization opportunity(ies)")
    
    if opportunities:
        for i, (join_id, opp) in enumerate(opportunities.items(), 1):
            print(f"\nOpportunity {i}:")
            print(f"  - PROJECT node ID: {opp['project_node'].id}")
            print(f"  - JOIN node ID: {join_id}")
            print(f"  - Can optimize: {opp['can_optimize']}")
            if 'projected_cols' in opp:
                print(f"  - Projected columns: {', '.join(sorted(opp['projected_cols']))}")
    
    print_separator("Applying Rule 8: Projection Pushdown")
    print("Rule: PROJECT(cols, JOIN(R, S)) -> PROJECT(cols, JOIN(PROJECT(R_cols), PROJECT(S_cols)))")
    print("Strategy: Push projections to join children to reduce tuple width")
    
    optimized = push_projection_over_joins(query)
    
    print("\nOptimized Query Tree (projections pushed to join children):")
    print(optimized.query_tree.tree())
    
    # Count PROJECT nodes
    def count_projects(node):
        if node is None:
            return 0
        count = 1 if node.type == "PROJECT" else 0
        for child in node.childs:
            count += count_projects(child)
        return count
    
    original_projects = count_projects(query.query_tree)
    optimized_projects = count_projects(optimized.query_tree)
    
    print(f"\nPROJECT nodes before: {original_projects}")
    print(f"PROJECT nodes after: {optimized_projects}")
    
    if optimized_projects > original_projects:
        print(f"Successfully pushed projections! (+{optimized_projects - original_projects} PROJECT nodes)")
    
    print("\nBenefit: Reduced tuple width before join, less data to transfer and compare")
    print("Each relation only projects columns needed for output and join condition")
    
    print_separator("DEMO 3.7 COMPLETED")
    
    return optimized

def demo_rule_1():
    """Demo 4: Rule 1 - Seleksi Konjungtif (Cascade Filters)"""
    print_separator("DEMO 4: Rule 1 - Seleksi Konjungtif (Manual Transformations)")
    
    from query_optimizer.query_tree import QueryTree
    from query_optimizer.optimization_engine import ParsedQuery, OptimizationEngine
    from query_optimizer.rule_1 import (
        cascade_filters,
        analyze_and_operators,
        uncascade_filters
    )
    
    # Build query with FILTER + OPERATOR(AND)
    # FILTER
    # ├── RELATION("users")
    # └── OPERATOR("AND")
    #     ├── COMPARISON(">") [age > 25]
    #     ├── COMPARISON("=") [status = 'active']
    #     └── COMPARISON("=") [city = 'Jakarta']
    
    print("\nBuilding query with conjunctive conditions...")
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
    
    print("\nOriginal Query Tree (with OPERATOR(AND)):")
    print(query.query_tree.tree(show_id=True))
    
    engine = OptimizationEngine()
    cost_original = engine.get_cost(query)
    print(f"\nOriginal Cost: {cost_original:.2f}")
    
    # Analyze AND operators
    print_separator("Analyzing AND operators")
    operators = analyze_and_operators(query)
    print(f"Found {len(operators)} AND operator(s) with 3 conditions each")
    for op_id, num_conditions in operators.items():
        print(f"  - Operator ID {op_id}: {num_conditions} conditions")
    
    print("\nRule 1 explores different cascade orders and groupings:")
    print("Format: list[int | list[int]]")
    print("  - int: cascade as single FILTER")
    print("  - list[int]: keep conditions grouped in AND")
    
    # Demo all possible cascade variations
    variations = [
        ([0, 1, 2], "All single (full cascade): c0 → c1 → c2"),
        ([2, 1, 0], "All single (reversed): c2 → c1 → c0"),
        ([1, 0, 2], "All single (reordered): c1 → c0 → c2"),
        ([[0, 1, 2]], "All grouped (no cascade): AND(c0, c1, c2)"),
        ([2, [0, 1]], "Mixed: c2 single, then AND(c0, c1)"),
        ([0, [1, 2]], "Mixed: c0 single, then AND(c1, c2)"),
        ([[0, 1], 2], "Mixed: AND(c0, c1), then c2 single"),
    ]
    
    print_separator("Exploring All Cascade Variations")
    
    for i, (order, description) in enumerate(variations, 1):
        print(f"\n{'-'*70}")
        print(f"Variation {i}: {description}")
        print(f"Parameter: {order}")
        
        operator_orders = {and_operator.id: order}
        transformed = cascade_filters(query, operator_orders)
        
        print("\nTransformed Query Tree:")
        print(transformed.query_tree.tree(show_id=True))
        
        cost = engine.get_cost(transformed)
        print(f"\nCost: {cost:.2f}")
        
        if cost < cost_original:
            print(f"✓ Better than original! Improvement: {cost_original - cost:.2f}")
        elif cost > cost_original:
            print(f"✗ Worse than original. Increase: {cost - cost_original:.2f}")
        else:
            print("= Same cost as original")
    
    # Demo uncascade
    print("\n" + "="*70)
    print_separator("Reverse Transformation: Uncascade")
    print("Can convert cascaded filters back to AND structure")
    
    operator_orders = {and_operator.id: [0, 1, 2]}
    cascaded = cascade_filters(query, operator_orders)
    print("\nCascaded Tree:")
    print(cascaded.query_tree.tree(show_id=True))
    
    uncascaded = uncascade_filters(cascaded)
    print("\nUncascaded Tree (back to original structure):")
    print(uncascaded.query_tree.tree(show_id=True))
    
    print_separator("DEMO 4 COMPLETED")
    print("\nKey Insights:")
    print("- Rule 1 has many possible cascade orders and groupings")
    print("- Each variation produces different cost")
    print("- Best order depends on selectivity of conditions")
    print("- More selective conditions should be evaluated first")
    print("- Genetic Algorithm can find optimal order automatically (see Demo 8)")
    
    return query


def demo_rule1():
    """Compatibility wrapper: call the Rule 1 demo implementation."""
    return demo_rule_1()


def demo_rule2():
    """Demo 7: Rule 2 - Reordering AND conditions (Seleksi Komutatif)"""
    print_separator("DEMO 7: RULE 2 - REORDER (Manual Transformations)")
    from query_optimizer.query_tree import QueryTree
    from query_optimizer.rule_2 import (
        analyze_and_operators_for_reorder,
        reorder_and_conditions,
    )
    from query_optimizer.optimization_engine import ParsedQuery, OptimizationEngine

    # Build example filter with AND conditions
    print("\nBuilding query with AND conditions...")
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

    print("\nOriginal Query Tree:")
    print(query.query_tree.tree(show_id=True))
    
    engine = OptimizationEngine()
    cost_original = engine.get_cost(query)
    print(f"\nOriginal Cost: {cost_original:.2f}")
    print(f"Original Order: [c0, c1, c2]")

    print_separator("Analyzing AND operators")
    operators = analyze_and_operators_for_reorder(query)
    print(f"Found {len(operators)} AND operator(s) with 3 conditions")
    for op_id, num in operators.items():
        print(f"  - Operator ID {op_id}: {num} conditions")
    
    print("\nRule 2 explores different orderings of AND conditions:")
    print("Format: list[int] (permutation of condition indices)")
    print("Tree structure preserved, only child order changed")

    # Demo all meaningful permutations
    permutations = [
        ([0, 1, 2], "Original order: c0, c1, c2"),
        ([2, 1, 0], "Reversed order: c2, c1, c0"),
        ([1, 0, 2], "Swap first two: c1, c0, c2"),
        ([0, 2, 1], "Swap last two: c0, c2, c1"),
        ([1, 2, 0], "Rotate right: c1, c2, c0"),
        ([2, 0, 1], "Rotate left: c2, c0, c1"),
    ]
    
    print_separator("Exploring All Reordering Permutations")
    
    for i, (order, description) in enumerate(permutations, 1):
        print(f"\n{'-'*70}")
        print(f"Permutation {i}: {description}")
        print(f"Parameter: {order}")
        
        operator_orders = {and_operator.id: order}
        transformed = reorder_and_conditions(query, operator_orders)
        
        print("\nReordered Query Tree:")
        print(transformed.query_tree.tree(show_id=True))
        
        cost = engine.get_cost(transformed)
        print(f"\nCost: {cost:.2f}")
        
        if cost < cost_original:
            print(f"✓ Better than original! Improvement: {cost_original - cost:.2f}")
        elif cost > cost_original:
            print(f"✗ Worse than original. Increase: {cost - cost_original:.2f}")
        else:
            print("= Same cost as original")

    print_separator("DEMO 7 COMPLETED")
    print("\nKey Insights:")
    print("- Rule 2 reorders AND conditions without changing structure")
    print("- All conditions remain in single AND operator")
    print("- Different orders can have different costs")
    print("- Best order evaluates most selective conditions first")
    print("- Genetic Algorithm can find optimal order automatically (see Demo 8)")
    
    return query


def demo_all():
    """Run all demos in sequence."""
    print_separator("DEMO: RUNNING ALL SAMPLES")
    # Parse
    demo_parse()
    # Rule 3 (deterministic)
    demo_rule_3()
    # Rule 7 (deterministic)
    demo_rule_7()
    # Rule 8 (deterministic)
    demo_rule_8()
    # Rule 1 (non-deterministic)
    demo_rule_1()
    # Rule 4 (non-deterministic)
    demo_rule_4()
    # Rule 2 (non-deterministic)
    demo_rule2()
    # Genetic / Rule integration (all rules)
    demo_genetic_with_rules()
    # Optimizer comparison
    demo_optimized()

    print_separator("ALL DEMOS COMPLETED")
    return None

def demo_rule_4():
    """Demo 5: Rule 4 - Push Selection into Joins (Manual Transformations)"""
    print_separator("DEMO 5: Rule 4 - Push Selection into Joins")
    
    from query_optimizer.optimization_engine import OptimizationEngine
    from query_optimizer import rule_4
    from query_optimizer.query_tree import QueryTree
    from query_optimizer.optimization_engine import ParsedQuery
    
    print_separator("Creating FILTER -> JOIN pattern")
    print("Building query manually to demonstrate Rule 4 transformation")
    print("Concept: FILTER(JOIN(R, S), condition) -> JOIN(R, S, condition)")
    
    # Build query manually: FILTER over CROSS JOIN
    # This creates the pattern Rule 4 is designed to optimize
    
    # Helper to create COLUMN_REF
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
    print_separator("Analyzing FILTER-JOIN patterns")
    patterns = rule_4.find_patterns(parsed)
    print(f"\nFound {len(patterns)} pattern(s):")
    for filter_id, metadata in patterns.items():
        print(f"\n  Filter Node {filter_id}:")
        print(f"    - Join ID: {metadata['join_id']}")
        print(f"    - Has Condition: {metadata['has_condition']}")
    
    print("\nRule 4 Decision: Merge FILTER into JOIN or keep separate?")
    print("Format: dict[filter_id, bool]")
    print("  - True: merge FILTER condition into JOIN (CROSS -> INNER)")
    print("  - False: keep FILTER separate above JOIN (remains CROSS)")
    print("\nThis demonstrates the equivalency:")
    print("  FILTER(JOIN(R, S), cond) ≡ JOIN(R, S, cond)")
    
    print_separator("Exploring Both Options")
    
    # Demo 1: Keep separate (decision = False)
    print("\nOption 1: Keep FILTER separate from JOIN (CROSS JOIN)")
    print("Decision: {filter_id: False}")
    print("\nStructure: PROJECT -> FILTER -> JOIN(CROSS)")
    print("Execution: Cartesian product of all rows, then filter")
    decisions_separate = {fid: False for fid in patterns.keys()}
    separate_query = rule_4.apply_merge(parsed, decisions_separate)
    print("\nQuery Tree (FILTER separate):")
    print(separate_query.query_tree.tree(show_id=True))
    cost_separate = engine.get_cost(separate_query)
    print(f"\nCost: {cost_separate:.2f}")
    
    # Demo 2: Merge (decision = True)
    print("\n" + "="*70)
    print("\nOption 2: Merge FILTER into JOIN (INNER JOIN)")
    print("Decision: {filter_id: True}")
    print("\nStructure: PROJECT -> JOIN(INNER, condition)")
    print("Execution: Conditional join directly, no cartesian product")
    decisions_merge = {fid: True for fid in patterns.keys()}
    merged_query = rule_4.apply_merge(parsed, decisions_merge)
    print("\nQuery Tree (FILTER merged):")
    print(merged_query.query_tree.tree(show_id=True))
    cost_merge = engine.get_cost(merged_query)
    print(f"\nCost: {cost_merge:.2f}")
    
    print("\n" + "="*70)
    print("\nCost Comparison:")
    print(f"  Keep Separate: {cost_separate:.2f}")
    print(f"  Merge:         {cost_merge:.2f}")
    
    if cost_merge < cost_separate:
        print(f"\n✓ Merging is better! Cost reduction: {cost_separate - cost_merge:.2f}")
        print("\nReason: INNER JOIN with condition reduces intermediate result size")
    elif cost_merge > cost_separate:
        print(f"\n✗ Keeping separate is better! Cost increase: {cost_merge - cost_separate:.2f}")
        print("\nReason: FILTER after JOIN allows more flexible optimization")
    else:
        print("\n= Same cost for both options")
    
    print_separator("Transformation Details")
    print("\nBefore (decision=False, keep separate):")
    print("  PROJECT(*)")
    print("  └── FILTER")
    print("      ├── JOIN (CROSS)")
    print("      │   ├── ALIAS(e) -> RELATION(employees)")
    print("      │   └── ALIAS(p) -> RELATION(payroll)")
    print("      └── COMPARISON(=) [e.id = p.employee_id]")
    print("\n  Result: Cartesian product THEN filter")
    print("  Cost: High - joins all rows first, then filters")
    print("\nAfter (decision=True, merged):")
    print("  PROJECT(*)")
    print("  └── JOIN (INNER)")
    print("      ├── ALIAS(e) -> RELATION(employees)")
    print("      ├── ALIAS(p) -> RELATION(payroll)")
    print("      └── COMPARISON(=) [e.id = p.employee_id]")
    print("\n  Result: Conditional join directly")
    print("  Cost: Lower - filters during join operation")
    
    # Demo additional scenario: FILTER over INNER JOIN with existing condition
    print("\n" + "="*70)
    print_separator("Additional Scenario: FILTER over INNER JOIN")
    print("What if JOIN already has a condition (INNER JOIN)?")
    print("Can we add another filter condition to it?")
    
    # Build INNER JOIN with condition, then add FILTER above it
    print("\nBuilding: FILTER(JOIN(INNER, cond1), cond2)")
    
    # INNER JOIN with condition: e.department_id = p.department_id
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
    
    project2 = QueryTree("PROJECT", "*")
    project2.add_child(filter_over_inner)
    
    parsed2 = ParsedQuery(project2, "SELECT * FROM employees e INNER JOIN payroll p ON e.department_id = p.department_id WHERE e.salary > 5000")
    
    print("\nOriginal Query Tree (FILTER over INNER JOIN):")
    print(parsed2.query_tree.tree(show_id=True))
    
    patterns2 = rule_4.find_patterns(parsed2)
    print(f"\nFound {len(patterns2)} pattern(s) in INNER JOIN scenario")
    
    # Option 1: Keep separate
    print("\n" + "-"*70)
    print("Option 1: Keep FILTER separate (decision = False)")
    print("Structure: PROJECT -> FILTER -> JOIN(INNER, cond1)")
    decisions_sep2 = {fid: False for fid in patterns2.keys()}
    separate2 = rule_4.apply_merge(parsed2, decisions_sep2)
    print("\nQuery Tree:")
    print(separate2.query_tree.tree(show_id=True))
    cost_sep2 = engine.get_cost(separate2)
    print(f"\nCost: {cost_sep2:.2f}")
    
    # Option 2: Merge filter condition into JOIN
    print("\n" + "-"*70)
    print("Option 2: Merge FILTER into JOIN (decision = True)")
    print("Structure: PROJECT -> JOIN(INNER, cond1 AND cond2)")
    decisions_merge2 = {fid: True for fid in patterns2.keys()}
    merged2 = rule_4.apply_merge(parsed2, decisions_merge2)
    print("\nQuery Tree:")
    print(merged2.query_tree.tree(show_id=True))
    cost_merge2 = engine.get_cost(merged2)
    print(f"\nCost: {cost_merge2:.2f}")
    
    print("\n" + "-"*70)
    print(f"\nCost Comparison:")
    print(f"  Keep Separate: {cost_sep2:.2f}")
    print(f"  Merge:         {cost_merge2:.2f}")
    
    if cost_merge2 < cost_sep2:
        print(f"\n✓ Merging is better! Cost reduction: {cost_sep2 - cost_merge2:.2f}")
        print("Reason: Multiple conditions in JOIN more efficient than cascaded filters")
    elif cost_merge2 > cost_sep2:
        print(f"\n✗ Keeping separate is better! Cost increase: {cost_merge2 - cost_sep2:.2f}")
    else:
        print("\n= Same cost for both options")
    
    print("\nNote: When JOIN already has condition, merge adds to AND operator")
    print("Result: JOIN(INNER, cond1 AND cond2)")
    
    # Demo reverse transformation: Can we undo merge?
    print("\n" + "="*70)
    print_separator("Reverse Transformation: Undo Merge")
    print("Can we convert INNER JOIN back to CROSS JOIN + FILTER?")
    print("Answer: Yes, using undo_merge function")
    
    print("\nStarting with merged query (INNER JOIN with condition):")
    print(merged_query.query_tree.tree(show_id=True))
    print(f"Cost: {cost_merge:.2f}")
    
    # Apply undo_merge
    undone = rule_4.undo_merge(merged_query)
    
    print("\nAfter undo_merge (back to CROSS JOIN + FILTER):")
    print(undone.query_tree.tree(show_id=True))
    cost_undone = engine.get_cost(undone)
    print(f"Cost: {cost_undone:.2f}")
    
    print("\nTransformation flow:")
    print("  1. Original:  FILTER -> JOIN(CROSS)")
    print("  2. Merge:     JOIN(INNER, condition)")
    print("  3. Undo:      FILTER -> JOIN(CROSS)  [back to step 1]")
    
    print("\nThis demonstrates bidirectional transformation:")
    print("  - apply_merge(decision=True):  CROSS -> INNER")
    print("  - undo_merge():                INNER -> CROSS")
    print("  - Both transformations preserve query semantics")
    
    print_separator("DEMO 5 COMPLETED")
    print("\nKey Insights:")
    print("- Rule 4 has 2 options: merge or keep separate (boolean decision)")
    print("- Works with both CROSS JOIN and INNER JOIN")
    print("- Can merge additional FILTER into existing INNER JOIN")
    print("- Merging typically reduces cost by filtering during join")
    print("- undo_merge() provides reverse transformation (INNER -> CROSS)")
    print("- Transformations are bidirectional and semantics-preserving")
    print("- Decision depends on query structure and selectivity")
    print("- Genetic Algorithm can find optimal decision automatically (see Demo 8)")
    
    return merged_query


def demo_genetic_with_rules():
    """Demo 6: Genetic Optimizer with Rule 1 + Rule 2 + Rule 4 Integration"""
    print_separator("DEMO 6: Genetic Optimizer with Unified Rules (1, 2, 4)")
    
    from query_optimizer.query_tree import QueryTree
    from query_optimizer.optimization_engine import ParsedQuery
    from query_optimizer.genetic_optimizer import GeneticOptimizer
    
    # Build query with multiple AND conditions
    print("\nBuilding query with 4 conjunctive conditions...")
    
    relation = QueryTree("RELATION", "orders")
    
    comp1 = QueryTree("COMPARISON", ">")   # amount > 1000
    comp2 = QueryTree("COMPARISON", "=")   # status = 'pending'
    comp3 = QueryTree("COMPARISON", "<")   # date < '2024-01-01'
    comp4 = QueryTree("COMPARISON", "!=")  # customer_id != null
    
    and_operator = QueryTree("OPERATOR", "AND")
    and_operator.add_child(comp1)
    and_operator.add_child(comp2)
    and_operator.add_child(comp3)
    and_operator.add_child(comp4)
    
    filter_node = QueryTree("FILTER")
    filter_node.add_child(relation)
    filter_node.add_child(and_operator)
    
    query = ParsedQuery(
        filter_node,
        "SELECT * FROM orders WHERE amount > 1000 AND status = 'pending' AND date < '2024-01-01' AND customer_id != null"
    )
    
    print("\nOriginal Query Tree:")
    print_query_tree(query.query_tree)
    
    print_separator("Running Genetic Optimizer...")
    print("Population Size: 20")
    print("Generations: 10")
    print("Mutation Rate: 0.2")
    print("Using Unified Filter Parameters (combines reordering and cascading)")
    
    ga = GeneticOptimizer(
        population_size=20,
        generations=10,
        mutation_rate=0.2,
        crossover_rate=0.8,
        elitism=2,
    )
    
    optimized_query = ga.optimize(query)
    
    print_separator("Optimization Results")
    
    print("\nOptimized Query Tree:")
    print_query_tree(optimized_query.query_tree)
    
    print_separator("Statistics")
    
    stats = ga.get_statistics()
    print(f"\nBest Fitness: {stats['best_fitness']:.2f}")
    print(f"Total Generations: {stats['generations']}")
    
    if stats['best_params']:
        print("\nBest Parameters Found:")
        for param_type, node_params in stats['best_params'].items():
            if node_params:
                print(f"\n  {param_type}:")
                for node_id, params in node_params.items():
                    print(f"    Node {node_id}: {params}")
                    if param_type == 'filter_params' and isinstance(params, list):
                        # Unified format: explains both reorder and cascade
                        explanations = []
                        for item in params:
                            if isinstance(item, list):
                                explanations.append(f"({', '.join(map(str, item))} grouped)")
                            else:
                                explanations.append(f"{item} single")
                        if explanations:
                            print(f"      → {' -> '.join(explanations)}")
                            print("      → Unified format combines reordering and cascading")
    
    print_separator("Fitness Progress")
    print("\nGen | Best    | Average | Worst")
    print("----|---------|---------|--------")
    
    for record in stats['history'][::max(1, len(stats['history'])//5)]:  # Show 5 samples
        print(f"{record['generation']:3d} | "
              f"{record['best_fitness']:7.2f} | "
              f"{record['avg_fitness']:7.2f} | "
              f"{record['worst_fitness']:7.2f}")
    
    # Show last generation
    if stats['history']:
        last = stats['history'][-1]
        if last not in stats['history'][::max(1, len(stats['history'])//5)]:
            print(f"{last['generation']:3d} | "
                  f"{last['best_fitness']:7.2f} | "
                  f"{last['avg_fitness']:7.2f} | "
                  f"{last['worst_fitness']:7.2f}")
    
    print_separator("DEMO 5 COMPLETED")
    print("\nKey Insights:")
    print("- Rule 3 (projection elimination) applied ONCE before genetic algorithm")
    print("- Unified filter_params format combines reordering and cascading in one parameter")
    print("- Genetic Algorithm explores different combinations of:")
    print("  * Condition reordering (position in list)")
    print("  * Cascade grouping patterns (int vs list[int])")
    print("- Best solution balances tree structure for optimal execution")
    print("- Example: [2, [0,1]] means condition 2 cascades as single filter,")
    print("           then conditions 0 and 1 stay grouped in AND operator")
    
    return optimized_query

def main():
    """Main program"""
    import sys
    import io
    
    # Fix encoding for Windows
    if sys.platform == 'win32':
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    
    print("\n")
    print("╔═══════════════════════════════════════════════════════════════════╗")
    print("║                                                                   ║")
    print("║                         QUERY OPTIMIZER                           ║")
    print("║                                                                   ║")
    print("╚═══════════════════════════════════════════════════════════════════╝")
    
    # Parse command line arguments
    if len(sys.argv) > 1:
        try:
            demo_num = int(sys.argv[1])
        except ValueError:
            print("\n Error: Argument must be a number")
            print("\nUsage:")
            print("  python -m query_optimizer.demo 1  # Run demo parse")
            print("  python -m query_optimizer.demo 2  # Run demo optimized")
            print("  python -m query_optimizer.demo 3  # Run demo Rule 3 (projection elimination)")
            print("  python -m query_optimizer.demo 4  # Run demo Rule 7 (filter pushdown)")
            print("  python -m query_optimizer.demo 5  # Run demo Rule 8 (projection over join)")
            print("  python -m query_optimizer.demo 6  # Run demo Rule 1 (cascade filters)")
            print("  python -m query_optimizer.demo 7  # Run demo genetic with Rule 1 + Rule 2")
            print("  python -m query_optimizer.demo 8  # Run demo Rule 2 (reorder AND conditions)")
            print("  python -m query_optimizer.demo 9  # Run all demos sequentially")
            return
    else:
        demo_num = 0
    
    try:
        if demo_num == 1:
            demo_parse()
            print_separator("DEMO COMPLETED")
        elif demo_num == 2:
            demo_optimized()
            print_separator("DEMO COMPLETED")
        elif demo_num == 3:
            demo_rule_3()
        elif demo_num == 4:
            demo_rule_7()
        elif demo_num == 5:
            demo_rule_8()
        elif demo_num == 6:
            demo_rule_1()
        elif demo_num == 7:
            demo_rule_4()
        elif demo_num == 8:
            demo_genetic_with_rules()
        elif demo_num == 9:
            demo_rule2()
        elif demo_num == 10:
            demo_all()
        else:
            print_help()
        
    except KeyboardInterrupt:
        print("\n\n  Demo interrupted by user.")
    except Exception as e:
        print(f"\n\n Error occurred: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
