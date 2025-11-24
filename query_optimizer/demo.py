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
    print("  python -m query_optimizer.demo 7  # Run demo genetic with Rule 1 + Rule 2")
    print("  python -m query_optimizer.demo 8  # Run demo Rule 2 (reorder AND conditions)")
    print("  python -m query_optimizer.demo 9  # Run all demos sequentially")
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
    print_separator("DEMO 4: Rule 1 - Seleksi Konjungtif")
    
    from query_optimizer.query_tree import QueryTree
    from query_optimizer.optimization_engine import ParsedQuery
    from query_optimizer.rule_1 import (
        cascade_filters,
        analyze_and_operators,
        generate_random_rule_1_params,
        seleksi_konjungtif,
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
    print(query.query_tree.tree())
    
    # Analyze AND operators
    print("\nAnalyzing AND operators...")
    operators = analyze_and_operators(query)
    print(f"Found {len(operators)} AND operator(s):")
    for op_id, num_conditions in operators.items():
        print(f"  - Operator ID {op_id}: {num_conditions} conditions")
    
    # Demo 1: Cascade with default order (all single)
    print_separator("Demo 4a: Cascade all conditions (default order)")
    
    cascaded = seleksi_konjungtif(query)
    
    print("\nCascaded Query Tree (fully cascaded):")
    print(cascaded.query_tree.tree())
    
    # Demo 2: Cascade with mixed order
    print_separator("Demo 4b: Cascade with mixed order [2, [0, 1]]")
    print("This means: condition2 single, then (condition0 AND condition1) grouped")
    
    # Generate operator_orders for mixed cascade
    operator_orders = {and_operator.id: [2, [0, 1]]}
    mixed_cascaded = cascade_filters(query, operator_orders)
    
    print("\nMixed Cascaded Query Tree:")
    print(mixed_cascaded.query_tree.tree())
    
    # Demo 3: Random order
    print_separator("Demo 4c: Cascade with random order")
    
    random_order = generate_random_rule_1_params(3)
    print(f"Generated random order: {random_order}")
    
    operator_orders_random = {and_operator.id: random_order}
    random_cascaded = cascade_filters(query, operator_orders_random)
    
    print("\nRandom Order Cascaded Query Tree:")
    print(random_cascaded.query_tree.tree())
    
    # Demo 4: Uncascade back to AND
    print_separator("Demo 4d: Uncascade back to OPERATOR(AND)")
    
    uncascaded = uncascade_filters(cascaded)
    
    print("\nUncascaded Query Tree (back to AND structure):")
    print(uncascaded.query_tree.tree())
    
    print_separator("DEMO 4 COMPLETED")
    return query


def demo_rule1():
    """Compatibility wrapper: call the Rule 1 demo implementation."""
    return demo_rule_1()


def demo_rule2():
    """Demo: Rule 2 - Reordering AND conditions (Seleksi Komutatif)"""
    print_separator("DEMO RULE 2 - REORDER")
    from query_optimizer.query_tree import QueryTree
    from query_optimizer.rule_2 import (
        analyze_and_operators_for_reorder,
        generate_random_rule_2_params,
        reorder_and_conditions,
    )
    from query_optimizer.optimization_engine import ParsedQuery

    # Build example filter with AND conditions
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

    print("Original Query Tree:")
    print(query.query_tree.tree())

    operators = analyze_and_operators_for_reorder(query)
    print(f"Found AND operators: {operators}")

    # For each operator generate a random permutation and apply
    operator_orders = {}
    for op_id, num in operators.items():
        order = generate_random_rule_2_params(num)
        operator_orders[op_id] = order

    print(f"Applying reorder params: {operator_orders}")
    transformed = reorder_and_conditions(query, operator_orders)

    print("Reordered Query Tree:")
    print(transformed.query_tree.tree())

    print_separator("DEMO RULE 2 COMPLETED")
    return transformed


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
    # Rule 2 (non-deterministic)
    demo_rule2()
    # Genetic / Rule integration
    demo_genetic_with_rules()
    # Optimizer comparison
    demo_optimized()

    print_separator("ALL DEMOS COMPLETED")
    return None

def demo_genetic_with_rules():
    """Demo 5: Genetic Optimizer with Rule 1 + Rule 2 Integration"""
    print_separator("DEMO 5: Genetic Optimizer with Rule 1 + Rule 2")
    
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
            demo_genetic_with_rules()
        elif demo_num == 8:
            demo_rule2()
        elif demo_num == 9:
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
