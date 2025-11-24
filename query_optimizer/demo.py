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
    print("  python -m query_optimizer.demo 4  # Run demo Rule 1 (cascade filters)")
    print("  python -m query_optimizer.demo 5  # Run demo genetic with Rule 1 + Rule 2")
    print("  python -m query_optimizer.demo 6  # Run demo Rule 2 (reorder AND conditions)")
    print("  python -m query_optimizer.demo 7  # Run all demos sequentially")
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
    # Rule 1
    demo_rule_1()
    # Rule 2
    demo_rule2()
    # Rule 3
    demo_rule_3()
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
            print("  python -m query_optimizer.demo 4  # Run demo Rule 1 (cascade filters)")
            print("  python -m query_optimizer.demo 5  # Run demo genetic with Rule 1 + Rule 2")
            print("  python -m query_optimizer.demo 6  # Run demo Rule 2 (reorder AND conditions)")
            print("  python -m query_optimizer.demo 7  # Run all demos sequentially")
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
            demo_rule_1()
        elif demo_num == 5:
            demo_genetic_with_rules()
        elif demo_num == 6:
            demo_rule2()
        elif demo_num == 7:
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
