"""
Demo Program untuk Query Optimizer
"""

from query_optimizer.optimization_engine import OptimizationEngine
import sys


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
    print("  python -m query_optimizer.demo 2  # Run demo optimized")
    print("  python -m query_optimizer.demo 3  # Run demo Rule 3 (projection elimination)")
    print("  python -m query_optimizer.demo 4  # Run demo Rule 1 (cascade filters)")
    print("  python -m query_optimizer.demo 5  # Run demo genetic with Rule 1 + Rule 2")
    print_separator()

def demo_parse():
    pass

def demo_optimized():
    pass

def demo_rule_3():
    """Demo 3: Rule 3 - Projection Elimination"""
    print_separator("DEMO 3: Rule 3 - Projection Elimination")
    
    from query_optimizer.query_tree import QueryTree
    from query_optimizer.optimization_engine import ParsedQuery
    from query_optimizer.rule_3 import seleksi_proyeksi
    
    print("\n" + "="*70)
    print("Creating query with nested projections (redundant)")
    print("="*70)
    
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
    
    print("\n" + "="*70)
    print("Applying Rule 3: Projection Elimination")
    print("="*70)
    print("Rule: PROJECT_1(PROJECT_2(Source)) ≡ PROJECT_1(Source)")
    
    optimized = seleksi_proyeksi(query)
    
    print("\nOptimized Query Tree (projection eliminated):")
    print(optimized.query_tree.tree())
    
    optimized_count = count_projects(optimized.query_tree)
    print(f"\nPROJECT nodes after: {optimized_count}")
    
    if optimized_count < original_count:
        print(f"✓ Successfully eliminated {original_count - optimized_count} nested projection(s)!")
    
    print("\n" + "="*70)
    print("Integration with Optimization Engine")
    print("="*70)
    print("Rule 3 is applied ONCE at the start of optimize_query()")
    print("It runs BEFORE genetic algorithm, not during GA iterations")
    
    from query_optimizer.optimization_engine import OptimizationEngine
    
    engine = OptimizationEngine()
    
    # Test with simple query
    simple_query = engine.parse_query("SELECT * FROM users WHERE age > 18")
    print("\n\nTesting optimization flow:")
    print("1. Parse query")
    print("2. Apply Rule 3 (projection elimination) - ONE TIME")
    print("3. Apply genetic algorithm (filter optimization)")
    
    optimized_with_ga = engine.optimize_query(
        simple_query,
        use_genetic=True,
        population_size=10,
        generations=5
    )
    
    print("\n✓ Rule 3 applied before genetic algorithm")
    print("✓ Genetic algorithm works on Rule 3 result")
    
    print_separator("DEMO 3 COMPLETED")
    print("\nKey Points:")
    print("- Rule 3 eliminates redundant nested projections")
    print("- It's deterministic (no parameters to optimize)")
    print("- Applied ONCE at start, NOT in genetic algorithm")
    print("- Always beneficial (no trade-offs)")
    
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
    print_query_tree(query.query_tree)
    
    # Analyze AND operators
    print("\nAnalyzing AND operators...")
    operators = analyze_and_operators(query)
    print(f"Found {len(operators)} AND operator(s):")
    for op_id, num_conditions in operators.items():
        print(f"  - Operator ID {op_id}: {num_conditions} conditions")
    
    # Demo 1: Cascade with default order (all single)
    print("\n" + "="*70)
    print("Demo 4a: Cascade all conditions (default order)")
    print("="*70)
    
    cascaded = seleksi_konjungtif(query)
    
    print("\nCascaded Query Tree (fully cascaded):")
    print_query_tree(cascaded.query_tree)
    
    # Demo 2: Cascade with mixed order
    print("\n" + "="*70)
    print("Demo 4b: Cascade with mixed order [2, [0, 1]]")
    print("="*70)
    print("This means: condition2 single, then (condition0 AND condition1) grouped")
    
    # Generate operator_orders for mixed cascade
    operator_orders = {and_operator.id: [2, [0, 1]]}
    mixed_cascaded = cascade_filters(query, operator_orders)
    
    print("\nMixed Cascaded Query Tree:")
    print_query_tree(mixed_cascaded.query_tree)
    
    # Demo 3: Random order
    print("\n" + "="*70)
    print("Demo 4c: Cascade with random order")
    print("="*70)
    
    random_order = generate_random_rule_1_params(3)
    print(f"Generated random order: {random_order}")
    
    operator_orders_random = {and_operator.id: random_order}
    random_cascaded = cascade_filters(query, operator_orders_random)
    
    print("\nRandom Order Cascaded Query Tree:")
    print_query_tree(random_cascaded.query_tree)
    
    # Demo 4: Uncascade back to AND
    print("\n" + "="*70)
    print("Demo 4d: Uncascade back to OPERATOR(AND)")
    print("="*70)
    
    uncascaded = uncascade_filters(cascaded)
    
    print("\nUncascaded Query Tree (back to AND structure):")
    print_query_tree(uncascaded.query_tree)
    
    print_separator("DEMO 4 COMPLETED")
    return query

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
    
    print("\n" + "="*70)
    print("Running Genetic Optimizer...")
    print("="*70)
    print(f"Population Size: 20")
    print(f"Generations: 10")
    print(f"Mutation Rate: 0.2")
    print(f"Using Unified Filter Parameters (combines reordering and cascading)")
    
    ga = GeneticOptimizer(
        population_size=20,
        generations=10,
        mutation_rate=0.2,
        crossover_rate=0.8,
        elitism=2,
    )
    
    optimized_query = ga.optimize(query)
    
    print("\n" + "="*70)
    print("Optimization Results")
    print("="*70)
    
    print("\nOptimized Query Tree:")
    print_query_tree(optimized_query.query_tree)
    
    print("\n" + "="*70)
    print("Statistics")
    print("="*70)
    
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
                            print(f"      → Unified format combines reordering and cascading")
    
    print("\n" + "="*70)
    print("Fitness Progress")
    print("="*70)
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
    print("║           QUERY OPTIMIZER - GENETIC ALGORITHM DEMO                ║")
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
            print("  python -m query_optimizer.demo 3  # Run demo rule 1")
            print("  python -m query_optimizer.demo 4  # Run demo genetic")
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
