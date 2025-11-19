"""
Demo Program untuk Query Optimizer
"""

from query_optimizer.optimization_engine import OptimizationEngine
from query_optimizer.genetic_optimizer import GeneticOptimizer
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
    print_separator()


def demo_parse():
    """Demo 1: Basic optimization tanpa GA"""
    print_separator("DEMO 1: Basic Query Optimization")
    
    engine = OptimizationEngine()
    
    # Parse query dengan multiple conditions
    sql = "SELECT id, name, email FROM users WHERE age > 25 AND status = 'active' AND city = 'Jakarta'"
    print(f"\nOriginal SQL:\n{sql}")
    
    query = engine.parse_query(sql)
    
    print("\n Query Tree Structure:")
    print_query_tree(query.query_tree)
    
    # Calculate cost
    cost = engine.get_cost(query)
    print(f"\n Estimated Cost: {cost}")
    
    return query


def demo_optimized():
    """Demo 2: Optimization menggunakan Genetic Algorithm"""
    print_separator("DEMO 2: Genetic Algorithm Optimization")
    
    engine = OptimizationEngine()
    
    # Query yang lebih kompleks untuk optimization
    sql = "SELECT * FROM users WHERE age > 25 AND name = 'John' AND status = 'active' AND city = 'Jakarta'"
    print(f"\nOriginal SQL:\n{sql}")
    
    # Manual ParsedQuery dengan struktur AND yang benar untuk GA
    from query_optimizer.query_tree import QueryTree
    from query_optimizer.optimization_engine import ParsedQuery
    
    # Build query tree manually with proper AND structure
    # PROJECT
    #   FILTER(AND)
    #     RELATION(users)
    #     FILTER(age > 25)
    #     FILTER(name = John)
    #     FILTER(status = active)
    #     FILTER(city = Jakarta)
    
    relation = QueryTree("RELATION", "users")
    filter1 = QueryTree("FILTER", "age > 25")
    filter2 = QueryTree("FILTER", "name = John")
    filter3 = QueryTree("FILTER", "status = active")
    filter4 = QueryTree("FILTER", "city = Jakarta")
    
    filter_and = QueryTree("FILTER", "AND")
    filter_and.add_child(relation)
    filter_and.add_child(filter1)
    filter_and.add_child(filter2)
    filter_and.add_child(filter3)
    filter_and.add_child(filter4)
    
    project = QueryTree("PROJECT", "*")
    project.add_child(filter_and)
    
    query = ParsedQuery(project, sql)
    
    print("\n Original Query Tree:")
    print_query_tree(query.query_tree)
    
    original_cost = engine.get_cost(query)
    print(f"\n Original Cost: {original_cost}")
    
    # Setup Genetic Algorithm
    print("\n Running Genetic Algorithm...")
    print("Parameters:")
    print("  - Population Size: 30")
    print("  - Generations: 50")
    print("  - Mutation Rate: 0.15")
    print("  - Crossover Rate: 0.8")
    
    ga = GeneticOptimizer(
        population_size=30,
        generations=50,
        mutation_rate=0.15,
        crossover_rate=0.8,
        elitism=3
    )
    
    # Run optimization
    optimized_query = ga.optimize(query)
    
    print("\n Optimized Query Tree:")
    print_query_tree(optimized_query.query_tree)
    
    optimized_cost = engine.get_cost(optimized_query)
    print(f"\n Optimized Cost: {optimized_cost}")
    
    # Show improvement
    improvement = original_cost - optimized_cost
    improvement_pct = (improvement / original_cost * 100) if original_cost > 0 else 0
    
    print(f"\n Improvement:")
    print(f"  - Cost Reduction: {improvement:.2f}")
    print(f"  - Percentage: {improvement_pct:.2f}%")
    
    # Show best individual
    if ga.best_individual:
        print(f"\n Best Solution:")
        print(f"  - Filter Orders: {ga.best_individual.filter_orders}")
        print(f"  - Applied Rules: {ga.best_individual.applied_rules}")
        
        if not ga.best_individual.filter_orders:
            print("\n Note: No AND filters found in query tree structure.")
            print("       GA is optimizing by applying random transformation rules only.")
            print("       Current parser creates FILTER nodes with full WHERE clause,")
            print("       not decomposed AND structure needed for filter reordering.")
    
    # Show evolution progress
    if ga.history:
        print("\n Evolution Progress (Every 10 generations):")
        print("Gen | Best Cost | Avg Cost | Worst Cost")
        print("----|-----------|----------|------------")
        
        for i, record in enumerate(ga.history):
            if i % 10 == 0 or i == len(ga.history) - 1:
                print(f"{record['generation']:3d} | "
                      f"{record['best_fitness']:9.2f} | "
                      f"{record['avg_fitness']:8.2f} | "
                      f"{record['worst_fitness']:10.2f}")
    else:
        print("\n No evolution occurred - query structure not suitable for GA optimization.")
    
    return optimized_query, ga


def main():
    """Main program"""
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
