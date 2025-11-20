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
    print("  python -m query_optimizer.demo 3  # Run demo rule 1 (cascade filters)")
    print_separator()

def demo_parse():
    pass

def demo_optimized():
    pass

def demo_rule_1():
    """Demo 3: Rule 1 - Seleksi Konjungtif (Cascade Filters)"""
    print_separator("DEMO 3: Rule 1 - Seleksi Konjungtif")
    
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
    print("Demo 3a: Cascade all conditions (default order)")
    print("="*70)
    
    cascaded = seleksi_konjungtif(query)
    
    print("\nCascaded Query Tree (fully cascaded):")
    print_query_tree(cascaded.query_tree)
    
    # Demo 2: Cascade with mixed order
    print("\n" + "="*70)
    print("Demo 3b: Cascade with mixed order [2, [0, 1]]")
    print("="*70)
    print("This means: condition2 single, then (condition0 AND condition1) grouped")
    
    # Generate operator_orders for mixed cascade
    operator_orders = {and_operator.id: [2, [0, 1]]}
    mixed_cascaded = cascade_filters(query, operator_orders)
    
    print("\nMixed Cascaded Query Tree:")
    print_query_tree(mixed_cascaded.query_tree)
    
    # Demo 3: Random order
    print("\n" + "="*70)
    print("Demo 3c: Cascade with random order")
    print("="*70)
    
    random_order = generate_random_rule_1_params(3)
    print(f"Generated random order: {random_order}")
    
    operator_orders_random = {and_operator.id: random_order}
    random_cascaded = cascade_filters(query, operator_orders_random)
    
    print("\nRandom Order Cascaded Query Tree:")
    print_query_tree(random_cascaded.query_tree)
    
    # Demo 4: Uncascade back to AND
    print("\n" + "="*70)
    print("Demo 3d: Uncascade back to OPERATOR(AND)")
    print("="*70)
    
    uncascaded = uncascade_filters(cascaded)
    
    print("\nUncascaded Query Tree (back to AND structure):")
    print_query_tree(uncascaded.query_tree)
    
    print_separator("DEMO 3 COMPLETED")
    return query

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
        elif demo_num == 3:
            demo_rule_1()
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
