"""
Demo Program untuk Query Optimizer

New Structure:
- Demo 0: Help
- Demo 1-8: Individual Rules with scenarios
- Demo 9: Parse
- Demo 10: Optimize
- Demo 11: Genetic Algorithm
- Demo 12: All demos
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
    print("Usage: python -m query_optimizer.demo [N] or [N.S]")
    print("")
    print("Rules (with scenarios):")
    print("  1    - Rule 1: Cascade Filters (Seleksi Konjungtif)")
    print("  1.1  -   Scenario: Full cascade")
    print("  1.2  -   Scenario: No cascade")
    print("  1.3  -   Scenario: Mixed cascade")
    print("  1.4  -   Scenario: Uncascade (reverse)")
    print("")
    print("  2    - Rule 2: Reorder AND Conditions (Seleksi Komutatif)")
    print("  2.1  -   Scenario: Original order baseline")
    print("  2.2  -   Scenario: Reversed order")
    print("  2.3  -   Scenario: Finding optimal order")
    print("")
    print("  3    - Rule 3: Projection Elimination")
    print("  3.1  -   Scenario: Nested projections")
    print("  3.2  -   Scenario: Triple nested")
    print("")
    print("  4    - Rule 4: Push Selection into Joins")
    print("  4.1  -   Scenario: FILTER over CROSS JOIN")
    print("  4.2  -   Scenario: FILTER over INNER JOIN")
    print("  4.3  -   Scenario: Undo merge")
    print("  4.4  -   Scenario: Nested filters")
    print("  4.5  -   Scenario: Merge into already-merged JOIN & undo options")
    print("")
    print("  5    - Rule 5: Join Commutativity")
    print("  5.1  -   Scenario: Basic JOIN swap")
    print("  5.2  -   Scenario: Multiple JOINs selective swap")
    print("  5.3  -   Scenario: NATURAL JOIN commutativity")
    print("  5.4  -   Scenario: Bidirectional transformation")
    print("  5.5  -   Scenario: GA exploration of join orders")
    print("")
    print("  6    - Rule 6: Associativity/Commutativity of Joins (Coming soon)")
    print("")
    print("  7    - Rule 7: Filter Pushdown over Join")
    print("  7.1  -   Scenario: Single condition pushdown")
    print("  7.2  -   Scenario: Multiple conditions pushdown")
    print("")
    print("  8    - Rule 8: Projection over Join")
    print("  8.1  -   Scenario: Basic pushdown")
    print("  8.2  -   Scenario: Selective projection")
    print("")
    print("General demos:")
    print("  9    - Parse: Parse SQL queries and show query trees")
    print("  10   - Optimize: Compare with/without Genetic Algorithm")
    print("  11   - Genetic Algorithm: Full demo with all rules")
    print("  12   - All: Run all demos sequentially")
    print_separator()


# =============================================================================
# RULE 1: CASCADE FILTERS
# =============================================================================

def demo_rule_1():
    """Demo 1: Rule 1 - Cascade Filters (all scenarios)"""
    print_separator("DEMO 1: Rule 1 - Cascade Filters (Seleksi Konjungtif)")
    
    print("\nThis demo has multiple scenarios:")
    print("  1.1 - Full cascade (all single filters)")
    print("  1.2 - No cascade (keep all grouped)")
    print("  1.3 - Mixed cascade (some single, some grouped)")
    print("  1.4 - Uncascade (reverse transformation)")
    
    print("\nRunning all scenarios...")
    
    from query_optimizer.subdemo.demo_rule1_scenarios import (
        scenario_1_full_cascade,
        scenario_2_no_cascade,
        scenario_3_mixed_cascade,
        scenario_4_uncascade
    )
    
    scenario_1_full_cascade()
    scenario_2_no_cascade()
    scenario_3_mixed_cascade()
    scenario_4_uncascade()
    
    print_separator("DEMO 1 COMPLETED")
    print("\nKey Insights:")
    print("- Rule 1 allows flexible cascade/grouping of AND conditions")
    print("- Each variation produces different cost")
    print("- Best order depends on selectivity of conditions")
    print("- More selective conditions should be evaluated first")
    print("- Genetic Algorithm can find optimal configuration (see Demo 11)")


# =============================================================================
# RULE 2: REORDER AND CONDITIONS
# =============================================================================

def demo_rule_2():
    """Demo 2: Rule 2 - Reorder AND Conditions (all scenarios)"""
    print_separator("DEMO 2: Rule 2 - Reorder AND Conditions (Seleksi Komutatif)")
    
    print("\nThis demo has multiple scenarios:")
    print("  2.1 - Original order baseline")
    print("  2.2 - Reversed order")
    print("  2.3 - Finding optimal order")
    
    print("\nRunning all scenarios...")
    
    from query_optimizer.subdemo.demo_rule2_scenarios import (
        scenario_1_original_order,
        scenario_2_reversed_order,
        scenario_3_optimal_order
    )
    
    scenario_1_original_order()
    scenario_2_reversed_order()
    scenario_3_optimal_order()
    
    print_separator("DEMO 2 COMPLETED")
    print("\nKey Insights:")
    print("- Rule 2 reorders AND conditions without changing structure")
    print("- All conditions remain in single AND operator")
    print("- Different orders can have different costs")
    print("- Best order evaluates most selective conditions first")
    print("- Genetic Algorithm can find optimal order (see Demo 11)")


# =============================================================================
# RULE 3: PROJECTION ELIMINATION
# =============================================================================

def demo_rule_3():
    """Demo 3: Rule 3 - Projection Elimination (all scenarios)"""
    print_separator("DEMO 3: Rule 3 - Projection Elimination")
    
    print("\nThis demo has multiple scenarios:")
    print("  3.1 - Nested projections elimination")
    print("  3.2 - Triple nested projections")
    
    print("\nRunning all scenarios...")
    
    from query_optimizer.subdemo.demo_rule3_scenarios import (
        scenario_1_nested_projections,
        scenario_2_triple_nested
    )
    
    scenario_1_nested_projections()
    scenario_2_triple_nested()
    
    print_separator("DEMO 3 COMPLETED")
    print("\nKey Insights:")
    print("- Rule 3 eliminates redundant nested projections")
    print("- Keeps only the outermost projection")
    print("- Applies recursively for multiple nesting levels")
    print("- Reduces overhead of unnecessary projection operations")


# =============================================================================
# RULE 4: PUSH SELECTION INTO JOINS
# =============================================================================

def demo_rule_4():
    """Demo 4: Rule 4 - Push Selection into Joins (all scenarios)"""
    print_separator("DEMO 4: Rule 4 - Push Selection into Joins")
    
    print("\nThis demo has multiple scenarios:")
    print("  4.1 - Basic: FILTER over CROSS JOIN")
    print("  4.2 - Additional FILTER over INNER JOIN")
    print("  4.3 - Reverse Transformation (Undo Merge)")
    print("  4.4 - Nested FILTERs from Undo (Advanced)")
    print("  4.5 - Merge into Already-Merged INNER JOIN")
    
    print("\nRunning all scenarios...")
    
    from query_optimizer.subdemo.demo_rule4_scenarios import (
        scenario_1_basic_cross_join,
        scenario_2_filter_over_inner,
        scenario_3_undo_merge,
        scenario_4_nested_filters,
        scenario_5_merge_into_merged
    )
    
    scenario_1_basic_cross_join()
    scenario_2_filter_over_inner()
    scenario_3_undo_merge()
    scenario_4_nested_filters()
    scenario_5_merge_into_merged()
    
    print_separator("DEMO 4 COMPLETED")
    print("\nKey Insights:")
    print("- Rule 4 has 2 options: merge or keep separate (boolean decision)")
    print("- Works with both CROSS JOIN and INNER JOIN")
    print("- Can merge additional FILTER into existing INNER JOIN")
    print("- Merging typically reduces cost by filtering during join")
    print("- Undo can create either combined (AND) or stair-like (nested) FILTERs")
    print("- undo_merge() provides reverse transformation (INNER -> CROSS)")
    print("- Transformations are bidirectional and semantics-preserving")
    print("- Genetic Algorithm can find optimal decision (see Demo 11)")


# =============================================================================
# RULE 5: JOIN COMMUTATIVITY
# =============================================================================

def demo_rule_5():
    """Demo 5: Rule 5 - Join Commutativity (all scenarios)"""
    print_separator("DEMO 5: Rule 5 - Join Commutativity")
    
    print("\nThis demo has multiple scenarios:")
    print("  5.1 - Basic JOIN swap (A JOIN B → B JOIN A)")
    print("  5.2 - Multiple JOINs selective swap")
    print("  5.3 - NATURAL JOIN commutativity")
    print("  5.4 - Bidirectional transformation (swap twice)")
    print("  5.5 - GA exploration of join orders")
    
    print("\nRunning all scenarios...")
    
    from query_optimizer.subdemo.demo_rule5_scenarios import (
        scenario_1_basic_swap,
        scenario_2_multiple_joins,
        scenario_3_natural_join,
        scenario_4_bidirectional,
        scenario_5_ga_exploration
    )
    
    scenario_1_basic_swap()
    scenario_2_multiple_joins()
    scenario_3_natural_join()
    scenario_4_bidirectional()
    scenario_5_ga_exploration()
    
    print_separator("DEMO 5 COMPLETED")
    print("\nKey Insights:")
    print("- Rule 5 enables swapping JOIN children: JOIN(A,B) ≡ JOIN(B,A)")
    print("- Works with all JOIN types: INNER, LEFT, RIGHT, CROSS, NATURAL")
    print("- join_child_params uses boolean per JOIN (True=swap, False=keep)")
    print("- Different orders can have different costs")
    print("- Transformation is bidirectional (swap twice returns original)")
    print("- Genetic Algorithm can find optimal join order (see Demo 11)")


# =============================================================================
# RULE 7: FILTER PUSHDOWN OVER JOIN
# =============================================================================

def demo_rule_7():
    """Demo 7: Rule 7 - Filter Pushdown over Join (all scenarios)"""
    print_separator("DEMO 7: Rule 7 - Filter Pushdown over Join")
    
    print("\nThis demo has multiple scenarios:")
    print("  7.1 - Single condition pushdown")
    print("  7.2 - Multiple conditions pushdown")
    
    print("\nRunning all scenarios...")
    
    from query_optimizer.subdemo.demo_rule7_scenarios import (
        scenario_1_single_condition,
        scenario_2_multiple_conditions
    )
    
    scenario_1_single_condition()
    scenario_2_multiple_conditions()
    
    print_separator("DEMO 7 COMPLETED")
    print("\nKey Insights:")
    print("- Rule 7 pushes filters closer to data sources")
    print("- Reduces data volume before join operation")
    print("- Can split AND conditions to multiple relations")
    print("- Significantly improves performance for large datasets")


# =============================================================================
# RULE 8: PROJECTION OVER JOIN
# =============================================================================

def demo_rule_8():
    """Demo 8: Rule 8 - Projection over Join (all scenarios)"""
    print_separator("DEMO 8: Rule 8 - Projection over Join")
    
    print("\nThis demo has multiple scenarios:")
    print("  8.1 - Basic projection pushdown")
    print("  8.2 - Selective projection (many columns)")
    
    print("\nRunning all scenarios...")
    
    from query_optimizer.subdemo.demo_rule8_scenarios import (
        scenario_1_basic_pushdown,
        scenario_2_selective_projection
    )
    
    scenario_1_basic_pushdown()
    scenario_2_selective_projection()
    
    print_separator("DEMO 8 COMPLETED")
    print("\nKey Insights:")
    print("- Rule 8 pushes projections to join children")
    print("- Reduces tuple width before join")
    print("- Each relation projects only needed columns + join keys")
    print("- More beneficial for wide tables with many columns")


# =============================================================================
# DEMO 9: PARSE
# =============================================================================

def demo_parse():
    """Demo 9: Parse SQL queries and show query trees"""
    print_separator("DEMO 9: PARSE SQL QUERIES")
    
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
            print("\nQuery Tree:")
            print(parsed.query_tree.tree())
        except Exception as e:
            print(f"Error parsing '{sql}': {e}")
    
    print_separator("DEMO 9 COMPLETED")
    return None


# =============================================================================
# DEMO 10: OPTIMIZE
# =============================================================================

def demo_optimized():
    """Demo 10: Compare optimization with/without Genetic Algorithm"""
    print_separator("DEMO 10: OPTIMIZE (Compare with/without GA)")
    
    from query_optimizer.optimization_engine import OptimizationEngine

    engine = OptimizationEngine()

    sql = "SELECT * FROM orders WHERE amount > 1000 * 2 AND status = 'pending'"
    print_separator(f"Parsing and optimizing: {sql}")
    parsed = engine.parse_query(sql)

    print("Original Query Tree:")
    print(parsed.query_tree.tree())

    print_separator("Optimize without Genetic Algorithm (deterministic rules only)")
    optimized_no_ga = engine.optimize_query(parsed, use_genetic=False)
    print(optimized_no_ga.query_tree.tree())
    cost_no_ga = engine.get_cost(optimized_no_ga)
    print(f"\nEstimated cost (no GA): {cost_no_ga:.2f}")

    print_separator("Optimize with Genetic Algorithm")
    optimized_ga = engine.optimize_query(parsed, use_genetic=True, population_size=10, generations=5)
    print(optimized_ga.query_tree.tree())
    cost_ga = engine.get_cost(optimized_ga)
    print(f"\nEstimated cost (with GA): {cost_ga:.2f}")
    
    print_separator("DEMO 10 COMPLETED")
    print("\nComparison:")
    print(f"  Without GA: {cost_no_ga:.2f}")
    print(f"  With GA:    {cost_ga:.2f}")
    if cost_ga < cost_no_ga:
        print(f"  Improvement: {cost_no_ga - cost_ga:.2f} ({((cost_no_ga - cost_ga) / cost_no_ga * 100):.1f}%)")

    return optimized_ga


# =============================================================================
# DEMO 11: GENETIC ALGORITHM
# =============================================================================

def demo_genetic_with_rules():
    """Demo 11: Genetic Optimizer with Rule 1 + Rule 2 + Rule 4 Integration"""
    print_separator("DEMO 11: GENETIC ALGORITHM with Unified Rules (1, 2, 4)")
    
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
    
    stats = ga.get_ga_statistics()
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
    
    print_separator("DEMO 11 COMPLETED")
    print("\nKey Insights:")
    print("- Genetic Algorithm explores large search space efficiently")
    print("- Unified filter_params format combines reordering and cascading")
    print("- Finds near-optimal solutions without exhaustive search")
    print("- Scalable to complex queries with many optimization choices")
    
    return optimized_query


# =============================================================================
# DEMO 12: ALL DEMOS
# =============================================================================

def demo_all():
    """Demo 12: Run all demos in sequence"""
    print_separator("DEMO 12: RUNNING ALL DEMOS")
    
    # Rules
    demo_rule_1()
    demo_rule_2()
    demo_rule_3()
    demo_rule_4()
    demo_rule_5()
    demo_rule_7()
    demo_rule_8()
    
    # General demos
    demo_parse()
    demo_optimized()
    demo_genetic_with_rules()

    print_separator("DEMO 12: ALL DEMOS COMPLETED")
    return None


# =============================================================================
# MAIN
# =============================================================================

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
    demo_num = 0
    scenario_num = None
    
    if len(sys.argv) > 1:
        try:
            # Check if it's a decimal number (e.g., 1.1, 4.2)
            if '.' in sys.argv[1]:
                parts = sys.argv[1].split('.')
                demo_num = int(parts[0])
                scenario_num = int(parts[1])
            else:
                demo_num = int(sys.argv[1])
        except ValueError:
            print("\n Error: Argument must be a number or decimal (e.g., 1 or 1.1)")
            print_help()
            return
    
    try:
        # Rule 1: Cascade Filters
        if demo_num == 1:
            if scenario_num is None:
                demo_rule_1()
            else:
                from query_optimizer.subdemo.demo_rule1_scenarios import (
                    scenario_1_full_cascade,
                    scenario_2_no_cascade,
                    scenario_3_mixed_cascade,
                    scenario_4_uncascade
                )
                
                if scenario_num == 1:
                    scenario_1_full_cascade()
                elif scenario_num == 2:
                    scenario_2_no_cascade()
                elif scenario_num == 3:
                    scenario_3_mixed_cascade()
                elif scenario_num == 4:
                    scenario_4_uncascade()
                else:
                    print(f"\n Error: Invalid scenario number {scenario_num}")
                    print("Valid scenarios: 1.1, 1.2, 1.3, 1.4")
                    return
                
                print_separator("DEMO COMPLETED")
        
        # Rule 2: Reorder AND Conditions
        elif demo_num == 2:
            if scenario_num is None:
                demo_rule_2()
            else:
                from query_optimizer.subdemo.demo_rule2_scenarios import (
                    scenario_1_original_order,
                    scenario_2_reversed_order,
                    scenario_3_optimal_order
                )
                
                if scenario_num == 1:
                    scenario_1_original_order()
                elif scenario_num == 2:
                    scenario_2_reversed_order()
                elif scenario_num == 3:
                    scenario_3_optimal_order()
                else:
                    print(f"\n Error: Invalid scenario number {scenario_num}")
                    print("Valid scenarios: 2.1, 2.2, 2.3")
                    return
                
                print_separator("DEMO COMPLETED")
        
        # Rule 3: Projection Elimination
        elif demo_num == 3:
            if scenario_num is None:
                demo_rule_3()
            else:
                from query_optimizer.subdemo.demo_rule3_scenarios import (
                    scenario_1_nested_projections,
                    scenario_2_triple_nested
                )
                
                if scenario_num == 1:
                    scenario_1_nested_projections()
                elif scenario_num == 2:
                    scenario_2_triple_nested()
                else:
                    print(f"\n Error: Invalid scenario number {scenario_num}")
                    print("Valid scenarios: 3.1, 3.2")
                    return
                
                print_separator("DEMO COMPLETED")
        
        # Rule 4: Push Selection into Joins
        elif demo_num == 4:
            if scenario_num is None:
                demo_rule_4()
            else:
                from query_optimizer.subdemo.demo_rule4_scenarios import (
                    scenario_1_basic_cross_join,
                    scenario_2_filter_over_inner,
                    scenario_3_undo_merge,
                    scenario_4_nested_filters,
                    scenario_5_merge_into_merged
                )
                
                if scenario_num == 1:
                    scenario_1_basic_cross_join()
                elif scenario_num == 2:
                    scenario_2_filter_over_inner()
                elif scenario_num == 3:
                    scenario_3_undo_merge()
                elif scenario_num == 4:
                    scenario_4_nested_filters()
                elif scenario_num == 5:
                    scenario_5_merge_into_merged()
                else:
                    print(f"\n Error: Invalid scenario number {scenario_num}")
                    print("Valid scenarios: 4.1, 4.2, 4.3, 4.4, 4.5")
                    return
                
                print_separator("DEMO COMPLETED")
        
        # Rule 5: Join Commutativity
        elif demo_num == 5:
            if scenario_num is None:
                demo_rule_5()
            else:
                from query_optimizer.subdemo.demo_rule5_scenarios import (
                    scenario_1_basic_swap,
                    scenario_2_multiple_joins,
                    scenario_3_natural_join,
                    scenario_4_bidirectional,
                    scenario_5_ga_exploration
                )
                
                if scenario_num == 1:
                    scenario_1_basic_swap()
                elif scenario_num == 2:
                    scenario_2_multiple_joins()
                elif scenario_num == 3:
                    scenario_3_natural_join()
                elif scenario_num == 4:
                    scenario_4_bidirectional()
                elif scenario_num == 5:
                    scenario_5_ga_exploration()
                else:
                    print(f"\n Error: Invalid scenario number {scenario_num}")
                    print("Valid scenarios: 5.1, 5.2, 5.3, 5.4, 5.5")
                    return
                
                print_separator("DEMO COMPLETED")
        
        # Rule 6: Coming soon
        elif demo_num == 6:
            print_separator("DEMO 6: Rule 6 - Associativity/Commutativity of Joins")
            print("\n⚠ This rule is not yet implemented.")
            print("Coming soon...")
        
        # Rule 7: Filter Pushdown over Join
        elif demo_num == 7:
            if scenario_num is None:
                demo_rule_7()
            else:
                from query_optimizer.subdemo.demo_rule7_scenarios import (
                    scenario_1_single_condition,
                    scenario_2_multiple_conditions
                )
                
                if scenario_num == 1:
                    scenario_1_single_condition()
                elif scenario_num == 2:
                    scenario_2_multiple_conditions()
                else:
                    print(f"\n Error: Invalid scenario number {scenario_num}")
                    print("Valid scenarios: 7.1, 7.2")
                    return
                
                print_separator("DEMO COMPLETED")
        
        # Rule 8: Projection over Join
        elif demo_num == 8:
            if scenario_num is None:
                demo_rule_8()
            else:
                from query_optimizer.subdemo.demo_rule8_scenarios import (
                    scenario_1_basic_pushdown,
                    scenario_2_selective_projection
                )
                
                if scenario_num == 1:
                    scenario_1_basic_pushdown()
                elif scenario_num == 2:
                    scenario_2_selective_projection()
                else:
                    print(f"\n Error: Invalid scenario number {scenario_num}")
                    print("Valid scenarios: 8.1, 8.2")
                    return
                
                print_separator("DEMO COMPLETED")
        
        # Demo 9: Parse
        elif demo_num == 9:
            demo_parse()
        
        # Demo 10: Optimize
        elif demo_num == 10:
            demo_optimized()
        
        # Demo 11: Genetic Algorithm
        elif demo_num == 11:
            demo_genetic_with_rules()
        
        # Demo 12: All demos
        elif demo_num == 12:
            demo_all()
        
        # Default: Show help
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
