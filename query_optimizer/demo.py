"""
Demo Program
"""

import sys
import io
from unittest.mock import patch

MOCK_METADATA = {
    "tables": [
        "users", "profiles", "orders", "products", 
        "employees", "payroll", "accounts", 
        "logs", "a", "b"
    ],
    "columns": {
        "users": ["id", "name", "email", "age", "city", "status", "score", "type", "col_A", "col_B", "col_C", "col_D"],
        "profiles": ["id", "user_id", "bio", "verified"],
        "orders": ["id", "user_id", "amount", "total", "product_id", "date", "status", "discount"],
        "products": ["id", "name", "category", "price", "stock", "description", "discount"],
        "employees": ["id", "name", "salary", "dept_id", "employee_id"],
        "payroll": ["id", "employee_id", "amount", "date", "dept_id", "payroll_id"],
        "accounts": ["id", "payroll_id", "bank_name", "balance"],
        "logs": ["id", "user_id", "message"],
        "a": ["id", "name", "x"],
        "b": ["id", "a_id", "name"]
    }
}

def use_mock_metadata(func):
    def wrapper(*args, **kwargs):
        with patch('query_optimizer.query_check.get_metadata', return_value=MOCK_METADATA):
            return func(*args, **kwargs)
    return wrapper

def print_separator(title=""):
    print("\n" + "="*70)
    if title:
        print(f"  {title}")
        print("="*70)

def print_help():
    print_separator("QUERY OPTIMIZER DEMO HELP")
    print("Usage: python -m query_optimizer.demo [N] or [N.S]")
    print("")
    print("Rules (with scenarios):")
    print("  1    - Rule 1: Cascade Filters (Seleksi Konjungtif)")
    print("  1.1  -   Scenario: Full cascade")
    print("  1.2  -   Scenario: No cascade")
    print("  1.3  -   Scenario: Mixed cascade")
    print("  1.4  -   Scenario: Cycle transitions")
    print("")
    print("  2    - Rule 2: Reorder AND Conditions (Seleksi Komutatif)")
    print("  2.1  -   Scenario: Vertical reordering")
    print("  2.2  -   Scenario: Horizontal reordering")
    print("  2.3  -   Scenario: Complex shuffle")
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
    print("  8.2  -   Scenario: Join Key Preservation")
    print("  8.3  -   Scenario: Undo Optimization")
    print("  8.4  -   Scenario: SELECT * (Negative Test)")
    print("")
    print("General demos:")
    print("  9    - Parse: Parse SQL queries and show query trees")
    print("  10   - Optimize: Compare with/without Genetic Algorithm")
    print("  11   - Genetic Algorithm Step-by-Step (Internal Mechanics)")
    print("         Demonstrates Initialization, Selection, Crossover, and Mutation")
    print("  12   - All: Run all demos sequentially")
    print_separator()


# =============================================================================
# RULE 1: CASCADE FILTERS
# =============================================================================

@use_mock_metadata
def demo_rule_1():
    print_separator("DEMO 1: Rule 1 - (Seleksi Konjungtif)")
    print("\nRunning all scenarios...")
    
    from query_optimizer.subdemo.demo_rule1_scenarios import (
        scenario_1_full_cascade,
        scenario_2_no_cascade,
        scenario_3_mixed_cascade,
        scenario_4_cycle_transitions
    )
    
    scenario_1_full_cascade()
    scenario_2_no_cascade()
    scenario_3_mixed_cascade()
    scenario_4_cycle_transitions()
    
    print_separator("DEMO 1 COMPLETED")

# =============================================================================
# RULE 2: REORDER AND CONDITIONS
# =============================================================================

@use_mock_metadata
def demo_rule_2():
    print_separator("DEMO 2: Rule 2 - (Seleksi Komutatif)")
    print("\nRunning all scenarios...")
    
    from query_optimizer.subdemo.demo_rule2_scenarios import (
        scenario_1_vertical_reordering,
        scenario_2_horizontal_reordering,
        scenario_3_complex_shuffle
    )
    
    scenario_1_vertical_reordering()
    scenario_2_horizontal_reordering()
    scenario_3_complex_shuffle()

    print_separator("DEMO 2 COMPLETED")

# =============================================================================
# RULE 3: PROJECTION ELIMINATION
# =============================================================================

@use_mock_metadata
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

@use_mock_metadata
def demo_rule_4():
    print_separator("DEMO 4: Rule 4 - Push Selection into Joins")
    print("\nRunning all scenarios...")

    print_separator("DEMO 4: Rule 4 - Masih error")
    
    # from query_optimizer.subdemo.demo_rule4_scenarios import (
    #     scenario_1_basic_cross_join,
    #     scenario_2_filter_over_inner,
    #     scenario_3_undo_merge,
    #     scenario_4_nested_filters,
    #     scenario_5_merge_into_merged
    # )
    
    # scenario_1_basic_cross_join()
    # scenario_2_filter_over_inner()
    # scenario_3_undo_merge()
    # scenario_4_nested_filters()
    # scenario_5_merge_into_merged()
    
    print_separator("DEMO 4 COMPLETED")


# =============================================================================
# RULE 5: JOIN COMMUTATIVITY
# =============================================================================

@use_mock_metadata
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

@use_mock_metadata
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

@use_mock_metadata
def demo_rule_8():
    print_separator("DEMO 8: Rule 8 - Projection over Join")
    print("\nRunning all scenarios...")
    
    from query_optimizer.subdemo.demo_rule8_scenarios import (
        scenario_1_basic_pushdown,
        scenario_2_join_key_preservation,
        scenario_4_undo_optimization,
        scenario_5_star_query
    )
    
    scenario_1_basic_pushdown()
    scenario_2_join_key_preservation()
    scenario_4_undo_optimization()
    scenario_5_star_query()
    
    print_separator("DEMO 8 COMPLETED")


# =============================================================================
# DEMO 9: PARSE
# =============================================================================

@use_mock_metadata
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

@use_mock_metadata
def demo_optimized():
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

@use_mock_metadata
def demo_genetic_with_rules():
    """
    Demo 11: Genetic Algorithm Internal Mechanics (Step-by-Step)
    """
    print_separator("DEMO 11: GENETIC ALGORITHM (Internal Mechanics)")
    
    from query_optimizer.genetic_optimizer import GeneticOptimizer, Individual
    from query_optimizer.optimization_engine import OptimizationEngine, ParsedQuery
    from query_optimizer.rule_params_manager import get_rule_params_manager
    
    # 1. SETUP & QUERY AWAL
    sql = """
    SELECT u.name, prod.name, o.total 
    FROM users u 
    JOIN profiles prof ON u.id = prof.user_id 
    JOIN orders o ON u.id = o.user_id 
    JOIN products prod ON o.product_id = prod.id 
    WHERE u.email LIKE '%@gmail.com' 
    AND prod.category = 'Electronics' 
    AND o.total > 100000 
    AND prof.bio IS NOT NULL
    """

    print(f"\nExecuting Query: {sql}\n")

    engine = OptimizationEngine()
    query = engine.parse_query(sql)
    optimizer_helper = GeneticOptimizer() 
    mgr = get_rule_params_manager()

    print("==================================================")
    print(" 1. QUERY TREE AWAL (BASE)")
    print("==================================================")
    print(query.query_tree.tree(show_id=True))

    # Analisa Rules
    ops = mgr.get_registered_operations()
    base_analysis = {}
    for op in ops:
        base_analysis[op] = mgr.analyze_query(query, op)

    # 2. POPULASI GENERASI 1
    print("\n==================================================")
    print(" 2. GENERASI 1 (INITIAL POPULATION)")
    print("==================================================")

    population_size = 20
    population = []

    for _ in range(population_size):
        params = {}
        for op, metadata in base_analysis.items():
            params[op] = {}
            for key, meta in metadata.items():
                params[op][key] = mgr.generate_random_params(op, meta)
        population.append(Individual(params, query))

    for ind in population:
        if ind.fitness is None:
            ind.fitness = engine.get_cost(ind.query)

    population.sort(key=lambda x: x.fitness)

    def print_individual(title, ind, show_genealogy=False):
        print(f"\n--- {title} ---")
        print(f"Fitness (Cost): {ind.fitness}")
        print(f"Params Applied: {ind.operation_params}")
        if show_genealogy and hasattr(ind, 'genealogy'):
            print(f"Genealogy Source: {ind.genealogy}")
        print(ind.query.query_tree.tree(show_id=True))

    print("\n>>> BEST Child Generasi 1:")
    print_individual("Best Gen 1", population[0])

    # 3. SIMULASI CROSSOVER & MUTASI
    print("\n==================================================")
    print(" 3. DEMO CROSSOVER & MUTASI (3 PAIRS)")
    print("==================================================")

    parents_pool = population[:10]
    if len(parents_pool) < 6:
        print("Populasi terlalu kecil.")
        return

    demo_pairs = [
        (parents_pool[0], parents_pool[1]), 
        (parents_pool[2], parents_pool[3]), 
        (parents_pool[4], parents_pool[5]) 
    ]

    for i, (p1, p2) in enumerate(demo_pairs):
        print(f"\n################ PASANGAN KE-{i+1} ################")
        print(f"Parent A (Cost: {p1.fitness}) Params: {p1.operation_params}")
        print(f"Parent B (Cost: {p2.fitness}) Params: {p2.operation_params}")
        
        c1, c2 = optimizer_helper._crossover(p1, p2, query)
        
        mutation_note = ""
        if i == 2:
            print("\n[!] MUTASI DIPICU PADA CHILD A...")
            c1 = optimizer_helper._mutate(c1)
            mutation_note = f" (MUTATED!)\n"
        
        c1.fitness = engine.get_cost(c1.query)
        c2.fitness = engine.get_cost(c2.query)
        
        print(f"\n-> Hasil Crossover Child A{mutation_note}:")
        print_individual("Child A", c1, show_genealogy=True)
        
        print(f"\n-> Hasil Crossover Child B:")
        print_individual("Child B", c2, show_genealogy=True)
        
        population.append(c1)
        population.append(c2)
    
    print_separator("DEMO 11 COMPLETED")
    return population[0].query


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
    
    # Fix encoding for Windows
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except AttributeError:
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
        if demo_num == 1:
            if scenario_num is None:
                demo_rule_1()
            else:
                from query_optimizer.subdemo.demo_rule1_scenarios import (
                    scenario_1_full_cascade,
                    scenario_2_no_cascade,
                    scenario_3_mixed_cascade,
                    scenario_4_cycle_transitions
                )
                with patch('query_optimizer.query_check.get_metadata', return_value=MOCK_METADATA):
                    if scenario_num == 1: scenario_1_full_cascade()
                    elif scenario_num == 2: scenario_2_no_cascade()
                    elif scenario_num == 3: scenario_3_mixed_cascade()
                    elif scenario_num == 4: scenario_4_cycle_transitions()
                    else: print(f"Invalid scenario 1.{scenario_num}"); return
                print_separator("DEMO COMPLETED")
        
        elif demo_num == 2:
            if scenario_num is None:
                demo_rule_2()
            else:
                from query_optimizer.subdemo.demo_rule2_scenarios import (
                    scenario_1_vertical_reordering, scenario_2_horizontal_reordering, scenario_3_complex_shuffle
                )
                with patch('query_optimizer.query_check.get_metadata', return_value=MOCK_METADATA):
                    if scenario_num == 1: scenario_1_vertical_reordering()
                    elif scenario_num == 2: scenario_2_horizontal_reordering()
                    elif scenario_num == 3: scenario_3_complex_shuffle()
                    else: print(f"Invalid scenario 2.{scenario_num}"); return
                print_separator("DEMO COMPLETED")
               
        elif demo_num == 3:
            if scenario_num is None:
                demo_rule_3()
            else:
                from query_optimizer.subdemo.demo_rule3_scenarios import (
                    scenario_1_nested_projections, scenario_2_triple_nested
                )
                with patch('query_optimizer.query_check.get_metadata', return_value=MOCK_METADATA):
                    if scenario_num == 1: scenario_1_nested_projections()
                    elif scenario_num == 2: scenario_2_triple_nested()
                    else: print(f"Invalid scenario 3.{scenario_num}"); return
                print_separator("DEMO COMPLETED")
        
        elif demo_num == 4:
            if scenario_num is None:
                demo_rule_4()
            else:
                print_separator("DEMO 4: Rule 4 - Masih error")
                # from query_optimizer.subdemo.demo_rule4_scenarios import (
                #     scenario_1_basic_cross_join, scenario_2_filter_over_inner, scenario_3_undo_merge, 
                #     scenario_4_nested_filters, scenario_5_merge_into_merged
                # )
                # with patch('query_optimizer.query_check.get_metadata', return_value=MOCK_METADATA):
                #     if scenario_num == 1: scenario_1_basic_cross_join()
                #     elif scenario_num == 2: scenario_2_filter_over_inner()
                #     elif scenario_num == 3: scenario_3_undo_merge()
                #     elif scenario_num == 4: scenario_4_nested_filters()
                #     elif scenario_num == 5: scenario_5_merge_into_merged()
                #     else: print(f"Invalid scenario 4.{scenario_num}"); return
                print_separator("DEMO COMPLETED")
        
        elif demo_num == 5:
            if scenario_num is None:
                demo_rule_5()
            else:
                from query_optimizer.subdemo.demo_rule5_scenarios import (
                    scenario_1_basic_swap, scenario_2_multiple_joins, scenario_3_natural_join,
                    scenario_4_bidirectional, scenario_5_ga_exploration
                )
                with patch('query_optimizer.query_check.get_metadata', return_value=MOCK_METADATA):
                    if scenario_num == 1: scenario_1_basic_swap()
                    elif scenario_num == 2: scenario_2_multiple_joins()
                    elif scenario_num == 3: scenario_3_natural_join()
                    elif scenario_num == 4: scenario_4_bidirectional()
                    elif scenario_num == 5: scenario_5_ga_exploration()
                    else: print(f"Invalid scenario 5.{scenario_num}"); return
                print_separator("DEMO COMPLETED")
        
        elif demo_num == 6:
            print_separator("DEMO 6: Rule 6 - Associativity/Commutativity of Joins")
            print("\n⚠ This rule is not yet implemented.")
            print("Coming soon...")
        
        elif demo_num == 7:
            if scenario_num is None:
                demo_rule_7()
            else:
                from query_optimizer.subdemo.demo_rule7_scenarios import (
                    scenario_1_single_condition, scenario_2_multiple_conditions
                )
                with patch('query_optimizer.query_check.get_metadata', return_value=MOCK_METADATA):
                    if scenario_num == 1: scenario_1_single_condition()
                    elif scenario_num == 2: scenario_2_multiple_conditions()
                    else: print(f"Invalid scenario 7.{scenario_num}"); return
                print_separator("DEMO COMPLETED")
        
        elif demo_num == 8:
            if scenario_num is None:
                demo_rule_8()
            else:
                from query_optimizer.subdemo.demo_rule8_scenarios import (
                    scenario_1_basic_pushdown, scenario_2_join_key_preservation, 
                    scenario_4_undo_optimization, scenario_5_star_query
                )
                with patch('query_optimizer.query_check.get_metadata', return_value=MOCK_METADATA):
                    if scenario_num == 1: scenario_1_basic_pushdown()
                    elif scenario_num == 2: scenario_2_join_key_preservation()
                    elif scenario_num == 3: scenario_4_undo_optimization()
                    elif scenario_num == 4: scenario_5_star_query()
                    else: print(f"Invalid scenario 8.{scenario_num}"); return
                print_separator("DEMO COMPLETED")
        
        elif demo_num == 9:
            demo_parse()
        elif demo_num == 10:
            demo_optimized()
        elif demo_num == 11:
            demo_genetic_with_rules()
        elif demo_num == 12:
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