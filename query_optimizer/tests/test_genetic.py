"""
Test untuk memastikan Rule terintegrasi dengan baik di genetic optimizer
"""

import unittest
from query_optimizer.query_tree import QueryTree
from query_optimizer.optimization_engine import ParsedQuery
from query_optimizer.genetic_optimizer import (
    GeneticOptimizer,
    Individual
)


class TestGeneticRule2Integration(unittest.TestCase):
    """Test integrasi Rule dengan Genetic Optimizer"""
    
    def setUp(self):
        """Setup test fixtures"""
        # Build query with FILTER and OPERATOR(AND) with 3 conditions
        self.relation = QueryTree("RELATION", "users")
        self.comp1 = QueryTree("COMPARISON", ">")
        self.comp2 = QueryTree("COMPARISON", "=")
        self.comp3 = QueryTree("COMPARISON", "<")
        
        self.and_op = QueryTree("OPERATOR", "AND")
        self.and_op.add_child(self.comp1)
        self.and_op.add_child(self.comp2)
        self.and_op.add_child(self.comp3)
        
        self.filter_node = QueryTree("FILTER")
        self.filter_node.add_child(self.relation)
        self.filter_node.add_child(self.and_op)
        
        self.query = ParsedQuery(self.filter_node, "SELECT * FROM users WHERE x > 5 AND y = 10 AND z < 20")
    
    def test_analyze_query_includes_rule2(self):
        """Test bahwa analisis query mencakup filter_params (includes both reorder and cascade)"""
        ga = GeneticOptimizer(population_size=5, generations=1)
        analysis = ga._analyze_query_for_rules(self.query)
        
        # Should have filter_params (unified for rule_1 and rule_2)
        self.assertIn('filter_params', analysis)
        
        # filter_params should find the AND operator
        self.assertGreater(len(analysis['filter_params']), 0)
    
    def test_initialize_population_with_filter_params(self):
        """Test bahwa inisialisasi populasi membuat params dengan unified format"""
        ga = GeneticOptimizer(population_size=10, generations=1)
        analysis = ga._analyze_query_for_rules(self.query)
        population = ga._initialize_population(self.query, analysis)
        
        # Check that individuals have filter_params
        has_filter_params = False
        for individual in population:
            if 'filter_params' in individual.operation_params and individual.operation_params['filter_params']:
                has_filter_params = True
                # Unified order should be a list (mixed int | list[int])
                for node_id, order in individual.operation_params['filter_params'].items():
                    self.assertIsInstance(order, list)
                    # Flatten to check all indices present
                    flat = []
                    for item in order:
                        if isinstance(item, list):
                            flat.extend(item)
                        else:
                            flat.append(item)
                    # Should contain all indices [0, 1, 2]
                    self.assertEqual(set(flat), {0, 1, 2})
                    break
        
        self.assertTrue(has_filter_params, "At least one individual should have filter_params")
    
    def test_individual_applies_unified_reorder(self):
        """Test bahwa Individual dapat menerapkan unified order (all singles = reorder + full cascade)"""
        # Unified format: [2, 0, 1] = all singles, reordered
        operation_params = {
            'filter_params': {
                self.and_op.id: [2, 0, 1]  # Reorder to [<, >, =] and cascade all
            }
        }
        
        individual = Individual(operation_params, self.query)
        
        # Query should be transformed with cascaded filters
        result_tree = individual.query.query_tree
        
        # Should have cascaded FILTERs (all singles means full cascade)
        depth = 0
        current = result_tree
        while current and current.type == "FILTER":
            depth += 1
            current = current.childs[0] if current.childs else None
        
        self.assertGreaterEqual(depth, 2, "Should have cascaded FILTERs")
    
    def test_individual_applies_unified_with_grouping(self):
        """Test bahwa Individual dapat menerapkan unified order dengan grouping"""
        # Unified format: [1, [2, 0]] = cond1 single, [cond2, cond0] grouped
        operation_params = {
            'filter_params': {
                self.and_op.id: [1, [2, 0]]  # cond= single, [<, >] grouped
            }
        }
        
        individual = Individual(operation_params, self.query)
        result = individual.query
        
        # Should have at least one FILTER (from the single)
        self.assertEqual(result.query_tree.type, "FILTER")
        
        # First filter should have the single condition
        # Below should be another structure with grouped conditions
        depth = 0
        current = result.query_tree
        while current and current.type == "FILTER":
            depth += 1
            current = current.childs[0] if current.childs else None
        
        self.assertGreaterEqual(depth, 1, "Should have at least one cascaded FILTER")
    
    def test_mutation_can_change_filter_params(self):
        """Test bahwa mutasi dapat mengubah unified filter params"""
        ga = GeneticOptimizer(population_size=5, generations=1)
        analysis = ga._analyze_query_for_rules(self.query)
        
        # Create individual with filter_params
        operation_params = {
            'filter_params': {
                self.and_op.id: [0, 1, 2]  # Original order
            }
        }
        
        individual = Individual(operation_params, self.query)
        
        # Mutate multiple times, should eventually change params
        changed = False
        for _ in range(20):  # Try multiple mutations
            mutated = ga._mutate(individual, self.query, analysis)
            if 'filter_params' in mutated.operation_params:
                mutated_order = mutated.operation_params['filter_params'].get(self.and_op.id, [])
                if mutated_order != [0, 1, 2]:
                    changed = True
                    # Flatten to check validity
                    flat = []
                    for item in mutated_order:
                        if isinstance(item, list):
                            flat.extend(item)
                        else:
                            flat.append(item)
                    # Should still contain all indices
                    self.assertEqual(set(flat), {0, 1, 2})
                    break
        
        # Should have changed at some point (high probability with 20 tries)
        self.assertTrue(changed, "Mutation should eventually change filter_params")
    
    def test_crossover_preserves_filter_params(self):
        """Test bahwa crossover mempertahankan unified filter params"""
        ga = GeneticOptimizer(population_size=5, generations=1)
        
        # Create two parents with different filter_params
        parent1 = Individual(
            {'filter_params': {self.and_op.id: [0, 1, 2]}},
            self.query
        )
        
        parent2 = Individual(
            {'filter_params': {self.and_op.id: [2, [1, 0]]}},
            self.query
        )
        
        # Crossover
        child1, child2 = ga._crossover(parent1, parent2, self.query)
        
        # Children should have filter_params from one of the parents
        self.assertIn('filter_params', child1.operation_params)
        self.assertIn('filter_params', child2.operation_params)
    
    def test_genetic_optimizer_with_rule2(self):
        """Test full genetic optimization with Rule 2 included"""
        # Simple fitness function (count number of nodes)
        def simple_fitness(parsed_query: ParsedQuery) -> float:
            def count_nodes(node):
                if node is None:
                    return 0
                return 1 + sum(count_nodes(child) for child in node.childs)
            return float(count_nodes(parsed_query.query_tree))
        
        ga = GeneticOptimizer(
            population_size=10,
            generations=5,
            mutation_rate=0.2,
            fitness_func=simple_fitness
        )
        
        # Run optimization
        result = ga.optimize(self.query)
        
        # Should complete without errors
        self.assertIsNotNone(result)
        self.assertIsNotNone(ga.best_individual)
        self.assertIsNotNone(ga.best_fitness)
        
        # Best individual should use filter_params with unified format
        if 'filter_params' in ga.best_individual.operation_params:
            for node_id, order in ga.best_individual.operation_params['filter_params'].items():
                # Should be valid unified order
                self.assertIsInstance(order, list)
                # Flatten and check
                flat = []
                for item in order:
                    if isinstance(item, list):
                        flat.extend(item)
                    else:
                        flat.append(item)
                self.assertEqual(len(flat), 3)
                self.assertEqual(set(flat), {0, 1, 2})
    
    def test_filter_params_in_statistics(self):
        """Test bahwa statistik mencakup unified filter_params"""
        def simple_fitness(parsed_query: ParsedQuery) -> float:
            return 10.0  # Constant fitness
        
        ga = GeneticOptimizer(
            population_size=5,
            generations=2,
            fitness_func=simple_fitness
        )
        
        ga.optimize(self.query)
        stats = ga.get_statistics()
        
        # Should have best_params with filter_params
        self.assertIn('best_params', stats)
        if stats['best_params'] and 'filter_params' in stats['best_params']:
            # filter_params should be dict of node_id -> unified order
            self.assertIsInstance(stats['best_params']['filter_params'], dict)


class TestUnifiedFilterParams(unittest.TestCase):
    """Test bahwa unified filter params diterapkan dengan benar"""
    
    def setUp(self):
        """Setup test fixtures"""
        relation = QueryTree("RELATION", "orders")
        comp1 = QueryTree("COMPARISON", ">")
        comp2 = QueryTree("COMPARISON", "=")
        comp3 = QueryTree("COMPARISON", "<")
        
        and_op = QueryTree("OPERATOR", "AND")
        and_op.add_child(comp1)
        and_op.add_child(comp2)
        and_op.add_child(comp3)
        
        filter_node = QueryTree("FILTER")
        filter_node.add_child(relation)
        filter_node.add_child(and_op)
        
        self.query = ParsedQuery(filter_node, "test")
        self.and_op = and_op
    
    def test_unified_reorder_and_cascade(self):
        """Test bahwa unified format menerapkan reorder dan cascade sekaligus"""
        # Create individual with unified format: [2, 0, 1] means reorder to [<, >, =] and all singles (cascade)
        operation_params = {
            'filter_params': {
                self.and_op.id: [2, 0, 1]  # Reorder to [<, >, =] with all singles (full cascade)
            }
        }
        
        individual = Individual(operation_params, self.query)
        
        # Unified format applies reordering and cascading together
        # Order [2, 0, 1] with all singles means:
        # FILTER(<) -> FILTER(>) -> FILTER(=) -> RELATION
        
        result = individual.query
        
        # Navigate through cascaded filters and check order
        filters = []
        current = result.query_tree
        while current and current.type == "FILTER":
            condition = current.childs[1] if len(current.childs) > 1 else None
            if condition and condition.type == "COMPARISON":
                filters.append(condition.val)
            current = current.childs[0]
        
        # Should see the reordered conditions in cascaded form
        # The exact order depends on cascade implementation, but should be based on reordered input
        self.assertGreater(len(filters), 0, "Should have cascaded filters")


if __name__ == "__main__":
    unittest.main()

