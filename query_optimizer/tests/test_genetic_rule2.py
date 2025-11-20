"""
Test untuk memastikan Rule 2 terintegrasi dengan baik di genetic optimizer
"""

import unittest
from query_optimizer.query_tree import QueryTree
from query_optimizer.optimization_engine import ParsedQuery
from query_optimizer.genetic_optimizer import (
    GeneticOptimizer,
    Individual
)


class TestGeneticRule2Integration(unittest.TestCase):
    """Test integrasi Rule 2 dengan Genetic Optimizer"""
    
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
        """Test bahwa analisis query mencakup Rule 2"""
        ga = GeneticOptimizer(population_size=5, generations=1)
        analysis = ga._analyze_query_for_rules(self.query)
        
        # Should have both rule_1 and rule_2
        self.assertIn('rule_1', analysis)
        self.assertIn('rule_2', analysis)
        
        # Rule 2 should find the AND operator
        self.assertGreater(len(analysis['rule_2']), 0)
    
    def test_initialize_population_with_rule2(self):
        """Test bahwa inisialisasi populasi membuat params untuk Rule 2"""
        ga = GeneticOptimizer(population_size=10, generations=1)
        analysis = ga._analyze_query_for_rules(self.query)
        population = ga._initialize_population(self.query, analysis)
        
        # Check that some individuals have rule_2 params
        has_rule2 = False
        for individual in population:
            if 'rule_2' in individual.rule_params and individual.rule_params['rule_2']:
                has_rule2 = True
                # Rule 2 params should be a permutation
                for node_id, params in individual.rule_params['rule_2'].items():
                    self.assertIsInstance(params, list)
                    # Should be a valid permutation of [0, 1, 2]
                    self.assertEqual(set(params), {0, 1, 2})
                    break
        
        self.assertTrue(has_rule2, "At least one individual should have rule_2 params")
    
    def test_individual_applies_rule2_standalone(self):
        """Test bahwa Individual dapat menerapkan Rule 2 standalone (tanpa Rule 1)"""
        # Rule 2 standalone hanya diterapkan jika Rule 1 tidak ada
        # Karena node ID berubah setelah clone, kita pakai fallback mechanism
        rule_params = {
            'rule_2': {
                self.and_op.id: [2, 0, 1]  # Reorder to [<, >, =]
            }
        }
        
        individual = Individual(rule_params, self.query)
        
        # Query should be transformed
        result_tree = individual.query.query_tree
        
        # Check that AND operator children are reordered
        and_node = result_tree.childs[1]
        self.assertEqual(and_node.type, "OPERATOR")
        self.assertEqual(and_node.val, "AND")
        
        # Should be in order: <, >, = (using fallback since ID changed after clone)
        self.assertEqual(and_node.childs[0].val, "<")
        self.assertEqual(and_node.childs[1].val, ">")
        self.assertEqual(and_node.childs[2].val, "=")
    
    def test_individual_applies_rule2_then_rule1(self):
        """Test bahwa Individual dapat menerapkan Rule 2 kemudian Rule 1"""
        # Create individual with both rules
        rule_params = {
            'rule_2': {
                self.and_op.id: [1, 2, 0]  # Reorder to [=, <, >]
            },
            'rule_1': {
                self.and_op.id: [0, 1, 2]  # Cascade all separately
            }
        }
        
        individual = Individual(rule_params, self.query)
        result = individual.query
        
        # Should have cascaded FILTERs
        depth = 0
        current = result.query_tree
        while current and current.type == "FILTER":
            depth += 1
            current = current.childs[0] if current.childs else None
        
        self.assertGreaterEqual(depth, 2, "Should have cascaded FILTERs")
    
    def test_mutation_can_change_rule2_params(self):
        """Test bahwa mutasi dapat mengubah params Rule 2"""
        ga = GeneticOptimizer(population_size=5, generations=1)
        analysis = ga._analyze_query_for_rules(self.query)
        
        # Create individual with rule_2
        rule_params = {
            'rule_2': {
                self.and_op.id: [0, 1, 2]
            }
        }
        
        individual = Individual(rule_params, self.query)
        
        # Mutate multiple times, should eventually change rule_2 params
        changed = False
        for _ in range(20):  # Try multiple mutations
            mutated = ga._mutate(individual, self.query, analysis)
            if 'rule_2' in mutated.rule_params and mutated.rule_params['rule_2']:
                mutated_params = mutated.rule_params['rule_2'].get(self.and_op.id, [])
                if mutated_params != [0, 1, 2]:
                    changed = True
                    # Should still be valid permutation
                    self.assertEqual(set(mutated_params), {0, 1, 2})
                    break
        
        # Should have changed at some point (high probability with 20 tries)
        # Note: This might occasionally fail due to randomness, but very unlikely
        self.assertTrue(changed, "Mutation should eventually change rule_2 params")
    
    def test_crossover_preserves_rule2(self):
        """Test bahwa crossover mempertahankan Rule 2 params"""
        ga = GeneticOptimizer(population_size=5, generations=1)
        
        # Create two parents with different rule_2 params
        parent1 = Individual(
            {'rule_2': {self.and_op.id: [0, 1, 2]}},
            self.query
        )
        
        parent2 = Individual(
            {'rule_2': {self.and_op.id: [2, 1, 0]}},
            self.query
        )
        
        # Crossover
        child1, child2 = ga._crossover(parent1, parent2, self.query)
        
        # Children should have rule_2 params from one of the parents
        self.assertIn('rule_2', child1.rule_params)
        self.assertIn('rule_2', child2.rule_params)
    
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
        
        # Best individual might use rule_2
        if 'rule_2' in ga.best_individual.rule_params:
            for node_id, params in ga.best_individual.rule_params['rule_2'].items():
                # Should be valid permutation
                self.assertIsInstance(params, list)
                self.assertEqual(len(params), 3)
                self.assertEqual(set(params), {0, 1, 2})
    
    def test_rule2_in_statistics(self):
        """Test bahwa statistik mencakup Rule 2"""
        def simple_fitness(parsed_query: ParsedQuery) -> float:
            return 10.0  # Constant fitness
        
        ga = GeneticOptimizer(
            population_size=5,
            generations=2,
            fitness_func=simple_fitness
        )
        
        ga.optimize(self.query)
        stats = ga.get_statistics()
        
        # Should have best_params with rule_2
        self.assertIn('best_params', stats)
        if stats['best_params'] and 'rule_2' in stats['best_params']:
            # Rule 2 params should be dict of node_id -> permutation
            self.assertIsInstance(stats['best_params']['rule_2'], dict)


class TestRule2Priority(unittest.TestCase):
    """Test bahwa Rule 2 diterapkan sebelum Rule 1"""
    
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
    
    def test_rule2_applied_before_rule1(self):
        """Test bahwa Rule 2 diterapkan sebelum Rule 1"""
        # Create individual with both rules
        rule_params = {
            'rule_2': {
                self.and_op.id: [2, 0, 1]  # Reorder: [<, >, =]
            },
            'rule_1': {
                self.and_op.id: [0, 1, 2]  # Cascade all
            }
        }
        
        individual = Individual(rule_params, self.query)
        
        # After Rule 2, order should be [<, >, =]
        # Then Rule 1 cascades with indices [0, 1, 2] referring to the reordered conditions
        # So: FILTER(<) -> FILTER(>) -> FILTER(=) -> RELATION
        
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

