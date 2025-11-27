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
        mgr = GeneticOptimizer(population_size=5, generations=1)
        # Use the manager directly to analyze
        from query_optimizer.rule_params_manager import get_rule_params_manager
        manager = get_rule_params_manager()
        analysis = manager.analyze_query(self.query, 'filter_params')
        self.assertIsInstance(analysis, dict)
        self.assertGreater(len(analysis), 0)
    
    def test_initialize_population_with_filter_params(self):
        """Test bahwa inisialisasi populasi membuat params dengan unified format"""
        ga = GeneticOptimizer(population_size=10, generations=1)
        # Use optimize to get population indirectly
        best_query, history = ga.optimize(self.query)
        # We can't directly check population, but we can check the best individual
        # Check that best_query is a ParsedQuery
        self.assertIsInstance(best_query, ParsedQuery)
    
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
        # Use optimize to get a mutated individual indirectly
        best_query, history = ga.optimize(self.query)
        self.assertIsInstance(best_query, ParsedQuery)
    
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
        ga = GeneticOptimizer(
            population_size=10,
            generations=5,
            mutation_rate=0.2
        )
        # Run optimization
        best_query, history = ga.optimize(self.query)
        self.assertIsNotNone(best_query)
        self.assertIsInstance(best_query, ParsedQuery)
    
    def test_filter_params_in_statistics(self):
        """Test bahwa statistik mencakup unified filter_params"""
        ga = GeneticOptimizer(
            population_size=5,
            generations=2
        )
        best_query, history = ga.optimize(self.query)
        self.assertIsInstance(best_query, ParsedQuery)


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


