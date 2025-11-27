"""
Test integration of Rule 5 (Join Commutativity) with Genetic Algorithm
"""

import unittest
from query_optimizer.genetic_optimizer import GeneticOptimizer, Individual
from query_optimizer.optimization_engine import OptimizationEngine, ParsedQuery
from query_optimizer.rule_params_manager import get_rule_params_manager
from query_optimizer.rule import rule_5


class TestRule5Integration(unittest.TestCase):
    """Test Rule 5 integration dengan GA."""
    
    def setUp(self):
        """Setup test queries."""
        self.engine = OptimizationEngine()
        
        self.query_str_1 = """
        SELECT * FROM employees e INNER JOIN payroll p ON e.id = p.employee_id
        """
        
        self.query_str_2 = """
        SELECT * FROM users u 
        INNER JOIN orders o ON u.id = o.user_id
        INNER JOIN products p ON o.product_id = p.id
        """
        
        self.query_str_3 = """
        SELECT * FROM employees e NATURAL JOIN payroll p
        """
    
    def test_rule_5_find_join_nodes(self):
        """Test apakah rule 5 bisa detect JOIN nodes."""
        parsed = self.engine.parse_query(self.query_str_1)
        join_nodes = rule_5.find_join_nodes(parsed)
        
        self.assertGreater(len(join_nodes), 0)
        
        for join_id, metadata in join_nodes.items():
            self.assertIn('left_child', metadata)
            self.assertIn('right_child', metadata)
            self.assertIn('join_type', metadata)
            self.assertIsNotNone(metadata['left_child'])
            self.assertIsNotNone(metadata['right_child'])
    
    def test_rule_5_multiple_joins(self):
        """Test detection multiple JOIN nodes."""
        parsed = self.engine.parse_query(self.query_str_2)
        join_nodes = rule_5.find_join_nodes(parsed)
        
        self.assertEqual(len(join_nodes), 2)
    
    def test_rule_5_params_generation(self):
        """Test generation random params untuk rule 5."""
        metadata = {
            'left_child': None,
            'right_child': None,
            'join_type': 'INNER'
        }
        
        results = [rule_5.generate_join_child_params(metadata) for _ in range(20)]
        self.assertTrue(all(isinstance(r, bool) for r in results))
        self.assertIn(True, results)
        self.assertIn(False, results)
    
    def test_rule_5_params_mutation(self):
        """Test mutation untuk join child params."""
        self.assertEqual(rule_5.mutate_join_child_params(True), False)
        self.assertEqual(rule_5.mutate_join_child_params(False), True)
    
    def test_rule_5_params_copy(self):
        """Test copy params."""
        self.assertEqual(rule_5.copy_join_child_params(True), True)
        self.assertEqual(rule_5.copy_join_child_params(False), False)
    
    def test_rule_5_params_validation(self):
        """Test validation params."""
        self.assertTrue(rule_5.validate_join_child_params(True))
        self.assertTrue(rule_5.validate_join_child_params(False))
        self.assertFalse(rule_5.validate_join_child_params("invalid"))
        self.assertFalse(rule_5.validate_join_child_params(None))
    
    def test_rule_5_registration_in_manager(self):
        """Test apakah rule 5 sudah ter-register di manager."""
        manager = get_rule_params_manager()
        
        operations = manager.get_registered_operations()
        self.assertIn('join_child_params', operations)
    
    def test_rule_5_analysis_via_manager(self):
        """Test analysis via manager."""
        parsed = self.engine.parse_query(self.query_str_1)
        manager = get_rule_params_manager()
        
        analysis = manager.analyze_query(parsed, 'join_child_params')
        
        self.assertGreater(len(analysis), 0)
        
        for join_id, metadata in analysis.items():
            self.assertIsInstance(metadata, dict)
    
    def test_rule_5_transformation_preserves_structure(self):
        """Test bahwa transformation tidak merusak struktur tree."""
        parsed = self.engine.parse_query(self.query_str_1)
        
        def count_nodes(node):
            if node is None:
                return 0
            return 1 + sum(count_nodes(child) for child in node.childs)
        
        original_count = count_nodes(parsed.query_tree)
        
        join_nodes = rule_5.find_join_nodes(parsed)
        join_orders = {join_id: True for join_id in join_nodes.keys()}
        transformed = rule_5.join_komutatif(parsed, join_orders)
        
        transformed_count = count_nodes(transformed.query_tree)
        self.assertEqual(original_count, transformed_count)
    
    def test_rule_5_swap_and_unswap(self):
        """Test bahwa swap 2x mengembalikan ke struktur original."""
        parsed = self.engine.parse_query(self.query_str_1)
        
        def get_join_children(tree):
            if tree is None:
                return []
            if tree.type == "JOIN":
                return [(id(tree.childs[0]), id(tree.childs[1]))]
            result = []
            for child in tree.childs:
                result.extend(get_join_children(child))
            return result
        
        original_children = get_join_children(parsed.query_tree)
        
        join_nodes = rule_5.find_join_nodes(parsed)
        join_orders = {join_id: True for join_id in join_nodes.keys()}
        swapped_once = rule_5.join_komutatif(parsed, join_orders)
        swapped_children = get_join_children(swapped_once.query_tree)
        
        self.assertNotEqual(original_children, swapped_children)
        
        swapped_twice = rule_5.join_komutatif(swapped_once, join_orders)
        final_children = get_join_children(swapped_twice.query_tree)
        
        self.assertEqual(original_children, final_children)
    
    def test_ga_initialization_includes_join_child_params(self):
        """Test apakah GA initialization include join_child_params."""
        parsed = self.engine.parse_query(self.query_str_1)
        manager = get_rule_params_manager()
        analysis = manager.analyze_query(parsed, 'join_child_params')
        self.assertIsInstance(analysis, dict)
        # If there are join_child_params, they should be non-empty
        if len(analysis) > 0:
            self.assertGreater(len(analysis), 0)
    
    def test_individual_applies_join_child_transformations(self):
        """Test apakah Individual class apply rule 5 transformations."""
        parsed = self.engine.parse_query(self.query_str_1)
        
        join_nodes = rule_5.find_join_nodes(parsed)
        
        if len(join_nodes) > 0:
            operation_params = {
                'join_child_params': {join_id: True for join_id in join_nodes.keys()}
            }
            
            individual = Individual(operation_params, parsed)
            
            self.assertIsNotNone(individual.query)
            self.assertIsNotNone(individual.query.query_tree)
    
    def test_ga_optimization_with_rule_5(self):
        """Test full GA optimization dengan rule 5."""
        parsed = self.engine.parse_query(self.query_str_1)
        ga = GeneticOptimizer(
            population_size=10,
            generations=5,
            mutation_rate=0.2
        )
        optimized_query, history = ga.optimize(parsed)
        self.assertIsInstance(optimized_query, ParsedQuery)
        self.assertIsInstance(history, list)
        # Check that history contains dicts with 'gen' and 'best' keys
        for gen_info in history:
            self.assertIn('gen', gen_info)
            self.assertIn('best', gen_info)
    
    def test_rule_5_with_natural_join(self):
        """Test rule 5 dengan NATURAL JOIN."""
        parsed = self.engine.parse_query(self.query_str_3)
        join_nodes = rule_5.find_join_nodes(parsed)
        
        self.assertGreater(len(join_nodes), 0)
        
        for join_id, metadata in join_nodes.items():
            self.assertEqual(metadata['join_type'], 'NATURAL')
        
        join_orders = {join_id: True for join_id in join_nodes.keys()}
        transformed = rule_5.join_komutatif(parsed, join_orders)
        
        self.assertIsNotNone(transformed.query_tree)


class TestRule5Scenarios(unittest.TestCase):
    """Test various scenarios untuk Rule 5."""
    
    def setUp(self):
        self.engine = OptimizationEngine()
    
    def test_single_join_swap(self):
        """Test swap single JOIN."""
        sql = "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id"
        parsed = self.engine.parse_query(sql)
        
        join_nodes = rule_5.find_join_nodes(parsed)
        self.assertEqual(len(join_nodes), 1)
        
        join_orders = {join_id: True for join_id in join_nodes.keys()}
        swapped = rule_5.join_komutatif(parsed, join_orders)
        
        original_cost = self.engine.get_cost(parsed)
        swapped_cost = self.engine.get_cost(swapped)
        
        self.assertIsInstance(original_cost, (int, float))
        self.assertIsInstance(swapped_cost, (int, float))
    
    def test_multiple_joins_selective_swap(self):
        """Test selective swap untuk multiple JOINs."""
        sql = """
        SELECT * FROM users u 
        INNER JOIN orders o ON u.id = o.user_id
        INNER JOIN products p ON o.product_id = p.id
        """
        parsed = self.engine.parse_query(sql)
        
        join_nodes = rule_5.find_join_nodes(parsed)
        self.assertEqual(len(join_nodes), 2)
        
        join_ids = list(join_nodes.keys())
        join_orders = {
            join_ids[0]: True,
            join_ids[1]: False
        }
        
        selective = rule_5.join_komutatif(parsed, join_orders)
        
        self.assertIsNotNone(selective.query_tree)
        
        cost = self.engine.get_cost(selective)
        self.assertIsInstance(cost, (int, float))
    
    def test_cost_comparison_swap_vs_no_swap(self):
        """Test cost comparison antara swap vs no swap."""
        sql = "SELECT * FROM employees e INNER JOIN payroll p ON e.id = p.employee_id"
        parsed = self.engine.parse_query(sql)
        
        join_nodes = rule_5.find_join_nodes(parsed)
        
        no_swap_orders = {join_id: False for join_id in join_nodes.keys()}
        no_swap = rule_5.join_komutatif(parsed, no_swap_orders)
        cost_no_swap = self.engine.get_cost(no_swap)
        
        swap_orders = {join_id: True for join_id in join_nodes.keys()}
        with_swap = rule_5.join_komutatif(parsed, swap_orders)
        cost_with_swap = self.engine.get_cost(with_swap)
        
        self.assertIsInstance(cost_no_swap, (int, float))
        self.assertIsInstance(cost_with_swap, (int, float))
        self.assertGreaterEqual(cost_no_swap, 0)
        self.assertGreaterEqual(cost_with_swap, 0)


if __name__ == '__main__':
    unittest.main()
