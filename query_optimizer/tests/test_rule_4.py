"""
Test integration of Rule 4 (Push Selection into Joins) with Genetic Algorithm
Menggunakan Mock Metadata untuk isolasi pengujian.
"""

import unittest
from unittest.mock import patch, MagicMock
from query_optimizer.genetic_optimizer import GeneticOptimizer, Individual
from query_optimizer.optimization_engine import OptimizationEngine, ParsedQuery
from query_optimizer.rule_params_manager import get_rule_params_manager
from query_optimizer.rule import rule_4
from query_optimizer.query_tree import QueryTree

# --- MOCK METADATA ---
MOCK_METADATA = {
    "tables": ["employees", "payroll", "accounts", "users", "orders", "products"],
    "columns": {
        "employees": ["id", "name", "salary", "dept_id", "employee_id"], # employee_id for join mapping
        "payroll": ["id", "employee_id", "amount", "date", "dept_id"],
        "accounts": ["id", "payroll_id", "bank_name"],
        "users": ["id", "name"],
        "orders": ["id", "user_id", "amount", "product_id"],
        "products": ["id", "name", "price"]
    }
}

@patch('query_optimizer.query_check.get_metadata', return_value=MOCK_METADATA)
class TestRule4Integration(unittest.TestCase):
    """Test Rule 4 integration dengan GA."""
    
    def setUp(self):
        """Setup test queries."""
        self.engine = OptimizationEngine()
        
        # Query dengan FILTER di atas JOIN (bisa di-merge)
        # Menggunakan tabel yang didefinisikan di MOCK_METADATA
        self.query_str_1 = """
        SELECT * FROM employees e NATURAL JOIN payroll p
        WHERE e.id = p.employee_id
        """
        
        # Query dengan multiple FILTERs di atas JOIN
        self.query_str_2 = """
        SELECT * FROM employees e NATURAL JOIN payroll p
        WHERE e.id = p.employee_id AND e.salary > 50000
        """
    
    def test_rule_4_pattern_detection(self, mock_meta):
        """Test apakah rule 4 bisa detect FILTER-JOIN patterns."""
        parsed = self.engine.parse_query(self.query_str_1)
        patterns = rule_4.find_patterns(parsed)
        
        # Harus ada minimal 1 pattern
        self.assertGreater(len(patterns), 0)
        
        # Setiap pattern harus punya filter_conditions dan existing_conditions
        for join_id, metadata in patterns.items():
            self.assertIn('filter_conditions', metadata)
            self.assertIn('existing_conditions', metadata)
            self.assertIsInstance(metadata['filter_conditions'], list)
            self.assertIsInstance(metadata['existing_conditions'], list)
    
    def test_rule_4_params_generation(self, mock_meta):
        """Test generation random params untuk rule 4."""
        metadata = {
            'filter_conditions': [10, 15, 20],
            'existing_conditions': []
        }
        
        # Generate multiple times untuk test randomness
        results = [rule_4.generate_params(metadata) for _ in range(20)]
        
        # Semua hasil harus list of int
        self.assertTrue(all(isinstance(r, list) for r in results))
        self.assertTrue(all(all(isinstance(x, int) for x in r) for r in results))
        
        # Should have variation in results (different selections)
        unique_results = [tuple(sorted(r)) for r in results]
        self.assertGreater(len(set(unique_results)), 1, "Should have variation in selections")
    
    def test_rule_4_params_mutation(self, mock_meta):
        """Test mutation untuk list of condition IDs."""
        original = [10, 15, 20]
        mutated = rule_4.mutate_params(original)
        
        # Mutation should return list
        self.assertIsInstance(mutated, list)
        self.assertTrue(all(isinstance(x, int) for x in mutated))
    
    def test_rule_4_params_copy(self, mock_meta):
        """Test copy params."""
        original = [10, 15, 20]
        copied = rule_4.copy_params(original)
        
        self.assertEqual(original, copied)
        self.assertIsInstance(copied, list)
        # Ensure it's a copy, not same reference
        self.assertIsNot(original, copied)
    
    def test_rule_4_registration_in_manager(self, mock_meta):
        """Test apakah rule 4 sudah ter-register di manager."""
        manager = get_rule_params_manager()
        
        # join_params harus ada di registered operations
        operations = manager.get_registered_operations()
        self.assertIn('join_params', operations)
    
    def test_rule_4_analysis_via_manager(self, mock_meta):
        """Test analysis via manager."""
        parsed = self.engine.parse_query(self.query_str_1)
        manager = get_rule_params_manager()
        
        # Analyze via manager
        analysis = manager.analyze_query(parsed, 'join_params')
        
        # Harus return dict dengan structure yang benar
        self.assertIsInstance(analysis, dict)
        for join_id, metadata in analysis.items():
            self.assertIn('filter_conditions', metadata)
            self.assertIn('existing_conditions', metadata)
            self.assertIsInstance(metadata['filter_conditions'], list)
            self.assertIsInstance(metadata['existing_conditions'], list)
    
    def test_rule_4_param_generation_via_manager(self, mock_meta):
        """Test param generation via manager."""
        manager = get_rule_params_manager()
        metadata = {
            'filter_conditions': [10, 15, 20],
            'existing_conditions': []
        }
        
        # Generate via manager
        params = manager.generate_random_params('join_params', metadata)
        
        self.assertIsInstance(params, list)
        self.assertTrue(all(isinstance(x, int) for x in params))
    
    def test_individual_applies_join_transformations(self, mock_meta):
        """Test apakah Individual class apply rule 4 transformations."""
        parsed = self.engine.parse_query(self.query_str_1)
        
        # Create operation params dengan join_params
        # Note: join_params now maps join_id -> [condition_ids]
        # Kita perlu pattern detection dulu untuk tau ID join-nya
        patterns = rule_4.find_patterns(parsed)
        if not patterns:
            self.skipTest("No patterns found for join params test")
            
        join_id = list(patterns.keys())[0]
        cond_id = patterns[join_id]['filter_conditions'][0]
        
        operation_params = {
            'filter_params': {},
            'join_params': {join_id: [cond_id]}  # Merge condition into JOIN
        }
        
        # Create individual
        individual = Individual(operation_params, parsed)
        
        # Query harus ter-transform
        transformed = individual.query
        self.assertIsNotNone(transformed)
        self.assertIsInstance(transformed, ParsedQuery)
    
    def test_ga_optimization_with_rule_4(self, mock_meta):
        """Test full GA optimization dengan rule 4."""
        parsed = self.engine.parse_query(self.query_str_1)
        ga = GeneticOptimizer(
            population_size=10,
            generations=2,
            mutation_rate=0.2
        )
        best_query, history = ga.optimize(parsed)
        self.assertIsNotNone(best_query)
        self.assertIsInstance(best_query, ParsedQuery)
    
    def test_rule_4_transformation_order(self, mock_meta):
        """Test apakah rule 4 di-apply sebelum filter operations."""
        parsed = self.engine.parse_query(self.query_str_2)
        patterns = rule_4.find_patterns(parsed)
        if not patterns:
             self.skipTest("No patterns found")
             
        join_id = list(patterns.keys())[0]
        # Ambil satu kondisi untuk di-merge
        cond_ids = patterns[join_id]['filter_conditions']
        
        # Create params dengan both join and filter
        operation_params = {
            'filter_params': {
                # Some filter params logic here usually
            },
            'join_params': {
                join_id: cond_ids  # Merge all found conditions
            }
        }
        
        # Create individual
        individual = Individual(operation_params, parsed)
        
        # Should not raise error (join applied before filter)
        try:
            transformed = individual.query
            self.assertIsNotNone(transformed)
        except Exception as e:
            self.fail(f"Transformation failed: {e}")


@patch('query_optimizer.query_check.get_metadata', return_value=MOCK_METADATA)
class TestRule4EdgeCases(unittest.TestCase):
    """Test edge cases untuk rule 4 integration."""
    
    def setUp(self):
        self.engine = OptimizationEngine()
    
    def test_empty_join_params(self, mock_meta):
        """Test dengan join_params kosong."""
        query_str = "SELECT * FROM employees"
        parsed = self.engine.parse_query(query_str)
        
        operation_params = {
            'filter_params': {},
            'join_params': {}  # Empty
        }
        
        individual = Individual(operation_params, parsed)
        
        # Should not crash
        self.assertIsNotNone(individual.query)
    
    def test_no_join_patterns(self, mock_meta):
        """Test query tanpa FILTER-JOIN patterns."""
        query_str = "SELECT * FROM employees WHERE salary > 50000"
        parsed = self.engine.parse_query(query_str)
        
        patterns = rule_4.find_patterns(parsed)
        
        # Might be empty or have patterns depending on query structure
        self.assertIsInstance(patterns, dict)
    
    def test_multiple_join_merges(self, mock_meta):
        """Test multiple FILTER-JOIN merges dalam satu query."""
        query_str = """
        SELECT * FROM employees e 
        NATURAL JOIN payroll p 
        NATURAL JOIN accounts a
        WHERE e.id = p.employee_id AND p.id = a.payroll_id
        """
        parsed = self.engine.parse_query(query_str)
        
        patterns = rule_4.find_patterns(parsed)
        
        if patterns:
            # Apply merge untuk semua patterns - merge all conditions
            join_params = {}
            for join_id, metadata in patterns.items():
                join_params[join_id] = metadata['filter_conditions']  # Merge all
            merged, _, _ = rule_4.apply_merge(parsed, join_params, {})
            
            self.assertIsNotNone(merged)


@patch('query_optimizer.query_check.get_metadata', return_value=MOCK_METADATA)
class TestRule4Scenarios(unittest.TestCase):
    """Test additional scenarios from demo (FILTER over INNER JOIN and undo)."""
    
    def setUp(self):
        self.engine = OptimizationEngine()
    
    def test_filter_over_cross_join_merge(self, mock_meta):
        """Test merging FILTER over CROSS JOIN transforms to INNER JOIN."""
        
        # Build CROSS JOIN manually
        rel1 = QueryTree("RELATION", "employees")
        rel2 = QueryTree("RELATION", "payroll")
        
        join = QueryTree("JOIN", "CROSS")
        join.add_child(rel1)
        join.add_child(rel2)
        
        # Add condition
        condition = QueryTree("COMPARISON", "=")
        condition.add_child(QueryTree("COLUMN_REF", ""))
        condition.add_child(QueryTree("COLUMN_REF", ""))
        
        filter_node = QueryTree("FILTER", "")
        filter_node.add_child(join)
        filter_node.add_child(condition)
        
        project = QueryTree("PROJECT", "*")
        project.add_child(filter_node)
        
        parsed = ParsedQuery(project, "SELECT * FROM employees, payroll WHERE ...")
        
        # Find patterns
        patterns = rule_4.find_patterns(parsed)
        self.assertGreater(len(patterns), 0, "Should find FILTER-JOIN pattern")
        
        # Test merge - merge all conditions
        join_params = {}
        for join_id, metadata in patterns.items():
            join_params[join_id] = metadata['filter_conditions']  # Merge all
        merged, _, _ = rule_4.apply_merge(parsed, join_params, {})
        
        # Find JOIN node in merged tree
        def find_join_type(node):
            if node is None:
                return None
            if node.type == "JOIN":
                return node.val
            for child in node.childs:
                result = find_join_type(child)
                if result:
                    return result
            return None
        
        join_type = find_join_type(merged.query_tree)
        self.assertEqual(join_type, "INNER", "JOIN should be converted to INNER")
    
    def test_filter_over_inner_join_merge(self, mock_meta):
        """Test merging additional FILTER over INNER JOIN creates AND condition."""
        
        # Build INNER JOIN with existing condition
        rel1 = QueryTree("RELATION", "employees")
        rel2 = QueryTree("RELATION", "payroll")
        
        join_cond = QueryTree("COMPARISON", "=")
        join_cond.add_child(QueryTree("COLUMN_REF", "dept_id"))
        join_cond.add_child(QueryTree("COLUMN_REF", "dept_id"))
        
        inner_join = QueryTree("JOIN", "INNER")
        inner_join.add_child(rel1)
        inner_join.add_child(rel2)
        inner_join.add_child(join_cond)
        
        # Add FILTER above INNER JOIN
        filter_cond = QueryTree("COMPARISON", ">")
        filter_cond.add_child(QueryTree("COLUMN_REF", "salary"))
        filter_cond.add_child(QueryTree("LITERAL_NUMBER", "5000"))
        
        filter_node = QueryTree("FILTER", "")
        filter_node.add_child(inner_join)
        filter_node.add_child(filter_cond)
        
        project = QueryTree("PROJECT", "*")
        project.add_child(filter_node)
        
        parsed = ParsedQuery(project, "SELECT * FROM employees INNER JOIN payroll ON ... WHERE ...")
        
        # Find patterns
        patterns = rule_4.find_patterns(parsed)
        self.assertGreater(len(patterns), 0, "Should find FILTER over INNER JOIN pattern")
        
        # Merge filter into JOIN - merge all conditions
        join_params = {}
        for join_id, metadata in patterns.items():
            join_params[join_id] = metadata['filter_conditions']  # Merge all
        merged, _, _ = rule_4.apply_merge(parsed, join_params, {})
        
        # Check that JOIN has AND operator with both conditions
        def find_and_in_join(node):
            if node is None:
                return False
            if node.type == "JOIN" and len(node.childs) >= 3:
                condition = node.childs[2]
                if condition.type == "OPERATOR" and condition.val == "AND":
                    return True
            for child in node.childs:
                if find_and_in_join(child):
                    return True
            return False
        
        has_and = find_and_in_join(merged.query_tree)
        self.assertTrue(has_and, "Merged JOIN should have AND operator with multiple conditions")


@patch('query_optimizer.query_check.get_metadata', return_value=MOCK_METADATA)
class TestRule4WithCommaSeparatedTables(unittest.TestCase):
    """Test Rule 4 dengan comma-separated tables (implicit CROSS JOIN)."""
    
    def setUp(self):
        self.engine = OptimizationEngine()
    
    def test_comma_separated_tables_creates_cross_join(self, mock_meta):
        """Test bahwa FROM table1, table2 menciptakan CROSS JOIN."""
        query_str = "SELECT * FROM employees, payroll"
        parsed = self.engine.parse_query(query_str)
        
        # Find JOIN node in tree
        def find_join_type(node):
            if node is None:
                return None
            if node.type == "JOIN":
                return node.val
            for child in node.childs:
                result = find_join_type(child)
                if result:
                    return result
            return None
        
        join_type = find_join_type(parsed.query_tree)
        self.assertEqual(join_type, "CROSS", "Comma-separated tables should create CROSS JOIN")
    
    def test_comma_separated_with_where_filter(self, mock_meta):
        """Test comma-separated tables dengan WHERE clause (FILTER over CROSS JOIN)."""
        query_str = """
        SELECT * FROM employees e, payroll p
        WHERE e.id = p.employee_id
        """
        parsed = self.engine.parse_query(query_str)
        
        # Should have FILTER over CROSS JOIN
        patterns = rule_4.find_patterns(parsed)
        self.assertGreater(len(patterns), 0, "Should detect FILTER-CROSS JOIN pattern")
        
    def test_merge_comma_separated_tables(self, mock_meta):
        """Test merging FILTER over comma-separated tables (CROSS -> INNER)."""
        query_str = """
        SELECT * FROM employees e, payroll p
        WHERE e.id = p.employee_id
        """
        parsed = self.engine.parse_query(query_str)
        
        # Find patterns and apply merge
        patterns = rule_4.find_patterns(parsed)
        self.assertGreater(len(patterns), 0)
        
        # Merge all conditions
        join_params = {}
        for join_id, metadata in patterns.items():
            join_params[join_id] = metadata['filter_conditions']  # Merge all
        merged, _, _ = rule_4.apply_merge(parsed, join_params, {})
        
        # Find JOIN type in merged tree
        def find_join_type(node):
            if node is None:
                return None
            if node.type == "JOIN":
                return node.val
            for child in node.childs:
                result = find_join_type(child)
                if result:
                    return result
            return None
        
        join_type = find_join_type(merged.query_tree)
        self.assertEqual(join_type, "INNER", "Merged CROSS JOIN should become INNER JOIN")
    
    def test_comma_separated_with_aliases(self, mock_meta):
        """Test comma-separated tables dengan table aliases."""
        query_str = """
        SELECT e.name, p.amount 
        FROM employees e, payroll p
        WHERE e.id = p.employee_id
        """
        parsed = self.engine.parse_query(query_str)
        
        # Should parse correctly with aliases
        patterns = rule_4.find_patterns(parsed)
        self.assertIsInstance(patterns, dict)
        
        # If patterns found, test merge
        if patterns:
            join_params = {}
            for join_id, metadata in patterns.items():
                join_params[join_id] = metadata['filter_conditions']  # Merge all
            merged, _, _ = rule_4.apply_merge(parsed, join_params, {})
            self.assertIsNotNone(merged)


if __name__ == '__main__':
    unittest.main()