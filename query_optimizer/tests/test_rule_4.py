"""
Test integration of Rule 4 (Push Selection into Joins) with Genetic Algorithm
"""

import unittest
from query_optimizer.genetic_optimizer import GeneticOptimizer, Individual
from query_optimizer.optimization_engine import OptimizationEngine, ParsedQuery
from query_optimizer.rule_params_manager import get_rule_params_manager
from query_optimizer.rule import rule_4


class TestRule4Integration(unittest.TestCase):
    """Test Rule 4 integration dengan GA."""
    
    def setUp(self):
        """Setup test queries."""
        self.engine = OptimizationEngine()
        
        # Query dengan FILTER di atas JOIN (bisa di-merge)
        # Note: Parser requires explicit JOIN syntax, not comma-separated tables
        # Using available tables: employees, payroll
        self.query_str_1 = """
        SELECT * FROM employees e NATURAL JOIN payroll p
        WHERE e.id = p.employee_id
        """
        
        # Query dengan multiple FILTERs di atas JOIN
        self.query_str_2 = """
        SELECT * FROM employees e NATURAL JOIN payroll p
        WHERE e.id = p.employee_id AND e.salary > 50000
        """
    
    def test_rule_4_pattern_detection(self):
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
    
    def test_rule_4_params_generation(self):
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
    
    def test_rule_4_params_mutation(self):
        """Test mutation untuk list of condition IDs."""
        original = [10, 15, 20]
        mutated = rule_4.mutate_params(original)
        
        # Mutation should return list
        self.assertIsInstance(mutated, list)
        self.assertTrue(all(isinstance(x, int) for x in mutated))
        
        # Mutation should potentially change the list (add/remove items)
        # Note: Mutation might be the same by chance, so we test structure only
        self.assertIsInstance(mutated, list)
    
    def test_rule_4_params_copy(self):
        """Test copy params."""
        original = [10, 15, 20]
        copied = rule_4.copy_params(original)
        
        self.assertEqual(original, copied)
        self.assertIsInstance(copied, list)
        # Ensure it's a copy, not same reference
        self.assertIsNot(original, copied)
    
    def test_rule_4_registration_in_manager(self):
        """Test apakah rule 4 sudah ter-register di manager."""
        manager = get_rule_params_manager()
        
        # join_params harus ada di registered operations
        operations = manager.get_registered_operations()
        self.assertIn('join_params', operations)
    
    def test_rule_4_analysis_via_manager(self):
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
    
    def test_rule_4_param_generation_via_manager(self):
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
    
    def test_individual_applies_join_transformations(self):
        """Test apakah Individual class apply rule 4 transformations."""
        parsed = self.engine.parse_query(self.query_str_1)
        
        # Create operation params dengan join_params
        # Note: join_params now maps join_id -> [condition_ids]
        operation_params = {
            'filter_params': {},
            'join_params': {1: [10, 15]}  # Merge conditions 10 and 15 into JOIN 1
        }
        
        # Create individual
        individual = Individual(operation_params, parsed)
        
        # Query harus ter-transform
        transformed = individual.query
        self.assertIsNotNone(transformed)
        self.assertIsInstance(transformed, ParsedQuery)
    
    def test_ga_initialization_includes_join_params(self):
        """Test apakah GA initialization include join_params."""
        parsed = self.engine.parse_query(self.query_str_1)
        
        ga = GeneticOptimizer(
            population_size=10,
            generations=1,  # Only 1 generation untuk test
        )
        
        # Analyze query
        manager = get_rule_params_manager()
        analysis = manager.analyze_query(parsed, 'join_params')
        
        if analysis:  # Jika ada join patterns
            # Initialize population
            population = ga._initialize_population(parsed, {
                'filter_params': {},
                'join_params': analysis
            })
            
            # Check apakah population punya join_params
            for individual in population:
                self.assertIn('join_params', individual.operation_params)
                # join_params values should be list[int]
                for params in individual.operation_params['join_params'].values():
                    self.assertIsInstance(params, list)
    
    def test_ga_optimization_with_rule_4(self):
        """Test full GA optimization dengan rule 4."""
        parsed = self.engine.parse_query(self.query_str_1)
        
        ga = GeneticOptimizer(
            population_size=20,
            generations=5,  # Few generations untuk test cepat
            mutation_rate=0.2
        )
        
        # Run optimization
        optimized = ga.optimize(parsed)
        
        # Check results
        self.assertIsNotNone(optimized)
        self.assertIsInstance(optimized, ParsedQuery)
        
        # Check statistics
        stats = ga.get_statistics()
        self.assertIn('best_params', stats)
        
        # join_params harus ada
        if 'join_params' in stats['best_params']:
            join_params = stats['best_params']['join_params']
            self.assertIsInstance(join_params, dict)
            
            # All params harus list[int]
            for join_id, condition_ids in join_params.items():
                self.assertIsInstance(condition_ids, list)
                self.assertTrue(all(isinstance(x, int) for x in condition_ids))
    
    def test_rule_4_transformation_order(self):
        """Test apakah rule 4 di-apply sebelum filter operations."""
        parsed = self.engine.parse_query(self.query_str_2)
        
        # Create params dengan both join and filter
        operation_params = {
            'filter_params': {
                # Some filter params (empty untuk simplicity)
            },
            'join_params': {
                1: [10, 15]  # Merge conditions 10 and 15 into JOIN 1
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
    
    def test_rule_4_undo_functionality(self):
        """Test undo merge functionality dari rule 4."""
        parsed = self.engine.parse_query(self.query_str_1)
        
        # Apply merge - merge all conditions
        patterns = rule_4.find_patterns(parsed)
        join_params = {}
        for join_id, metadata in patterns.items():
            # Merge all filter conditions
            join_params[join_id] = metadata['filter_conditions']
        
        merged = rule_4.apply_merge(parsed, join_params)
        
        # Undo merge
        unmerged = rule_4.undo_merge(merged)
        
        # Check structure restored
        self.assertIsNotNone(unmerged)
        self.assertIsInstance(unmerged, ParsedQuery)


class TestRule4EdgeCases(unittest.TestCase):
    """Test edge cases untuk rule 4 integration."""
    
    def setUp(self):
        self.engine = OptimizationEngine()
    
    def test_empty_join_params(self):
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
    
    def test_no_join_patterns(self):
        """Test query tanpa FILTER-JOIN patterns."""
        query_str = "SELECT * FROM employees WHERE salary > 50000"
        parsed = self.engine.parse_query(query_str)
        
        patterns = rule_4.find_patterns(parsed)
        
        # Might be empty or have patterns depending on query structure
        self.assertIsInstance(patterns, dict)
    
    def test_multiple_join_merges(self):
        """Test multiple FILTER-JOIN merges dalam satu query."""
        query_str = """
        SELECT * FROM employees e 
        NATURAL JOIN payroll p 
        NATURAL JOIN accounts a
        WHERE e.id = p.employee_id AND p.id = a.id
        """
        parsed = self.engine.parse_query(query_str)
        
        patterns = rule_4.find_patterns(parsed)
        
        if patterns:
            # Apply merge untuk semua patterns - merge all conditions
            join_params = {}
            for join_id, metadata in patterns.items():
                join_params[join_id] = metadata['filter_conditions']  # Merge all
            merged = rule_4.apply_merge(parsed, join_params)
            
            self.assertIsNotNone(merged)


class TestRule4Scenarios(unittest.TestCase):
    """Test additional scenarios from demo (FILTER over INNER JOIN and undo)."""
    
    def setUp(self):
        self.engine = OptimizationEngine()
    
    def test_filter_over_cross_join_merge(self):
        """Test merging FILTER over CROSS JOIN transforms to INNER JOIN."""
        from query_optimizer.query_tree import QueryTree
        from query_optimizer.optimization_engine import ParsedQuery
        
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
        merged = rule_4.apply_merge(parsed, join_params)
        
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
        
        # Test keep separate - merge no conditions
        join_params_separate = {}
        for join_id, metadata in patterns.items():
            join_params_separate[join_id] = []  # Merge nothing (keep separate)
        separate = rule_4.apply_merge(parsed, join_params_separate)
        
        join_type_separate = find_join_type(separate.query_tree)
        self.assertEqual(join_type_separate, "CROSS", "JOIN should remain CROSS")
    
    def test_filter_over_inner_join_merge(self):
        """Test merging additional FILTER over INNER JOIN creates AND condition."""
        from query_optimizer.query_tree import QueryTree
        from query_optimizer.optimization_engine import ParsedQuery
        
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
        merged = rule_4.apply_merge(parsed, join_params)
        
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
    
    def test_undo_merge_converts_inner_to_cross(self):
        """Test undo_merge converts INNER JOIN back to CROSS JOIN with FILTER."""
        from query_optimizer.query_tree import QueryTree
        from query_optimizer.optimization_engine import ParsedQuery
        
        # Start with INNER JOIN with condition
        rel1 = QueryTree("RELATION", "employees")
        rel2 = QueryTree("RELATION", "payroll")
        
        condition = QueryTree("COMPARISON", "=")
        condition.add_child(QueryTree("COLUMN_REF", "id"))
        condition.add_child(QueryTree("COLUMN_REF", "emp_id"))
        
        inner_join = QueryTree("JOIN", "INNER")
        inner_join.add_child(rel1)
        inner_join.add_child(rel2)
        inner_join.add_child(condition)
        
        project = QueryTree("PROJECT", "*")
        project.add_child(inner_join)
        
        parsed = ParsedQuery(project, "SELECT * FROM employees INNER JOIN payroll ON ...")
        
        # Apply undo_merge
        undone = rule_4.undo_merge(parsed)
        
        # Check structure: should have FILTER above JOIN
        root = undone.query_tree
        self.assertEqual(root.type, "PROJECT", "Root should be PROJECT")
        
        # Find FILTER and JOIN
        def check_structure(node, parent_type=None):
            if node is None:
                return False, None
            
            if node.type == "FILTER" and len(node.childs) >= 2:
                join_child = node.childs[0]
                if join_child.type == "JOIN":
                    return True, join_child.val
            
            for child in node.childs:
                result, join_type = check_structure(child, node.type)
                if result:
                    return result, join_type
            
            return False, None
        
        has_filter_join, join_type = check_structure(root)
        self.assertTrue(has_filter_join, "Should have FILTER -> JOIN structure")
        self.assertEqual(join_type, "CROSS", "JOIN should be converted to CROSS")
    
    def test_bidirectional_transformation(self):
        """Test complete bidirectional transformation: CROSS -> INNER -> CROSS."""
        from query_optimizer.query_tree import QueryTree
        from query_optimizer.optimization_engine import ParsedQuery
        
        # Build initial FILTER over CROSS JOIN
        rel1 = QueryTree("RELATION", "employees")
        rel2 = QueryTree("RELATION", "payroll")
        
        join = QueryTree("JOIN", "CROSS")
        join.add_child(rel1)
        join.add_child(rel2)
        
        condition = QueryTree("COMPARISON", "=")
        condition.add_child(QueryTree("COLUMN_REF", "id"))
        condition.add_child(QueryTree("COLUMN_REF", "emp_id"))
        
        filter_node = QueryTree("FILTER", "")
        filter_node.add_child(join)
        filter_node.add_child(condition)
        
        project = QueryTree("PROJECT", "*")
        project.add_child(filter_node)
        
        original = ParsedQuery(project, "SELECT * FROM employees, payroll WHERE ...")
        
        # Step 1: Merge (CROSS -> INNER) - merge all conditions
        patterns = rule_4.find_patterns(original)
        join_params = {}
        for join_id, metadata in patterns.items():
            join_params[join_id] = metadata['filter_conditions']  # Merge all
        merged = rule_4.apply_merge(original, join_params)
        
        # Step 2: Undo merge (INNER -> CROSS)
        restored = rule_4.undo_merge(merged)
        
        # Verify: should have similar structure to original
        # Both should have FILTER with CROSS JOIN
        def get_join_type(query):
            def find_join(node):
                if node is None:
                    return None
                if node.type == "JOIN":
                    return node.val
                for child in node.childs:
                    result = find_join(child)
                    if result:
                        return result
                return None
            return find_join(query.query_tree)
        
        original_join = get_join_type(original)
        restored_join = get_join_type(restored)
        
        self.assertEqual(original_join, "CROSS")
        self.assertEqual(restored_join, "CROSS")
        self.assertEqual(original_join, restored_join, 
                        "Bidirectional transformation should restore CROSS JOIN")
    
    def test_cost_comparison_merge_vs_separate(self):
        """Test that merge typically has lower cost than keeping separate."""
        from query_optimizer.query_tree import QueryTree
        from query_optimizer.optimization_engine import ParsedQuery
        
        # Build FILTER over CROSS JOIN
        rel1 = QueryTree("RELATION", "employees")
        rel2 = QueryTree("RELATION", "payroll")
        
        join = QueryTree("JOIN", "CROSS")
        join.add_child(rel1)
        join.add_child(rel2)
        
        condition = QueryTree("COMPARISON", "=")
        condition.add_child(QueryTree("COLUMN_REF", "id"))
        condition.add_child(QueryTree("COLUMN_REF", "emp_id"))
        
        filter_node = QueryTree("FILTER", "")
        filter_node.add_child(join)
        filter_node.add_child(condition)
        
        project = QueryTree("PROJECT", "*")
        project.add_child(filter_node)
        
        parsed = ParsedQuery(project, "SELECT * FROM employees, payroll WHERE ...")
        
        # Find patterns
        patterns = rule_4.find_patterns(parsed)
        
        if patterns:
            # Keep separate - merge no conditions
            join_params_sep = {}
            for join_id, metadata in patterns.items():
                join_params_sep[join_id] = []  # Don't merge
            separate = rule_4.apply_merge(parsed, join_params_sep)
            cost_separate = self.engine.get_cost(separate)
            
            # Merge all conditions
            join_params_merge = {}
            for join_id, metadata in patterns.items():
                join_params_merge[join_id] = metadata['filter_conditions']  # Merge all
            merged = rule_4.apply_merge(parsed, join_params_merge)
            cost_merge = self.engine.get_cost(merged)
            
            # Merging should typically be better (lower cost)
            # Note: This might not always be true for all queries, 
            # but for simple CROSS -> INNER it should be
            self.assertLessEqual(cost_merge, cost_separate,
                               "Merge should have equal or lower cost than separate")


class TestRule4WithCommaSeparatedTables(unittest.TestCase):
    """Test Rule 4 dengan comma-separated tables (implicit CROSS JOIN)."""
    
    def setUp(self):
        self.engine = OptimizationEngine()
    
    def test_comma_separated_tables_creates_cross_join(self):
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
    
    def test_comma_separated_with_where_filter(self):
        """Test comma-separated tables dengan WHERE clause (FILTER over CROSS JOIN)."""
        query_str = """
        SELECT * FROM employees e, payroll p
        WHERE e.id = p.employee_id
        """
        parsed = self.engine.parse_query(query_str)
        
        # Should have FILTER over CROSS JOIN
        patterns = rule_4.find_patterns(parsed)
        self.assertGreater(len(patterns), 0, "Should detect FILTER-CROSS JOIN pattern")
        
        # Verify structure: FILTER -> JOIN(CROSS)
        def verify_filter_cross_structure(node):
            if node is None:
                return False
            
            if node.type == "FILTER" and len(node.childs) >= 1:
                join_child = node.childs[0]
                if join_child.type == "JOIN" and join_child.val == "CROSS":
                    return True
            
            for child in node.childs:
                if verify_filter_cross_structure(child):
                    return True
            
            return False
        
        has_pattern = verify_filter_cross_structure(parsed.query_tree)
        self.assertTrue(has_pattern, "Should have FILTER -> CROSS JOIN structure")
    
    def test_merge_comma_separated_tables(self):
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
        merged = rule_4.apply_merge(parsed, join_params)
        
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
    
    def test_comma_separated_three_tables(self):
        """Test comma-separated dengan 3 tables (nested CROSS JOIN)."""
        query_str = """
        SELECT * FROM employees e, payroll p, accounts a
        WHERE e.id = p.employee_id AND p.id = a.payroll_id
        """
        parsed = self.engine.parse_query(query_str)
        
        # Should have multiple CROSS JOINs (nested)
        def count_joins(node):
            if node is None:
                return 0
            count = 1 if node.type == "JOIN" else 0
            for child in node.childs:
                count += count_joins(child)
            return count
        
        join_count = count_joins(parsed.query_tree)
        self.assertGreaterEqual(join_count, 2, "Three tables should create at least 2 CROSS JOINs")
        
        # Should find FILTER-JOIN patterns
        patterns = rule_4.find_patterns(parsed)
        self.assertGreater(len(patterns), 0, "Should find patterns in nested CROSS JOINs")
    
    def test_ga_optimization_with_comma_separated_tables(self):
        """Test GA optimization dengan comma-separated tables."""
        query_str = """
        SELECT * FROM employees e, payroll p
        WHERE e.id = p.employee_id AND e.salary > 50000
        """
        parsed = self.engine.parse_query(query_str)
        
        ga = GeneticOptimizer(
            population_size=15,
            generations=3,
            mutation_rate=0.2
        )
        
        optimized = ga.optimize(parsed)
        
        # Should optimize successfully
        self.assertIsNotNone(optimized)
        self.assertIsInstance(optimized, ParsedQuery)
        
        # Check if join_params were used
        stats = ga.get_statistics()
        if 'best_params' in stats and 'join_params' in stats['best_params']:
            join_params = stats['best_params']['join_params']
            self.assertIsInstance(join_params, dict)
    
    def test_comma_separated_with_aliases(self):
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
            merged = rule_4.apply_merge(parsed, join_params)
            self.assertIsNotNone(merged)
    
    def test_mixed_comma_and_explicit_join(self):
        """Test mixing comma-separated tables dengan explicit JOIN."""
        query_str = """
        SELECT * FROM employees e, payroll p
        INNER JOIN accounts a ON p.id = a.payroll_id
        WHERE e.id = p.employee_id
        """
        parsed = self.engine.parse_query(query_str)
        
        # Should have both CROSS and INNER JOINs
        def find_join_types(node, types=None):
            if types is None:
                types = []
            if node is None:
                return types
            
            if node.type == "JOIN":
                types.append(node.val)
            
            for child in node.childs:
                find_join_types(child, types)
            
            return types
        
        join_types = find_join_types(parsed.query_tree)
        self.assertIn("CROSS", join_types, "Should have CROSS JOIN from comma-separated tables")
        self.assertIn("INNER", join_types, "Should have explicit INNER JOIN")
    
    def test_cost_benefit_of_merging_comma_separated(self):
        """Test cost comparison untuk merging comma-separated tables."""
        query_str = """
        SELECT * FROM employees e, payroll p
        WHERE e.id = p.employee_id
        """
        parsed = self.engine.parse_query(query_str)
        
        patterns = rule_4.find_patterns(parsed)
        
        if patterns:
            # Keep as CROSS JOIN - merge no conditions
            join_params_separate = {}
            for join_id, metadata in patterns.items():
                join_params_separate[join_id] = []  # Don't merge
            separate = rule_4.apply_merge(parsed, join_params_separate)
            cost_separate = self.engine.get_cost(separate)
            
            # Merge to INNER JOIN - merge all conditions
            join_params_merge = {}
            for join_id, metadata in patterns.items():
                join_params_merge[join_id] = metadata['filter_conditions']  # Merge all
            merged = rule_4.apply_merge(parsed, join_params_merge)
            cost_merge = self.engine.get_cost(merged)
            
            # INNER JOIN should typically be more efficient than CROSS + FILTER
            self.assertLessEqual(cost_merge, cost_separate,
                               "Merging to INNER JOIN should have lower or equal cost")


class TestRule4WithRule1And2(unittest.TestCase):
    """Test interaction antara Rule 4 dengan Rule 1 & 2."""
    
    def setUp(self):
        self.engine = OptimizationEngine()
    
    def test_combined_filter_and_join_params(self):
        """Test kombinasi filter_params dan join_params."""
        query_str = """
        SELECT * FROM employees e NATURAL JOIN payroll p
        WHERE e.id = p.employee_id AND e.salary > 50000 AND p.amount > 1000
        """
        parsed = self.engine.parse_query(query_str)
        
        # Analyze untuk both operations
        manager = get_rule_params_manager()
        filter_analysis = manager.analyze_query(parsed, 'filter_params')
        join_analysis = manager.analyze_query(parsed, 'join_params')
        
        # Generate params
        filter_params = {}
        for node_id, num_conditions in filter_analysis.items():
            filter_params[node_id] = manager.generate_random_params(
                'filter_params', num_conditions
            )
        
        join_params = {}
        for filter_id, metadata in join_analysis.items():
            join_params[filter_id] = manager.generate_random_params(
                'join_params', metadata
            )
        
        # Create individual dengan both
        operation_params = {
            'filter_params': filter_params,
            'join_params': join_params
        }
        
        individual = Individual(operation_params, parsed)
        
        # Should apply both transformations successfully
        self.assertIsNotNone(individual.query)
    
    def test_ga_optimizes_both_rules(self):
        """Test GA optimize dengan both rule 1/2 dan rule 4."""
        query_str = """
        SELECT * FROM employees e NATURAL JOIN payroll p
        WHERE e.id = p.employee_id AND e.salary > 50000
        """
        parsed = self.engine.parse_query(query_str)
        
        ga = GeneticOptimizer(
            population_size=15,
            generations=3,
            mutation_rate=0.15
        )
        
        _ = ga.optimize(parsed)
        
        # Check both params in best individual
        if ga.best_individual:
            params = ga.best_individual.operation_params
            
            # Bisa ada atau tidak tergantung query structure
            self.assertIsInstance(params, dict)
            
            # If ada, validate structure
            if 'filter_params' in params:
                self.assertIsInstance(params['filter_params'], dict)
            
            if 'join_params' in params:
                self.assertIsInstance(params['join_params'], dict)
                for condition_ids in params['join_params'].values():
                    self.assertIsInstance(condition_ids, list)
                    self.assertTrue(all(isinstance(x, int) for x in condition_ids))


def run_tests():
    """Run all tests."""
    unittest.main(argv=[''], exit=False, verbosity=2)


if __name__ == '__main__':
    run_tests()
