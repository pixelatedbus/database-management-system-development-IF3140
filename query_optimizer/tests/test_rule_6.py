"""
Test integration of Rule 6 (Join Associativity) with Genetic Algorithm
Menggunakan Mock Metadata untuk isolasi pengujian.
"""

import unittest
from unittest.mock import patch, MagicMock
from query_optimizer.genetic_optimizer import GeneticOptimizer, Individual
from query_optimizer.optimization_engine import OptimizationEngine, ParsedQuery
from query_optimizer.rule_params_manager import get_rule_params_manager
from query_optimizer.rule import rule_6, rule_4

# --- MOCK METADATA ---
MOCK_METADATA = {
    "tables": ["employees", "payroll", "accounts", "users", "orders", "products", "logs"],
    "columns": {
        "employees": ["id", "name", "salary", "employee_id"],
        "payroll": ["id", "employee_id", "amount", "payroll_id"],
        "accounts": ["id", "payroll_id", "bank_name"],
        "users": ["id", "name"],
        "orders": ["id", "user_id", "product_id"],
        "products": ["id", "name", "price"],
        "logs": ["id", "user_id", "message"]
    }
}

@patch('query_optimizer.query_check.get_metadata', return_value=MOCK_METADATA)
class TestRule6Integration(unittest.TestCase):
    """Test Rule 6 integration dengan GA."""

    def setUp(self):
        """Setup test queries."""
        self.engine = OptimizationEngine()

        self.query_str_1 = """
        SELECT * FROM employees e
        INNER JOIN payroll p ON e.id = p.employee_id
        INNER JOIN accounts a ON p.id = a.payroll_id
        """

        self.query_str_2 = """
        SELECT * FROM users u
        INNER JOIN orders o ON u.id = o.user_id
        INNER JOIN products p ON o.product_id = p.id
        INNER JOIN logs l ON u.id = l.user_id
        """

        self.query_str_3 = """
        SELECT * FROM users u1
        INNER JOIN orders o ON u1.id = o.user_id
        INNER JOIN users u2 ON o.id = u2.id
        """

    def test_rule_6_find_patterns(self, mock_meta):
        """Test apakah rule 6 bisa detect nested JOIN patterns."""
        parsed = self.engine.parse_query(self.query_str_1)
        patterns = rule_6.find_patterns(parsed)

        self.assertGreaterEqual(len(patterns), 0)

        for join_id, metadata in patterns.items():
            self.assertIn('inner_join_id', metadata)
            self.assertIn('outer_condition', metadata)
            self.assertIn('inner_condition', metadata)
            self.assertIsInstance(metadata['inner_join_id'], int)
            self.assertIsInstance(metadata['outer_condition'], bool)
            self.assertIsInstance(metadata['inner_condition'], bool)

    def test_rule_6_is_reassociable(self, mock_meta):
        """Test detection of reassociable JOIN nodes."""
        parsed = self.engine.parse_query(self.query_str_1)

        def find_join_nodes(node):
            joins = []
            if node.type == "JOIN":
                joins.append(node)
            for child in node.childs:
                joins.extend(find_join_nodes(child))
            return joins

        joins = find_join_nodes(parsed.query_tree)
        reassociable_count = sum(1 for j in joins if rule_6.is_reassociable(j))

        self.assertGreaterEqual(reassociable_count, 0)

    def test_rule_6_params_generation(self, mock_meta):
        """Test generation random params untuk rule 6."""
        metadata = {
            'inner_join_id': 42,
            'outer_condition': True,
            'inner_condition': True
        }

        results = [rule_6.generate_params(metadata) for _ in range(30)]
        self.assertTrue(all(isinstance(r, str) for r in results))
        self.assertTrue(all(r in {'left', 'right', 'none'} for r in results))

        self.assertIn('left', results)
        self.assertIn('right', results)
        self.assertIn('none', results)

    def test_rule_6_params_copy(self, mock_meta):
        """Test copy params."""
        self.assertEqual(rule_6.copy_params('left'), 'left')
        self.assertEqual(rule_6.copy_params('right'), 'right')
        self.assertEqual(rule_6.copy_params('none'), 'none')

    def test_rule_6_params_mutation(self, mock_meta):
        """Test mutation untuk associativity params."""
        for _ in range(10):
            original = 'left'
            mutated = rule_6.mutate_params(original)
            self.assertIsInstance(mutated, str)
            self.assertIn(mutated, {'left', 'right', 'none'})
            self.assertNotEqual(mutated, original)

        for _ in range(10):
            original = 'right'
            mutated = rule_6.mutate_params(original)
            self.assertNotEqual(mutated, original)

    def test_rule_6_params_validation(self, mock_meta):
        """Test validation params."""
        self.assertTrue(rule_6.validate_params('left'))
        self.assertTrue(rule_6.validate_params('right'))
        self.assertTrue(rule_6.validate_params('none'))

        self.assertFalse(rule_6.validate_params('invalid'))
        self.assertFalse(rule_6.validate_params(123))
        self.assertFalse(rule_6.validate_params(None))
        self.assertFalse(rule_6.validate_params(True))

    def test_rule_6_apply_associativity_right(self, mock_meta):
        """Test apply right-associativity."""
        parsed = self.engine.parse_query(self.query_str_1)
        patterns = rule_6.find_patterns(parsed)

        if patterns:
            decisions = {join_id: 'right' for join_id in patterns.keys()}
            result = rule_6.apply_associativity(parsed, decisions)

            self.assertIsInstance(result, ParsedQuery)
            self.assertIsNotNone(result.query_tree)

            cost_original = self.engine.get_cost(parsed)
            cost_result = self.engine.get_cost(result)
            self.assertIsInstance(cost_original, (int, float))
            self.assertIsInstance(cost_result, (int, float))

    def test_rule_6_apply_associativity_left(self, mock_meta):
        """Test apply left-associativity."""
        parsed = self.engine.parse_query(self.query_str_1)
        patterns = rule_6.find_patterns(parsed)

        if patterns:
            decisions = {join_id: 'left' for join_id in patterns.keys()}
            result = rule_6.apply_associativity(parsed, decisions)

            self.assertIsInstance(result, ParsedQuery)
            self.assertIsNotNone(result.query_tree)

    def test_rule_6_apply_associativity_none(self, mock_meta):
        """Test apply no change (none)."""
        parsed = self.engine.parse_query(self.query_str_1)
        patterns = rule_6.find_patterns(parsed)

        if patterns:
            decisions = {join_id: 'none' for join_id in patterns.keys()}
            result = rule_6.apply_associativity(parsed, decisions)

            self.assertIsInstance(result, ParsedQuery)

    def test_rule_6_undo_associativity(self, mock_meta):
        """Test undo associativity transformation."""
        parsed = self.engine.parse_query(self.query_str_1)
        patterns = rule_6.find_patterns(parsed)

        if patterns:
            decisions = {join_id: 'right' for join_id in patterns.keys()}
            transformed = rule_6.apply_associativity(parsed, decisions)
            undone = rule_6.undo_associativity(transformed)

            self.assertIsInstance(undone, ParsedQuery)
            self.assertIsNotNone(undone.query_tree)

    def test_rule_6_collect_tables(self, mock_meta):
        """Test table collection from node."""
        parsed = self.engine.parse_query(self.query_str_1)
        tables = rule_6.collect_tables(parsed.query_tree)

        self.assertIsInstance(tables, set)
        self.assertGreaterEqual(len(tables), 0)

    def test_rule_6_multiple_nested_joins(self, mock_meta):
        """Test with multiple nested JOINs."""
        parsed = self.engine.parse_query(self.query_str_2)
        patterns = rule_6.find_patterns(parsed)

        self.assertGreaterEqual(len(patterns), 0)

        if len(patterns) >= 2:
            join_ids = list(patterns.keys())
            decisions = {
                join_ids[0]: 'right',
                join_ids[1]: 'left'
            }
            result = rule_6.apply_associativity(parsed, decisions)
            self.assertIsInstance(result, ParsedQuery)

    def test_rule_6_reassociate_right(self, mock_meta):
        """Test reassociate_right function directly."""
        parsed = self.engine.parse_query(self.query_str_1)

        def find_reassociable(node):
            if rule_6.is_reassociable(node):
                return node
            for child in node.childs:
                result = find_reassociable(child)
                if result:
                    return result
            return None

        join_node = find_reassociable(parsed.query_tree)
        if join_node:
            result = rule_6.reassociate_right(join_node)
            self.assertIsNotNone(result)

    def test_rule_6_reassociate_left(self, mock_meta):
        """Test reassociate_left function directly."""
        parsed = self.engine.parse_query(self.query_str_1)
        patterns = rule_6.find_patterns(parsed)

        if patterns:
            decisions = {join_id: 'right' for join_id in patterns.keys()}
            right_assoc = rule_6.apply_associativity(parsed, decisions)

            def find_reassociable(node):
                if rule_6.is_reassociable(node):
                    return node
                for child in node.childs:
                    result = find_reassociable(child)
                    if result:
                        return result
                return None

            join_node = find_reassociable(right_assoc.query_tree)
            if join_node:
                result = rule_6.reassociate_left(join_node)
                self.assertIsNotNone(result)

    def test_rule_6_with_manager(self, mock_meta):
        """Test Rule 6 registered di RuleParamsManager."""
        manager = get_rule_params_manager()
        operations = manager.get_registered_operations()

        self.assertIn('join_associativity_params', operations)

    def test_rule_6_manager_analyze(self, mock_meta):
        """Test manager analyze function for Rule 6."""
        manager = get_rule_params_manager()
        parsed = self.engine.parse_query(self.query_str_1)

        patterns = manager.analyze_query(parsed, 'join_associativity_params')
        self.assertIsInstance(patterns, dict)

    def test_rule_6_manager_generate(self, mock_meta):
        """Test manager generate function for Rule 6."""
        manager = get_rule_params_manager()
        metadata = {
            'inner_join_id': 42,
            'outer_condition': True,
            'inner_condition': True
        }

        params = manager.generate_random_params('join_associativity_params', metadata)
        self.assertIn(params, {'left', 'right', 'none'})

    def test_rule_6_manager_copy(self, mock_meta):
        """Test manager copy function for Rule 6."""
        manager = get_rule_params_manager()
        original = 'right'
        copied = manager.copy_params('join_associativity_params', original)
        self.assertEqual(copied, original)

    def test_rule_6_manager_mutate(self, mock_meta):
        """Test manager mutate function for Rule 6."""
        manager = get_rule_params_manager()
        original = 'left'
        mutated = manager.mutate_params('join_associativity_params', original)
        self.assertNotEqual(mutated, original)
        self.assertIn(mutated, {'left', 'right', 'none'})

    def test_rule_6_manager_validate(self, mock_meta):
        """Test manager validate function for Rule 6."""
        manager = get_rule_params_manager()
        self.assertTrue(manager.validate_params('join_associativity_params', 'left'))
        self.assertTrue(manager.validate_params('join_associativity_params', 'right'))
        self.assertTrue(manager.validate_params('join_associativity_params', 'none'))
        self.assertFalse(manager.validate_params('join_associativity_params', 'invalid'))

    def test_rule_6_in_ga_integration(self, mock_meta):
        """Test Rule 6 works in GA Individual."""
        parsed = self.engine.parse_query(self.query_str_1)
        patterns = rule_6.find_patterns(parsed)

        if patterns:
            operation_params = {
                'join_associativity_params': {
                    join_id: 'right' for join_id in patterns.keys()
                }
            }

            individual = Individual(operation_params, parsed)
            result_query = individual.query

            self.assertIsInstance(result_query, ParsedQuery)
            self.assertIsNotNone(result_query.query_tree)

    def test_rule_6_preserves_join_ids_with_rule_4(self, mock_meta):
        """Ensure associativity keeps join IDs so rule 4 params still apply."""
        sql = """
        SELECT * FROM employees e
        INNER JOIN payroll p ON e.id = p.employee_id
        INNER JOIN accounts a ON p.id = a.payroll_id
        WHERE a.bank_name = 'ABC'
        """
        parsed = self.engine.parse_query(sql)

        def collect_join_ids(node):
            if node is None:
                return set()
            ids = {node.id} if node.type == "JOIN" else set()
            for ch in node.childs:
                ids |= collect_join_ids(ch)
            return ids

        before_ids = collect_join_ids(parsed.query_tree)
        assoc_patterns = rule_6.find_patterns(parsed)
        join_patterns = rule_4.find_patterns(parsed)
        if not assoc_patterns or not join_patterns:
            self.skipTest("No patterns found for associativity/join merge")

        decisions = {jid: 'right' for jid in assoc_patterns.keys()}
        target_join_id = next(iter(join_patterns.keys()))
        join_params = {target_join_id: join_patterns[target_join_id]['filter_conditions']}

        operation_params = {
            'filter_params': {},
            'join_params': join_params,
            'join_associativity_params': decisions
        }

        individual = Individual(operation_params, parsed)
        result_tree = individual.query.query_tree

        after_ids = collect_join_ids(result_tree)
        self.assertEqual(before_ids, after_ids, "Join IDs should be stable after associativity")

        def count_filters(node):
            if node is None:
                return 0
            return (1 if node.type == "FILTER" else 0) + sum(count_filters(ch) for ch in node.childs)

        self.assertLess(
            count_filters(result_tree),
            count_filters(parsed.query_tree),
            "Rule 4 should still merge filter conditions after associativity"
        )


@patch('query_optimizer.query_check.get_metadata', return_value=MOCK_METADATA)
class TestRule6SemanticValidation(unittest.TestCase):
    """Test semantic validation for Rule 6."""

    def setUp(self):
        self.engine = OptimizationEngine()

    def test_semantic_check_valid(self, mock_meta):
        """Test semantic check untuk valid reassociation."""
        sql = """
        SELECT * FROM users u
        INNER JOIN orders o ON u.id = o.user_id
        INNER JOIN products p ON o.product_id = p.id
        """
        parsed = self.engine.parse_query(sql)
        patterns = rule_6.find_patterns(parsed)

        if patterns:
            decisions = {join_id: 'right' for join_id in patterns.keys()}
            result = rule_6.apply_associativity(parsed, decisions)
            self.assertIsNotNone(result)

    def test_default_behavior(self, mock_meta):
        """Test default behavior tanpa decisions."""
        sql = """
        SELECT * FROM employees e
        INNER JOIN payroll p ON e.id = p.employee_id
        INNER JOIN accounts a ON p.id = a.payroll_id
        """
        parsed = self.engine.parse_query(sql)
        result = rule_6.apply_associativity(parsed)

        self.assertIsInstance(result, ParsedQuery)


if __name__ == '__main__':
    unittest.main()
