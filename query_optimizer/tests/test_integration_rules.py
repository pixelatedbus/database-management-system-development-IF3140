"""
Integration tests untuk Rule yang masuk Genetic Algorithm (Rule 1 dan Rule 2)

Rule ini bersifat heuristik dan memerlukan parameter space exploration dengan GA:
- Rule 1: Selection Cascade (cascade/group filters)
- Rule 2: Selection Reorder (reorder AND conditions)

Kedua rule ini diintegrasikan dalam unified filter_params untuk GA.
Testing kombinasi dan interaksi antar rules dalam parameter space.
"""

import unittest
from query_optimizer.query_tree import QueryTree
from query_optimizer.optimization_engine import ParsedQuery
from query_optimizer.rule.rule_1 import (
    seleksi_konjungtif,
    cascade_filters,
    analyze_and_operators
)
from query_optimizer.rule.rule_2 import (
    reorder_and_conditions,
    analyze_and_operators_for_reorder
)
from query_optimizer.rule_params_manager import get_rule_params_manager


class TestRule1AndRule2Integration(unittest.TestCase):
    """Test integration between Rule 1 (Konjungtif) and Rule 2 (Komutatif)"""
    
    def setUp(self):
        """Setup test fixtures"""
        # Build query with FILTER and OPERATOR(AND)
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
        
        self.query = ParsedQuery(self.filter_node, "test")
    
    def test_rule2_then_rule1(self):
        """Test applying Rule 2 (reorder) then Rule 1 (cascade)"""
        # Step 1: Apply Rule 2 - Reorder
        operator_orders = {self.and_op.id: [2, 0, 1]}  # [<, >, =]
        q2 = reorder_and_conditions(self.query, operator_orders)
        
        # Verify reordering
        and_node = q2.query_tree.childs[1]
        self.assertEqual(and_node.childs[0].val, "<")
        self.assertEqual(and_node.childs[1].val, ">")
        self.assertEqual(and_node.childs[2].val, "=")
        
        # Step 2: Apply Rule 1 - Cascade
        q3 = seleksi_konjungtif(q2)
        
        # Verify cascading
        depth = 0
        current = q3.query_tree
        while current and current.type == "FILTER":
            depth += 1
            current = current.childs[0] if current.childs else None
        
        self.assertEqual(depth, 3, "Should have 3 cascaded FILTERs")
    
    def test_rule1_then_rule2(self):
        """Test applying Rule 1 (cascade) then Rule 2 (reorder)"""
        # Note: After Rule 1, AND is broken into cascaded FILTERs
        # So Rule 2 won't have much effect (no AND to reorder)
        
        # Step 1: Apply Rule 1 - Cascade
        q2 = seleksi_konjungtif(self.query)
        
        # Verify cascading happened
        self.assertEqual(q2.query_tree.type, "FILTER")
        
        # Step 2: Apply Rule 2 - Should have no effect (no AND left)
        operators = analyze_and_operators_for_reorder(q2)
        self.assertEqual(len(operators), 0, "No AND operators left after cascading")
    
    def test_rule2_with_mixed_cascade(self):
        """Test Rule 2 reorder then Rule 1 with mixed cascading"""
        # Apply Rule 2 first
        operator_orders = {self.and_op.id: [1, 0, 2]}  # [=, >, <]
        q2 = reorder_and_conditions(self.query, operator_orders)
        
        # Apply Rule 1 with mixed order: [2, [0, 1]]
        # This means: condition_2 single, then (condition_0 AND condition_1) grouped
        cascade_orders = {self.and_op.id: [2, [0, 1]]}
        q3 = cascade_filters(q2, cascade_orders)
        
        # Verify structure: should have 2 FILTERs
        # Top FILTER with single condition
        self.assertEqual(q3.query_tree.type, "FILTER")
        
        # Inner FILTER should have AND with 2 conditions
        inner = q3.query_tree.childs[0]
        self.assertEqual(inner.type, "FILTER")
        
        inner_cond = inner.childs[1]
        if inner_cond.type == "OPERATOR":
            self.assertEqual(inner_cond.val, "AND")
            self.assertEqual(len(inner_cond.childs), 2)
    
    def test_both_rules_preserve_relation(self):
        """Test that both rules preserve the RELATION node"""
        # Apply both rules
        q2 = reorder_and_conditions(self.query, {self.and_op.id: [2, 0, 1]})
        q3 = seleksi_konjungtif(q2)
        
        # Find RELATION node (should be at the bottom)
        current = q3.query_tree
        while current and current.type == "FILTER":
            current = current.childs[0] if current.childs else None
        
        # Should reach RELATION
        self.assertIsNotNone(current)
        self.assertEqual(current.type, "RELATION")
        self.assertEqual(current.val, "users")


class TestMultipleAndOperators(unittest.TestCase):
    """Test with multiple OPERATOR(AND) nodes"""
    
    def setUp(self):
        """Setup query with nested AND operators"""
        self.relation = QueryTree("RELATION", "orders")
        
        # Inner AND
        self.inner_and = QueryTree("OPERATOR", "AND")
        self.inner_and.add_child(QueryTree("COMPARISON", ">"))
        self.inner_and.add_child(QueryTree("COMPARISON", "="))
        
        self.inner_filter = QueryTree("FILTER")
        self.inner_filter.add_child(self.relation)
        self.inner_filter.add_child(self.inner_and)
        
        # Outer AND
        self.outer_and = QueryTree("OPERATOR", "AND")
        self.outer_and.add_child(QueryTree("COMPARISON", "<"))
        self.outer_and.add_child(QueryTree("IN_EXPR"))
        
        self.outer_filter = QueryTree("FILTER")
        self.outer_filter.add_child(self.inner_filter)
        self.outer_filter.add_child(self.outer_and)
        
        self.query = ParsedQuery(self.outer_filter, "test")
    
    def test_reorder_both_operators(self):
        """Test reordering both inner and outer AND operators"""
        # Analyze
        operators = analyze_and_operators_for_reorder(self.query)
        self.assertEqual(len(operators), 2)
        
        # Reorder both
        operator_orders = {
            self.inner_and.id: [1, 0],  # Swap inner
            self.outer_and.id: [1, 0]   # Swap outer
        }
        result = reorder_and_conditions(self.query, operator_orders)
        
        # Verify both were reordered
        # Find outer AND
        outer_and_node = result.query_tree.childs[1]
        self.assertEqual(outer_and_node.type, "OPERATOR")
        self.assertEqual(outer_and_node.childs[0].type, "IN_EXPR")
        self.assertEqual(outer_and_node.childs[1].type, "COMPARISON")
        
        # Find inner AND
        inner_filter_node = result.query_tree.childs[0]
        inner_and_node = inner_filter_node.childs[1]
        self.assertEqual(inner_and_node.type, "OPERATOR")
        self.assertEqual(inner_and_node.childs[0].val, "=")
        self.assertEqual(inner_and_node.childs[1].val, ">")
    
    def test_cascade_nested_operators(self):
        """Test cascading nested AND operators"""
        # Cascade all
        result = seleksi_konjungtif(self.query)
        
        # Should cascade both inner and outer
        # Count total FILTERs
        filter_count = 0
        
        def count_filters(node):
            nonlocal filter_count
            if node is None:
                return
            if node.type == "FILTER":
                filter_count += 1
            for child in node.childs:
                count_filters(child)
        
        count_filters(result.query_tree)
        
        # Should have at least 4 FILTERs (2 from inner AND, 2 from outer AND)
        self.assertGreaterEqual(filter_count, 4)


class TestRuleParamsManagerIntegration(unittest.TestCase):
    """Test integration with RuleParamsManager"""
    
    def setUp(self):
        """Setup test fixtures"""
        self.manager = get_rule_params_manager()
        
        # Build simple query
        relation = QueryTree("RELATION", "users")
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
    
    def test_manager_has_both_rules(self):
        """Test that manager has filter_params operation"""
        ops = self.manager.get_registered_operations()
        self.assertIn('filter_params', ops)
    
    def test_analyze_both_rules(self):
        """Test analyzing query with filter_params"""
        # Analyze with filter_params
        ops = self.manager.analyze_query(self.query, 'filter_params')
        self.assertGreater(len(ops), 0)
        # Should find one AND operator with 3 conditions
        op_ids = list(ops.keys())
        self.assertEqual(len(op_ids), 1)
        self.assertEqual(ops[op_ids[0]], 3)
    
    def test_generate_params_both_rules(self):
        """Test generating params for filter_params"""
        # filter_params returns unified format (mixed list of int | list[int])
        params = self.manager.generate_random_params('filter_params', 3)
        self.assertIsNotNone(params)
        self.assertIsInstance(params, list)
    
    def test_mutate_params_both_rules(self):
        """Test mutating params for filter_params"""
        # Unified format
        params = [2, [0, 1]]
        mutated = self.manager.mutate_params('filter_params', params)
        self.assertIsNotNone(mutated)
        self.assertIsInstance(mutated, list)
    
    def test_copy_params_both_rules(self):
        """Test copying params for filter_params"""
        # Unified format
        params = [2, [0, 1]]
        copied = self.manager.copy_params('filter_params', params)
        self.assertEqual(params, copied)
        self.assertIsNot(params, copied)


class TestEdgeCases(unittest.TestCase):
    """Test edge cases and error conditions"""
    
    def test_reorder_with_empty_orders(self):
        """Test reordering with empty operator_orders"""
        relation = QueryTree("RELATION", "users")
        comp1 = QueryTree("COMPARISON", ">")
        comp2 = QueryTree("COMPARISON", "=")
        
        and_op = QueryTree("OPERATOR", "AND")
        and_op.add_child(comp1)
        and_op.add_child(comp2)
        
        filter_node = QueryTree("FILTER")
        filter_node.add_child(relation)
        filter_node.add_child(and_op)
        
        query = ParsedQuery(filter_node, "test")
        
        # Reorder with empty dict
        result = reorder_and_conditions(query, {})
        
        # Should remain unchanged
        self.assertEqual(result.query_tree.childs[1].childs[0].val, ">")
        self.assertEqual(result.query_tree.childs[1].childs[1].val, "=")
    
    def test_cascade_single_condition(self):
        """Test cascading with single condition (should not cascade)"""
        relation = QueryTree("RELATION", "users")
        comp = QueryTree("COMPARISON", ">")
        
        and_op = QueryTree("OPERATOR", "AND")
        and_op.add_child(comp)
        
        filter_node = QueryTree("FILTER")
        filter_node.add_child(relation)
        filter_node.add_child(and_op)
        
        query = ParsedQuery(filter_node, "test")
        
        # Should not cascade (only 1 condition)
        result = seleksi_konjungtif(query)
        
        # Structure should remain similar (AND with 1 child is not valid for cascade)
        self.assertEqual(result.query_tree.type, "FILTER")
    
    def test_reorder_single_condition(self):
        """Test reordering with single condition (no change)"""
        relation = QueryTree("RELATION", "users")
        comp = QueryTree("COMPARISON", ">")
        
        and_op = QueryTree("OPERATOR", "AND")
        and_op.add_child(comp)
        
        filter_node = QueryTree("FILTER")
        filter_node.add_child(relation)
        filter_node.add_child(and_op)
        
        query = ParsedQuery(filter_node, "test")
        
        # Analyze should find it but with 1 condition
        operators = analyze_and_operators_for_reorder(query)
        
        # Should find the operator but with 1 condition
        if len(operators) > 0:
            self.assertEqual(list(operators.values())[0], 1)
    
    def test_query_without_and(self):
        """Test query without any AND operator"""
        relation = QueryTree("RELATION", "users")
        comp = QueryTree("COMPARISON", ">")
        
        filter_node = QueryTree("FILTER")
        filter_node.add_child(relation)
        filter_node.add_child(comp)
        
        query = ParsedQuery(filter_node, "test")
        
        # Analyze should find no operators
        ops1 = analyze_and_operators(query)
        self.assertEqual(len(ops1), 0)
        
        ops2 = analyze_and_operators_for_reorder(query)
        self.assertEqual(len(ops2), 0)
        
        # Rules should not transform
        result1 = seleksi_konjungtif(query)
        self.assertEqual(result1.query_tree.type, "FILTER")
        
        result2 = reorder_and_conditions(query, None)
        self.assertEqual(result2.query_tree.type, "FILTER")


if __name__ == "__main__":
    unittest.main()
