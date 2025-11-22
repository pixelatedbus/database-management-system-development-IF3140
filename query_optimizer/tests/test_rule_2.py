"""
Unit tests untuk Rule 2: Seleksi Komutatif
Testing transformasi reordering kondisi dalam OPERATOR(AND)

Berdasarkan sifat komutatif: A AND B = B AND A

Input:
OPERATOR("AND")
├── condition_0
├── condition_1
└── condition_2

Output dengan order [2, 0, 1]:
OPERATOR("AND")
├── condition_2
├── condition_0
└── condition_1
"""

import unittest
from query_optimizer.query_tree import QueryTree
from query_optimizer.optimization_engine import ParsedQuery
from query_optimizer.query_check import check_query
from query_optimizer.rule_2 import (
    analyze_and_operators_for_reorder,
    generate_random_rule_2_params,
    copy_rule_2_params,
    count_conditions_in_rule_2_params,
    mutate_rule_2_params,
    reorder_and_conditions,
    validate_rule_2_params,
    clone_tree
)


def make_comparison(operator=">", left_val="x", right_val=10):
    """Helper to create valid COMPARISON node"""
    comp = QueryTree("COMPARISON", operator)
    
    # Left side: COLUMN_REF
    left = QueryTree("COLUMN_REF", "")
    left_name = QueryTree("COLUMN_NAME", "")
    left_id = QueryTree("IDENTIFIER", left_val)
    left_name.add_child(left_id)
    left.add_child(left_name)
    
    # Right side: LITERAL
    if isinstance(right_val, str):
        right = QueryTree("LITERAL_STRING", right_val)
    else:
        right = QueryTree("LITERAL_NUMBER", right_val)
    
    comp.add_child(left)
    comp.add_child(right)
    return comp


class TestHelperFunctions(unittest.TestCase):
    """Test cases untuk helper functions"""
    
    def test_copy_rule_2_params(self):
        """Test copy of rule_2_params"""
        params = [2, 0, 1]
        copied = copy_rule_2_params(params)
        
        # Verify copy
        self.assertEqual(params, copied)
        self.assertIsNot(params, copied)
        
        # Modify copied should not affect original
        copied[0] = 99
        self.assertNotEqual(params, copied)
    
    def test_count_conditions_in_rule_2_params(self):
        """Test counting conditions"""
        self.assertEqual(count_conditions_in_rule_2_params([0, 1, 2]), 3)
        self.assertEqual(count_conditions_in_rule_2_params([2, 0, 1]), 3)
        self.assertEqual(count_conditions_in_rule_2_params([1, 0]), 2)
    
    def test_generate_random_rule_2_params(self):
        """Test random generation of rule_2_params"""
        for num_conditions in range(2, 6):
            params = generate_random_rule_2_params(num_conditions)
            
            # Verify all indices present
            self.assertEqual(len(params), num_conditions)
            self.assertEqual(set(params), set(range(num_conditions)))
    
    def test_validate_rule_2_params(self):
        """Test validation of rule_2_params"""
        # Valid
        self.assertTrue(validate_rule_2_params([0, 1, 2], 3))
        self.assertTrue(validate_rule_2_params([2, 0, 1], 3))
        
        # Invalid - wrong length
        self.assertFalse(validate_rule_2_params([0, 1], 3))
        
        # Invalid - duplicates
        self.assertFalse(validate_rule_2_params([0, 0, 1], 3))
        
        # Invalid - missing indices
        self.assertFalse(validate_rule_2_params([0, 1, 3], 3))


class TestMutateFunction(unittest.TestCase):
    """Test cases untuk mutate_rule_2_params"""
    
    def test_mutate_swap(self):
        """Test mutate with swap operation"""
        params = [0, 1, 2, 3]
        
        # Run multiple times
        swapped = False
        for _ in range(30):
            mutated = mutate_rule_2_params(params.copy())
            if mutated != params:
                swapped = True
                # Verify still valid permutation
                self.assertEqual(set(mutated), set(params))
                break
        
        self.assertTrue(swapped, "Should be able to swap")
    
    def test_mutate_reverse_subseq(self):
        """Test mutate with reverse subsequence"""
        params = [0, 1, 2, 3, 4]
        
        reversed_found = False
        for _ in range(50):
            mutated = mutate_rule_2_params(params.copy())
            if mutated != params:
                reversed_found = True
                # Verify still valid permutation
                self.assertEqual(set(mutated), set(params))
                break
        
        self.assertTrue(reversed_found, "Should be able to reverse subsequence")
    
    def test_mutate_rotate(self):
        """Test mutate with rotation"""
        params = [0, 1, 2, 3]
        
        rotated = False
        for _ in range(30):
            mutated = mutate_rule_2_params(params.copy())
            if mutated != params:
                rotated = True
                # Verify still valid permutation
                self.assertEqual(set(mutated), set(params))
                break
        
        self.assertTrue(rotated, "Should be able to rotate")
    
    def test_mutate_preserves_all_indices(self):
        """Test that mutation preserves all indices"""
        params = [2, 0, 3, 1, 4]
        
        for _ in range(20):
            mutated = mutate_rule_2_params(params.copy())
            # Must have same set of indices
            self.assertEqual(set(mutated), set(params))
            self.assertEqual(len(mutated), len(params))


class TestAnalyzeAndOperators(unittest.TestCase):
    """Test cases untuk analyze_and_operators_for_reorder"""
    
    def test_analyze_simple_and(self):
        """Test analyze single OPERATOR(AND)"""
        comp1 = QueryTree("COMPARISON", ">")
        comp2 = QueryTree("COMPARISON", "=")
        
        and_op = QueryTree("OPERATOR", "AND")
        and_op.add_child(comp1)
        and_op.add_child(comp2)
        
        query = ParsedQuery(and_op, "test")
        operators = analyze_and_operators_for_reorder(query)
        
        self.assertEqual(len(operators), 1)
        self.assertEqual(operators[and_op.id], 2)
    
    def test_analyze_nested_and(self):
        """Test analyze nested OPERATOR(AND)"""
        # Inner AND
        inner_and = QueryTree("OPERATOR", "AND")
        inner_and.add_child(QueryTree("COMPARISON", ">"))
        inner_and.add_child(QueryTree("COMPARISON", "="))
        
        # Outer AND
        outer_and = QueryTree("OPERATOR", "AND")
        outer_and.add_child(inner_and)
        outer_and.add_child(QueryTree("COMPARISON", "<"))
        outer_and.add_child(QueryTree("IN_EXPR"))
        
        query = ParsedQuery(outer_and, "test")
        operators = analyze_and_operators_for_reorder(query)
        
        # Should find both ANDs
        self.assertEqual(len(operators), 2)
        self.assertEqual(operators[inner_and.id], 2)
        self.assertEqual(operators[outer_and.id], 3)
    
    def test_analyze_in_filter(self):
        """Test analyze OPERATOR(AND) inside FILTER"""
        relation = QueryTree("RELATION", "users")
        
        and_op = QueryTree("OPERATOR", "AND")
        and_op.add_child(QueryTree("COMPARISON", ">"))
        and_op.add_child(QueryTree("COMPARISON", "="))
        and_op.add_child(QueryTree("COMPARISON", "<"))
        
        filter_node = QueryTree("FILTER")
        filter_node.add_child(relation)
        filter_node.add_child(and_op)
        
        query = ParsedQuery(filter_node, "test")
        operators = analyze_and_operators_for_reorder(query)
        
        self.assertEqual(len(operators), 1)
        self.assertEqual(operators[and_op.id], 3)


class TestReorderAndConditions(unittest.TestCase):
    """Test cases untuk reorder_and_conditions"""
    
    def test_reorder_simple(self):
        """Test simple reordering of 3 conditions"""
        # Build OPERATOR(AND) with 3 conditions
        comp1 = make_comparison(">", "age", 10)
        comp2 = make_comparison("=", "status", "active")
        comp3 = make_comparison("<", "score", 100)
        
        and_op = QueryTree("OPERATOR", "AND")
        and_op.add_child(comp1)
        and_op.add_child(comp2)
        and_op.add_child(comp3)
        
        query = ParsedQuery(and_op, "test")
        
        # Reorder to [2, 0, 1]
        operator_orders = {and_op.id: [2, 0, 1]}
        result = reorder_and_conditions(query, operator_orders)
        
        # Validate result
        check_query(result.query_tree)
        
        # Verify structure
        result_and = result.query_tree
        self.assertEqual(result_and.type, "OPERATOR")
        self.assertEqual(result_and.val, "AND")
        self.assertEqual(len(result_and.childs), 3)
        
        # Verify order: should be comp3, comp1, comp2
        self.assertEqual(result_and.childs[0].val, "<")
        self.assertEqual(result_and.childs[1].val, ">")
        self.assertEqual(result_and.childs[2].val, "=")
    
    def test_reorder_preserves_structure(self):
        """Test that reordering preserves child structure"""
        # Build conditions with nested structure
        comp1 = make_comparison(">", "age", 25)
        comp2 = make_comparison("=", "status", "test")
        
        and_op = QueryTree("OPERATOR", "AND")
        and_op.add_child(comp1)
        and_op.add_child(comp2)
        
        query = ParsedQuery(and_op, "test")
        
        # Reorder to [1, 0]
        operator_orders = {and_op.id: [1, 0]}
        result = reorder_and_conditions(query, operator_orders)
        
        # Validate result
        check_query(result.query_tree)
        
        # Verify first child (was comp2) is now COMPARISON(=)
        self.assertEqual(result.query_tree.childs[0].val, "=")
        
        # Verify second child (was comp1) still has nested structure
        second_child = result.query_tree.childs[1]
        self.assertEqual(second_child.val, ">")
        self.assertEqual(len(second_child.childs), 2)
        self.assertEqual(second_child.childs[0].type, "COLUMN_REF")
    
    def test_reorder_multiple_operators(self):
        """Test reordering multiple OPERATOR(AND) nodes"""
        # Inner AND
        inner_and = QueryTree("OPERATOR", "AND")
        inner_and.add_child(make_comparison(">", "age", 18))
        inner_and.add_child(make_comparison("=", "status", "active"))
        
        # Outer AND
        outer_and = QueryTree("OPERATOR", "AND")
        outer_and.add_child(inner_and)
        outer_and.add_child(make_comparison("<", "score", 100))
        outer_and.add_child(make_comparison("!=", "type", "banned"))
        
        query = ParsedQuery(outer_and, "test")
        
        # Reorder both
        operator_orders = {
            inner_and.id: [1, 0],  # Swap inner
            outer_and.id: [2, 0, 1]  # Reorder outer
        }
        result = reorder_and_conditions(query, operator_orders)
        
        # Validate result
        check_query(result.query_tree)
        
        # Verify outer order (after reorder [2, 0, 1])
        # Child 0 should be index 2 (third COMPARISON)
        # Child 1 should be index 0 (inner AND)
        # Child 2 should be index 1 (second COMPARISON)
        self.assertEqual(result.query_tree.childs[0].type, "COMPARISON")
        self.assertEqual(result.query_tree.childs[1].type, "OPERATOR")
        self.assertEqual(result.query_tree.childs[2].type, "COMPARISON")
        
        # Verify inner order (now at position 1)
        inner = result.query_tree.childs[1]
        self.assertEqual(inner.childs[0].val, "=")
        self.assertEqual(inner.childs[1].val, ">")
    
    def test_reorder_no_operator_orders(self):
        """Test that no reordering happens when operator_orders is None"""
        comp1 = make_comparison(">", "age", 18)
        comp2 = make_comparison("=", "status", "active")
        
        and_op = QueryTree("OPERATOR", "AND")
        and_op.add_child(comp1)
        and_op.add_child(comp2)
        
        query = ParsedQuery(and_op, "test")
        result = reorder_and_conditions(query, None)
        
        # Validate result
        check_query(result.query_tree)
        
        # Should remain unchanged
        self.assertEqual(result.query_tree.childs[0].val, ">")
        self.assertEqual(result.query_tree.childs[1].val, "=")


class TestCloneTree(unittest.TestCase):
    """Test cases untuk clone_tree"""
    
    def test_clone_simple(self):
        """Test cloning simple tree"""
        node = QueryTree("COMPARISON", ">")
        cloned = clone_tree(node)
        
        self.assertIsNot(node, cloned)
        self.assertEqual(node.type, cloned.type)
        self.assertEqual(node.val, cloned.val)
    
    def test_clone_with_children(self):
        """Test cloning tree with children"""
        parent = QueryTree("OPERATOR", "AND")
        child1 = QueryTree("COMPARISON", ">")
        child2 = QueryTree("COMPARISON", "=")
        
        parent.add_child(child1)
        parent.add_child(child2)
        
        cloned = clone_tree(parent)
        
        self.assertIsNot(parent, cloned)
        self.assertIsNot(parent.childs[0], cloned.childs[0])
        self.assertIsNot(parent.childs[1], cloned.childs[1])
        
        self.assertEqual(parent.type, cloned.type)
        self.assertEqual(len(parent.childs), len(cloned.childs))


if __name__ == "__main__":
    unittest.main()
