"""
Unit tests untuk Rule 1: Seleksi Konjungtif
Testing transformasi FILTER dengan OPERATOR(AND) menjadi cascaded filters

Berdasarkan GRAMMAR_PLAN.md (Parse_Query.md):
Input:
FILTER
├── source_tree
└── OPERATOR("AND")
    ├── condition1
    ├── condition2
    └── condition3

Output (cascaded):
FILTER
├── FILTER
│   ├── FILTER
│   │   ├── source_tree
│   │   └── condition1
│   └── condition2
└── condition3

Output dengan mixed order [2, [0,1]]:
FILTER (condition2 single)
├── FILTER (condition0 AND condition1 grouped)
│   ├── source_tree
│   └── OPERATOR("AND")
│       ├── condition0
│       └── condition1
└── condition2
"""

import unittest
from query_optimizer.query_tree import QueryTree
from query_optimizer.optimization_engine import ParsedQuery
from query_optimizer.rule_1 import (
    analyze_and_operators,
    generate_random_rule_1_params,
    copy_rule_1_params,
    count_conditions_in_rule_1_params,
    mutate_rule_1_params,
    seleksi_konjungtif,
    cascade_filters,
    cascade_and_mixed,
    uncascade_filters,
    is_conjunctive_filter,
    can_transform,
    clone_tree
)


class TestHelperFunctions(unittest.TestCase):
    """Test cases untuk helper functions"""
    
    def test_copy_rule_1_params(self):
        """Test deep copy of rule_1_params"""
        params = [2, [0, 1], 3]
        copied = copy_rule_1_params(params)
        
        # Verify deep copy
        self.assertEqual(params, copied)
        self.assertIsNot(params, copied)
        self.assertIsNot(params[1], copied[1])
        
        # Modify copied should not affect original
        copied[1].append(5)
        self.assertNotEqual(params, copied)
    
    def test_count_conditions_in_rule_1_params(self):
        """Test counting conditions in mixed order"""
        # All single
        self.assertEqual(count_conditions_in_rule_1_params([0, 1, 2]), 3)
        
        # Mixed
        self.assertEqual(count_conditions_in_rule_1_params([2, [0, 1]]), 3)
        self.assertEqual(count_conditions_in_rule_1_params([[0, 1, 2], 3]), 4)
        self.assertEqual(count_conditions_in_rule_1_params([1, [0], 2]), 3)
    
    def test_generate_random_rule_1_params(self):
        """Test random generation of rule_1_params"""
        for num_conditions in range(2, 6):
            params = generate_random_rule_1_params(num_conditions)
            
            # Verify all indices are present
            count = count_conditions_in_rule_1_params(params)
            self.assertEqual(count, num_conditions)
            
            # Verify no duplicates
            indices = []
            for item in params:
                if isinstance(item, list):
                    indices.extend(item)
                else:
                    indices.append(item)
            self.assertEqual(len(indices), len(set(indices)))
    
    def test_mutate_rule_1_params_group(self):
        """Test mutate with group operation"""
        # All singles: [0, 1, 2] -> should be able to group adjacent
        params = [0, 1, 2]
        
        # Run multiple times to test randomness
        grouped = False
        for _ in range(20):
            mutated = mutate_rule_1_params(params.copy())
            # Check if any grouping happened
            if any(isinstance(item, list) for item in mutated):
                grouped = True
                break
        
        self.assertTrue(grouped, "Should be able to group singles")
    
    def test_mutate_rule_1_params_ungroup(self):
        """Test mutate with ungroup operation"""
        # Has group: [2, [0, 1]] -> should be able to ungroup
        params = [2, [0, 1]]
        
        ungrouped = False
        for _ in range(20):
            mutated = mutate_rule_1_params(params.copy())
            # Check if ungrouping happened (all singles)
            if all(not isinstance(item, list) for item in mutated):
                ungrouped = True
                break
        
        self.assertTrue(ungrouped, "Should be able to ungroup")
    
    def test_mutate_rule_1_params_regroup(self):
        """Test mutate with regroup operation"""
        # Has large group: [[0, 1, 2, 3]] -> should be able to split into 2 groups
        params = [[0, 1, 2, 3]]
        
        regrouped = False
        for _ in range(30):
            mutated = mutate_rule_1_params(params.copy())
            # Check if split into 2 groups (regroup creates 2 lists from 1)
            if len(mutated) == 2 and all(isinstance(item, list) for item in mutated):
                regrouped = True
                break
        
        self.assertTrue(regrouped, "Should be able to regroup (split into 2 groups)")
    
    def test_mutate_preserves_indices(self):
        """Test that mutation preserves all condition indices"""
        params = [2, [0, 1], 3]
        original_count = count_conditions_in_rule_1_params(params)
        
        for _ in range(10):
            mutated = mutate_rule_1_params(params.copy())
            mutated_count = count_conditions_in_rule_1_params(mutated)
            self.assertEqual(original_count, mutated_count, "Mutation should preserve all indices")


class TestAnalyzeAndOperators(unittest.TestCase):
    """Test cases untuk analyze_and_operators"""
    
    def test_analyze_simple_and(self):
        """Test analyze single FILTER with OPERATOR(AND)"""
        # FILTER
        # ├── RELATION("users")
        # └── OPERATOR("AND")
        #     ├── COMPARISON(">")
        #     └── COMPARISON("=")
        
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
        operators = analyze_and_operators(query)
        
        self.assertEqual(len(operators), 1)
        self.assertEqual(operators[and_op.id], 2)
    
    def test_analyze_multiple_and(self):
        """Test analyze multiple FILTER nodes with OPERATOR(AND)"""
        # PROJECT
        # └── FILTER1
        #     ├── FILTER2
        #     │   ├── RELATION
        #     │   └── OPERATOR(AND) [3 conditions]
        #     └── OPERATOR(AND) [2 conditions]
        
        relation = QueryTree("RELATION", "users")
        
        # Inner AND with 3 conditions
        and_op1 = QueryTree("OPERATOR", "AND")
        and_op1.add_child(QueryTree("COMPARISON", ">"))
        and_op1.add_child(QueryTree("COMPARISON", "="))
        and_op1.add_child(QueryTree("COMPARISON", "<"))
        
        filter1 = QueryTree("FILTER")
        filter1.add_child(relation)
        filter1.add_child(and_op1)
        
        # Outer AND with 2 conditions
        and_op2 = QueryTree("OPERATOR", "AND")
        and_op2.add_child(QueryTree("COMPARISON", "!="))
        and_op2.add_child(QueryTree("IN_EXPR"))
        
        filter2 = QueryTree("FILTER")
        filter2.add_child(filter1)
        filter2.add_child(and_op2)
        
        project = QueryTree("PROJECT")
        project.add_child(filter2)
        
        query = ParsedQuery(project, "test")
        operators = analyze_and_operators(query)
        
        self.assertEqual(len(operators), 2)
        self.assertEqual(operators[and_op1.id], 3)
        self.assertEqual(operators[and_op2.id], 2)


class TestIsConjunctiveFilter(unittest.TestCase):
    """Test cases untuk is_conjunctive_filter"""
    
    def test_valid_conjunctive_filter(self):
        """Test valid FILTER with OPERATOR(AND)"""
        relation = QueryTree("RELATION", "users")
        comp1 = QueryTree("COMPARISON", ">")
        comp2 = QueryTree("COMPARISON", "=")
        
        and_op = QueryTree("OPERATOR", "AND")
        and_op.add_child(comp1)
        and_op.add_child(comp2)
        
        filter_node = QueryTree("FILTER")
        filter_node.add_child(relation)
        filter_node.add_child(and_op)
        
        self.assertTrue(is_conjunctive_filter(filter_node))
    
    def test_invalid_not_filter(self):
        """Test node that is not FILTER"""
        relation = QueryTree("RELATION", "users")
        self.assertFalse(is_conjunctive_filter(relation))
    
    def test_invalid_not_and_operator(self):
        """Test FILTER without OPERATOR(AND)"""
        relation = QueryTree("RELATION", "users")
        comp = QueryTree("COMPARISON", ">")
        
        filter_node = QueryTree("FILTER")
        filter_node.add_child(relation)
        filter_node.add_child(comp)
        
        self.assertFalse(is_conjunctive_filter(filter_node))
    
    def test_invalid_or_operator(self):
        """Test FILTER with OPERATOR(OR)"""
        relation = QueryTree("RELATION", "users")
        comp1 = QueryTree("COMPARISON", ">")
        comp2 = QueryTree("COMPARISON", "=")
        
        or_op = QueryTree("OPERATOR", "OR")
        or_op.add_child(comp1)
        or_op.add_child(comp2)
        
        filter_node = QueryTree("FILTER")
        filter_node.add_child(relation)
        filter_node.add_child(or_op)
        
        self.assertFalse(is_conjunctive_filter(filter_node))
    
    def test_invalid_single_condition(self):
        """Test OPERATOR(AND) with only 1 condition"""
        relation = QueryTree("RELATION", "users")
        comp = QueryTree("COMPARISON", ">")
        
        and_op = QueryTree("OPERATOR", "AND")
        and_op.add_child(comp)
        
        filter_node = QueryTree("FILTER")
        filter_node.add_child(relation)
        filter_node.add_child(and_op)
        
        self.assertFalse(is_conjunctive_filter(filter_node))


class TestSeleksiKonjunktif(unittest.TestCase):
    """Test cases untuk seleksi_konjungtif transformation"""
    
    def test_simple_two_conditions(self):
        """Test basic transformation with 2 conditions"""
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
        result = seleksi_konjungtif(query)
        
        # Result should be cascaded: FILTER -> FILTER -> RELATION
        self.assertEqual(result.query_tree.type, "FILTER")
        self.assertEqual(len(result.query_tree.childs), 2)
        
        # First child should be another FILTER
        inner = result.query_tree.get_child(0)
        self.assertEqual(inner.type, "FILTER")
        self.assertEqual(len(inner.childs), 2)
        
        # Bottom should be RELATION
        bottom = inner.get_child(0)
        self.assertEqual(bottom.type, "RELATION")
    
    def test_three_conditions(self):
        """Test transformation with 3 conditions"""
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
        
        query = ParsedQuery(filter_node, "test")
        result = seleksi_konjungtif(query)
        
        # Count cascade depth
        depth = 0
        current = result.query_tree
        while current.type == "FILTER":
            depth += 1
            if len(current.childs) > 0:
                current = current.get_child(0)
            else:
                break
        
        self.assertEqual(depth, 3)
    
    def test_selective_transformation(self):
        """Test transforming only specific operators"""
        relation = QueryTree("RELATION", "users")
        
        # First AND
        and_op1 = QueryTree("OPERATOR", "AND")
        and_op1.add_child(QueryTree("COMPARISON", ">"))
        and_op1.add_child(QueryTree("COMPARISON", "="))
        
        filter1 = QueryTree("FILTER")
        filter1.add_child(relation)
        filter1.add_child(and_op1)
        
        # Second AND
        and_op2 = QueryTree("OPERATOR", "AND")
        and_op2.add_child(QueryTree("COMPARISON", "<"))
        and_op2.add_child(QueryTree("IN_EXPR"))
        
        filter2 = QueryTree("FILTER")
        filter2.add_child(filter1)
        filter2.add_child(and_op2)
        
        query = ParsedQuery(filter2, "test")
        
        # Transform only second AND
        result = seleksi_konjungtif(query, [and_op2.id])
        
        # Top should be cascaded
        self.assertEqual(result.query_tree.type, "FILTER")
        
        # Find first AND - should still be OPERATOR(AND)
        current = result.query_tree
        found_and = False
        for _ in range(5):
            if current.type == "FILTER" and len(current.childs) == 2:
                cond = current.get_child(1)
                if cond.type == "OPERATOR" and cond.val == "AND":
                    found_and = True
                    break
            if len(current.childs) > 0:
                current = current.get_child(0)
            else:
                break
        
        self.assertTrue(found_and, "First AND should remain untransformed")


class TestCascadeFilters(unittest.TestCase):
    """Test cases untuk cascade_filters with mixed ordering"""
    
    def test_cascade_all_single(self):
        """Test cascade with all single conditions"""
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
        
        query = ParsedQuery(filter_node, "test")
        
        # Order: [2, 1, 0] - all single, reverse order
        operator_orders = {and_op.id: [2, 1, 0]}
        result = cascade_filters(query, operator_orders)
        
        # Should be fully cascaded
        depth = 0
        current = result.query_tree
        while current.type == "FILTER":
            depth += 1
            if len(current.childs) > 0:
                current = current.get_child(0)
            else:
                break
        
        self.assertEqual(depth, 3)
    
    def test_cascade_mixed_order(self):
        """Test cascade with mixed single/grouped conditions"""
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
        
        query = ParsedQuery(filter_node, "test")
        
        # Order: [2, [0, 1]] - comp3 single, then comp1 AND comp2 grouped
        operator_orders = {and_op.id: [2, [0, 1]]}
        result = cascade_filters(query, operator_orders)
        
        # Top should be FILTER with comp3
        self.assertEqual(result.query_tree.type, "FILTER")
        
        # Child 0 should be another FILTER
        inner = result.query_tree.get_child(0)
        self.assertEqual(inner.type, "FILTER")
        
        # Inner filter's condition should be OPERATOR(AND) with 2 conditions
        inner_cond = inner.get_child(1)
        self.assertEqual(inner_cond.type, "OPERATOR")
        self.assertEqual(inner_cond.val, "AND")
        self.assertEqual(len(inner_cond.childs), 2)
    
    def test_cascade_all_grouped(self):
        """Test cascade with all conditions in one group"""
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
        
        query = ParsedQuery(filter_node, "test")
        
        # Order: [[0, 1, 2]] - all grouped
        operator_orders = {and_op.id: [[0, 1, 2]]}
        result = cascade_filters(query, operator_orders)
        
        # Should have single FILTER with OPERATOR(AND)
        self.assertEqual(result.query_tree.type, "FILTER")
        self.assertEqual(len(result.query_tree.childs), 2)
        
        # Condition should still be OPERATOR(AND)
        cond = result.query_tree.get_child(1)
        self.assertEqual(cond.type, "OPERATOR")
        self.assertEqual(cond.val, "AND")
        self.assertEqual(len(cond.childs), 3)


class TestUncascadeFilters(unittest.TestCase):
    """Test cases untuk uncascade_filters"""
    
    def test_uncascade_simple(self):
        """Test uncascade of simple cascaded filters"""
        relation = QueryTree("RELATION", "users")
        comp1 = QueryTree("COMPARISON", ">")
        comp2 = QueryTree("COMPARISON", "=")
        
        # Create cascaded: FILTER(comp2) -> FILTER(comp1) -> RELATION
        filter1 = QueryTree("FILTER")
        filter1.add_child(relation)
        filter1.add_child(comp1)
        
        filter2 = QueryTree("FILTER")
        filter2.add_child(filter1)
        filter2.add_child(comp2)
        
        query = ParsedQuery(filter2, "test")
        result = uncascade_filters(query)
        
        # Should have FILTER with OPERATOR(AND)
        self.assertEqual(result.query_tree.type, "FILTER")
        self.assertEqual(len(result.query_tree.childs), 2)
        
        # Condition should be OPERATOR(AND)
        cond = result.query_tree.get_child(1)
        self.assertEqual(cond.type, "OPERATOR")
        self.assertEqual(cond.val, "AND")
        self.assertEqual(len(cond.childs), 2)
    
    def test_cascade_then_uncascade(self):
        """Test that cascade -> uncascade returns to AND structure"""
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
        
        query = ParsedQuery(filter_node, "test")
        
        # Cascade
        cascaded = seleksi_konjungtif(query)
        
        # Uncascade
        uncascaded = uncascade_filters(cascaded)
        
        # Should have FILTER with OPERATOR(AND)
        self.assertEqual(uncascaded.query_tree.type, "FILTER")
        
        # Find the AND operator
        found_and = False
        current = uncascaded.query_tree
        for _ in range(5):
            if current.type == "FILTER" and len(current.childs) == 2:
                cond = current.get_child(1)
                if cond.type == "OPERATOR" and cond.val == "AND":
                    found_and = True
                    self.assertGreaterEqual(len(cond.childs), 2)
                    break
            if len(current.childs) > 0:
                current = current.get_child(0)
            else:
                break
        
        self.assertTrue(found_and)


class TestCloneTree(unittest.TestCase):
    """Test cases untuk clone_tree"""
    
    def test_clone_simple(self):
        """Test cloning simple tree"""
        node = QueryTree("RELATION", "users")
        cloned = clone_tree(node)
        
        self.assertIsNot(node, cloned)
        self.assertEqual(node.type, cloned.type)
        self.assertEqual(node.val, cloned.val)
    
    def test_clone_with_children(self):
        """Test cloning tree with children"""
        relation = QueryTree("RELATION", "users")
        comp = QueryTree("COMPARISON", ">")
        
        filter_node = QueryTree("FILTER")
        filter_node.add_child(relation)
        filter_node.add_child(comp)
        
        cloned = clone_tree(filter_node)
        
        self.assertIsNot(filter_node, cloned)
        self.assertIsNot(filter_node.childs[0], cloned.childs[0])
        self.assertIsNot(filter_node.childs[1], cloned.childs[1])
        
        self.assertEqual(filter_node.type, cloned.type)
        self.assertEqual(len(filter_node.childs), len(cloned.childs))


if __name__ == "__main__":
    unittest.main()
