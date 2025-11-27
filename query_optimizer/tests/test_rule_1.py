"""
Unit tests untuk Rule 1: Seleksi Konjungtif (Signature-Based Edition)
Testing transformasi FILTER dengan OPERATOR(AND) menjadi cascaded filters

Updated for:
- Signature Keys (frozenset) instead of Node IDs
- Aggressive Uncascading (Merging chains)
"""

import unittest
from query_optimizer.query_tree import QueryTree
from query_optimizer.optimization_engine import ParsedQuery
from query_optimizer.query_check import check_query
from query_optimizer.rule.rule_1_2 import (
    analyze_and_operators,
    generate_random_rule_1_params,
    copy_rule_1_params,
    mutate_rule_1_params,
    uncascade_filters,
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
    right = QueryTree("LITERAL_NUMBER", right_val)
    
    comp.add_child(left)
    comp.add_child(right)
    return comp


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
    
    def test_generate_random_rule_1_params(self):
        """Test random generation of rule_1_params"""
        # Simulate condition IDs as [100, 101, 102]
        for num_conditions in range(2, 6):
            cond_ids = list(range(100, 100 + num_conditions))
            params = generate_random_rule_1_params(cond_ids)
            # Flatten
            flat = []
            for item in params:
                if isinstance(item, list):
                    flat.extend(item)
                else:
                    flat.append(item)
            self.assertEqual(set(flat), set(cond_ids))
    
    def test_mutate_rule_1_params_group(self):
        """Test mutate with group operation"""
        # All singles: [0, 1, 2] -> should be able to group adjacent
        params = [0, 1, 2]
        
        grouped = False
        for _ in range(30):
            mutated = mutate_rule_1_params(params.copy())
            if any(isinstance(item, list) for item in mutated):
                grouped = True
                break
        
        self.assertTrue(grouped, "Should be able to group singles")
    
    def test_mutate_rule_1_params_ungroup(self):
        """Test mutate with ungroup operation"""
        # Has group: [2, [0, 1]] -> should be able to ungroup
        params = [2, [0, 1]]
        
        ungrouped = False
        for _ in range(30):
            mutated = mutate_rule_1_params(params.copy())
            if all(not isinstance(item, list) for item in mutated):
                ungrouped = True
                break
        
        self.assertTrue(ungrouped, "Should be able to ungroup")
    
    def test_mutate_preserves_ids(self):
        """Test that mutation preserves all condition IDs"""
        params = [102, [100, 101], 103]
        original_ids = set()
        for item in params:
            if isinstance(item, list):
                original_ids.update(item)
            else:
                original_ids.add(item)
        
        for _ in range(10):
            mutated = mutate_rule_1_params(params.copy())
            mutated_ids = set()
            for item in mutated:
                if isinstance(item, list):
                    mutated_ids.update(item)
                else:
                    mutated_ids.add(item)
            self.assertEqual(original_ids, mutated_ids, "Mutation should preserve all IDs")


class TestAnalyzeAndOperators(unittest.TestCase):
    """Test cases untuk analyze_and_operators (Signature Based)"""
    
    def test_analyze_simple_and(self):
        """Test analyze single FILTER with OPERATOR(AND)"""
        relation = QueryTree("RELATION", "users")
        comp1 = QueryTree("COMPARISON", ">")
        comp1.id = 100
        comp2 = QueryTree("COMPARISON", "=")
        comp2.id = 101
        
        and_op = QueryTree("OPERATOR", "AND")
        and_op.add_child(comp1)
        and_op.add_child(comp2)
        
        filter_node = QueryTree("FILTER")
        filter_node.add_child(relation)
        filter_node.add_child(and_op)
        
        query = ParsedQuery(filter_node, "test")
        operators = analyze_and_operators(query)
        
        # Expect 1 signature
        self.assertEqual(len(operators), 1)
        
        # Get the signature (frozenset)
        sig = list(operators.keys())[0]
        self.assertTrue(isinstance(sig, frozenset))
        self.assertEqual(sig, frozenset([100, 101]))
        
        # Check value (list of ids)
        self.assertEqual(len(operators[sig]), 2)
    
    def test_analyze_multiple_and_merged(self):
        """Test analyze multiple ADJACENT FILTER nodes - SHOULD MERGE"""
        # Rule 1 new logic: Aggressive Uncascade merges consecutive filters
        relation = QueryTree("RELATION", "users")
        
        # Inner AND
        and_op1 = QueryTree("OPERATOR", "AND")
        c1 = QueryTree("COMPARISON", ">"); c1.id = 1
        c2 = QueryTree("COMPARISON", "="); c2.id = 2
        and_op1.add_child(c1); and_op1.add_child(c2)
        
        filter1 = QueryTree("FILTER")
        filter1.add_child(relation)
        filter1.add_child(and_op1)
        
        # Outer AND
        and_op2 = QueryTree("OPERATOR", "AND")
        c3 = QueryTree("COMPARISON", "!="); c3.id = 3
        c4 = QueryTree("IN_EXPR", ""); c4.id = 4
        and_op2.add_child(c3); and_op2.add_child(c4)
        
        filter2 = QueryTree("FILTER")
        filter2.add_child(filter1)
        filter2.add_child(and_op2)
        
        query = ParsedQuery(filter2, "test")
        operators = analyze_and_operators(query)
        
        # Expect 1 MERGED signature containing all 4 conditions
        self.assertEqual(len(operators), 1)
        
        sig = list(operators.keys())[0]
        self.assertEqual(sig, frozenset([1, 2, 3, 4]))
    
    def test_analyze_separated_filters(self):
        """Test analyze separated FILTER nodes - SHOULD NOT MERGE"""
        relation = QueryTree("RELATION", "users")
        
        # Filter 1
        c1 = QueryTree("COMPARISON", ">"); c1.id = 1
        filter1 = QueryTree("FILTER")
        filter1.add_child(relation)
        filter1.add_child(c1)
        
        # Separator (e.g., PROJECT)
        project = QueryTree("PROJECT")
        project.add_child(filter1)
        
        # Filter 2
        c2 = QueryTree("COMPARISON", "="); c2.id = 2
        filter2 = QueryTree("FILTER")
        filter2.add_child(project)
        filter2.add_child(c2)
        
        query = ParsedQuery(filter2, "test")
        operators = analyze_and_operators(query)
        
        # Expect 2 Separate signatures
        self.assertEqual(len(operators), 2)
        self.assertTrue(frozenset([1]) in operators)
        self.assertTrue(frozenset([2]) in operators)


class TestSeleksiKonjunktif(unittest.TestCase):
    """Test cases untuk seleksi_konjungtif transformation"""
    
    def test_simple_two_conditions(self):
        """Test basic transformation with 2 conditions"""
        relation = QueryTree("RELATION", "users")
        comp1 = make_comparison(">", "age", 18); comp1.id = 10
        comp2 = make_comparison("=", "status", "active"); comp2.id = 20
        
        and_op = QueryTree("OPERATOR", "AND")
        and_op.add_child(comp1)
        and_op.add_child(comp2)
        
        filter_node = QueryTree("FILTER")
        filter_node.add_child(relation)
        filter_node.add_child(and_op)
        
        query = ParsedQuery(filter_node, "test")
        
        # Analyze using Signature
        op_map = analyze_and_operators(query)
        self.assertEqual(len(op_map), 1)
        sig = list(op_map.keys())[0]
        
        # Use generated order
        order = op_map[sig]
        params = {sig: order}
        
        from query_optimizer.rule.rule_1_2 import apply_rule1_rule2
        result, _ = apply_rule1_rule2(query, params)
        
        check_query(result.query_tree)
        
        # Validate Cascading Structure
        self.assertEqual(result.query_tree.type, "FILTER")
        self.assertEqual(len(result.query_tree.childs), 2)
        
        inner = result.query_tree.get_child(0)
        self.assertEqual(inner.type, "FILTER")
    
    def test_selective_transformation(self):
        """Test transforming specific operators when they are SEPARATED"""
        relation = QueryTree("RELATION", "users")
        
        # Filter 1 (Bottom)
        c1 = make_comparison(">", "age", 18); c1.id = 1
        c2 = make_comparison("=", "status", "active"); c2.id = 2
        and1 = QueryTree("OPERATOR", "AND")
        and1.add_child(c1); and1.add_child(c2)
        
        filter1 = QueryTree("FILTER")
        filter1.add_child(relation)
        filter1.add_child(and1)
        
        # Separator to prevent merging
        project = QueryTree("PROJECT")
        project.add_child(filter1)
        
        # Filter 2 (Top)
        c3 = make_comparison("<", "score", 100); c3.id = 3
        c4 = make_comparison("!=", "type", "banned"); c4.id = 4
        and2 = QueryTree("OPERATOR", "AND")
        and2.add_child(c3); and2.add_child(c4)
        
        filter2 = QueryTree("FILTER")
        filter2.add_child(project)
        filter2.add_child(and2)
        
        query = ParsedQuery(filter2, "test")
        op_map = analyze_and_operators(query)
        
        # Should have 2 signatures
        self.assertEqual(len(op_map), 2)
        
        # We only optimize Filter 2 ({3, 4})
        sig_target = frozenset([3, 4])
        self.assertIn(sig_target, op_map)
        
        # Define params only for Filter 2
        params = {sig_target: [3, 4]} # Reorder or keep as is, but explicit param triggers cascade
        
        from query_optimizer.rule.rule_1_2 import apply_rule1_rule2
        result, _ = apply_rule1_rule2(query, params)
        
        check_query(result.query_tree)
        
        # Filter 2 (Top) should be cascaded (FILTER -> FILTER)
        top = result.query_tree
        self.assertEqual(top.type, "FILTER")
        # Check if top filter is cascaded:
        # If cascaded: Top Filter -> Child is Filter containing 3 or 4
        # Since param is [3, 4], bottom is 3, top is 4.
        # Top Filter(cond=4) -> Inner Filter(cond=3) -> Project
        
        inner = top.get_child(0)
        self.assertEqual(inner.type, "FILTER")
        
        # Check Separator
        proj_node = inner.get_child(0)
        self.assertEqual(proj_node.type, "PROJECT")


class TestCascadeFilters(unittest.TestCase):
    """Test cases untuk cascade_filters logic"""
    
    def test_cascade_mixed_order(self):
        """Test cascade with mixed single/grouped conditions"""
        relation = QueryTree("RELATION", "users")
        c1 = make_comparison(">", "age", 18); c1.id = 1
        c2 = make_comparison("=", "status", "active"); c2.id = 2
        c3 = make_comparison("<", "score", 100); c3.id = 3
        
        and_op = QueryTree("OPERATOR", "AND")
        and_op.add_child(c1); and_op.add_child(c2); and_op.add_child(c3)
        
        filter_node = QueryTree("FILTER")
        filter_node.add_child(relation)
        filter_node.add_child(and_op)
        
        query = ParsedQuery(filter_node, "test")
        op_map = analyze_and_operators(query)
        sig = list(op_map.keys())[0]
        
        # Order: [c3, [c1, c2]] (Bottom to Top stack means: c3 on top, group(c1,c2) below?
        # Wait, cascade builds from bottom up. 
        # cascade_mixed_signature iterates reversed(order).
        # If order = [A, B], reversed = [B, A].
        # 1. New Filter with A. Current = Filter(A)
        # 2. New Filter with B on top of Current. Result = Filter(B) -> Filter(A)
        # So List Order [A, B] results in Top=B, Bottom=A.
        # Ideally usually [Top, Middle, Bottom].
        
        # Let's try order = [3, [1, 2]]
        # Expectation: Top=3, Bottom=[1, 2]
        
        order = [3, [1, 2]]
        params = {sig: order}
        
        from query_optimizer.rule.rule_1_2 import apply_rule1_rule2
        result, _ = apply_rule1_rule2(query, params)
        
        check_query(result.query_tree)
        
        # Top Filter -> cond 3 ? 
        # Logic: reversed([3, [1,2]]) -> [[1,2], 3]
        # 1. Filter([1,2]) created.
        # 2. Filter(3) created on top.
        # So Top cond is 3. Correct.
        
        top_cond = result.query_tree.get_child(1)
        # 3 is single comparison (or AND with 1 child if implementation wraps it, but signatures usually keep IDs)
        # implementation cascade_mixed_signature:
        # if item is single: add child node from id_map.
        self.assertEqual(top_cond.id, 3)
        
        inner = result.query_tree.get_child(0)
        inner_cond = inner.get_child(1)
        # Inner cond should be AND(1, 2)
        self.assertEqual(inner_cond.type, "OPERATOR")
        self.assertEqual(inner_cond.val, "AND")
        self.assertEqual(len(inner_cond.childs), 2)


class TestUncascadeFilters(unittest.TestCase):
    """Test cases untuk uncascade_filters (Aggressive)"""
    
    def test_uncascade_simple(self):
        """Test uncascade of simple cascaded filters into ONE giant AND"""
        relation = QueryTree("RELATION", "users")
        c1 = make_comparison(">", "age", 18); c1.id = 1
        c2 = make_comparison("=", "status", "active"); c2.id = 2
        
        # FILTER(c2) -> FILTER(c1) -> RELATION
        filter1 = QueryTree("FILTER")
        filter1.add_child(relation)
        filter1.add_child(c1)
        
        filter2 = QueryTree("FILTER")
        filter2.add_child(filter1)
        filter2.add_child(c2)
        
        query = ParsedQuery(filter2, "test")
        result = uncascade_filters(query)
        
        check_query(result.query_tree)
        
        # Should have 1 FILTER with OPERATOR(AND) containing c2 and c1
        self.assertEqual(result.query_tree.type, "FILTER")
        self.assertEqual(len(result.query_tree.childs), 2)
        
        cond = result.query_tree.get_child(1)
        self.assertEqual(cond.type, "OPERATOR")
        self.assertEqual(cond.val, "AND")
        self.assertEqual(len(cond.childs), 2)
        
        # Verify IDs present
        ids = {c.id for c in cond.childs}
        self.assertEqual(ids, {1, 2})

if __name__ == "__main__":
    unittest.main()