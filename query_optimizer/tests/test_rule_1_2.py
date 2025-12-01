"""
Unit tests untuk Rule 1 & Rule 2: Seleksi Konjungtif & Komutatif

Fitur:
- Rule 1: Cascade/Uncascade Filters
- Rule 2: Reorder Conditions (Vertical & Horizontal)
- Mocked Metadata: Tidak bergantung pada dummy data di check_query.py
"""

import unittest
from unittest.mock import patch, MagicMock
from query_optimizer.query_tree import QueryTree
from query_optimizer.optimization_engine import ParsedQuery
from query_optimizer.query_check import check_query
from query_optimizer.rule.rule_1_2 import (
    analyze_and_operators,
    generate_random_rule_1_params,
    copy_rule_1_params,
    mutate_rule_1_params,
    uncascade_filters,
    apply_rule1_rule2
)

# --- MOCK METADATA ---
# Metadata palsu agar check_query tidak error tanpa harus connect DB/Dummy asli
MOCK_METADATA = {
    "tables": ["users", "orders", "products"],
    "columns": {
        "users": ["id", "name", "age", "status", "score", "type"],
        "orders": ["id", "user_id", "amount", "status"],
        "products": ["id", "name", "price"]
    }
}

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
    """Test cases untuk helper functions (Params Manipulation)"""
    
    def test_copy_rule_1_params(self):
        """Test deep copy of rule parameters"""
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
        """Test random generation of rule parameters"""
        for num_conditions in range(2, 6):
            cond_ids = list(range(100, 100 + num_conditions))
            params = generate_random_rule_1_params(cond_ids)
            # Flatten to check coverage
            flat = []
            for item in params:
                if isinstance(item, list):
                    flat.extend(item)
                else:
                    flat.append(item)
            self.assertEqual(set(flat), set(cond_ids))
    
    def test_mutate_rule_1_params_mechanics(self):
        """Test mutation mechanics (Group/Ungroup/Swap)"""
        params = [0, 1, 2, 3]
        
        # Test distinct mutations over several iterations
        seen_changes = False
        for _ in range(20):
            mutated = mutate_rule_1_params(params.copy())
            if mutated != params:
                seen_changes = True
                break
        self.assertTrue(seen_changes, "Mutation should eventually change params")
        
        # Test preservation of IDs
        params_nested = [10, [20, 30], 40]
        original_set = {10, 20, 30, 40}
        
        mutated = mutate_rule_1_params(params_nested.copy())
        mutated_set = set()
        for item in mutated:
            if isinstance(item, list):
                mutated_set.update(item)
            else:
                mutated_set.add(item)
        self.assertEqual(original_set, mutated_set, "Mutation must preserve all Condition IDs")


@patch('query_optimizer.query_check.get_metadata', return_value=MOCK_METADATA)
class TestAnalyzeAndOperators(unittest.TestCase):
    """Test cases untuk analyze_and_operators (Signature Detection)"""
    
    def test_analyze_merged_chain(self, mock_meta):
        """Test detecting signature from chained filters (Rule 1 logic)"""
        relation = QueryTree("RELATION", "users")
        
        # Filter 1
        c1 = QueryTree("COMPARISON", ">"); c1.id = 1
        filter1 = QueryTree("FILTER")
        filter1.add_child(relation)
        filter1.add_child(c1)
        
        # Filter 2 (Nested)
        c2 = QueryTree("COMPARISON", "="); c2.id = 2
        filter2 = QueryTree("FILTER")
        filter2.add_child(filter1)
        filter2.add_child(c2)
        
        query = ParsedQuery(filter2, "test")
        operators = analyze_and_operators(query)
        
        # Analyzer should see this as one group [1, 2] due to aggressive uncascading
        self.assertEqual(len(operators), 1)
        sig = list(operators.keys())[0]
        self.assertEqual(sig, frozenset([1, 2]))


@patch('query_optimizer.query_check.get_metadata', return_value=MOCK_METADATA)
class TestRule1Cascade(unittest.TestCase):
    """Test Cases for Rule 1 (Splitting/Cascading Conditions)"""
    
    def setUp(self):
        self.relation = QueryTree("RELATION", "users")
        # Valid columns from MOCK_METADATA: age, status
        self.c1 = make_comparison(">", "age", 18); self.c1.id = 1
        self.c2 = make_comparison("=", "status", "active"); self.c2.id = 2
        
        # Initial: 1 Filter with AND
        self.and_op = QueryTree("OPERATOR", "AND")
        self.and_op.add_child(self.c1)
        self.and_op.add_child(self.c2)
        
        self.filter_node = QueryTree("FILTER")
        self.filter_node.add_child(self.relation)
        self.filter_node.add_child(self.and_op)
        
        self.query = ParsedQuery(self.filter_node, "test")
        self.sig = frozenset([1, 2])

    def test_full_cascade(self, mock_meta):
        """Rule 1: Split AND(1,2) into Filter(1) -> Filter(2)"""
        # Params: Flat list [1, 2] means separate filters
        # Order: [1, 2]. Reversed build -> Filter(2) is Top, Filter(1) is Bottom.
        params = {self.sig: [1, 2]}
        
        result, _ = apply_rule1_rule2(self.query, params)
        check_query(result.query_tree) # Validasi menggunakan Mock Metadata
        
        # Check Structure
        # Top Node = FILTER
        self.assertEqual(result.query_tree.type, "FILTER")
        
        # Child 0 = FILTER (Cascaded)
        inner = result.query_tree.childs[0]
        self.assertEqual(inner.type, "FILTER")
        
        # Child 0 of Inner = RELATION
        self.assertEqual(inner.childs[0].type, "RELATION")
        
        # Check Logic: No AND operator anymore
        # Top Condition
        top_cond = result.query_tree.childs[1]
        self.assertNotEqual(top_cond.type, "OPERATOR") 
        
        # Inner Condition
        inner_cond = inner.childs[1]
        self.assertNotEqual(inner_cond.type, "OPERATOR")

    def test_full_grouping(self, mock_meta):
        """Rule 1: Keep/Make AND(1,2) in single Filter"""
        # Params: Nested list [[1, 2]]
        params = {self.sig: [[1, 2]]}
        
        result, _ = apply_rule1_rule2(self.query, params)
        check_query(result.query_tree)
        
        # Check Structure: Single Filter
        self.assertEqual(result.query_tree.type, "FILTER")
        self.assertEqual(result.query_tree.childs[0].type, "RELATION")
        
        # Condition should be AND
        cond = result.query_tree.childs[1]
        self.assertEqual(cond.type, "OPERATOR")
        self.assertEqual(cond.val, "AND")
        self.assertEqual(len(cond.childs), 2)


@patch('query_optimizer.query_check.get_metadata', return_value=MOCK_METADATA)
class TestRule2Reorder(unittest.TestCase):
    """Test Cases for Rule 2 (Commutative / Reordering)"""
    
    def setUp(self):
        self.relation = QueryTree("RELATION", "users")
        # Use IDs for tracking
        self.c1 = make_comparison(">", "age", 18); self.c1.id = 10
        self.c2 = make_comparison("=", "status", "active"); self.c2.id = 20
        self.c3 = make_comparison(">", "score", 100); self.c3.id = 30
        
        # Base: AND(10, 20, 30)
        self.and_op = QueryTree("OPERATOR", "AND")
        self.and_op.add_child(self.c1)
        self.and_op.add_child(self.c2)
        self.and_op.add_child(self.c3)
        
        self.filter_node = QueryTree("FILTER")
        self.filter_node.add_child(self.relation)
        self.filter_node.add_child(self.and_op)
        
        self.query = ParsedQuery(self.filter_node, "test")
        self.sig = frozenset([10, 20, 30])

    def test_vertical_reordering(self, mock_meta):
        """Rule 2: Vertical Swap (Filter Order)"""
        # Scenario A: [10, 20, 30]
        # Build logic uses reversed list.
        # Reversed: 30, 20, 10
        # 1. Filter(30) created (Bottom)
        # 2. Filter(20) created (Middle)
        # 3. Filter(10) created (Top)
        # So Result: Top=10, Mid=20, Bottom=30
        
        params_a = {self.sig: [10, 20, 30]}
        res_a, _ = apply_rule1_rule2(self.query, params_a)
        
        top_a = res_a.query_tree
        mid_a = top_a.childs[0]
        bot_a = mid_a.childs[0]
        
        self.assertEqual(top_a.childs[1].id, 10)
        self.assertEqual(mid_a.childs[1].id, 20)
        self.assertEqual(bot_a.childs[1].id, 30)
        
        # Scenario B: Swap to [30, 10, 20]
        # Expect: Top=30, Mid=10, Bottom=20
        params_b = {self.sig: [30, 10, 20]}
        res_b, _ = apply_rule1_rule2(self.query, params_b)
        
        top_b = res_b.query_tree
        mid_b = top_b.childs[0]
        bot_b = mid_b.childs[0]
        
        self.assertEqual(top_b.childs[1].id, 30)
        self.assertEqual(mid_b.childs[1].id, 10)
        self.assertEqual(bot_b.childs[1].id, 20)

    def test_horizontal_reordering(self, mock_meta):
        """Rule 2: Horizontal Swap (Inside AND)"""
        # Params: [[10, 20, 30]] -> Single Filter, AND children order
        params_normal = {self.sig: [[10, 20, 30]]}
        res_normal, _ = apply_rule1_rule2(self.query, params_normal)
        
        and_node = res_normal.query_tree.childs[1]
        self.assertEqual([c.id for c in and_node.childs], [10, 20, 30])
        
        # Params: [[30, 10, 20]] -> Swapped inside AND
        params_swapped = {self.sig: [[30, 10, 20]]}
        res_swapped, _ = apply_rule1_rule2(self.query, params_swapped)
        
        and_node_swapped = res_swapped.query_tree.childs[1]
        self.assertEqual([c.id for c in and_node_swapped.childs], [30, 10, 20])

    def test_mixed_reordering(self, mock_meta):
        """Rule 1 & 2: Mixed Cascade and Reorder"""
        # Params: [20, [30, 10]]
        # Expect: Top=20, Bottom=AND(30, 10)
        params = {self.sig: [20, [30, 10]]}
        result, _ = apply_rule1_rule2(self.query, params)
        check_query(result.query_tree)
        
        # Top Filter
        top_cond = result.query_tree.childs[1]
        self.assertEqual(top_cond.id, 20)
        
        # Bottom Filter
        bottom_filter = result.query_tree.childs[0]
        bottom_cond = bottom_filter.childs[1]
        
        self.assertEqual(bottom_cond.type, "OPERATOR")
        self.assertEqual(bottom_cond.val, "AND")
        self.assertEqual([c.id for c in bottom_cond.childs], [30, 10])


@patch('query_optimizer.query_check.get_metadata', return_value=MOCK_METADATA)
class TestUncascade(unittest.TestCase):
    """Test Aggressive Uncascading (Merging filters)"""
    
    def test_uncascade_filters(self, mock_meta):
        """Test merging separate FILTER nodes into one"""
        relation = QueryTree("RELATION", "users")
        c1 = make_comparison(">", "age", 18); c1.id = 1
        c2 = make_comparison("=", "status", "active"); c2.id = 2
        
        # Tree: Filter(2) -> Filter(1) -> Relation
        filter1 = QueryTree("FILTER")
        filter1.add_child(relation)
        filter1.add_child(c1)
        
        filter2 = QueryTree("FILTER")
        filter2.add_child(filter1)
        filter2.add_child(c2)
        
        query = ParsedQuery(filter2, "test")
        
        # Apply Uncascade
        result = uncascade_filters(query)
        check_query(result.query_tree)
        
        # Expect Single Filter with AND(2, 1) or AND(1, 2)
        # Logic: collects while walking down. c2 is top, c1 is bottom.
        # Collection usually appends.
        self.assertEqual(result.query_tree.type, "FILTER")
        self.assertEqual(result.query_tree.childs[0].type, "RELATION")
        
        cond = result.query_tree.childs[1]
        self.assertEqual(cond.type, "OPERATOR")
        self.assertEqual(cond.val, "AND")
        
        ids = {c.id for c in cond.childs}
        self.assertEqual(ids, {1, 2})


if __name__ == "__main__":
    unittest.main()