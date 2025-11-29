"""
Unit tests untuk Rule 7: Seleksi Push Down ke Join (Filter Pushdown over Join)
Menggunakan Mock Metadata untuk isolasi pengujian.
"""

import unittest
from unittest.mock import patch, MagicMock
from query_optimizer.query_tree import QueryTree
from query_optimizer.optimization_engine import ParsedQuery
from query_optimizer.query_check import check_query
from query_optimizer.rule.rule_7 import (
    find_patterns,
    is_pushable,
    apply_pushdown,
    decide_pushdown,
    collect_tables,
    push_filter,
    wrap_filter,
    undo_pushdown,
    generate_params,
    copy_params,
    mutate_params,
    validate_params
)

# --- MOCK METADATA ---
MOCK_METADATA = {
    "tables": ["users", "profiles", "orders", "table1", "table2"],
    "columns": {
        "users": ["id", "name", "age", "status"],
        "profiles": ["id", "user_id", "verified", "bio"],
        "orders": ["id", "user_id", "amount", "status", "discount"],
        "table1": ["id", "name"],
        "table2": ["id", "description"]
    }
}

def make_column_ref(col_name: str, table_name: str = None):
    """Helper to create COLUMN_REF node"""
    col_ref = QueryTree("COLUMN_REF", "")
    col_name_node = QueryTree("COLUMN_NAME", "")
    identifier = QueryTree("IDENTIFIER", col_name)
    col_name_node.add_child(identifier)
    col_ref.add_child(col_name_node)
    
    if table_name:
        table_node = QueryTree("TABLE_NAME", "")
        table_id = QueryTree("IDENTIFIER", table_name)
        table_node.add_child(table_id)
        col_ref.add_child(table_node)
    
    return col_ref


def make_comparison(operator: str, left_col: str, right_val: str, left_table: str = None, is_literal: bool = True):
    """Helper to create COMPARISON node"""
    comp = QueryTree("COMPARISON", operator)
    
    # Left side: COLUMN_REF
    left = make_column_ref(left_col, left_table)
    
    # Right side: LITERAL or COLUMN_REF
    if is_literal:
        if right_val.isdigit():
            right = QueryTree("LITERAL_NUMBER", right_val)
        else:
            right = QueryTree("LITERAL_STRING", right_val)
    else:
        right = make_column_ref(right_val, left_table)
    
    comp.add_child(left)
    comp.add_child(right)
    return comp


def make_join_condition(left_col: str, right_col: str, left_table: str, right_table: str):
    """Helper to create join condition"""
    comp = QueryTree("COMPARISON", "=")
    left = make_column_ref(left_col, left_table)
    right = make_column_ref(right_col, right_table)
    comp.add_child(left)
    comp.add_child(right)
    return comp


@patch('query_optimizer.query_check.get_metadata', return_value=MOCK_METADATA)
class TestFindPatterns(unittest.TestCase):
    """Test detection of FILTER → JOIN patterns"""
    
    def test_find_single_filter_over_join(self, mock_meta):
        """Test finding single filter over join"""
        # Build: FILTER → JOIN
        rel1 = QueryTree("RELATION", "users")
        rel2 = QueryTree("RELATION", "profiles")
        
        join_cond = make_join_condition("id", "user_id", "users", "profiles")
        
        join = QueryTree("JOIN", "INNER")
        join.add_child(rel1)
        join.add_child(rel2)
        join.add_child(join_cond)
        
        # Filter condition: users.age > 18
        filter_cond = make_comparison(">", "age", "18", "users")
        
        filter_node = QueryTree("FILTER", "")
        filter_node.add_child(join)
        filter_node.add_child(filter_cond)
        
        query = ParsedQuery(filter_node, "SELECT * FROM users JOIN profiles WHERE age > 18")
        
        patterns = find_patterns(query)
        
        self.assertEqual(len(patterns), 1)
        self.assertIn(filter_node.id, patterns)
        self.assertEqual(patterns[filter_node.id]['join_id'], join.id)
        self.assertEqual(patterns[filter_node.id]['num_conditions'], 1)
        self.assertFalse(patterns[filter_node.id]['has_and'])
    
    def test_find_filter_with_and_over_join(self, mock_meta):
        """Test finding filter with AND over join"""
        # Build: FILTER → JOIN with AND conditions
        rel1 = QueryTree("RELATION", "users")
        rel2 = QueryTree("RELATION", "orders")
        
        join_cond = make_join_condition("id", "user_id", "users", "orders")
        
        join = QueryTree("JOIN", "INNER")
        join.add_child(rel1)
        join.add_child(rel2)
        join.add_child(join_cond)
        
        # Multiple filter conditions
        cond1 = make_comparison(">", "age", "18", "users")
        cond2 = make_comparison("=", "status", "active", "orders")
        cond3 = make_comparison(">", "amount", "1000", "orders")
        
        and_op = QueryTree("OPERATOR", "AND")
        and_op.add_child(cond1)
        and_op.add_child(cond2)
        and_op.add_child(cond3)
        
        filter_node = QueryTree("FILTER", "")
        filter_node.add_child(join)
        filter_node.add_child(and_op)
        
        query = ParsedQuery(filter_node, "SELECT * FROM users JOIN orders WHERE age > 18 AND status = 'active'")
        
        patterns = find_patterns(query)
        
        self.assertEqual(len(patterns), 1)
        self.assertIn(filter_node.id, patterns)
        self.assertEqual(patterns[filter_node.id]['num_conditions'], 3)
        self.assertTrue(patterns[filter_node.id]['has_and'])
    
    def test_no_pattern_found(self, mock_meta):
        """Test when there's no FILTER → JOIN pattern"""
        # Just a relation
        rel = QueryTree("RELATION", "users")
        query = ParsedQuery(rel, "SELECT * FROM users")
        
        patterns = find_patterns(query)
        
        self.assertEqual(len(patterns), 0)


class TestIsPushable(unittest.TestCase):
    """Test conditions for pushable filters"""
    
    def test_is_pushable_valid(self):
        """Test valid FILTER → JOIN"""
        rel1 = QueryTree("RELATION", "table1")
        rel2 = QueryTree("RELATION", "table2")
        
        join = QueryTree("JOIN", "INNER")
        join.add_child(rel1)
        join.add_child(rel2)
        
        cond = make_comparison("=", "id", "1")
        
        filter_node = QueryTree("FILTER", "")
        filter_node.add_child(join)
        filter_node.add_child(cond)
        
        self.assertTrue(is_pushable(filter_node))
    
    def test_not_pushable_no_filter(self):
        """Test non-FILTER node"""
        rel = QueryTree("RELATION", "table1")
        self.assertFalse(is_pushable(rel))
    
    def test_not_pushable_no_join_child(self):
        """Test FILTER without JOIN child"""
        rel = QueryTree("RELATION", "table1")
        cond = make_comparison("=", "id", "1")
        
        filter_node = QueryTree("FILTER", "")
        filter_node.add_child(rel)
        filter_node.add_child(cond)
        
        self.assertFalse(is_pushable(filter_node))
    
    def test_not_pushable_invalid_structure(self):
        """Test FILTER with wrong number of children"""
        filter_node = QueryTree("FILTER", "")
        self.assertFalse(is_pushable(filter_node))


class TestCollectTables(unittest.TestCase):
    """Test table collection from tree"""
    
    def test_collect_single_relation(self):
        """Test collecting single RELATION"""
        rel = QueryTree("RELATION", "users")
        tables = collect_tables(rel)
        
        self.assertIn("users", tables)
        self.assertEqual(len(tables), 1)
    
    def test_collect_from_column_ref(self):
        """Test collecting table from COLUMN_REF"""
        col_ref = make_column_ref("id", "users")
        tables = collect_tables(col_ref)
        
        self.assertIn("users", tables)
    
    def test_collect_from_comparison(self):
        """Test collecting tables from COMPARISON"""
        comp = make_comparison(">", "age", "18", "users")
        tables = collect_tables(comp)
        
        self.assertIn("users", tables)
    
    def test_collect_multiple_tables(self):
        """Test collecting multiple tables from join"""
        rel1 = QueryTree("RELATION", "users")
        rel2 = QueryTree("RELATION", "profiles")
        
        join = QueryTree("JOIN", "INNER")
        join.add_child(rel1)
        join.add_child(rel2)
        
        tables = collect_tables(join)
        
        self.assertIn("users", tables)
        self.assertIn("profiles", tables)
        self.assertEqual(len(tables), 2)


class TestDecidePushdown(unittest.TestCase):
    """Test pushdown decision logic"""
    
    def test_decide_pushdown_left_only(self):
        """Test when filter can only be pushed to left"""
        rel1 = QueryTree("RELATION", "users")
        rel2 = QueryTree("RELATION", "profiles")
        
        join_cond = make_join_condition("id", "user_id", "users", "profiles")
        
        join = QueryTree("JOIN", "INNER")
        join.add_child(rel1)
        join.add_child(rel2)
        join.add_child(join_cond)
        
        # Filter only references users table
        filter_cond = make_comparison(">", "age", "18", "users")
        
        filter_node = QueryTree("FILTER", "")
        filter_node.add_child(join)
        filter_node.add_child(filter_cond)
        
        plan = decide_pushdown(filter_node)
        
        self.assertEqual(plan['distribution'], 'left')
        self.assertEqual(plan['left_conditions'], [0])
        self.assertEqual(plan['right_conditions'], [])
    
    def test_decide_pushdown_right_only(self):
        """Test when filter can only be pushed to right"""
        rel1 = QueryTree("RELATION", "users")
        rel2 = QueryTree("RELATION", "profiles")
        
        join_cond = make_join_condition("id", "user_id", "users", "profiles")
        
        join = QueryTree("JOIN", "INNER")
        join.add_child(rel1)
        join.add_child(rel2)
        join.add_child(join_cond)
        
        # Filter only references profiles table
        filter_cond = make_comparison("=", "verified", "true", "profiles")
        
        filter_node = QueryTree("FILTER", "")
        filter_node.add_child(join)
        filter_node.add_child(filter_cond)
        
        plan = decide_pushdown(filter_node)
        
        self.assertEqual(plan['distribution'], 'right')
        self.assertEqual(plan['left_conditions'], [])
        self.assertEqual(plan['right_conditions'], [0])
    
    def test_decide_pushdown_both(self):
        """Test when filters can be pushed to both sides"""
        rel1 = QueryTree("RELATION", "users")
        rel2 = QueryTree("RELATION", "orders")
        
        join_cond = make_join_condition("id", "user_id", "users", "orders")
        
        join = QueryTree("JOIN", "INNER")
        join.add_child(rel1)
        join.add_child(rel2)
        join.add_child(join_cond)
        
        # Multiple conditions for both tables
        cond1 = make_comparison(">", "age", "18", "users")
        cond2 = make_comparison(">", "amount", "1000", "orders")
        
        and_op = QueryTree("OPERATOR", "AND")
        and_op.add_child(cond1)
        and_op.add_child(cond2)
        
        filter_node = QueryTree("FILTER", "")
        filter_node.add_child(join)
        filter_node.add_child(and_op)
        
        plan = decide_pushdown(filter_node)
        
        self.assertEqual(plan['distribution'], 'both')
        self.assertIn(0, plan['left_conditions'])
        self.assertIn(1, plan['right_conditions'])
    
    def test_decide_pushdown_none(self):
        """Test when filter cannot be pushed (references both tables)"""
        rel1 = QueryTree("RELATION", "users")
        rel2 = QueryTree("RELATION", "orders")
        
        join_cond = make_join_condition("id", "user_id", "users", "orders")
        
        join = QueryTree("JOIN", "INNER")
        join.add_child(rel1)
        join.add_child(rel2)
        join.add_child(join_cond)
        
        # Condition that references both tables (like a join condition in WHERE)
        filter_cond = QueryTree("COMPARISON", ">")
        left_col = make_column_ref("age", "users")
        right_col = make_column_ref("discount", "orders")
        filter_cond.add_child(left_col)
        filter_cond.add_child(right_col)
        
        filter_node = QueryTree("FILTER", "")
        filter_node.add_child(join)
        filter_node.add_child(filter_cond)
        
        plan = decide_pushdown(filter_node)
        
        self.assertEqual(plan['distribution'], 'none')


class TestPushFilter(unittest.TestCase):
    """Test filter pushing transformation"""
    
    def test_push_to_left(self):
        """Test pushing filter to left side only"""
        rel1 = QueryTree("RELATION", "users")
        rel2 = QueryTree("RELATION", "profiles")
        
        join_cond = make_join_condition("id", "user_id", "users", "profiles")
        
        join = QueryTree("JOIN", "INNER")
        join.add_child(rel1)
        join.add_child(rel2)
        join.add_child(join_cond)
        
        filter_cond = make_comparison(">", "age", "18", "users")
        
        filter_node = QueryTree("FILTER", "")
        filter_node.add_child(join)
        filter_node.add_child(filter_cond)
        
        plan = {
            'distribution': 'left',
            'left_conditions': [0],
            'right_conditions': []
        }
        
        result = push_filter(filter_node, plan)
        
        # Result should be JOIN with FILTER on left child
        self.assertEqual(result.type, "JOIN")
        self.assertEqual(result.childs[0].type, "FILTER")
        self.assertEqual(result.childs[1].type, "RELATION")
    
    def test_push_to_right(self):
        """Test pushing filter to right side only"""
        rel1 = QueryTree("RELATION", "users")
        rel2 = QueryTree("RELATION", "profiles")
        
        join_cond = make_join_condition("id", "user_id", "users", "profiles")
        
        join = QueryTree("JOIN", "INNER")
        join.add_child(rel1)
        join.add_child(rel2)
        join.add_child(join_cond)
        
        filter_cond = make_comparison("=", "verified", "true", "profiles")
        
        filter_node = QueryTree("FILTER", "")
        filter_node.add_child(join)
        filter_node.add_child(filter_cond)
        
        plan = {
            'distribution': 'right',
            'left_conditions': [],
            'right_conditions': [0]
        }
        
        result = push_filter(filter_node, plan)
        
        # Result should be JOIN with FILTER on right child
        self.assertEqual(result.type, "JOIN")
        self.assertEqual(result.childs[0].type, "RELATION")
        self.assertEqual(result.childs[1].type, "FILTER")
    
    def test_push_to_both(self):
        """Test pushing filters to both sides"""
        rel1 = QueryTree("RELATION", "users")
        rel2 = QueryTree("RELATION", "orders")
        
        join_cond = make_join_condition("id", "user_id", "users", "orders")
        
        join = QueryTree("JOIN", "INNER")
        join.add_child(rel1)
        join.add_child(rel2)
        join.add_child(join_cond)
        
        cond1 = make_comparison(">", "age", "18", "users")
        cond2 = make_comparison(">", "amount", "1000", "orders")
        
        and_op = QueryTree("OPERATOR", "AND")
        and_op.add_child(cond1)
        and_op.add_child(cond2)
        
        filter_node = QueryTree("FILTER", "")
        filter_node.add_child(join)
        filter_node.add_child(and_op)
        
        plan = {
            'distribution': 'both',
            'left_conditions': [0],
            'right_conditions': [1]
        }
        
        result = push_filter(filter_node, plan)
        
        # Result should be JOIN with FILTER on both children
        self.assertEqual(result.type, "JOIN")
        self.assertEqual(result.childs[0].type, "FILTER")
        self.assertEqual(result.childs[1].type, "FILTER")
    
    def test_push_with_remaining_conditions(self):
        """Test pushing some filters while keeping others above join"""
        rel1 = QueryTree("RELATION", "users")
        rel2 = QueryTree("RELATION", "orders")
        
        join_cond = make_join_condition("id", "user_id", "users", "orders")
        
        join = QueryTree("JOIN", "INNER")
        join.add_child(rel1)
        join.add_child(rel2)
        join.add_child(join_cond)
        
        cond1 = make_comparison(">", "age", "18", "users")
        cond2 = QueryTree("COMPARISON", ">")  # Cross-table condition
        cond2.add_child(make_column_ref("age", "users"))
        cond2.add_child(make_column_ref("discount", "orders"))
        
        and_op = QueryTree("OPERATOR", "AND")
        and_op.add_child(cond1)
        and_op.add_child(cond2)
        
        filter_node = QueryTree("FILTER", "")
        filter_node.add_child(join)
        filter_node.add_child(and_op)
        
        plan = {
            'distribution': 'left',
            'left_conditions': [0],
            'right_conditions': []
        }
        
        result = push_filter(filter_node, plan)
        
        # Result should be FILTER on top (for remaining condition)
        # with JOIN below (with pushed filter on left)
        self.assertEqual(result.type, "FILTER")
        self.assertEqual(result.childs[0].type, "JOIN")


@patch('query_optimizer.query_check.get_metadata', return_value=MOCK_METADATA)
class TestApplyPushdown(unittest.TestCase):
    """Test apply_pushdown main function"""
    
    def test_apply_pushdown_with_auto_decision(self, mock_meta):
        """Test apply pushdown with automatic decision"""
        rel1 = QueryTree("RELATION", "users")
        rel2 = QueryTree("RELATION", "profiles")
        
        join_cond = make_join_condition("id", "user_id", "users", "profiles")
        
        join = QueryTree("JOIN", "INNER")
        join.add_child(rel1)
        join.add_child(rel2)
        join.add_child(join_cond)
        
        filter_cond = make_comparison(">", "age", "18", "users")
        
        filter_node = QueryTree("FILTER", "")
        filter_node.add_child(join)
        filter_node.add_child(filter_cond)
        
        query = ParsedQuery(filter_node, "SELECT * FROM users JOIN profiles WHERE age > 18")
        
        # Apply pushdown without explicit plan
        result = apply_pushdown(query)
        
        # Validate result
        check_query(result.query_tree)
        
        # Should have pushed filter down
        self.assertIsNotNone(result)
        self.assertIsInstance(result, ParsedQuery)
    
    def test_apply_pushdown_with_explicit_plan(self, mock_meta):
        """Test apply pushdown with explicit plan"""
        rel1 = QueryTree("RELATION", "users")
        rel2 = QueryTree("RELATION", "orders")
        
        join_cond = make_join_condition("id", "user_id", "users", "orders")
        
        join = QueryTree("JOIN", "INNER")
        join.add_child(rel1)
        join.add_child(rel2)
        join.add_child(join_cond)
        
        cond1 = make_comparison(">", "age", "18", "users")
        cond2 = make_comparison(">", "amount", "1000", "orders")
        
        and_op = QueryTree("OPERATOR", "AND")
        and_op.add_child(cond1)
        and_op.add_child(cond2)
        
        filter_node = QueryTree("FILTER", "")
        filter_node.add_child(join)
        filter_node.add_child(and_op)
        
        query = ParsedQuery(filter_node, "SELECT * FROM users JOIN orders WHERE ...")
        
        # Explicit plan: push both
        plans = {
            filter_node.id: {
                'distribution': 'both',
                'left_conditions': [0],
                'right_conditions': [1]
            }
        }
        
        result = apply_pushdown(query, plans)
        
        # Validate result
        check_query(result.query_tree)
        
        self.assertIsNotNone(result)


@patch('query_optimizer.query_check.get_metadata', return_value=MOCK_METADATA)
class TestUndoPushdown(unittest.TestCase):
    """Test undo pushdown transformation"""
    
    def test_undo_pushdown_left(self, mock_meta):
        """Test undoing pushdown from left side"""
        # Build structure with filter on left child of join
        rel1 = QueryTree("RELATION", "users")
        filter_cond = make_comparison(">", "age", "18", "users")
        
        filter_left = QueryTree("FILTER", "")
        filter_left.add_child(rel1)
        filter_left.add_child(filter_cond)
        
        rel2 = QueryTree("RELATION", "profiles")
        
        join_cond = make_join_condition("id", "user_id", "users", "profiles")
        
        join = QueryTree("JOIN", "INNER")
        join.add_child(filter_left)
        join.add_child(rel2)
        join.add_child(join_cond)
        
        query = ParsedQuery(join, "SELECT * FROM users JOIN profiles")
        
        # Undo pushdown
        result = undo_pushdown(query)
        
        # Validate result
        check_query(result.query_tree)
        
        # Should have FILTER on top
        self.assertEqual(result.query_tree.type, "FILTER")
        self.assertEqual(result.query_tree.childs[0].type, "JOIN")
    
    def test_undo_pushdown_both(self, mock_meta):
        """Test undoing pushdown from both sides"""
        # Build structure with filters on both children of join
        rel1 = QueryTree("RELATION", "users")
        cond1 = make_comparison(">", "age", "18", "users")
        
        filter_left = QueryTree("FILTER", "")
        filter_left.add_child(rel1)
        filter_left.add_child(cond1)
        
        rel2 = QueryTree("RELATION", "orders")
        cond2 = make_comparison(">", "amount", "1000", "orders")
        
        filter_right = QueryTree("FILTER", "")
        filter_right.add_child(rel2)
        filter_right.add_child(cond2)
        
        join_cond = make_join_condition("id", "user_id", "users", "orders")
        
        join = QueryTree("JOIN", "INNER")
        join.add_child(filter_left)
        join.add_child(filter_right)
        join.add_child(join_cond)
        
        query = ParsedQuery(join, "SELECT * FROM users JOIN orders")
        
        # Undo pushdown
        result = undo_pushdown(query)
        
        # Validate result
        check_query(result.query_tree)
        
        # Should have FILTER on top with AND operator
        self.assertEqual(result.query_tree.type, "FILTER")


class TestWrapFilter(unittest.TestCase):
    """Test wrap_filter utility function"""
    
    def test_wrap_single_condition(self):
        """Test wrapping with single condition"""
        rel = QueryTree("RELATION", "users")
        cond = make_comparison(">", "age", "18")
        
        result = wrap_filter(rel, [cond])
        
        self.assertEqual(result.type, "FILTER")
        self.assertEqual(result.childs[0].type, "RELATION")
        self.assertEqual(result.childs[1].type, "COMPARISON")
    
    def test_wrap_multiple_conditions(self):
        """Test wrapping with multiple conditions"""
        rel = QueryTree("RELATION", "users")
        cond1 = make_comparison(">", "age", "18")
        cond2 = make_comparison("=", "status", "active")
        
        result = wrap_filter(rel, [cond1, cond2])
        
        self.assertEqual(result.type, "FILTER")
        self.assertEqual(result.childs[0].type, "RELATION")
        self.assertEqual(result.childs[1].type, "OPERATOR")
        self.assertEqual(result.childs[1].val, "AND")
    
    def test_wrap_no_conditions(self):
        """Test wrapping with no conditions returns source"""
        rel = QueryTree("RELATION", "users")