"""
Unit tests untuk Rule 8: Projection Operation over Theta Join Operation
Menggunakan Mock Metadata untuk isolasi pengujian.
"""

import unittest
from unittest.mock import patch, MagicMock
from query_optimizer.query_tree import QueryTree
from query_optimizer.optimization_engine import ParsedQuery
from query_optimizer.query_check import check_query
from query_optimizer.rule.rule_8 import (
    analyze_projection_over_join,
    push_projection_to_join,
    can_apply_rule8,
    undo_rule8,
    extract_projected_columns,
    extract_join_condition_columns
)

# --- MOCK METADATA ---
MOCK_METADATA = {
    "tables": ["users", "profiles", "orders", "table1", "table2", "t1", "t2"],
    "columns": {
        "users": ["id", "name", "age", "bio"],
        "profiles": ["id", "user_id", "bio"],
        "orders": ["id", "user_id", "amount"],
        "table1": ["id", "name"],
        "table2": ["id", "name"],
        "t1": ["id", "name"],
        "t2": ["id", "name"]
    }
}

def make_comparison(operator="=", left_col="id", right_col="id", left_table=None, right_table=None):
    """Helper to create valid COMPARISON node for join conditions"""
    comp = QueryTree("COMPARISON", operator)
    
    # Left side: COLUMN_REF
    left = QueryTree("COLUMN_REF", "")
    left_name = QueryTree("COLUMN_NAME", "")
    left_id = QueryTree("IDENTIFIER", left_col)
    left_name.add_child(left_id)
    left.add_child(left_name)
    if left_table:
        left_table_node = QueryTree("TABLE_NAME", "")
        left_table_id = QueryTree("IDENTIFIER", left_table)
        left_table_node.add_child(left_table_id)
        left.add_child(left_table_node)
    
    # Right side: COLUMN_REF
    right = QueryTree("COLUMN_REF", "")
    right_name = QueryTree("COLUMN_NAME", "")
    right_id = QueryTree("IDENTIFIER", right_col)
    right_name.add_child(right_id)
    right.add_child(right_name)
    if right_table:
        right_table_node = QueryTree("TABLE_NAME", "")
        right_table_id = QueryTree("IDENTIFIER", right_table)
        right_table_node.add_child(right_table_id)
        right.add_child(right_table_node)
    
    comp.add_child(left)
    comp.add_child(right)
    return comp


@patch('query_optimizer.query_check.get_metadata', return_value=MOCK_METADATA)
class TestAnalyzeProjectionOverJoin(unittest.TestCase):
    """Test analisis pattern PROJECT → JOIN"""
    
    def test_detect_project_join_pattern(self, mock_meta):
        """Test detection of PROJECT → JOIN pattern"""
        # Build: PROJECT → JOIN
        rel1 = QueryTree("RELATION", "users")
        rel2 = QueryTree("RELATION", "profiles")
        
        # Join condition: users.id = profiles.user_id
        condition = make_comparison("=", "id", "user_id", "users", "profiles")
        
        join = QueryTree("JOIN", "INNER")
        join.add_child(rel1)
        join.add_child(rel2)
        join.add_child(condition)
        
        # Project: name, bio
        col1 = QueryTree("COLUMN_REF", "")
        col1_name = QueryTree("COLUMN_NAME", "")
        col1_id = QueryTree("IDENTIFIER", "name")
        col1_name.add_child(col1_id)
        col1.add_child(col1_name)
        
        project = QueryTree("PROJECT", "")
        project.add_child(col1)
        project.add_child(join)
        
        query = ParsedQuery(project, "SELECT name FROM users JOIN profiles")
        
        opportunities = analyze_projection_over_join(query)
        
        self.assertGreater(len(opportunities), 0)
        self.assertIn(join.id, opportunities)
        self.assertTrue(opportunities[join.id]['can_optimize'])
    
    def test_no_join_pattern(self, mock_meta):
        """Test when there's no JOIN"""
        rel = QueryTree("RELATION", "users")
        project = QueryTree("PROJECT", "")
        project.add_child(rel)
        
        query = ParsedQuery(project, "SELECT * FROM users")
        
        opportunities = analyze_projection_over_join(query)
        
        self.assertEqual(len(opportunities), 0)


@patch('query_optimizer.query_check.get_metadata', return_value=MOCK_METADATA)
class TestExtractColumns(unittest.TestCase):
    """Test extraction of columns"""
    
    def test_extract_projected_columns(self, mock_meta):
        """Test extraction of projected column names"""
        # Build PROJECT with columns
        col1 = QueryTree("COLUMN_REF", "")
        col1_name = QueryTree("COLUMN_NAME", "")
        col1_id = QueryTree("IDENTIFIER", "name")
        col1_name.add_child(col1_id)
        col1.add_child(col1_name)
        
        col2 = QueryTree("COLUMN_REF", "")
        col2_name = QueryTree("COLUMN_NAME", "")
        col2_id = QueryTree("IDENTIFIER", "age")
        col2_name.add_child(col2_id)
        col2.add_child(col2_name)
        
        project = QueryTree("PROJECT", "")
        project.add_child(col1)
        project.add_child(col2)
        
        cols = extract_projected_columns(project)
        
        self.assertIn("name", cols)
        self.assertIn("age", cols)
        self.assertEqual(len(cols), 2)
    
    def test_extract_join_condition_columns(self, mock_meta):
        """Test extraction of columns from join condition"""
        # Build: emp.dept_id = dept.id
        left_col = QueryTree("COLUMN_REF", "")
        left_col_name = QueryTree("COLUMN_NAME", "")
        left_id = QueryTree("IDENTIFIER", "dept_id")
        left_col_name.add_child(left_id)
        left_col.add_child(left_col_name)
        
        right_col = QueryTree("COLUMN_REF", "")
        right_col_name = QueryTree("COLUMN_NAME", "")
        right_id = QueryTree("IDENTIFIER", "id")
        right_col_name.add_child(right_id)
        right_col.add_child(right_col_name)
        
        condition = QueryTree("COMPARISON", "=")
        condition.add_child(left_col)
        condition.add_child(right_col)
        
        left_cols, right_cols = extract_join_condition_columns(condition)
        
        self.assertIn("dept_id", left_cols)
        self.assertIn("id", right_cols)


@patch('query_optimizer.query_check.get_metadata', return_value=MOCK_METADATA)
class TestCanApplyRule8(unittest.TestCase):
    """Test kondisi penerapan Rule 8"""
    
    def test_can_apply_basic_case(self, mock_meta):
        """Test basic case where Rule 8 can be applied"""
        # Build valid structure
        rel1 = QueryTree("RELATION", "table1")
        rel2 = QueryTree("RELATION", "table2")
        condition = QueryTree("COMPARISON", "=")
        
        join = QueryTree("JOIN", "INNER")
        join.add_child(rel1)
        join.add_child(rel2)
        join.add_child(condition)
        
        col1 = QueryTree("COLUMN_REF", "")
        project = QueryTree("PROJECT", "")
        project.add_child(col1)
        project.add_child(join)
        
        self.assertTrue(can_apply_rule8(project))
    
    def test_cannot_apply_select_star(self, mock_meta):
        """Test SELECT * cannot use Rule 8"""
        rel1 = QueryTree("RELATION", "table1")
        rel2 = QueryTree("RELATION", "table2")
        condition = QueryTree("COMPARISON", "=")
        
        join = QueryTree("JOIN", "INNER")
        join.add_child(rel1)
        join.add_child(rel2)
        join.add_child(condition)
        
        project = QueryTree("PROJECT", "*")
        project.add_child(join)
        
        self.assertFalse(can_apply_rule8(project))
    
    def test_cannot_apply_no_join(self, mock_meta):
        """Test when there's no JOIN child"""
        rel = QueryTree("RELATION", "table1")
        col1 = QueryTree("COLUMN_REF", "")
        
        project = QueryTree("PROJECT", "")
        project.add_child(col1)
        project.add_child(rel)
        
        self.assertFalse(can_apply_rule8(project))
    
    def test_cannot_apply_natural_join(self, mock_meta):
        """Test NATURAL JOIN is not supported"""
        rel1 = QueryTree("RELATION", "table1")
        rel2 = QueryTree("RELATION", "table2")
        
        join = QueryTree("JOIN", "NATURAL")
        join.add_child(rel1)
        join.add_child(rel2)
        
        col1 = QueryTree("COLUMN_REF", "")
        project = QueryTree("PROJECT", "")
        project.add_child(col1)
        project.add_child(join)
        
        self.assertFalse(can_apply_rule8(project))


@patch('query_optimizer.query_check.get_metadata', return_value=MOCK_METADATA)
class TestPushProjection(unittest.TestCase):
    """Test push projection transformation"""
    
    def test_push_projection_basic(self, mock_meta):
        """Test basic push projection transformation"""
        # Build query structure
        rel1 = QueryTree("RELATION", "users")
        rel2 = QueryTree("RELATION", "profiles")
        
        condition = make_comparison("=", "id", "user_id")
        
        join = QueryTree("JOIN", "INNER")
        join.add_child(rel1)
        join.add_child(rel2)
        join.add_child(condition)
        
        col1 = QueryTree("COLUMN_REF", "")
        col1_name = QueryTree("COLUMN_NAME", "")
        col1_id = QueryTree("IDENTIFIER", "name")
        col1_name.add_child(col1_id)
        col1.add_child(col1_name)
        
        project = QueryTree("PROJECT", "")
        project.add_child(col1)
        project.add_child(join)
        
        query = ParsedQuery(project, "SELECT name FROM users JOIN profiles")
        
        # Apply transformation
        optimized = push_projection_to_join(query)
        
        # Validate result
        if optimized:
            check_query(optimized.query_tree)
        
        # Verify structure changed
        self.assertIsNotNone(optimized)
        self.assertEqual(optimized.query_tree.type, "PROJECT")


@patch('query_optimizer.query_check.get_metadata', return_value=MOCK_METADATA)
class TestUndoRule8(unittest.TestCase):
    """Test undo Rule 8 transformation"""
    
    def test_undo_removes_projections(self, mock_meta):
        """Test that undo removes projections under join"""
        # Build structure with projections under join
        rel1 = QueryTree("RELATION", "users")
        rel2 = QueryTree("RELATION", "profiles")
        
        proj1 = QueryTree("PROJECT", "")
        proj1.add_child(rel1)
        
        proj2 = QueryTree("PROJECT", "")
        proj2.add_child(rel2)
        
        condition = make_comparison("=", "id", "id")
        
        join = QueryTree("JOIN", "INNER")
        join.add_child(proj1)
        join.add_child(proj2)
        join.add_child(condition)
        
        top_project = QueryTree("PROJECT", "")
        top_project.add_child(join)
        
        query = ParsedQuery(top_project, "SELECT * FROM t1 JOIN t2")
        
        # Undo Rule 8
        undone = undo_rule8(query)
        
        # Validate result
        check_query(undone.query_tree)
        
        # Check that projections under join are removed
        join_node = undone.query_tree.childs[0]
        self.assertEqual(join_node.type, "JOIN")
        
        # Left and right children should now be RELATION, not PROJECT
        self.assertEqual(join_node.childs[0].type, "RELATION")
        self.assertEqual(join_node.childs[1].type, "RELATION")


@patch('query_optimizer.query_check.get_metadata', return_value=MOCK_METADATA)
class TestRule8Integration(unittest.TestCase):
    """Test Rule 8 dengan query yang lebih kompleks"""
    
    def test_multiple_columns_projection(self, mock_meta):
        """Test dengan multiple columns dalam projection"""
        # Build: SELECT name, bio FROM users JOIN profiles
        rel1 = QueryTree("RELATION", "users")
        rel2 = QueryTree("RELATION", "profiles")
        
        condition = make_comparison("=", "id", "user_id")
        
        join = QueryTree("JOIN", "INNER")
        join.add_child(rel1)
        join.add_child(rel2)
        join.add_child(condition)
        
        col1 = QueryTree("COLUMN_REF", "")
        col1_name = QueryTree("COLUMN_NAME", "")
        col1_id = QueryTree("IDENTIFIER", "name")
        col1_name.add_child(col1_id)
        col1.add_child(col1_name)
        
        col2 = QueryTree("COLUMN_REF", "")
        col2_name = QueryTree("COLUMN_NAME", "")
        col2_id = QueryTree("IDENTIFIER", "bio")
        col2_name.add_child(col2_id)
        col2.add_child(col2_name)
        
        project = QueryTree("PROJECT", "")
        project.add_child(col1)
        project.add_child(col2)
        project.add_child(join)
        
        query = ParsedQuery(project, "SELECT name, bio FROM users JOIN profiles")
        
        # Should be able to apply Rule 8
        self.assertTrue(can_apply_rule8(project))
        
        # Apply optimization
        optimized = push_projection_to_join(query)
        self.assertIsNotNone(optimized)
        
        # Validate result
        if optimized:
            check_query(optimized.query_tree)


@patch('query_optimizer.query_check.get_metadata', return_value=MOCK_METADATA)
class TestPushProjectionOverJoins(unittest.TestCase):
    """Test main function push_projection_over_joins"""
    
    def test_push_projection_over_joins_applies_to_all(self, mock_meta):
        """Test that push_projection_over_joins applies to all opportunities"""
        from query_optimizer.rule.rule_8 import push_projection_over_joins
        
        # Build: PROJECT → JOIN
        rel1 = QueryTree("RELATION", "users")
        rel2 = QueryTree("RELATION", "profiles")
        
        condition = make_comparison("=", "id", "user_id")
        
        join = QueryTree("JOIN", "INNER")
        join.add_child(rel1)
        join.add_child(rel2)
        join.add_child(condition)
        
        col1 = QueryTree("COLUMN_REF", "")
        col1_name = QueryTree("COLUMN_NAME", "")
        col1_id = QueryTree("IDENTIFIER", "name")
        col1_name.add_child(col1_id)
        col1.add_child(col1_name)
        
        project = QueryTree("PROJECT", "")
        project.add_child(col1)
        project.add_child(join)
        
        query = ParsedQuery(project, "SELECT name FROM users JOIN profiles")
        
        # Apply Rule 8
        result = push_projection_over_joins(query)
        
        # Validate result
        check_query(result.query_tree)
        
        # Should return optimized query
        self.assertIsNotNone(result)
        self.assertIsInstance(result, ParsedQuery)
    
    def test_push_projection_no_opportunities(self, mock_meta):
        """Test when there are no opportunities"""
        from query_optimizer.rule.rule_8 import push_projection_over_joins
        
        # Just a relation, no PROJECT → JOIN pattern
        rel = QueryTree("RELATION", "users")
        query = ParsedQuery(rel, "SELECT * FROM users")
        
        # Should return original query unchanged
        result = push_projection_over_joins(query)
        
        # Validate result
        check_query(result.query_tree)
        
        self.assertEqual(result, query)


if __name__ == '__main__':
    unittest.main()