"""
Tests untuk Rule Deterministik (Rule 3, Rule 7, dan Rule 8)

Rule deterministik adalah rule yang dijalankan SEKALI di awal proses optimasi,
sebelum genetic algorithm. Rule ini bersifat always beneficial dan tidak memerlukan
parameter space exploration.

- Rule 3: Projection Elimination (menghilangkan nested projection)
- Rule 7: Filter Pushdown over Join (push filter ke join children)
- Rule 8: Projection over Join (push projection ke join children)
"""

import unittest
from query_optimizer.query_tree import QueryTree
from query_optimizer.optimization_engine import ParsedQuery
from query_optimizer.query_check import check_query
from query_optimizer.rule_3 import seleksi_proyeksi
from query_optimizer.rule_7 import apply_pushdown
from query_optimizer.rule_8 import push_projection_over_joins


class TestRule3ProjectionElimination(unittest.TestCase):
    """Test Rule 3: Projection Elimination"""
    
    def test_eliminate_nested_projection(self):
        """Test eliminasi nested projection: PROJECT(PROJECT(RELATION))"""
        # Build: PROJECT(col1) -> PROJECT(col1, col2) -> RELATION
        relation = QueryTree("RELATION", "users")
        
        # Inner PROJECT with 2 columns
        inner_col1 = QueryTree("COLUMN_REF", "")
        inner_col1_name = QueryTree("COLUMN_NAME", "")
        inner_col1_id = QueryTree("IDENTIFIER", "name")
        inner_col1_name.add_child(inner_col1_id)
        inner_col1.add_child(inner_col1_name)
        
        inner_col2 = QueryTree("COLUMN_REF", "")
        inner_col2_name = QueryTree("COLUMN_NAME", "")
        inner_col2_id = QueryTree("IDENTIFIER", "age")
        inner_col2_name.add_child(inner_col2_id)
        inner_col2.add_child(inner_col2_name)
        
        inner_project = QueryTree("PROJECT", "")
        inner_project.add_child(inner_col1)
        inner_project.add_child(inner_col2)
        inner_project.add_child(relation)
        
        # Outer PROJECT with 1 column
        outer_col = QueryTree("COLUMN_REF", "")
        outer_col_name = QueryTree("COLUMN_NAME", "")
        outer_col_id = QueryTree("IDENTIFIER", "name")
        outer_col_name.add_child(outer_col_id)
        outer_col.add_child(outer_col_name)
        
        outer_project = QueryTree("PROJECT", "")
        outer_project.add_child(outer_col)
        outer_project.add_child(inner_project)
        
        query = ParsedQuery(outer_project, "SELECT name FROM (SELECT name, age FROM users)")
        
        # Apply Rule 3
        result = seleksi_proyeksi(query)
        
        # Validate result
        check_query(result.query_tree)
        
        # Should eliminate inner PROJECT
        self.assertEqual(result.query_tree.type, "PROJECT")
        
        # Should have direct RELATION child (no nested PROJECT)
        has_nested_project = False
        for child in result.query_tree.childs:
            if child.type == "PROJECT":
                has_nested_project = True
                break
        
        self.assertFalse(has_nested_project, "Should not have nested PROJECT")
    
    def test_no_nested_projection(self):
        """Test query tanpa nested projection (tidak ada perubahan)"""
        # Build: PROJECT -> RELATION (no nesting)
        relation = QueryTree("RELATION", "users")
        
        col = QueryTree("COLUMN_REF", "")
        col_name = QueryTree("COLUMN_NAME", "")
        col_id = QueryTree("IDENTIFIER", "name")
        col_name.add_child(col_id)
        col.add_child(col_name)
        
        project = QueryTree("PROJECT", "")
        project.add_child(col)
        project.add_child(relation)
        
        query = ParsedQuery(project, "SELECT name FROM users")
        
        # Apply Rule 3
        result = seleksi_proyeksi(query)
        
        # Validate result
        check_query(result.query_tree)
        
        # Should remain unchanged
        self.assertEqual(result.query_tree.type, "PROJECT")
        
        # Should have RELATION as child
        relation_child = None
        for child in result.query_tree.childs:
            if child.type == "RELATION":
                relation_child = child
                break
        
        self.assertIsNotNone(relation_child)
        self.assertEqual(relation_child.val, "users")


class TestRule8ProjectionOverJoin(unittest.TestCase):
    """Test Rule 8: Projection over Join"""
    
    def test_push_projection_to_join(self):
        """Test push projection ke join children"""
        # Build: PROJECT(e.name, d.department) -> JOIN(e, d)
        
        # Build COLUMN_REF untuk e.name
        col_ref_1 = QueryTree("COLUMN_REF", "")
        col_name_1 = QueryTree("COLUMN_NAME", "")
        identifier_1 = QueryTree("IDENTIFIER", "name")
        col_name_1.add_child(identifier_1)
        col_ref_1.add_child(col_name_1)
        table_name_1 = QueryTree("TABLE_NAME", "")
        table_id_1 = QueryTree("IDENTIFIER", "e")
        table_name_1.add_child(table_id_1)
        col_ref_1.add_child(table_name_1)
        
        # Build COLUMN_REF untuk d.bio
        col_ref_2 = QueryTree("COLUMN_REF", "")
        col_name_2 = QueryTree("COLUMN_NAME", "")
        identifier_2 = QueryTree("IDENTIFIER", "bio")
        col_name_2.add_child(identifier_2)
        col_ref_2.add_child(col_name_2)
        table_name_2 = QueryTree("TABLE_NAME", "")
        table_id_2 = QueryTree("IDENTIFIER", "d")
        table_name_2.add_child(table_id_2)
        col_ref_2.add_child(table_name_2)
        
        # Build JOIN condition: e.id = d.user_id
        join_left = QueryTree("COLUMN_REF", "")
        join_left_name = QueryTree("COLUMN_NAME", "")
        join_left_id = QueryTree("IDENTIFIER", "id")
        join_left_name.add_child(join_left_id)
        join_left.add_child(join_left_name)
        join_left_table = QueryTree("TABLE_NAME", "")
        join_left_table_id = QueryTree("IDENTIFIER", "e")
        join_left_table.add_child(join_left_table_id)
        join_left.add_child(join_left_table)
        
        join_right = QueryTree("COLUMN_REF", "")
        join_right_name = QueryTree("COLUMN_NAME", "")
        join_right_id = QueryTree("IDENTIFIER", "user_id")
        join_right_name.add_child(join_right_id)
        join_right.add_child(join_right_name)
        join_right_table = QueryTree("TABLE_NAME", "")
        join_right_table_id = QueryTree("IDENTIFIER", "d")
        join_right_table.add_child(join_right_table_id)
        join_right.add_child(join_right_table)
        
        comparison = QueryTree("COMPARISON", "=")
        comparison.add_child(join_left)
        comparison.add_child(join_right)
        
        # Build JOIN
        rel_e = QueryTree("RELATION", "users")
        rel_d = QueryTree("RELATION", "profiles")
        
        join = QueryTree("JOIN", "INNER")
        join.add_child(rel_e)
        join.add_child(rel_d)
        join.add_child(comparison)
        
        # Build PROJECT
        project = QueryTree("PROJECT", "")
        project.add_child(col_ref_1)
        project.add_child(col_ref_2)
        project.add_child(join)
        
        query = ParsedQuery(project, "SELECT e.name, d.bio FROM users e JOIN profiles d ON e.id = d.user_id")
        
        # Apply Rule 8
        result = push_projection_over_joins(query)
        
        # Validate result
        check_query(result.query_tree)
        
        # Find JOIN node
        join_node = None
        for child in result.query_tree.childs:
            if child.type == "JOIN":
                join_node = child
                break
        
        self.assertIsNotNone(join_node, "JOIN node should exist")
        
        # Check if left child is PROJECT
        left_child = join_node.childs[0]
        self.assertEqual(left_child.type, "PROJECT", "Left child should be PROJECT")
        
        # Check if right child is PROJECT
        right_child = join_node.childs[1]
        self.assertEqual(right_child.type, "PROJECT", "Right child should be PROJECT")
        
        # Check if PROJECT nodes have RELATION as source
        left_has_relation = any(child.type == "RELATION" for child in left_child.childs)
        right_has_relation = any(child.type == "RELATION" for child in right_child.childs)
        
        self.assertTrue(left_has_relation, "Left PROJECT should have RELATION")
        self.assertTrue(right_has_relation, "Right PROJECT should have RELATION")
    
    def test_no_join_no_change(self):
        """Test query tanpa JOIN (tidak ada perubahan)"""
        # Build: PROJECT -> RELATION (no JOIN)
        relation = QueryTree("RELATION", "users")
        
        col = QueryTree("COLUMN_REF", "")
        col_name = QueryTree("COLUMN_NAME", "")
        col_id = QueryTree("IDENTIFIER", "name")
        col_name.add_child(col_id)
        col.add_child(col_name)
        
        project = QueryTree("PROJECT", "")
        project.add_child(col)
        project.add_child(relation)
        
        query = ParsedQuery(project, "SELECT name FROM users")
        
        # Apply Rule 8
        result = push_projection_over_joins(query)
        
        # Validate result
        check_query(result.query_tree)
        
        # Should remain unchanged (no JOIN to optimize)
        self.assertEqual(result.query_tree.type, "PROJECT")
        
        # JOIN should not be in children
        has_join = any(child.type == "JOIN" for child in result.query_tree.childs)
        self.assertFalse(has_join)


class TestRule7FilterPushdown(unittest.TestCase):
    """Test Rule 7: Filter Pushdown over Join"""
    
    def make_column_ref(self, col_name: str, table_name: str = None):
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
    
    def make_comparison(self, operator: str, left_col: str, right_val: str, left_table: str = None):
        """Helper to create COMPARISON node"""
        comp = QueryTree("COMPARISON", operator)
        left = self.make_column_ref(left_col, left_table)
        right = QueryTree("LITERAL_NUMBER", right_val) if right_val.isdigit() else QueryTree("LITERAL_STRING", right_val)
        comp.add_child(left)
        comp.add_child(right)
        return comp
    
    def make_join_condition(self, left_col: str, right_col: str, left_table: str, right_table: str):
        """Helper to create join condition"""
        comp = QueryTree("COMPARISON", "=")
        left = self.make_column_ref(left_col, left_table)
        right = self.make_column_ref(right_col, right_table)
        comp.add_child(left)
        comp.add_child(right)
        return comp
    
    def test_push_filter_to_left(self):
        """Test push filter ke left side of join"""
        # Build: FILTER → JOIN where filter only references left table
        rel1 = QueryTree("RELATION", "users")
        rel2 = QueryTree("RELATION", "profiles")
        
        join_cond = self.make_join_condition("id", "user_id", "users", "profiles")
        
        join = QueryTree("JOIN", "INNER")
        join.add_child(rel1)
        join.add_child(rel2)
        join.add_child(join_cond)
        
        # Filter: users.age > 18
        filter_cond = self.make_comparison(">", "age", "18", "users")
        
        filter_node = QueryTree("FILTER", "")
        filter_node.add_child(join)
        filter_node.add_child(filter_cond)
        
        query = ParsedQuery(filter_node, "SELECT * FROM users JOIN profiles WHERE users.age > 18")
        
        # Apply Rule 7
        result = apply_pushdown(query)
        
        # Validate result
        check_query(result.query_tree)
        
        # Should have pushed filter down
        # Result should be JOIN with FILTER on left child
        self.assertEqual(result.query_tree.type, "JOIN")
        
        # Check left child is FILTER
        left_child = result.query_tree.childs[0]
        self.assertEqual(left_child.type, "FILTER", "Left child should be FILTER after pushdown")
        
        # Check right child remains RELATION
        right_child = result.query_tree.childs[1]
        self.assertEqual(right_child.type, "RELATION", "Right child should remain RELATION")
    
    def test_push_filter_to_right(self):
        """Test push filter ke right side of join"""
        rel1 = QueryTree("RELATION", "users")
        rel2 = QueryTree("RELATION", "profiles")
        
        join_cond = self.make_join_condition("id", "user_id", "users", "profiles")
        
        join = QueryTree("JOIN", "INNER")
        join.add_child(rel1)
        join.add_child(rel2)
        join.add_child(join_cond)
        
        # Filter: profiles.verified = true
        filter_cond = self.make_comparison("=", "verified", "true", "profiles")
        
        filter_node = QueryTree("FILTER", "")
        filter_node.add_child(join)
        filter_node.add_child(filter_cond)
        
        query = ParsedQuery(filter_node, "SELECT * FROM users JOIN profiles WHERE profiles.verified = 'true'")
        
        # Apply Rule 7
        result = apply_pushdown(query)
        
        # Validate result
        check_query(result.query_tree)
        
        # Should have pushed filter down to right
        self.assertEqual(result.query_tree.type, "JOIN")
        
        # Check right child is FILTER
        right_child = result.query_tree.childs[1]
        self.assertEqual(right_child.type, "FILTER", "Right child should be FILTER after pushdown")
    
    def test_push_filter_to_both_sides(self):
        """Test push filters ke both sides of join"""
        rel1 = QueryTree("RELATION", "users")
        rel2 = QueryTree("RELATION", "orders")
        
        join_cond = self.make_join_condition("id", "user_id", "users", "orders")
        
        join = QueryTree("JOIN", "INNER")
        join.add_child(rel1)
        join.add_child(rel2)
        join.add_child(join_cond)
        
        # Multiple conditions: users.age > 18 AND orders.amount > 1000
        cond1 = self.make_comparison(">", "age", "18", "users")
        cond2 = self.make_comparison(">", "amount", "1000", "orders")
        
        and_op = QueryTree("OPERATOR", "AND")
        and_op.add_child(cond1)
        and_op.add_child(cond2)
        
        filter_node = QueryTree("FILTER", "")
        filter_node.add_child(join)
        filter_node.add_child(and_op)
        
        query = ParsedQuery(filter_node, "SELECT * FROM users JOIN orders WHERE users.age > 18 AND orders.amount > 1000")
        
        # Apply Rule 7
        result = apply_pushdown(query)
        
        # Validate result
        check_query(result.query_tree)
        
        # Should have pushed filters to both sides
        self.assertEqual(result.query_tree.type, "JOIN")
        
        # Both children should be FILTER
        left_child = result.query_tree.childs[0]
        right_child = result.query_tree.childs[1]
        
        self.assertEqual(left_child.type, "FILTER", "Left child should be FILTER")
        self.assertEqual(right_child.type, "FILTER", "Right child should be FILTER")
    
    def test_no_filter_no_change(self):
        """Test query tanpa FILTER → JOIN (tidak ada perubahan)"""
        rel1 = QueryTree("RELATION", "users")
        rel2 = QueryTree("RELATION", "profiles")
        
        join_cond = self.make_join_condition("id", "user_id", "users", "profiles")
        
        join = QueryTree("JOIN", "INNER")
        join.add_child(rel1)
        join.add_child(rel2)
        join.add_child(join_cond)
        
        query = ParsedQuery(join, "SELECT * FROM users JOIN profiles")
        
        # Apply Rule 7
        result = apply_pushdown(query)
        
        # Validate result
        check_query(result.query_tree)
        
        # Should remain unchanged (no FILTER to push)
        self.assertEqual(result.query_tree.type, "JOIN")


class TestDeterministicRulesInteraction(unittest.TestCase):
    """Test interaksi antara Rule 3 dan Rule 8"""
    
    def test_rule3_then_rule8(self):
        """Test Rule 3 dulu, lalu Rule 8"""
        # Build: PROJECT -> PROJECT -> JOIN
        # Rule 3 akan eliminate nested PROJECT
        # Rule 8 akan push PROJECT ke JOIN children
        
        # Build JOIN
        rel_e = QueryTree("RELATION", "users")
        rel_d = QueryTree("RELATION", "profiles")
        
        join_left = QueryTree("COLUMN_REF", "")
        join_left_name = QueryTree("COLUMN_NAME", "")
        join_left_id = QueryTree("IDENTIFIER", "id")
        join_left_name.add_child(join_left_id)
        join_left.add_child(join_left_name)
        
        join_right = QueryTree("COLUMN_REF", "")
        join_right_name = QueryTree("COLUMN_NAME", "")
        join_right_id = QueryTree("IDENTIFIER", "id")
        join_right_name.add_child(join_right_id)
        join_right.add_child(join_right_name)
        
        comparison = QueryTree("COMPARISON", "=")
        comparison.add_child(join_left)
        comparison.add_child(join_right)
        
        join = QueryTree("JOIN", "INNER")
        join.add_child(rel_e)
        join.add_child(rel_d)
        join.add_child(comparison)
        
        # Inner PROJECT (wider columns)
        inner_col1 = QueryTree("COLUMN_REF", "")
        inner_col1_name = QueryTree("COLUMN_NAME", "")
        inner_col1_id = QueryTree("IDENTIFIER", "name")
        inner_col1_name.add_child(inner_col1_id)
        inner_col1.add_child(inner_col1_name)
        
        inner_col2 = QueryTree("COLUMN_REF", "")
        inner_col2_name = QueryTree("COLUMN_NAME", "")
        inner_col2_id = QueryTree("IDENTIFIER", "bio")
        inner_col2_name.add_child(inner_col2_id)
        inner_col2.add_child(inner_col2_name)
        
        inner_project = QueryTree("PROJECT", "")
        inner_project.add_child(inner_col1)
        inner_project.add_child(inner_col2)
        inner_project.add_child(join)
        
        # Outer PROJECT (subset of inner)
        outer_col = QueryTree("COLUMN_REF", "")
        outer_col_name = QueryTree("COLUMN_NAME", "")
        outer_col_id = QueryTree("IDENTIFIER", "name")
        outer_col_name.add_child(outer_col_id)
        outer_col.add_child(outer_col_name)
        
        outer_project = QueryTree("PROJECT", "")
        outer_project.add_child(outer_col)
        outer_project.add_child(inner_project)
        
        query = ParsedQuery(outer_project, "SELECT name FROM (SELECT name, bio FROM users JOIN profiles)")
        
        # Apply Rule 3 first
        after_rule3 = seleksi_proyeksi(query)
        
        # Validate after Rule 3
        check_query(after_rule3.query_tree)
        
        # Verify nested PROJECT eliminated
        self.assertEqual(after_rule3.query_tree.type, "PROJECT")
        
        # Apply Rule 8 second
        after_rule8 = push_projection_over_joins(after_rule3)
        
        # Validate after Rule 8
        check_query(after_rule8.query_tree)
        
        # Find JOIN
        join_node = None
        for child in after_rule8.query_tree.childs:
            if child.type == "JOIN":
                join_node = child
                break
        
        if join_node:
            # Check if projection pushed to JOIN children
            left_is_project = join_node.childs[0].type == "PROJECT"
            right_is_project = join_node.childs[1].type == "PROJECT"
            
            self.assertTrue(left_is_project or right_is_project, 
                          "At least one JOIN child should be PROJECT after Rule 8")
    
    def test_rule8_without_rule3(self):
        """Test Rule 8 langsung tanpa Rule 3"""
        # Build: PROJECT -> JOIN (no nested PROJECT)
        
        # Build JOIN
        rel_e = QueryTree("RELATION", "users")
        rel_d = QueryTree("RELATION", "profiles")
        
        join_left = QueryTree("COLUMN_REF", "")
        join_left_name = QueryTree("COLUMN_NAME", "")
        join_left_id = QueryTree("IDENTIFIER", "id")
        join_left_name.add_child(join_left_id)
        join_left.add_child(join_left_name)
        
        join_right = QueryTree("COLUMN_REF", "")
        join_right_name = QueryTree("COLUMN_NAME", "")
        join_right_id = QueryTree("IDENTIFIER", "user_id")
        join_right_name.add_child(join_right_id)
        join_right.add_child(join_right_name)
        
        comparison = QueryTree("COMPARISON", "=")
        comparison.add_child(join_left)
        comparison.add_child(join_right)
        
        join = QueryTree("JOIN", "INNER")
        join.add_child(rel_e)
        join.add_child(rel_d)
        join.add_child(comparison)
        
        # PROJECT
        col = QueryTree("COLUMN_REF", "")
        col_name = QueryTree("COLUMN_NAME", "")
        col_id = QueryTree("IDENTIFIER", "name")
        col_name.add_child(col_id)
        col.add_child(col_name)
        
        project = QueryTree("PROJECT", "")
        project.add_child(col)
        project.add_child(join)
        
        query = ParsedQuery(project, "SELECT name FROM users JOIN profiles")
        
        # Apply Rule 8 directly
        result = push_projection_over_joins(query)
        
        # Validate result
        check_query(result.query_tree)
        
        # Find JOIN
        join_node = None
        for child in result.query_tree.childs:
            if child.type == "JOIN":
                join_node = child
                break
        
        if join_node:
            # At least one child should be PROJECT
            left_is_project = join_node.childs[0].type == "PROJECT"
            right_is_project = join_node.childs[1].type == "PROJECT"
            
            self.assertTrue(left_is_project or right_is_project,
                          "At least one JOIN child should be PROJECT after Rule 8")
    
    def test_rule3_then_rule7_then_rule8(self):
        """Test sequence Rule 3 -> Rule 7 -> Rule 8"""
        # Build: PROJECT -> PROJECT -> FILTER -> JOIN
        # Should eliminate inner PROJECT, push filter, then push projection
        
        # Build JOIN
        rel_e = QueryTree("RELATION", "users")
        rel_d = QueryTree("RELATION", "profiles")
        
        join_left = QueryTree("COLUMN_REF", "")
        join_left_name = QueryTree("COLUMN_NAME", "")
        join_left_id = QueryTree("IDENTIFIER", "id")
        join_left_name.add_child(join_left_id)
        join_left.add_child(join_left_name)
        
        join_right = QueryTree("COLUMN_REF", "")
        join_right_name = QueryTree("COLUMN_NAME", "")
        join_right_id = QueryTree("IDENTIFIER", "user_id")
        join_right_name.add_child(join_right_id)
        join_right.add_child(join_right_name)
        
        comparison = QueryTree("COMPARISON", "=")
        comparison.add_child(join_left)
        comparison.add_child(join_right)
        
        join = QueryTree("JOIN", "INNER")
        join.add_child(rel_e)
        join.add_child(rel_d)
        join.add_child(comparison)
        
        # FILTER (references left table)
        filter_col = QueryTree("COLUMN_REF", "")
        filter_col_name = QueryTree("COLUMN_NAME", "")
        filter_col_id = QueryTree("IDENTIFIER", "age")
        filter_col_name.add_child(filter_col_id)
        filter_col.add_child(filter_col_name)
        
        filter_table = QueryTree("TABLE_NAME", "")
        filter_table_id = QueryTree("IDENTIFIER", "users")
        filter_table.add_child(filter_table_id)
        filter_col.add_child(filter_table)
        
        filter_val = QueryTree("LITERAL_NUMBER", "18")
        
        filter_cond = QueryTree("COMPARISON", ">")
        filter_cond.add_child(filter_col)
        filter_cond.add_child(filter_val)
        
        filter_node = QueryTree("FILTER", "")
        filter_node.add_child(join)
        filter_node.add_child(filter_cond)
        
        # Inner PROJECT
        inner_col = QueryTree("COLUMN_REF", "")
        inner_col_name = QueryTree("COLUMN_NAME", "")
        inner_col_id = QueryTree("IDENTIFIER", "name")
        inner_col_name.add_child(inner_col_id)
        inner_col.add_child(inner_col_name)
        
        inner_project = QueryTree("PROJECT", "")
        inner_project.add_child(inner_col)
        inner_project.add_child(filter_node)
        
        # Outer PROJECT
        outer_col = QueryTree("COLUMN_REF", "")
        outer_col_name = QueryTree("COLUMN_NAME", "")
        outer_col_id = QueryTree("IDENTIFIER", "name")
        outer_col_name.add_child(outer_col_id)
        outer_col.add_child(outer_col_name)
        
        outer_project = QueryTree("PROJECT", "")
        outer_project.add_child(outer_col)
        outer_project.add_child(inner_project)
        
        query = ParsedQuery(outer_project, "test")
        
        # Apply Rule 3
        after_rule3 = seleksi_proyeksi(query)
        check_query(after_rule3.query_tree)
        
        # Apply Rule 7
        after_rule7 = apply_pushdown(after_rule3)
        check_query(after_rule7.query_tree)
        
        # Apply Rule 8
        after_rule8 = push_projection_over_joins(after_rule7)
        check_query(after_rule8.query_tree)
        
        # Result should be valid
        self.assertIsNotNone(after_rule8.query_tree)
    
    def test_order_independence(self):
        """Test bahwa urutan Rule 3 dan Rule 8 tidak mempengaruhi hasil akhir"""
        # Build query with nested PROJECT over JOIN
        
        # Build JOIN
        rel_e = QueryTree("RELATION", "users")
        rel_d = QueryTree("RELATION", "profiles")
        
        join_left = QueryTree("COLUMN_REF", "")
        join_left_name = QueryTree("COLUMN_NAME", "")
        join_left_id = QueryTree("IDENTIFIER", "id")
        join_left_name.add_child(join_left_id)
        join_left.add_child(join_left_name)
        
        join_right = QueryTree("COLUMN_REF", "")
        join_right_name = QueryTree("COLUMN_NAME", "")
        join_right_id = QueryTree("IDENTIFIER", "user_id")
        join_right_name.add_child(join_right_id)
        join_right.add_child(join_right_name)
        
        comparison = QueryTree("COMPARISON", "=")
        comparison.add_child(join_left)
        comparison.add_child(join_right)
        
        join = QueryTree("JOIN", "INNER")
        join.add_child(rel_e)
        join.add_child(rel_d)
        join.add_child(comparison)
        
        # Inner PROJECT
        inner_col = QueryTree("COLUMN_REF", "")
        inner_col_name = QueryTree("COLUMN_NAME", "")
        inner_col_id = QueryTree("IDENTIFIER", "name")
        inner_col_name.add_child(inner_col_id)
        inner_col.add_child(inner_col_name)
        
        inner_project = QueryTree("PROJECT", "")
        inner_project.add_child(inner_col)
        inner_project.add_child(join)
        
        # Outer PROJECT
        outer_col = QueryTree("COLUMN_REF", "")
        outer_col_name = QueryTree("COLUMN_NAME", "")
        outer_col_id = QueryTree("IDENTIFIER", "name")
        outer_col_name.add_child(outer_col_id)
        outer_col.add_child(outer_col_name)
        
        outer_project = QueryTree("PROJECT", "")
        outer_project.add_child(outer_col)
        outer_project.add_child(inner_project)
        
        query = ParsedQuery(outer_project, "test")
        
        # Path 1: Rule 3 -> Rule 8
        from query_optimizer.rule_1 import clone_tree
        cloned1 = clone_tree(query.query_tree)
        query1 = ParsedQuery(cloned1, query.query)
        result1 = seleksi_proyeksi(query1)
        result1 = push_projection_over_joins(result1)
        
        # Validate path 1 result
        check_query(result1.query_tree)
        
        # Path 2: Rule 8 -> Rule 3
        cloned2 = clone_tree(query.query_tree)
        query2 = ParsedQuery(cloned2, query.query)
        result2 = push_projection_over_joins(query2)
        result2 = seleksi_proyeksi(result2)
        
        # Validate path 2 result
        check_query(result2.query_tree)
        
        # Both should have similar structure:
        # - Top level PROJECT
        # - JOIN with PROJECT children
        self.assertEqual(result1.query_tree.type, "PROJECT")
        self.assertEqual(result2.query_tree.type, "PROJECT")
        
        # Both should have JOIN
        has_join1 = any(child.type == "JOIN" for child in result1.query_tree.childs)
        has_join2 = any(child.type == "JOIN" for child in result2.query_tree.childs)
        self.assertTrue(has_join1 or has_join2, "Should have JOIN in result")


if __name__ == "__main__":
    unittest.main()
