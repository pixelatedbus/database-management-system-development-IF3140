import unittest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from query_optimizer.query_check import QueryTree, check_query, QueryValidationError


class TestQueryValidator(unittest.TestCase):
    
    # ====================================================================
    # ATOMIC NODE TESTS
    # ====================================================================
    
    def test_valid_identifier(self):
        # IDENTIFIER is a leaf node with value
        identifier = QueryTree("IDENTIFIER", "users")
        check_query(identifier)
    
    def test_invalid_identifier_no_value(self):
        # IDENTIFIER must have value
        identifier = QueryTree("IDENTIFIER", "")
        with self.assertRaises(QueryValidationError):
            check_query(identifier)
    
    def test_valid_literal_number(self):
        # LITERAL_NUMBER is a leaf node with numeric value
        literal = QueryTree("LITERAL_NUMBER", 25)
        check_query(literal)
    
    def test_valid_literal_string(self):
        # LITERAL_STRING is a leaf node with string value
        literal = QueryTree("LITERAL_STRING", "active")
        check_query(literal)
    
    def test_valid_literal_boolean(self):
        # LITERAL_BOOLEAN is a leaf node with boolean value
        literal = QueryTree("LITERAL_BOOLEAN", True)
        check_query(literal)
    
    def test_valid_literal_null(self):
        # LITERAL_NULL is a leaf node (no value needed)
        literal = QueryTree("LITERAL_NULL")
        check_query(literal)
    
    def test_invalid_atomic_with_child(self):
        # Atomic nodes cannot have children
        identifier = QueryTree("IDENTIFIER", "users")
        child = QueryTree("IDENTIFIER", "id")
        identifier.add_child(child)
        
        with self.assertRaises(QueryValidationError):
            check_query(identifier)
    
    # ====================================================================
    # WRAPPER NODE TESTS
    # ====================================================================
    
    def test_valid_column_name(self):
        # COLUMN_NAME wraps IDENTIFIER
        column_name = QueryTree("COLUMN_NAME")
        identifier = QueryTree("IDENTIFIER", "age")
        column_name.add_child(identifier)
        check_query(column_name)
    
    def test_valid_table_name(self):
        # TABLE_NAME wraps IDENTIFIER
        table_name = QueryTree("TABLE_NAME")
        identifier = QueryTree("IDENTIFIER", "users")
        table_name.add_child(identifier)
        check_query(table_name)
    
    def test_invalid_wrapper_no_child(self):
        # Wrapper nodes must have exactly 1 child
        column_name = QueryTree("COLUMN_NAME")
        with self.assertRaises(QueryValidationError):
            check_query(column_name)
    
    def test_invalid_wrapper_wrong_child_type(self):
        # Wrapper child must be IDENTIFIER
        column_name = QueryTree("COLUMN_NAME")
        literal = QueryTree("LITERAL_NUMBER", 5)
        column_name.add_child(literal)
        
        with self.assertRaises(QueryValidationError):
            check_query(column_name)
    
    # ====================================================================
    # COLUMN_REF TESTS
    # ====================================================================
    
    def test_valid_column_ref_simple(self):
        # Simple column reference: COLUMN_REF -> COLUMN_NAME -> IDENTIFIER
        col_ref = QueryTree("COLUMN_REF")
        col_name = QueryTree("COLUMN_NAME")
        identifier = QueryTree("IDENTIFIER", "age")
        
        col_name.add_child(identifier)
        col_ref.add_child(col_name)
        
        check_query(col_ref)
    
    def test_valid_column_ref_qualified(self):
        # Qualified column reference: COLUMN_REF -> COLUMN_NAME, TABLE_NAME
        col_ref = QueryTree("COLUMN_REF")
        col_name = QueryTree("COLUMN_NAME")
        col_id = QueryTree("IDENTIFIER", "age")
        table_name = QueryTree("TABLE_NAME")
        table_id = QueryTree("IDENTIFIER", "users")
        
        col_name.add_child(col_id)
        table_name.add_child(table_id)
        col_ref.add_child(col_name)
        col_ref.add_child(table_name)
        
        check_query(col_ref)
    
    def test_invalid_column_ref_no_child(self):
        # COLUMN_REF must have at least 1 child
        col_ref = QueryTree("COLUMN_REF")
        with self.assertRaises(QueryValidationError):
            check_query(col_ref)
    
    def test_invalid_column_ref_wrong_first_child(self):
        # First child must be COLUMN_NAME
        col_ref = QueryTree("COLUMN_REF")
        table_name = QueryTree("TABLE_NAME")
        identifier = QueryTree("IDENTIFIER", "users")
        table_name.add_child(identifier)
        col_ref.add_child(table_name)
        
        with self.assertRaises(QueryValidationError):
            check_query(col_ref)
    
    def test_invalid_column_ref_too_many_children(self):
        # COLUMN_REF can have max 2 children
        col_ref = QueryTree("COLUMN_REF")
        col_name1 = QueryTree("COLUMN_NAME")
        col_name2 = QueryTree("COLUMN_NAME")
        col_name3 = QueryTree("COLUMN_NAME")
        id1 = QueryTree("IDENTIFIER", "a")
        id2 = QueryTree("IDENTIFIER", "b")
        id3 = QueryTree("IDENTIFIER", "c")
        
        col_name1.add_child(id1)
        col_name2.add_child(id2)
        col_name3.add_child(id3)
        col_ref.add_child(col_name1)
        col_ref.add_child(col_name2)
        col_ref.add_child(col_name3)
        
        with self.assertRaises(QueryValidationError):
            check_query(col_ref)
    
    # ====================================================================
    # RELATION TESTS
    # ====================================================================
    
    def test_valid_relation(self):
        # RELATION is a leaf node with table name
        relation = QueryTree("RELATION", "users")
        check_query(relation)
    
    def test_invalid_relation_no_value(self):
        # RELATION must have table name
        relation = QueryTree("RELATION", "")
        with self.assertRaises(QueryValidationError):
            check_query(relation)
    
    def test_invalid_relation_unknown_table(self):
        # Table must exist in database
        relation = QueryTree("RELATION", "nonexistent")
        with self.assertRaises(QueryValidationError):
            check_query(relation)
    
    def test_invalid_relation_with_child(self):
        # RELATION is a leaf and cannot have children
        relation = QueryTree("RELATION", "users")
        child = QueryTree("IDENTIFIER", "id")
        relation.add_child(child)
        
        with self.assertRaises(QueryValidationError):
            check_query(relation)
    
    # ====================================================================
    # PROJECT TESTS
    # ====================================================================
    
    def test_valid_project_select_all(self):
        # PROJECT(*) -> RELATION
        # Represents: SELECT * FROM users
        project = QueryTree("PROJECT", "*")
        relation = QueryTree("RELATION", "users")
        project.add_child(relation)
        
        check_query(project)
    
    def test_valid_project_with_columns(self):
        # PROJECT -> COLUMN_REF, COLUMN_REF, RELATION
        # Represents: SELECT age, name FROM users
        project = QueryTree("PROJECT")
        
        col_ref1 = QueryTree("COLUMN_REF")
        col_name1 = QueryTree("COLUMN_NAME")
        id1 = QueryTree("IDENTIFIER", "age")
        col_name1.add_child(id1)
        col_ref1.add_child(col_name1)
        
        col_ref2 = QueryTree("COLUMN_REF")
        col_name2 = QueryTree("COLUMN_NAME")
        id2 = QueryTree("IDENTIFIER", "name")
        col_name2.add_child(id2)
        col_ref2.add_child(col_name2)
        
        relation = QueryTree("RELATION", "users")
        
        project.add_child(col_ref1)
        project.add_child(col_ref2)
        project.add_child(relation)
        
        check_query(project)
    
    def test_invalid_project_no_child(self):
        # PROJECT must have at least 1 child (source)
        project = QueryTree("PROJECT")
        with self.assertRaises(QueryValidationError):
            check_query(project)
    
    def test_invalid_project_star_with_columns(self):
        # PROJECT with "*" should only have source (no COLUMN_REF)
        project = QueryTree("PROJECT", "*")
        col_ref = QueryTree("COLUMN_REF")
        col_name = QueryTree("COLUMN_NAME")
        identifier = QueryTree("IDENTIFIER", "age")
        col_name.add_child(identifier)
        col_ref.add_child(col_name)
        relation = QueryTree("RELATION", "users")
        
        project.add_child(col_ref)
        project.add_child(relation)
        
        with self.assertRaises(QueryValidationError):
            check_query(project)
    
    # ====================================================================
    # FILTER TESTS
    # ====================================================================
    
    def test_valid_filter_with_comparison(self):
        # FILTER -> RELATION, COMPARISON
        # Represents: SELECT * FROM users WHERE age > 25
        filter_node = QueryTree("FILTER")
        relation = QueryTree("RELATION", "users")
        comparison = QueryTree("COMPARISON", ">")
        
        col_ref = QueryTree("COLUMN_REF")
        col_name = QueryTree("COLUMN_NAME")
        identifier = QueryTree("IDENTIFIER", "age")
        col_name.add_child(identifier)
        col_ref.add_child(col_name)
        
        literal = QueryTree("LITERAL_NUMBER", 25)
        
        comparison.add_child(col_ref)
        comparison.add_child(literal)
        
        filter_node.add_child(relation)
        filter_node.add_child(comparison)
        
        check_query(filter_node)
    
    def test_invalid_filter_wrong_child_count(self):
        # FILTER must have exactly 2 children
        filter_node = QueryTree("FILTER")
        relation = QueryTree("RELATION", "users")
        filter_node.add_child(relation)
        
        with self.assertRaises(QueryValidationError):
            check_query(filter_node)
    
    # ====================================================================
    # JOIN TESTS
    # ====================================================================
    
    def test_valid_join_natural(self):
        # JOIN(NATURAL) -> RELATION, RELATION
        # Represents: SELECT * FROM users NATURAL JOIN profiles
        join = QueryTree("JOIN", "NATURAL")
        relation1 = QueryTree("RELATION", "users")
        relation2 = QueryTree("RELATION", "profiles")
        
        join.add_child(relation1)
        join.add_child(relation2)
        
        check_query(join)
    
    def test_valid_join_inner_with_condition(self):
        # JOIN(INNER) -> RELATION, RELATION, COMPARISON
        # Represents: SELECT * FROM users INNER JOIN profiles ON users.id = profiles.user_id
        join = QueryTree("JOIN", "INNER")
        relation1 = QueryTree("RELATION", "users")
        relation2 = QueryTree("RELATION", "profiles")
        
        comparison = QueryTree("COMPARISON", "=")
        col_ref1 = QueryTree("COLUMN_REF")
        col_name1 = QueryTree("COLUMN_NAME")
        id1 = QueryTree("IDENTIFIER", "id")
        table_name1 = QueryTree("TABLE_NAME")
        tid1 = QueryTree("IDENTIFIER", "users")
        col_name1.add_child(id1)
        table_name1.add_child(tid1)
        col_ref1.add_child(col_name1)
        col_ref1.add_child(table_name1)
        
        col_ref2 = QueryTree("COLUMN_REF")
        col_name2 = QueryTree("COLUMN_NAME")
        id2 = QueryTree("IDENTIFIER", "user_id")
        table_name2 = QueryTree("TABLE_NAME")
        tid2 = QueryTree("IDENTIFIER", "profiles")
        col_name2.add_child(id2)
        table_name2.add_child(tid2)
        col_ref2.add_child(col_name2)
        col_ref2.add_child(table_name2)
        
        comparison.add_child(col_ref1)
        comparison.add_child(col_ref2)
        
        join.add_child(relation1)
        join.add_child(relation2)
        join.add_child(comparison)
        
        check_query(join)
    
    def test_invalid_join_no_value(self):
        # JOIN must have value (INNER or NATURAL)
        join = QueryTree("JOIN", "")
        relation1 = QueryTree("RELATION", "users")
        relation2 = QueryTree("RELATION", "profiles")
        
        join.add_child(relation1)
        join.add_child(relation2)
        
        with self.assertRaises(QueryValidationError):
            check_query(join)
    
    def test_invalid_join_type(self):
        # JOIN type must be INNER or NATURAL
        join = QueryTree("JOIN", "LEFT")
        relation1 = QueryTree("RELATION", "users")
        relation2 = QueryTree("RELATION", "profiles")
        
        join.add_child(relation1)
        join.add_child(relation2)
        
        with self.assertRaises(QueryValidationError):
            check_query(join)
    
    def test_invalid_join_natural_wrong_children(self):
        # NATURAL JOIN must have exactly 2 children
        join = QueryTree("JOIN", "NATURAL")
        relation1 = QueryTree("RELATION", "users")
        
        join.add_child(relation1)
        
        with self.assertRaises(QueryValidationError):
            check_query(join)
    
    def test_invalid_join_inner_wrong_children(self):
        # INNER JOIN must have exactly 3 children
        join = QueryTree("JOIN", "INNER")
        relation1 = QueryTree("RELATION", "users")
        relation2 = QueryTree("RELATION", "profiles")
        
        join.add_child(relation1)
        join.add_child(relation2)
        
        with self.assertRaises(QueryValidationError):
            check_query(join)
    
    # ====================================================================
    # COMPARISON TESTS
    # ====================================================================
    
    def test_valid_comparison(self):
        # COMPARISON(=) -> COLUMN_REF, LITERAL_NUMBER
        comparison = QueryTree("COMPARISON", "=")
        
        col_ref = QueryTree("COLUMN_REF")
        col_name = QueryTree("COLUMN_NAME")
        identifier = QueryTree("IDENTIFIER", "age")
        col_name.add_child(identifier)
        col_ref.add_child(col_name)
        
        literal = QueryTree("LITERAL_NUMBER", 25)
        
        comparison.add_child(col_ref)
        comparison.add_child(literal)
        
        check_query(comparison)
    
    def test_invalid_comparison_no_value(self):
        # COMPARISON must have operator value
        comparison = QueryTree("COMPARISON", "")
        col_ref = QueryTree("COLUMN_REF")
        col_name = QueryTree("COLUMN_NAME")
        identifier = QueryTree("IDENTIFIER", "age")
        col_name.add_child(identifier)
        col_ref.add_child(col_name)
        literal = QueryTree("LITERAL_NUMBER", 25)
        comparison.add_child(col_ref)
        comparison.add_child(literal)
        
        with self.assertRaises(QueryValidationError):
            check_query(comparison)
    
    def test_invalid_comparison_wrong_operator(self):
        # COMPARISON operator must be valid
        comparison = QueryTree("COMPARISON", "LIKE")
        col_ref = QueryTree("COLUMN_REF")
        col_name = QueryTree("COLUMN_NAME")
        identifier = QueryTree("IDENTIFIER", "name")
        col_name.add_child(identifier)
        col_ref.add_child(col_name)
        literal = QueryTree("LITERAL_STRING", "John")
        comparison.add_child(col_ref)
        comparison.add_child(literal)
        
        with self.assertRaises(QueryValidationError):
            check_query(comparison)
    
    def test_invalid_comparison_wrong_children_count(self):
        # COMPARISON must have exactly 2 children
        comparison = QueryTree("COMPARISON", "=")
        col_ref = QueryTree("COLUMN_REF")
        col_name = QueryTree("COLUMN_NAME")
        identifier = QueryTree("IDENTIFIER", "age")
        col_name.add_child(identifier)
        col_ref.add_child(col_name)
        comparison.add_child(col_ref)
        
        with self.assertRaises(QueryValidationError):
            check_query(comparison)
    
    # ====================================================================
    # OPERATOR TESTS (Logical operators)
    # ====================================================================
    
    def test_valid_operator_and(self):
        # OPERATOR(AND) -> COMPARISON, COMPARISON
        # Represents: (age > 18) AND (status = 'active')
        operator = QueryTree("OPERATOR", "AND")
        
        comp1 = QueryTree("COMPARISON", ">")
        col_ref1 = QueryTree("COLUMN_REF")
        col_name1 = QueryTree("COLUMN_NAME")
        id1 = QueryTree("IDENTIFIER", "age")
        col_name1.add_child(id1)
        col_ref1.add_child(col_name1)
        lit1 = QueryTree("LITERAL_NUMBER", 18)
        comp1.add_child(col_ref1)
        comp1.add_child(lit1)
        
        comp2 = QueryTree("COMPARISON", "=")
        col_ref2 = QueryTree("COLUMN_REF")
        col_name2 = QueryTree("COLUMN_NAME")
        id2 = QueryTree("IDENTIFIER", "status")
        col_name2.add_child(id2)
        col_ref2.add_child(col_name2)
        lit2 = QueryTree("LITERAL_STRING", "active")
        comp2.add_child(col_ref2)
        comp2.add_child(lit2)
        
        operator.add_child(comp1)
        operator.add_child(comp2)
        
        check_query(operator)
    
    def test_valid_operator_or(self):
        # OPERATOR(OR) -> COMPARISON, COMPARISON
        operator = QueryTree("OPERATOR", "OR")
        
        comp1 = QueryTree("COMPARISON", "=")
        col_ref1 = QueryTree("COLUMN_REF")
        col_name1 = QueryTree("COLUMN_NAME")
        id1 = QueryTree("IDENTIFIER", "city")
        col_name1.add_child(id1)
        col_ref1.add_child(col_name1)
        lit1 = QueryTree("LITERAL_STRING", "Jakarta")
        comp1.add_child(col_ref1)
        comp1.add_child(lit1)
        
        comp2 = QueryTree("COMPARISON", "=")
        col_ref2 = QueryTree("COLUMN_REF")
        col_name2 = QueryTree("COLUMN_NAME")
        id2 = QueryTree("IDENTIFIER", "city")
        col_name2.add_child(id2)
        col_ref2.add_child(col_name2)
        lit2 = QueryTree("LITERAL_STRING", "Bandung")
        comp2.add_child(col_ref2)
        comp2.add_child(lit2)
        
        operator.add_child(comp1)
        operator.add_child(comp2)
        
        check_query(operator)
    
    def test_valid_operator_not(self):
        # OPERATOR(NOT) -> COMPARISON
        operator = QueryTree("OPERATOR", "NOT")
        
        comp = QueryTree("COMPARISON", "<")
        col_ref = QueryTree("COLUMN_REF")
        col_name = QueryTree("COLUMN_NAME")
        identifier = QueryTree("IDENTIFIER", "age")
        col_name.add_child(identifier)
        col_ref.add_child(col_name)
        literal = QueryTree("LITERAL_NUMBER", 18)
        comp.add_child(col_ref)
        comp.add_child(literal)
        
        operator.add_child(comp)
        
        check_query(operator)
    
    def test_invalid_operator_no_value(self):
        # OPERATOR must have value
        operator = QueryTree("OPERATOR", "")
        comp = QueryTree("COMPARISON", "=")
        col_ref = QueryTree("COLUMN_REF")
        col_name = QueryTree("COLUMN_NAME")
        identifier = QueryTree("IDENTIFIER", "age")
        col_name.add_child(identifier)
        col_ref.add_child(col_name)
        literal = QueryTree("LITERAL_NUMBER", 18)
        comp.add_child(col_ref)
        comp.add_child(literal)
        operator.add_child(comp)
        
        with self.assertRaises(QueryValidationError):
            check_query(operator)
    
    def test_invalid_operator_wrong_value(self):
        # OPERATOR value must be AND/OR/NOT
        operator = QueryTree("OPERATOR", "XOR")
        comp = QueryTree("COMPARISON", "=")
        col_ref = QueryTree("COLUMN_REF")
        col_name = QueryTree("COLUMN_NAME")
        identifier = QueryTree("IDENTIFIER", "age")
        col_name.add_child(identifier)
        col_ref.add_child(col_name)
        literal = QueryTree("LITERAL_NUMBER", 18)
        comp.add_child(col_ref)
        comp.add_child(literal)
        operator.add_child(comp)
        
        with self.assertRaises(QueryValidationError):
            check_query(operator)
    
    def test_invalid_operator_and_one_child(self):
        # AND/OR need at least 2 children
        operator = QueryTree("OPERATOR", "AND")
        comp = QueryTree("COMPARISON", "=")
        col_ref = QueryTree("COLUMN_REF")
        col_name = QueryTree("COLUMN_NAME")
        identifier = QueryTree("IDENTIFIER", "age")
        col_name.add_child(identifier)
        col_ref.add_child(col_name)
        literal = QueryTree("LITERAL_NUMBER", 18)
        comp.add_child(col_ref)
        comp.add_child(literal)
        operator.add_child(comp)
        
        with self.assertRaises(QueryValidationError):
            check_query(operator)
    
    def test_invalid_operator_not_two_children(self):
        # NOT must have exactly 1 child
        operator = QueryTree("OPERATOR", "NOT")
        
        comp1 = QueryTree("COMPARISON", ">")
        col_ref1 = QueryTree("COLUMN_REF")
        col_name1 = QueryTree("COLUMN_NAME")
        id1 = QueryTree("IDENTIFIER", "age")
        col_name1.add_child(id1)
        col_ref1.add_child(col_name1)
        lit1 = QueryTree("LITERAL_NUMBER", 18)
        comp1.add_child(col_ref1)
        comp1.add_child(lit1)
        
        comp2 = QueryTree("COMPARISON", "=")
        col_ref2 = QueryTree("COLUMN_REF")
        col_name2 = QueryTree("COLUMN_NAME")
        id2 = QueryTree("IDENTIFIER", "status")
        col_name2.add_child(id2)
        col_ref2.add_child(col_name2)
        lit2 = QueryTree("LITERAL_STRING", "active")
        comp2.add_child(col_ref2)
        comp2.add_child(lit2)
        
        operator.add_child(comp1)
        operator.add_child(comp2)
        
        with self.assertRaises(QueryValidationError):
            check_query(operator)
    
    # ====================================================================
    # CONDITION EXPRESSION TESTS
    # ====================================================================
    
    def test_valid_in_expr(self):
        # IN_EXPR -> COLUMN_REF, LIST
        in_expr = QueryTree("IN_EXPR")
        
        col_ref = QueryTree("COLUMN_REF")
        col_name = QueryTree("COLUMN_NAME")
        identifier = QueryTree("IDENTIFIER", "category")
        col_name.add_child(identifier)
        col_ref.add_child(col_name)
        
        list_node = QueryTree("LIST")
        lit1 = QueryTree("LITERAL_STRING", "Electronics")
        lit2 = QueryTree("LITERAL_STRING", "Books")
        list_node.add_child(lit1)
        list_node.add_child(lit2)
        
        in_expr.add_child(col_ref)
        in_expr.add_child(list_node)
        
        check_query(in_expr)
    
    def test_valid_between_expr(self):
        # BETWEEN_EXPR -> COLUMN_REF, LITERAL, LITERAL
        between_expr = QueryTree("BETWEEN_EXPR")
        
        col_ref = QueryTree("COLUMN_REF")
        col_name = QueryTree("COLUMN_NAME")
        identifier = QueryTree("IDENTIFIER", "price")
        col_name.add_child(identifier)
        col_ref.add_child(col_name)
        
        lit1 = QueryTree("LITERAL_NUMBER", 100)
        lit2 = QueryTree("LITERAL_NUMBER", 500)
        
        between_expr.add_child(col_ref)
        between_expr.add_child(lit1)
        between_expr.add_child(lit2)
        
        check_query(between_expr)
    
    def test_valid_is_null_expr(self):
        # IS_NULL_EXPR -> COLUMN_REF
        is_null = QueryTree("IS_NULL_EXPR")
        
        col_ref = QueryTree("COLUMN_REF")
        col_name = QueryTree("COLUMN_NAME")
        identifier = QueryTree("IDENTIFIER", "description")
        col_name.add_child(identifier)
        col_ref.add_child(col_name)
        
        is_null.add_child(col_ref)
        
        check_query(is_null)
    
    def test_valid_is_not_null_expr(self):
        # IS_NOT_NULL_EXPR -> COLUMN_REF
        is_not_null = QueryTree("IS_NOT_NULL_EXPR")
        
        col_ref = QueryTree("COLUMN_REF")
        col_name = QueryTree("COLUMN_NAME")
        identifier = QueryTree("IDENTIFIER", "description")
        col_name.add_child(identifier)
        col_ref.add_child(col_name)
        
        is_not_null.add_child(col_ref)
        
        check_query(is_not_null)
    
    def test_invalid_between_expr_wrong_children(self):
        # BETWEEN_EXPR must have exactly 3 children
        between_expr = QueryTree("BETWEEN_EXPR")
        
        col_ref = QueryTree("COLUMN_REF")
        col_name = QueryTree("COLUMN_NAME")
        identifier = QueryTree("IDENTIFIER", "price")
        col_name.add_child(identifier)
        col_ref.add_child(col_name)
        
        between_expr.add_child(col_ref)
        
        with self.assertRaises(QueryValidationError):
            check_query(between_expr)
    
    # ====================================================================
    # ARITH_EXPR TESTS
    # ====================================================================
    
    def test_valid_arith_expr(self):
        # ARITH_EXPR(*) -> COLUMN_REF, LITERAL
        # Represents: salary * 1.1
        arith = QueryTree("ARITH_EXPR", "*")
        
        col_ref = QueryTree("COLUMN_REF")
        col_name = QueryTree("COLUMN_NAME")
        identifier = QueryTree("IDENTIFIER", "salary")
        col_name.add_child(identifier)
        col_ref.add_child(col_name)
        
        literal = QueryTree("LITERAL_NUMBER", 1.1)
        
        arith.add_child(col_ref)
        arith.add_child(literal)
        
        check_query(arith)
    
    def test_invalid_arith_expr_no_operator(self):
        # ARITH_EXPR must have operator value
        arith = QueryTree("ARITH_EXPR", "")
        col_ref = QueryTree("COLUMN_REF")
        col_name = QueryTree("COLUMN_NAME")
        identifier = QueryTree("IDENTIFIER", "salary")
        col_name.add_child(identifier)
        col_ref.add_child(col_name)
        literal = QueryTree("LITERAL_NUMBER", 1.1)
        arith.add_child(col_ref)
        arith.add_child(literal)
        
        with self.assertRaises(QueryValidationError):
            check_query(arith)
    
    def test_invalid_arith_expr_wrong_operator(self):
        # ARITH_EXPR operator must be valid
        arith = QueryTree("ARITH_EXPR", "^")
        col_ref = QueryTree("COLUMN_REF")
        col_name = QueryTree("COLUMN_NAME")
        identifier = QueryTree("IDENTIFIER", "salary")
        col_name.add_child(identifier)
        col_ref.add_child(col_name)
        literal = QueryTree("LITERAL_NUMBER", 2)
        arith.add_child(col_ref)
        arith.add_child(literal)
        
        with self.assertRaises(QueryValidationError):
            check_query(arith)
    
    # ====================================================================
    # SORT TESTS
    # ====================================================================
    
    def test_valid_sort(self):
        # SORT(ASC) -> COLUMN_REF, RELATION
        sort = QueryTree("SORT", "ASC")
        
        col_ref = QueryTree("COLUMN_REF")
        col_name = QueryTree("COLUMN_NAME")
        identifier = QueryTree("IDENTIFIER", "name")
        col_name.add_child(identifier)
        col_ref.add_child(col_name)
        
        relation = QueryTree("RELATION", "users")
        
        sort.add_child(col_ref)
        sort.add_child(relation)
        
        check_query(sort)
    
    def test_valid_sort_desc(self):
        # SORT(DESC) -> COLUMN_REF, RELATION
        sort = QueryTree("SORT", "DESC")
        
        col_ref = QueryTree("COLUMN_REF")
        col_name = QueryTree("COLUMN_NAME")
        identifier = QueryTree("IDENTIFIER", "age")
        col_name.add_child(identifier)
        col_ref.add_child(col_name)
        
        relation = QueryTree("RELATION", "users")
        
        sort.add_child(col_ref)
        sort.add_child(relation)
        
        check_query(sort)
    
    def test_invalid_sort_wrong_direction(self):
        # SORT direction must be ASC or DESC
        sort = QueryTree("SORT", "INVALID")
        col_ref = QueryTree("COLUMN_REF")
        col_name = QueryTree("COLUMN_NAME")
        identifier = QueryTree("IDENTIFIER", "name")
        col_name.add_child(identifier)
        col_ref.add_child(col_name)
        relation = QueryTree("RELATION", "users")
        sort.add_child(col_ref)
        sort.add_child(relation)
        
        with self.assertRaises(QueryValidationError):
            check_query(sort)
    
    def test_invalid_sort_wrong_children(self):
        # SORT must have exactly 2 children
        sort = QueryTree("SORT", "ASC")
        relation = QueryTree("RELATION", "users")
        sort.add_child(relation)
        
        with self.assertRaises(QueryValidationError):
            check_query(sort)
    
    def test_invalid_sort_first_child_not_column_ref(self):
        # First child of SORT must be COLUMN_REF
        sort = QueryTree("SORT", "ASC")
        literal = QueryTree("LITERAL_NUMBER", 1)
        relation = QueryTree("RELATION", "users")
        sort.add_child(literal)
        sort.add_child(relation)
        
        with self.assertRaises(QueryValidationError):
            check_query(sort)
    
    # ====================================================================
    # ALIAS TESTS
    # ====================================================================
    
    def test_valid_alias(self):
        # ALIAS -> RELATION
        # Represents: users AS u
        alias = QueryTree("ALIAS", "u")
        relation = QueryTree("RELATION", "users")
        alias.add_child(relation)
        
        check_query(alias)
    
    def test_invalid_alias_no_value(self):
        # ALIAS must have alias name
        alias = QueryTree("ALIAS", "")
        relation = QueryTree("RELATION", "users")
        alias.add_child(relation)
        
        with self.assertRaises(QueryValidationError):
            check_query(alias)
    
    def test_invalid_alias_wrong_children(self):
        # ALIAS must have exactly 1 child
        alias = QueryTree("ALIAS", "u")
        
        with self.assertRaises(QueryValidationError):
            check_query(alias)
    
    # ====================================================================
    # LIMIT TESTS
    # ====================================================================
    
    def test_valid_limit(self):
        # LIMIT -> RELATION
        limit = QueryTree("LIMIT", 10)
        relation = QueryTree("RELATION", "users")
        limit.add_child(relation)
        
        check_query(limit)
    
    def test_invalid_limit_wrong_children(self):
        # LIMIT must have exactly 1 child
        limit = QueryTree("LIMIT", 10)
        
        with self.assertRaises(QueryValidationError):
            check_query(limit)
    
    # ====================================================================
    # COMPLEX QUERY TESTS
    # ====================================================================
    
    def test_complete_query_simple_where(self):
        # SELECT name FROM users WHERE age > 25
        # PROJECT -> COLUMN_REF, FILTER -> RELATION, COMPARISON
        project = QueryTree("PROJECT")
        
        col_ref = QueryTree("COLUMN_REF")
        col_name = QueryTree("COLUMN_NAME")
        id_name = QueryTree("IDENTIFIER", "name")
        col_name.add_child(id_name)
        col_ref.add_child(col_name)
        
        filter_node = QueryTree("FILTER")
        relation = QueryTree("RELATION", "users")
        
        comparison = QueryTree("COMPARISON", ">")
        col_ref_age = QueryTree("COLUMN_REF")
        col_name_age = QueryTree("COLUMN_NAME")
        id_age = QueryTree("IDENTIFIER", "age")
        col_name_age.add_child(id_age)
        col_ref_age.add_child(col_name_age)
        literal = QueryTree("LITERAL_NUMBER", 25)
        comparison.add_child(col_ref_age)
        comparison.add_child(literal)
        
        filter_node.add_child(relation)
        filter_node.add_child(comparison)
        
        project.add_child(col_ref)
        project.add_child(filter_node)
        
        check_query(project)
    
    def test_complete_query_with_and(self):
        # SELECT * FROM users WHERE age > 25 AND status = 'active'
        # PROJECT(*) -> FILTER -> RELATION, OPERATOR(AND) -> COMPARISON, COMPARISON
        project = QueryTree("PROJECT", "*")
        filter_node = QueryTree("FILTER")
        relation = QueryTree("RELATION", "users")
        
        operator_and = QueryTree("OPERATOR", "AND")
        
        comp1 = QueryTree("COMPARISON", ">")
        col_ref1 = QueryTree("COLUMN_REF")
        col_name1 = QueryTree("COLUMN_NAME")
        id1 = QueryTree("IDENTIFIER", "age")
        col_name1.add_child(id1)
        col_ref1.add_child(col_name1)
        lit1 = QueryTree("LITERAL_NUMBER", 25)
        comp1.add_child(col_ref1)
        comp1.add_child(lit1)
        
        comp2 = QueryTree("COMPARISON", "=")
        col_ref2 = QueryTree("COLUMN_REF")
        col_name2 = QueryTree("COLUMN_NAME")
        id2 = QueryTree("IDENTIFIER", "status")
        col_name2.add_child(id2)
        col_ref2.add_child(col_name2)
        lit2 = QueryTree("LITERAL_STRING", "active")
        comp2.add_child(col_ref2)
        comp2.add_child(lit2)
        
        operator_and.add_child(comp1)
        operator_and.add_child(comp2)
        
        filter_node.add_child(relation)
        filter_node.add_child(operator_and)
        
        project.add_child(filter_node)
        
        check_query(project)
    
    def test_complete_query_with_join(self):
        # SELECT users.name FROM users INNER JOIN profiles ON users.id = profiles.user_id
        # PROJECT -> COLUMN_REF, JOIN(INNER) -> RELATION, RELATION, COMPARISON
        project = QueryTree("PROJECT")
        
        col_ref = QueryTree("COLUMN_REF")
        col_name = QueryTree("COLUMN_NAME")
        id_name = QueryTree("IDENTIFIER", "name")
        table_name = QueryTree("TABLE_NAME")
        id_table = QueryTree("IDENTIFIER", "users")
        col_name.add_child(id_name)
        table_name.add_child(id_table)
        col_ref.add_child(col_name)
        col_ref.add_child(table_name)
        
        join = QueryTree("JOIN", "INNER")
        relation1 = QueryTree("RELATION", "users")
        relation2 = QueryTree("RELATION", "profiles")
        
        comparison = QueryTree("COMPARISON", "=")
        col_ref1 = QueryTree("COLUMN_REF")
        col_name1 = QueryTree("COLUMN_NAME")
        id1 = QueryTree("IDENTIFIER", "id")
        table_name1 = QueryTree("TABLE_NAME")
        tid1 = QueryTree("IDENTIFIER", "users")
        col_name1.add_child(id1)
        table_name1.add_child(tid1)
        col_ref1.add_child(col_name1)
        col_ref1.add_child(table_name1)
        
        col_ref2 = QueryTree("COLUMN_REF")
        col_name2 = QueryTree("COLUMN_NAME")
        id2 = QueryTree("IDENTIFIER", "user_id")
        table_name2 = QueryTree("TABLE_NAME")
        tid2 = QueryTree("IDENTIFIER", "profiles")
        col_name2.add_child(id2)
        table_name2.add_child(tid2)
        col_ref2.add_child(col_name2)
        col_ref2.add_child(table_name2)
        
        comparison.add_child(col_ref1)
        comparison.add_child(col_ref2)
        
        join.add_child(relation1)
        join.add_child(relation2)
        join.add_child(comparison)
        
        project.add_child(col_ref)
        project.add_child(join)
        
        check_query(project)


if __name__ == '__main__':
    unittest.main()