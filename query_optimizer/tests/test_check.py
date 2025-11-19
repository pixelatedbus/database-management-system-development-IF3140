import unittest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from query_optimizer.query_check import QueryTree, check_query, QueryValidationError


class TestQueryValidator(unittest.TestCase):
    
    # ====================================================================
    # PROJECT TESTS
    # ====================================================================
    
    def test_valid_project_with_filter(self):
        # Structure: PROJECT -> FILTER -> RELATION
        # Represents: SELECT id, name FROM users WHERE id = 1;
        project = QueryTree("PROJECT", "id, name")
        filter_node = QueryTree("FILTER", "WHERE id = 1")
        relation = QueryTree("RELATION", "users")
        
        project.add_child(filter_node)
        filter_node.add_child(relation)
        
        check_query(project)

    def test_invalid_project_no_child(self):
        # Structure: PROJECT (no children)
        # Represents: SELECT id, name;
        # Invalid because PROJECT must have one child
        project = QueryTree("PROJECT", "id, name")
        
        with self.assertRaises(QueryValidationError):
            check_query(project)
    
    # ====================================================================
    # JOIN TESTS
    # ====================================================================
    
    def test_valid_join_with_on(self):
        # Structure: PROJECT -> JOIN -> RELATION, RELATION
        # Represents: SELECT * FROM users JOIN profiles ON users.id = profiles.user_id;
        project = QueryTree("PROJECT", "*")
        join = QueryTree("JOIN", "ON users.id = profiles.user_id")
        relation1 = QueryTree("RELATION", "users")
        relation2 = QueryTree("RELATION", "profiles")
        
        project.add_child(join)
        join.add_child(relation1)
        join.add_child(relation2)
        
        check_query(project)

    def test_valid_join_natural(self):
        # Structure: JOIN -> RELATION, RELATION
        # Represents: SELECT * FROM users NATURAL JOIN profiles;
        join = QueryTree("JOIN", "NATURAL")
        relation1 = QueryTree("RELATION", "users")
        relation2 = QueryTree("RELATION", "profiles")
        
        join.add_child(relation1)
        join.add_child(relation2)
        
        check_query(join)

    def test_invalid_join_not_enough_children(self):
        # Structure: JOIN -> RELATION
        # Represents: SELECT * FROM users JOIN profiles ON users.id = profiles.user_id;
        # Invalid because JOIN must have two children
        join = QueryTree("JOIN", "ON users.id = profiles.user_id")
        relation1 = QueryTree("RELATION", "users")
        
        join.add_child(relation1)
        
        with self.assertRaises(QueryValidationError):
            check_query(join)
    
    def test_invalid_join_type(self):
        # Structure: JOIN -> RELATION, RELATION
        # Represents: SELECT * FROM users LEFT JOIN profiles ON users.id = profiles.user_id;
        # Invalid because JOIN type is not recognized
        join = QueryTree("JOIN", "LEFT ON users.id = profiles.user_id")
        relation1 = QueryTree("RELATION", "users")
        relation2 = QueryTree("RELATION", "profiles")
        
        join.add_child(relation1)
        join.add_child(relation2)
        
        with self.assertRaises(QueryValidationError):
            check_query(join)
    
    # ====================================================================
    # RELATION TESTS
    # ====================================================================
    
    def test_valid_relation_leaf(self):
        # Structure: RELATION
        # Represents: FROM users;
        # Should be valid, but not have any result
        relation = QueryTree("RELATION", "users")
        
        check_query(relation)
    
    def test_invalid_relation_with_child(self):
        # Structure: RELATION -> FILTER
        # Represents: FROM users WHERE id = 1;
        # Invalid because RELATION cannot have children, FILTER must be above RELATION
        relation = QueryTree("RELATION", "users")
        child = QueryTree("FILTER", "id = 1")
        relation.add_child(child)
        
        with self.assertRaises(QueryValidationError):
            check_query(relation)
    
    def test_invalid_relation_no_table_name(self):
        # Structure: RELATION
        # Represents: FROM ;
        # Invalid because table name is missing
        relation = QueryTree("RELATION", "")
        
        with self.assertRaises(QueryValidationError):
            check_query(relation)
    
    def test_invalid_table_name(self):
        # Structure: RELATION
        # Represents: FROM nonexistent_table;
        # Invalid because table name is not recognized
        relation = QueryTree("RELATION", "nonexistent_table")
        
        with self.assertRaises(QueryValidationError):
            check_query(relation)
    
    # ====================================================================
    # FILTER TESTS
    # ====================================================================
    
    def test_valid_filter_where_single_relation(self):
        # Structure: FILTER -> RELATION
        # Represents: SELECT * FROM users WHERE id = 1;
        # FILTER with WHERE condition and single RELATION child
        filter_node = QueryTree("FILTER", "WHERE id = 1")
        relation = QueryTree("RELATION", "users")
        
        filter_node.add_child(relation)
        
        check_query(filter_node)
    
    def test_valid_filter_in_with_array(self):
        # Structure: FILTER -> RELATION, ARRAY
        # Represents: SELECT * FROM users WHERE id IN (1, 2, 3);
        # FILTER with IN condition, RELATION and ARRAY as children
        filter_node = QueryTree("FILTER", "IN id")
        relation = QueryTree("RELATION", "users")
        array = QueryTree("ARRAY", "(1, 2, 3)")
        
        filter_node.add_child(relation)
        filter_node.add_child(array)
        
        check_query(filter_node)
    
    def test_valid_filter_exist(self):
        # Structure: FILTER -> RELATION
        # Represents: SELECT * FROM users WHERE EXISTS (SELECT 1 FROM profiles WHERE profiles.user_id = users.id);
        # FILTER with EXIST (subquery check) and single RELATION child
        filter_node = QueryTree("FILTER", "EXIST")
        relation = QueryTree("RELATION", "users")
        
        filter_node.add_child(relation)
        
        check_query(filter_node)
    
    def test_valid_filter_where_complex(self):
        # Structure: FILTER -> RELATION
        # Represents: SELECT * FROM users WHERE name = 'John' AND age > 25;
        # FILTER with complex WHERE condition
        filter_node = QueryTree("FILTER", "WHERE name = 'John' AND age > 25")
        relation = QueryTree("RELATION", "users")
        
        filter_node.add_child(relation)
        
        check_query(filter_node)
    
    def test_valid_filter_in_with_project(self):
        # Structure: PROJECT -> FILTER -> RELATION, ARRAY
        # Represents: SELECT name FROM users WHERE id IN (1, 2, 3);
        # Complete query with FILTER using IN - anak ke-2 adalah value (ARRAY)
        project = QueryTree("PROJECT", "name")
        filter_node = QueryTree("FILTER", "IN id")
        relation = QueryTree("RELATION", "users")
        array = QueryTree("ARRAY", "(1, 2, 3)")
        
        project.add_child(filter_node)
        filter_node.add_child(relation)
        filter_node.add_child(array)
        
        check_query(project)
    
    def test_valid_filter_on_sort(self):
        # Structure: PROJECT -> FILTER -> SORT -> RELATION
        # Represents: SELECT * FROM users ORDER BY name WHERE id > 10;
        # FILTER can accept SORT result as child
        project = QueryTree("PROJECT", "*")
        filter_node = QueryTree("FILTER", "WHERE id > 10")
        sort = QueryTree("SORT", "name")
        relation = QueryTree("RELATION", "users")
        
        project.add_child(filter_node)
        filter_node.add_child(sort)
        sort.add_child(relation)
        
        check_query(project)
    
    def test_valid_filter_on_project(self):
        # Structure: FILTER -> PROJECT -> RELATION
        # Represents: SELECT * FROM (SELECT id, name FROM users) WHERE id > 10;
        # FILTER can accept PROJECT result (subquery-like)
        filter_node = QueryTree("FILTER", "WHERE id > 10")
        project = QueryTree("PROJECT", "id, name")
        relation = QueryTree("RELATION", "users")
        
        filter_node.add_child(project)
        project.add_child(relation)
        
        check_query(filter_node)
    
    def test_invalid_filter_no_children(self):
        # Structure: FILTER (no children)
        # VALID: FILTER dengan WHERE/IN bisa jadi condition leaf (0 children)
        # Ini digunakan sebagai child dari AND/OR
        filter_node = QueryTree("FILTER", "WHERE id = 1")
        
        # Should NOT raise error - condition leaf is valid
        check_query(filter_node)
    
    def test_invalid_filter_wrong_second_child(self):
        # Structure: FILTER -> RELATION, FILTER
        # Invalid because second child must be ARRAY/RELATION/PROJECT for value
        filter_node = QueryTree("FILTER", "IN id")
        relation = QueryTree("RELATION", "users")
        filter2 = QueryTree("FILTER", "WHERE id = 1")
        
        filter_node.add_child(relation)
        filter_node.add_child(filter2)
        
        with self.assertRaises(QueryValidationError):
            check_query(filter_node)
    
    def test_invalid_filter_wrong_keyword_single_word(self):
        # Structure: FILTER -> RELATION
        # Invalid because single-word FILTER value must be 'EXIST'
        filter_node = QueryTree("FILTER", "INVALID")
        relation = QueryTree("RELATION", "users")
        
        filter_node.add_child(relation)
        
        with self.assertRaises(QueryValidationError):
            check_query(filter_node)
    
    def test_invalid_filter_wrong_keyword_multi_word(self):
        # Structure: FILTER -> RELATION
        # Invalid because multi-word FILTER value must start with 'WHERE' or 'IN'
        filter_node = QueryTree("FILTER", "HAVING age > 25")
        relation = QueryTree("RELATION", "users")
        
        filter_node.add_child(relation)
        
        with self.assertRaises(QueryValidationError):
            check_query(filter_node)
    
    def test_invalid_filter_too_many_children(self):
        # Structure: FILTER -> RELATION, ARRAY, ARRAY
        # Invalid because FILTER can have maximum 2 children
        filter_node = QueryTree("FILTER", "IN id")
        relation = QueryTree("RELATION", "users")
        array1 = QueryTree("ARRAY", "(1, 2, 3)")
        array2 = QueryTree("ARRAY", "(4, 5, 6)")
        
        filter_node.add_child(relation)
        filter_node.add_child(array1)
        filter_node.add_child(array2)
        
        with self.assertRaises(QueryValidationError):
            check_query(filter_node)
    
    # ====================================================================
    # UPDATE TESTS
    # ====================================================================
    
    def test_valid_update(self):
        # Structure: UPDATE -> RELATION
        # Represents: UPDATE users SET name = 'test', email = 'test@example.com';
        update = QueryTree("UPDATE", "name = 'test', email = 'test@example.com'")
        relation = QueryTree("RELATION", "users")
        
        update.add_child(relation)
        
        check_query(update)

    def test_invalid_update_no_child(self):
        # Structure: UPDATE (no children)
        # Represents: UPDATE ?? SET name = 'test';
        # Invalid because UPDATE must have one child
        update = QueryTree("UPDATE", "name = 'test'")
        
        with self.assertRaises(QueryValidationError):
            check_query(update)
    
    # ====================================================================
    # INSERT TESTS
    # ====================================================================
    
    def test_valid_insert(self):
        # Structure: INSERT -> RELATION
        # Represents: INSERT INTO users (name, email) VALUES ('John', 'john@example.com');
        insert = QueryTree("INSERT", "name = 'John', email = 'john@example.com'")
        relation = QueryTree("RELATION", "users")
        
        insert.add_child(relation)
        
        check_query(insert)
    
    def test_invalid_insert_no_child(self):
        # Structure: INSERT (no children)
        # Represents: INSERT INTO users (name, email) VALUES ('John', 'john@example.com');
        # Invalid because INSERT must have exactly one child (relation)
        insert = QueryTree("INSERT", "name = 'John', email = 'john@example.com'")
        
        with self.assertRaises(QueryValidationError):
            check_query(insert)
    
    def test_invalid_insert_multiple_children(self):
        # Structure: INSERT -> RELATION, RELATION
        # Invalid because INSERT cannot have subquery (multiple children)
        insert = QueryTree("INSERT", "name = 'John'")
        relation1 = QueryTree("RELATION", "users")
        relation2 = QueryTree("RELATION", "profiles")
        
        insert.add_child(relation1)
        insert.add_child(relation2)
        
        with self.assertRaises(QueryValidationError):
            check_query(insert)
    
    # ====================================================================
    # DELETE TESTS
    # ====================================================================
    
    def test_valid_delete(self):
        # Structure: DELETE -> RELATION
        # Represents: DELETE FROM users;
        delete = QueryTree("DELETE")
        relation = QueryTree("RELATION", "users")
        
        delete.add_child(relation)
        
        check_query(delete)
    
    def test_valid_delete_with_filter(self):
        # Structure: DELETE -> FILTER -> RELATION
        # Represents: DELETE FROM users WHERE id = 1;
        delete = QueryTree("DELETE")
        filter_node = QueryTree("FILTER", "WHERE id = 1")
        relation = QueryTree("RELATION", "users")
        
        delete.add_child(filter_node)
        filter_node.add_child(relation)
        
        check_query(delete)
    
    def test_invalid_delete_no_child(self):
        # Structure: DELETE (no children)
        # Represents: DELETE FROM ??;
        # Invalid because DELETE must have one child
        delete = QueryTree("DELETE")
        
        with self.assertRaises(QueryValidationError):
            check_query(delete)
    
    def test_invalid_delete_multiple_children(self):
        # Structure: DELETE -> RELATION, RELATION
        # Invalid because DELETE must have exactly one child
        delete = QueryTree("DELETE")
        relation1 = QueryTree("RELATION", "users")
        relation2 = QueryTree("RELATION", "profiles")
        
        delete.add_child(relation1)
        delete.add_child(relation2)
        
        with self.assertRaises(QueryValidationError):
            check_query(delete)
    
    # ====================================================================
    # TRANSACTION TESTS
    # ====================================================================
    
    def test_transaction_begin(self):
        # Structure: BEGIN_TRANSACTION -> UPDATE -> RELATION, UPDATE -> RELATION
        # Represents: BEGIN TRANSACTION; UPDATE users SET name = 'test'; UPDATE users SET email = 'test@example.com';
        begin_trans = QueryTree("BEGIN_TRANSACTION")
        update1 = QueryTree("UPDATE", "name = 'test'")
        update2 = QueryTree("UPDATE", "email = 'test@example.com'")
        relation1 = QueryTree("RELATION", "users")
        relation2 = QueryTree("RELATION", "users")
        
        update1.add_child(relation1)
        update2.add_child(relation2)
        begin_trans.add_child(update1)
        begin_trans.add_child(update2)
        
        check_query(begin_trans)
    
    def test_commit(self):
        # Structure: COMMIT
        # Represents: COMMIT;
        commit = QueryTree("COMMIT")
        check_query(commit)
    
    # ====================================================================
    # COMPLEX QUERY TESTS
    # ====================================================================
    
    def test_complex_query_with_filter_on_join(self):
        # Structure: PROJECT -> FILTER -> JOIN -> RELATION, RELATION
        # Represents: SELECT * FROM users JOIN profiles ON users.id = profiles.user_id WHERE users.id > 10;
        # FILTER applied on JOIN result (filtering after join)
        project = QueryTree("PROJECT", "*")
        filter_node = QueryTree("FILTER", "WHERE users.id > 10")
        join = QueryTree("JOIN", "ON users.id = profiles.user_id")
        relation1 = QueryTree("RELATION", "users")
        relation2 = QueryTree("RELATION", "profiles")
        
        project.add_child(filter_node)
        filter_node.add_child(join)
        join.add_child(relation1)
        join.add_child(relation2)
        
        check_query(project)
    
    # ====================================================================
    # OPERATOR TESTS (Logical operators: AND/OR/NOT)
    # ====================================================================
    
    def test_valid_operator_and_nested(self):
        # Structure: OPERATOR(AND) -> FILTER, FILTER
        # Represents: (age > 18) AND (status = 'active')
        # Nested AND without explicit source
        operator_and = QueryTree("OPERATOR", "AND")
        filter1 = QueryTree("FILTER", "WHERE age > 18")
        filter2 = QueryTree("FILTER", "WHERE status = 'active'")
        
        operator_and.add_child(filter1)
        operator_and.add_child(filter2)
        
        check_query(operator_and)
    
    def test_valid_operator_or_nested(self):
        # Structure: OPERATOR(OR) -> FILTER, FILTER, FILTER
        # Represents: (city = 'Jakarta') OR (city = 'Bandung') OR (city = 'Surabaya')
        operator_or = QueryTree("OPERATOR", "OR")
        filter1 = QueryTree("FILTER", "WHERE city = 'Jakarta'")
        filter2 = QueryTree("FILTER", "WHERE city = 'Bandung'")
        filter3 = QueryTree("FILTER", "WHERE city = 'Surabaya'")
        
        operator_or.add_child(filter1)
        operator_or.add_child(filter2)
        operator_or.add_child(filter3)
        
        check_query(operator_or)
    
    def test_valid_operator_not(self):
        # Structure: OPERATOR(NOT) -> FILTER
        # Represents: NOT (age < 18)
        operator_not = QueryTree("OPERATOR", "NOT")
        filter1 = QueryTree("FILTER", "WHERE age < 18")
        
        operator_not.add_child(filter1)
        
        check_query(operator_not)
    
    def test_valid_operator_not_with_operator_s(self):
        # Structure: OPERATOR(NOT) -> OPERATOR_S(AND)
        # Represents: NOT ((age > 18) AND (status = 'active'))
        operator_not = QueryTree("OPERATOR", "NOT")
        operator_and = QueryTree("OPERATOR_S", "AND")
        relation = QueryTree("RELATION", "users")
        filter1 = QueryTree("FILTER", "WHERE age > 18")
        filter2 = QueryTree("FILTER", "WHERE status = 'active'")
        
        operator_and.add_child(relation)
        operator_and.add_child(filter1)
        operator_and.add_child(filter2)
        operator_not.add_child(operator_and)
        
        check_query(operator_not)
    
    def test_invalid_operator_no_value(self):
        # Structure: OPERATOR -> FILTER, FILTER
        # Invalid because OPERATOR must have value (AND/OR/NOT)
        operator = QueryTree("OPERATOR", "")
        filter1 = QueryTree("FILTER", "WHERE age > 18")
        filter2 = QueryTree("FILTER", "WHERE status = 'active'")
        
        operator.add_child(filter1)
        operator.add_child(filter2)
        
        with self.assertRaises(QueryValidationError):
            check_query(operator)
    
    def test_invalid_operator_wrong_value(self):
        # Structure: OPERATOR(INVALID) -> FILTER, FILTER
        # Invalid because OPERATOR value must be AND/OR/NOT
        operator = QueryTree("OPERATOR", "INVALID")
        filter1 = QueryTree("FILTER", "WHERE age > 18")
        filter2 = QueryTree("FILTER", "WHERE status = 'active'")
        
        operator.add_child(filter1)
        operator.add_child(filter2)
        
        with self.assertRaises(QueryValidationError):
            check_query(operator)
    
    def test_invalid_operator_and_one_child(self):
        # Structure: OPERATOR(AND) -> FILTER
        # Invalid because AND/OR need minimum 2 children
        operator_and = QueryTree("OPERATOR", "AND")
        filter1 = QueryTree("FILTER", "WHERE age > 18")
        
        operator_and.add_child(filter1)
        
        with self.assertRaises(QueryValidationError):
            check_query(operator_and)
    
    def test_invalid_operator_not_multiple_children(self):
        # Structure: OPERATOR(NOT) -> FILTER, FILTER
        # Invalid because NOT must have exactly 1 child
        operator_not = QueryTree("OPERATOR", "NOT")
        filter1 = QueryTree("FILTER", "WHERE age > 18")
        filter2 = QueryTree("FILTER", "WHERE status = 'active'")
        
        operator_not.add_child(filter1)
        operator_not.add_child(filter2)
        
        with self.assertRaises(QueryValidationError):
            check_query(operator_not)
    
    def test_invalid_operator_non_filter_child(self):
        # Structure: OPERATOR(AND) -> RELATION, FILTER
        # Invalid because OPERATOR children must be FILTER/OPERATOR/OPERATOR_S
        operator_and = QueryTree("OPERATOR", "AND")
        relation = QueryTree("RELATION", "users")
        filter1 = QueryTree("FILTER", "WHERE age > 18")
        
        operator_and.add_child(relation)
        operator_and.add_child(filter1)
        
        with self.assertRaises(QueryValidationError):
            check_query(operator_and)
    
    # ====================================================================
    # OPERATOR_S TESTS (Logical operators with source)
    # ====================================================================
    
    def test_valid_operator_s_and_with_relation(self):
        # Structure: OPERATOR_S(AND) -> RELATION, FILTER, FILTER
        # Represents: SELECT * FROM users WHERE (age > 18) AND (status = 'active')
        operator_and = QueryTree("OPERATOR_S", "AND")
        relation = QueryTree("RELATION", "users")
        filter1 = QueryTree("FILTER", "WHERE age > 18")
        filter2 = QueryTree("FILTER", "WHERE status = 'active'")
        
        operator_and.add_child(relation)
        operator_and.add_child(filter1)
        operator_and.add_child(filter2)
        
        check_query(operator_and)
    
    def test_valid_operator_s_or_with_join(self):
        # Structure: OPERATOR_S(OR) -> JOIN, FILTER, FILTER
        # Represents: SELECT * FROM users JOIN profiles WHERE (age > 18) OR (status = 'active')
        operator_or = QueryTree("OPERATOR_S", "OR")
        join = QueryTree("JOIN", "NATURAL")
        relation1 = QueryTree("RELATION", "users")
        relation2 = QueryTree("RELATION", "profiles")
        filter1 = QueryTree("FILTER", "WHERE age > 18")
        filter2 = QueryTree("FILTER", "WHERE status = 'active'")
        
        join.add_child(relation1)
        join.add_child(relation2)
        operator_or.add_child(join)
        operator_or.add_child(filter1)
        operator_or.add_child(filter2)
        
        check_query(operator_or)
    
    def test_valid_operator_s_with_sort_source(self):
        # Structure: OPERATOR_S(AND) -> SORT, FILTER, FILTER
        # Represents: SELECT * FROM users ORDER BY name WHERE (age > 18) AND (status = 'active')
        operator_and = QueryTree("OPERATOR_S", "AND")
        sort = QueryTree("SORT", "name")
        relation = QueryTree("RELATION", "users")
        filter1 = QueryTree("FILTER", "WHERE age > 18")
        filter2 = QueryTree("FILTER", "WHERE status = 'active'")
        
        sort.add_child(relation)
        operator_and.add_child(sort)
        operator_and.add_child(filter1)
        operator_and.add_child(filter2)
        
        check_query(operator_and)
    
    def test_valid_operator_s_with_operator_s_source(self):
        # Structure: OPERATOR_S(OR) -> OPERATOR_S(AND), FILTER, FILTER
        # Nested OPERATOR_S as source (OPERATOR_S produces data)
        outer_or = QueryTree("OPERATOR_S", "OR")
        inner_and = QueryTree("OPERATOR_S", "AND")
        relation = QueryTree("RELATION", "users")
        filter1 = QueryTree("FILTER", "WHERE age > 18")
        filter2 = QueryTree("FILTER", "WHERE status = 'active'")
        filter3 = QueryTree("FILTER", "WHERE city = 'Jakarta'")
        filter4 = QueryTree("FILTER", "WHERE verified = true")
        
        inner_and.add_child(relation)
        inner_and.add_child(filter1)
        inner_and.add_child(filter2)
        
        outer_or.add_child(inner_and)
        outer_or.add_child(filter3)
        outer_or.add_child(filter4)
        
        check_query(outer_or)
    
    def test_valid_operator_s_with_filter_source(self):
        # Structure: OPERATOR_S(AND) -> FILTER(with child), FILTER, FILTER
        # FILTER with child (produces data) as source
        operator_and = QueryTree("OPERATOR_S", "AND")
        filter_source = QueryTree("FILTER", "WHERE verified = true")
        relation = QueryTree("RELATION", "users")
        filter1 = QueryTree("FILTER", "WHERE age > 18")
        filter2 = QueryTree("FILTER", "WHERE status = 'active'")
        
        filter_source.add_child(relation)
        operator_and.add_child(filter_source)
        operator_and.add_child(filter1)
        operator_and.add_child(filter2)
        
        check_query(operator_and)
    
    def test_invalid_operator_s_no_value(self):
        # Structure: OPERATOR_S -> RELATION, FILTER, FILTER
        # Invalid because OPERATOR_S must have value (AND/OR)
        operator_s = QueryTree("OPERATOR_S", "")
        relation = QueryTree("RELATION", "users")
        filter1 = QueryTree("FILTER", "WHERE age > 18")
        filter2 = QueryTree("FILTER", "WHERE status = 'active'")
        
        operator_s.add_child(relation)
        operator_s.add_child(filter1)
        operator_s.add_child(filter2)
        
        with self.assertRaises(QueryValidationError):
            check_query(operator_s)
    
    def test_invalid_operator_s_not_value(self):
        # Structure: OPERATOR_S(NOT) -> RELATION, FILTER
        # Invalid because NOT cannot have explicit source (use OPERATOR instead)
        operator_s = QueryTree("OPERATOR_S", "NOT")
        relation = QueryTree("RELATION", "users")
        filter1 = QueryTree("FILTER", "WHERE age > 18")
        
        operator_s.add_child(relation)
        operator_s.add_child(filter1)
        
        with self.assertRaises(QueryValidationError):
            check_query(operator_s)
    
    def test_invalid_operator_s_less_than_three_children(self):
        # Structure: OPERATOR_S(AND) -> RELATION, FILTER
        # Invalid because OPERATOR_S needs minimum 3 children (1 source + 2 conditions)
        operator_and = QueryTree("OPERATOR_S", "AND")
        relation = QueryTree("RELATION", "users")
        filter1 = QueryTree("FILTER", "WHERE age > 18")
        
        operator_and.add_child(relation)
        operator_and.add_child(filter1)
        
        with self.assertRaises(QueryValidationError):
            check_query(operator_and)
    
    def test_invalid_operator_s_operator_as_source(self):
        # Structure: OPERATOR_S(AND) -> OPERATOR(AND), FILTER, FILTER
        # Invalid because first child cannot be OPERATOR (nested logic, doesn't produce data)
        operator_s = QueryTree("OPERATOR_S", "AND")
        operator_nested = QueryTree("OPERATOR", "AND")
        filter1 = QueryTree("FILTER", "WHERE age > 18")
        filter2 = QueryTree("FILTER", "WHERE status = 'active'")
        filter3 = QueryTree("FILTER", "WHERE city = 'Jakarta'")
        
        operator_nested.add_child(filter1)
        operator_nested.add_child(filter2)
        
        operator_s.add_child(operator_nested)
        operator_s.add_child(filter3)
        operator_s.add_child(QueryTree("FILTER", "WHERE verified = true"))
        
        with self.assertRaises(QueryValidationError):
            check_query(operator_s)
    
    def test_invalid_operator_s_filter_leaf_as_source(self):
        # Structure: OPERATOR_S(AND) -> FILTER(no children), FILTER, FILTER
        # Invalid because first child FILTER must have children (produce data)
        operator_and = QueryTree("OPERATOR_S", "AND")
        filter_leaf = QueryTree("FILTER", "WHERE verified = true")  # No children = condition leaf
        filter1 = QueryTree("FILTER", "WHERE age > 18")
        filter2 = QueryTree("FILTER", "WHERE status = 'active'")
        
        operator_and.add_child(filter_leaf)
        operator_and.add_child(filter1)
        operator_and.add_child(filter2)
        
        with self.assertRaises(QueryValidationError):
            check_query(operator_and)
    
    def test_invalid_operator_s_non_filter_condition(self):
        # Structure: OPERATOR_S(AND) -> RELATION, RELATION, FILTER
        # Invalid because conditions (child 1+) must be FILTER/OPERATOR/OPERATOR_S
        operator_and = QueryTree("OPERATOR_S", "AND")
        relation1 = QueryTree("RELATION", "users")
        relation2 = QueryTree("RELATION", "profiles")
        filter1 = QueryTree("FILTER", "WHERE age > 18")
        
        operator_and.add_child(relation1)
        operator_and.add_child(relation2)
        operator_and.add_child(filter1)
        
        with self.assertRaises(QueryValidationError):
            check_query(operator_and)


if __name__ == '__main__':
    unittest.main()