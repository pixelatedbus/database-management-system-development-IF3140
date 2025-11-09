import unittest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from query_optimizer.query_check import QueryTree, check_query, QueryValidationError

class TestQueryValidator(unittest.TestCase):

    def test_valid_project_with_filter(self):
        # Structure: PROJECT -> FILTER -> RELATION
        # Represents: SELECT id, name FROM users WHERE id = 1;
        project = QueryTree("PROJECT", "id, name")
        filter_node = QueryTree("FILTER", "id = 1")
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
    
    def test_complex_query_with_filter_and_join(self):
        # Structure: PROJECT -> FILTER -> JOIN -> RELATION, RELATION
        # Represents: SELECT * FROM users JOIN profiles ON users.id = profiles.user_id WHERE users.id > 10;
        project = QueryTree("PROJECT", "*")
        filter_node = QueryTree("FILTER", "users.id > 10")
        join = QueryTree("JOIN", "ON users.id = profiles.user_id")
        relation1 = QueryTree("RELATION", "users")
        relation2 = QueryTree("RELATION", "profiles")
        
        project.add_child(filter_node)
        filter_node.add_child(join)
        join.add_child(relation1)
        join.add_child(relation2)
        
        check_query(project)
    
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

if __name__ == '__main__':
    unittest.main()