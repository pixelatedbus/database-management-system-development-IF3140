import unittest
from query_optimizer.tokenizer import Tokenizer
from query_optimizer.parser import Parser, ParserError
from query_optimizer.query_tree import QueryTree


class TestParserSelect(unittest.TestCase):
    """Test cases for SELECT statement parsing"""

    def test_simple_select(self):
        """Test basic SELECT with FROM"""
        sql = "SELECT id, name FROM users;"
        tokenizer = Tokenizer(sql)
        parser = Parser(tokenizer)
        tree = parser.parse()

        self.assertEqual(tree.type, "PROJECT")
        self.assertEqual(tree.val, "id, name")

        self.assertEqual(len(tree.childs), 1)
        self.assertEqual(tree.childs[0].type, "RELATION")
        self.assertEqual(tree.childs[0].val, "users")

    def test_select_star(self):
        """Test SELECT * syntax"""
        sql = "SELECT * FROM profiles"
        tokenizer = Tokenizer(sql)
        parser = Parser(tokenizer)
        tree = parser.parse()

        self.assertEqual(tree.type, "PROJECT")
        self.assertEqual(tree.val, "*")
        self.assertEqual(tree.childs[0].val, "profiles")

    def test_select_with_where(self):
        """Test SELECT with WHERE clause"""
        sql = "SELECT name FROM users WHERE id > 10;"
        tokenizer = Tokenizer(sql)
        parser = Parser(tokenizer)
        tree = parser.parse()

        self.assertEqual(tree.type, "PROJECT")

        filter_node = tree.childs[0]
        self.assertEqual(filter_node.type, "FILTER")
        self.assertIn("WHERE", filter_node.val)
        self.assertIn("id > 10", filter_node.val)

        self.assertEqual(filter_node.childs[0].type, "RELATION")

    def test_select_with_order_by(self):
        """Test SELECT with ORDER BY clause"""
        sql = "SELECT id, name FROM users ORDER BY name;"
        tokenizer = Tokenizer(sql)
        parser = Parser(tokenizer)
        tree = parser.parse()

        self.assertEqual(tree.type, "PROJECT")

        sort_node = tree.childs[0]
        self.assertEqual(sort_node.type, "SORT")
        self.assertEqual(sort_node.val, "name")

        self.assertEqual(sort_node.childs[0].type, "RELATION")

    def test_select_with_where_and_order_by(self):
        """Test SELECT with both WHERE and ORDER BY"""
        sql = "SELECT * FROM users WHERE id > 5 ORDER BY name;"
        tokenizer = Tokenizer(sql)
        parser = Parser(tokenizer)
        tree = parser.parse()

        self.assertEqual(tree.type, "PROJECT")
        self.assertEqual(tree.childs[0].type, "SORT")
        self.assertEqual(tree.childs[0].childs[0].type, "FILTER")
        self.assertEqual(tree.childs[0].childs[0].childs[0].type, "RELATION")

    def test_select_with_limit(self):
        """Test SELECT with LIMIT clause"""
        sql = "SELECT * FROM users LIMIT 10;"
        tokenizer = Tokenizer(sql)
        parser = Parser(tokenizer)
        tree = parser.parse()

        self.assertEqual(tree.type, "PROJECT")


class TestParserJoin(unittest.TestCase):
    """Test cases for JOIN statement parsing"""

    def test_join_with_on(self):
        """Test JOIN with ON condition"""
        sql = "SELECT * FROM users JOIN profiles ON users.id = profiles.user_id;"
        tokenizer = Tokenizer(sql)
        parser = Parser(tokenizer)
        tree = parser.parse()

        self.assertEqual(tree.type, "PROJECT")

        join_node = tree.childs[0]
        self.assertEqual(join_node.type, "JOIN")
        self.assertIn("ON", join_node.val)
        self.assertIn("users.id = profiles.user_id", join_node.val)

        self.assertEqual(len(join_node.childs), 2)
        self.assertEqual(join_node.childs[0].type, "RELATION")
        self.assertEqual(join_node.childs[0].val, "users")
        self.assertEqual(join_node.childs[1].type, "RELATION")
        self.assertEqual(join_node.childs[1].val, "profiles")

    def test_natural_join(self):
        """Test NATURAL JOIN"""
        sql = "SELECT * FROM users NATURAL JOIN profiles;"
        tokenizer = Tokenizer(sql)
        parser = Parser(tokenizer)
        tree = parser.parse()

        join_node = tree.childs[0]
        self.assertEqual(join_node.type, "JOIN")
        self.assertEqual(join_node.val, "NATURAL")
        self.assertEqual(len(join_node.childs), 2)

    def test_join_with_where(self):
        """Test JOIN with WHERE clause"""
        sql = "SELECT * FROM users JOIN profiles ON users.id = profiles.user_id WHERE users.id > 10;"
        tokenizer = Tokenizer(sql)
        parser = Parser(tokenizer)
        tree = parser.parse()

        self.assertEqual(tree.type, "PROJECT")
        self.assertEqual(tree.childs[0].type, "FILTER")
        self.assertEqual(tree.childs[0].childs[0].type, "JOIN")

    def test_multiple_joins(self):
        """Test multiple JOIN operations"""
        sql = "SELECT * FROM users JOIN profiles ON users.id = profiles.user_id JOIN orders ON users.id = orders.user_id;"
        tokenizer = Tokenizer(sql)
        parser = Parser(tokenizer)
        tree = parser.parse()

        self.assertEqual(tree.type, "PROJECT")

        join1 = tree.childs[0]
        self.assertEqual(join1.type, "JOIN")

        self.assertEqual(join1.childs[0].type, "JOIN")


class TestParserUpdate(unittest.TestCase):
    """Test cases for UPDATE statement parsing"""

    def test_update_simple(self):
        """Test basic UPDATE with SET"""
        sql = "UPDATE users SET name = 'John';"
        tokenizer = Tokenizer(sql)
        parser = Parser(tokenizer)
        tree = parser.parse()

        self.assertEqual(tree.type, "UPDATE")
        self.assertIn("name = 'John'", tree.val)

        self.assertEqual(tree.childs[0].type, "RELATION")
        self.assertEqual(tree.childs[0].val, "users")

    def test_update_multiple_columns(self):
        """Test UPDATE with multiple column assignments"""
        sql = "UPDATE users SET name = 'John', email = 'john@test.com';"
        tokenizer = Tokenizer(sql)
        parser = Parser(tokenizer)
        tree = parser.parse()

        self.assertEqual(tree.type, "UPDATE")
        self.assertIn("name = 'John'", tree.val)
        self.assertIn("email = 'john@test.com'", tree.val)

    def test_update_with_where(self):
        """Test UPDATE with WHERE clause"""
        sql = "UPDATE users SET name = 'John' WHERE id = 1;"
        tokenizer = Tokenizer(sql)
        parser = Parser(tokenizer)
        tree = parser.parse()

        self.assertEqual(tree.type, "UPDATE")

        filter_node = tree.childs[0]
        self.assertEqual(filter_node.type, "FILTER")
        self.assertIn("WHERE", filter_node.val)

        self.assertEqual(filter_node.childs[0].type, "RELATION")


class TestParserInsert(unittest.TestCase):
    """Test cases for INSERT statement parsing"""

    def test_insert_simple(self):
        """Test basic INSERT with VALUES"""
        sql = "INSERT INTO users (name, email) VALUES ('John', 'john@test.com');"
        tokenizer = Tokenizer(sql)
        parser = Parser(tokenizer)
        tree = parser.parse()

        self.assertEqual(tree.type, "INSERT")
        self.assertIn("name = 'John'", tree.val)
        self.assertIn("email = 'john@test.com'", tree.val)

        self.assertEqual(tree.childs[0].type, "RELATION")
        self.assertEqual(tree.childs[0].val, "users")

    def test_insert_single_column(self):
        """Test INSERT with single column"""
        sql = "INSERT INTO profiles (bio) VALUES ('Hello world');"
        tokenizer = Tokenizer(sql)
        parser = Parser(tokenizer)
        tree = parser.parse()

        self.assertEqual(tree.type, "INSERT")
        self.assertIn("bio = 'Hello world'", tree.val)

    def test_insert_with_numbers(self):
        """Test INSERT with numeric values"""
        sql = "INSERT INTO users (id, age) VALUES (1, 25);"
        tokenizer = Tokenizer(sql)
        parser = Parser(tokenizer)
        tree = parser.parse()

        self.assertEqual(tree.type, "INSERT")
        self.assertIn("id = 1", tree.val)
        self.assertIn("age = 25", tree.val)


class TestParserDelete(unittest.TestCase):
    """Test cases for DELETE statement parsing"""

    def test_delete_simple(self):
        """Test basic DELETE without WHERE"""
        sql = "DELETE FROM users;"
        tokenizer = Tokenizer(sql)
        parser = Parser(tokenizer)
        tree = parser.parse()

        self.assertEqual(tree.type, "DELETE")

        self.assertEqual(tree.childs[0].type, "RELATION")
        self.assertEqual(tree.childs[0].val, "users")

    def test_delete_with_where(self):
        """Test DELETE with WHERE clause"""
        sql = "DELETE FROM users WHERE id = 1;"
        tokenizer = Tokenizer(sql)
        parser = Parser(tokenizer)
        tree = parser.parse()

        self.assertEqual(tree.type, "DELETE")

        filter_node = tree.childs[0]
        self.assertEqual(filter_node.type, "FILTER")
        self.assertIn("WHERE", filter_node.val)

        self.assertEqual(filter_node.childs[0].type, "RELATION")


class TestParserTransaction(unittest.TestCase):
    """Test cases for TRANSACTION statement parsing"""

    def test_transaction_simple(self):
        """Test basic transaction with single statement"""
        sql = """
        BEGIN TRANSACTION;
        UPDATE users SET name = 'test';
        COMMIT;
        """
        tokenizer = Tokenizer(sql)
        parser = Parser(tokenizer)
        tree = parser.parse()

        self.assertEqual(tree.type, "BEGIN_TRANSACTION")

        self.assertEqual(len(tree.childs), 2)
        self.assertEqual(tree.childs[0].type, "UPDATE")
        self.assertEqual(tree.childs[1].type, "COMMIT")

    def test_transaction_multiple_statements(self):
        """Test transaction with multiple statements"""
        sql = """
        BEGIN TRANSACTION;
        UPDATE users SET name = 'test';
        DELETE FROM orders WHERE id = 1;
        INSERT INTO logs (message) VALUES ('done');
        COMMIT;
        """
        tokenizer = Tokenizer(sql)
        parser = Parser(tokenizer)
        tree = parser.parse()

        self.assertEqual(tree.type, "BEGIN_TRANSACTION")

        self.assertEqual(len(tree.childs), 4)
        self.assertEqual(tree.childs[0].type, "UPDATE")
        self.assertEqual(tree.childs[1].type, "DELETE")
        self.assertEqual(tree.childs[2].type, "INSERT")
        self.assertEqual(tree.childs[3].type, "COMMIT")


class TestParserFilterSpecial(unittest.TestCase):
    """Test cases for special FILTER operations (IN, EXISTS)"""

    def test_filter_in_clause(self):
        """Test WHERE with IN clause"""
        sql = "SELECT * FROM users WHERE id IN (1, 2, 3);"
        tokenizer = Tokenizer(sql)
        parser = Parser(tokenizer)
        tree = parser.parse()

        self.assertEqual(tree.type, "PROJECT")

        filter_node = tree.childs[0]
        self.assertEqual(filter_node.type, "FILTER")
        self.assertIn("IN", filter_node.val)
        self.assertIn("id", filter_node.val)

        self.assertEqual(len(filter_node.childs), 2)

        self.assertEqual(filter_node.childs[0].type, "RELATION")

        array_node = filter_node.childs[1]
        self.assertEqual(array_node.type, "ARRAY")
        self.assertIn("1", array_node.val)
        self.assertIn("2", array_node.val)
        self.assertIn("3", array_node.val)

    def test_filter_exists_clause(self):
        """Test WHERE with EXISTS clause"""
        sql = "SELECT * FROM users WHERE EXISTS (SELECT * FROM profiles WHERE profiles.user_id = users.id);"
        tokenizer = Tokenizer(sql)
        parser = Parser(tokenizer)
        tree = parser.parse()

        self.assertEqual(tree.type, "PROJECT")

        filter_node = tree.childs[0]
        self.assertEqual(filter_node.type, "FILTER")
        self.assertEqual(filter_node.val, "EXIST")

        self.assertEqual(len(filter_node.childs), 2)

        self.assertEqual(filter_node.childs[0].type, "RELATION")

        subquery = filter_node.childs[1]
        self.assertEqual(subquery.type, "PROJECT")


class TestParserComplexQueries(unittest.TestCase):
    """Test cases for complex nested queries"""

    def test_complex_select_full_features(self):
        """Test SELECT with all features combined"""
        sql = """
        SELECT users.id, users.name, profiles.bio
        FROM users
        JOIN profiles ON users.id = profiles.user_id
        WHERE users.id > 10
        ORDER BY users.name
        LIMIT 5;
        """
        tokenizer = Tokenizer(sql)
        parser = Parser(tokenizer)
        tree = parser.parse()

        self.assertEqual(tree.type, "PROJECT")

        self.assertEqual(tree.childs[0].type, "SORT")

        self.assertEqual(tree.childs[0].childs[0].type, "FILTER")

        self.assertEqual(tree.childs[0].childs[0].childs[0].type, "JOIN")

    def test_nested_filters(self):
        """Test multiple WHERE conditions as nested filters"""
        sql = "SELECT * FROM users WHERE id > 10 AND id IN (1, 2, 3);"
        tokenizer = Tokenizer(sql)
        parser = Parser(tokenizer)
        tree = parser.parse()

        self.assertEqual(tree.type, "PROJECT")

    def test_join_with_complex_condition(self):
        """Test JOIN with complex ON condition"""
        sql = "SELECT * FROM users JOIN profiles ON users.id = profiles.user_id AND users.active = 1;"
        tokenizer = Tokenizer(sql)
        parser = Parser(tokenizer)
        tree = parser.parse()

        join_node = tree.childs[0]
        self.assertEqual(join_node.type, "JOIN")
        self.assertIn("AND", join_node.val)


class TestParserErrors(unittest.TestCase):
    """Test cases for error handling"""

    def test_missing_from(self):
        """Test error when FROM is missing"""
        sql = "SELECT id, name;"
        tokenizer = Tokenizer(sql)
        parser = Parser(tokenizer)

        with self.assertRaises(ParserError):
            parser.parse()

    def test_missing_table_name(self):
        """Test error when table name is missing"""
        sql = "SELECT * FROM;"
        tokenizer = Tokenizer(sql)
        parser = Parser(tokenizer)

        with self.assertRaises(ParserError):
            parser.parse()

    def test_invalid_join_without_condition(self):
        """Test error when JOIN has no ON or NATURAL"""
        sql = "SELECT * FROM users JOIN profiles;"
        tokenizer = Tokenizer(sql)
        parser = Parser(tokenizer)

        with self.assertRaises(ParserError):
            parser.parse()

    def test_update_without_set(self):
        """Test error when UPDATE has no SET clause"""
        sql = "UPDATE users WHERE id = 1;"
        tokenizer = Tokenizer(sql)
        parser = Parser(tokenizer)

        with self.assertRaises(ParserError):
            parser.parse()

    def test_insert_column_value_mismatch(self):
        """Test error when INSERT columns and values don't match"""
        sql = "INSERT INTO users (name, email) VALUES ('John');"
        tokenizer = Tokenizer(sql)
        parser = Parser(tokenizer)

        with self.assertRaises(ParserError):
            parser.parse()

    def test_empty_query(self):
        """Test error on empty query"""
        sql = ""
        tokenizer = Tokenizer(sql)
        parser = Parser(tokenizer)

        with self.assertRaises(ParserError):
            parser.parse()


class TestParserIntegration(unittest.TestCase):
    """Integration tests matching examples from Rule.md"""

    def test_rule_md_example_1(self):
        """Test Example 1 from Rule.md"""
        sql = "SELECT id, name FROM users WHERE id = 1;"
        tokenizer = Tokenizer(sql)
        parser = Parser(tokenizer)
        tree = parser.parse()


        self.assertEqual(tree.type, "PROJECT")
        self.assertEqual(tree.val, "id, name")
        self.assertEqual(tree.childs[0].type, "FILTER")
        self.assertIn("WHERE id = 1", tree.childs[0].val)
        self.assertEqual(tree.childs[0].childs[0].type, "RELATION")
        self.assertEqual(tree.childs[0].childs[0].val, "users")

    def test_rule_md_example_2(self):
        """Test JOIN example from Rule.md"""
        sql = "SELECT * FROM users JOIN profiles ON users.id = profiles.user_id;"
        tokenizer = Tokenizer(sql)
        parser = Parser(tokenizer)
        tree = parser.parse()


        self.assertEqual(tree.type, "PROJECT")
        self.assertEqual(tree.val, "*")
        join_node = tree.childs[0]
        self.assertEqual(join_node.type, "JOIN")
        self.assertIn("ON users.id = profiles.user_id", join_node.val)
        self.assertEqual(len(join_node.childs), 2)
        self.assertEqual(join_node.childs[0].val, "users")
        self.assertEqual(join_node.childs[1].val, "profiles")

    def test_rule_md_example_3(self):
        """Test UPDATE example from Rule.md"""
        sql = "UPDATE users SET name = 'test', email = 'test@example.com' WHERE id = 1;"
        tokenizer = Tokenizer(sql)
        parser = Parser(tokenizer)
        tree = parser.parse()


        self.assertEqual(tree.type, "UPDATE")
        self.assertIn("name = 'test'", tree.val)
        self.assertEqual(tree.childs[0].type, "FILTER")
        self.assertEqual(tree.childs[0].childs[0].type, "RELATION")


def run_all_tests():
    """
    Run all parser tests and print results.

    Usage:
        python -m query_optimizer.tests.test_parser
        or
        from query_optimizer.tests.test_parser import run_all_tests
        run_all_tests()
    """
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    suite.addTests(loader.loadTestsFromTestCase(TestParserSelect))
    suite.addTests(loader.loadTestsFromTestCase(TestParserJoin))
    suite.addTests(loader.loadTestsFromTestCase(TestParserUpdate))
    suite.addTests(loader.loadTestsFromTestCase(TestParserInsert))
    suite.addTests(loader.loadTestsFromTestCase(TestParserDelete))
    suite.addTests(loader.loadTestsFromTestCase(TestParserTransaction))
    suite.addTests(loader.loadTestsFromTestCase(TestParserFilterSpecial))
    suite.addTests(loader.loadTestsFromTestCase(TestParserComplexQueries))
    suite.addTests(loader.loadTestsFromTestCase(TestParserErrors))
    suite.addTests(loader.loadTestsFromTestCase(TestParserIntegration))

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    print("\n" + "="*70)
    print("TEST SUMMARY")
    print("="*70)
    print(f"Tests run: {result.testsRun}")
    print(f"Successes: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print("="*70)

    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_all_tests()
    exit(0 if success else 1)
