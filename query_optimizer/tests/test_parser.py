import unittest
from query_optimizer.tokenizer import Tokenizer
from query_optimizer.parser import Parser, ParserError


def col_name(col_ref):
    return col_ref.childs[0].childs[0].val


class TestSelect(unittest.TestCase):
    def test_simple_select(self):
        sql = "SELECT id, name FROM users;"
        tree = Parser(Tokenizer(sql)).parse()

        self.assertEqual(tree.type, "PROJECT")
        self.assertEqual(tree.val, "")
        self.assertEqual(len(tree.childs), 3)
        self.assertEqual(tree.childs[0].type, "COLUMN_REF")
        self.assertEqual(col_name(tree.childs[0]), "id")
        self.assertEqual(tree.childs[1].type, "COLUMN_REF")
        self.assertEqual(col_name(tree.childs[1]), "name")
        self.assertEqual(tree.childs[2].type, "RELATION")
        self.assertEqual(tree.childs[2].val, "users")

    def test_select_star(self):
        sql = "SELECT * FROM profiles"
        tree = Parser(Tokenizer(sql)).parse()
        self.assertEqual(tree.type, "PROJECT")
        self.assertEqual(tree.val, "*")
        self.assertEqual(len(tree.childs), 1)
        self.assertEqual(tree.childs[0].type, "RELATION")
        self.assertEqual(tree.childs[0].val, "profiles")

    def test_where_and_order(self):
        sql = "SELECT * FROM users WHERE age > 5 ORDER BY name DESC;"
        tree = Parser(Tokenizer(sql)).parse()
        sort = tree.childs[-1]
        self.assertEqual(sort.type, "SORT")
        self.assertEqual(sort.val, "DESC")
        order_expr = sort.childs[0]
        self.assertEqual(order_expr.type, "COLUMN_REF")
        filt = sort.childs[1]
        self.assertEqual(filt.type, "FILTER")
        self.assertEqual(filt.val, "")
        self.assertEqual(filt.childs[0].type, "RELATION")
        self.assertEqual(filt.childs[1].type, "COMPARISON")

    def test_limit(self):
        sql = "SELECT * FROM users LIMIT 3;"
        tree = Parser(Tokenizer(sql)).parse()
        limit = tree.childs[-1]
        self.assertEqual(limit.type, "LIMIT")
        self.assertEqual(limit.val, "3")
        self.assertEqual(limit.childs[0].type, "RELATION")

    def test_function_call(self):
        sql = "SELECT SUM(salary) FROM payroll;"
        tree = Parser(Tokenizer(sql)).parse()
        func = tree.childs[0]
        self.assertEqual(func.type, "FUNCTION_CALL")
        self.assertEqual(func.val.upper(), "SUM")


class TestJoin(unittest.TestCase):
    def test_join_on(self):
        sql = "SELECT * FROM users JOIN profiles ON users.id = profiles.user_id;"
        tree = Parser(Tokenizer(sql)).parse()
        join = tree.childs[-1]
        self.assertEqual(join.type, "JOIN")
        self.assertEqual(join.val, "INNER")
        self.assertEqual(join.childs[0].val, "users")
        self.assertEqual(join.childs[1].val, "profiles")
        self.assertEqual(join.childs[2].type, "COMPARISON")

    def test_natural_join(self):
        sql = "SELECT * FROM users NATURAL JOIN profiles;"
        join = Parser(Tokenizer(sql)).parse().childs[-1]
        self.assertEqual(join.type, "JOIN")
        self.assertEqual(join.val, "NATURAL")
        self.assertEqual(len(join.childs), 2)

    def test_multiple_joins(self):
        sql = "SELECT * FROM a JOIN b ON a.id=b.aid JOIN c ON c.id=a.cid;"
        join1 = Parser(Tokenizer(sql)).parse().childs[-1]
        self.assertEqual(join1.type, "JOIN")
        self.assertEqual(join1.childs[0].type, "JOIN")
    
    def test_comma_separated_tables(self):
        """Test FROM table1, table2 creates CROSS JOIN"""
        sql = "SELECT * FROM users, profiles;"
        tree = Parser(Tokenizer(sql)).parse()
        join = tree.childs[-1]
        
        self.assertEqual(join.type, "JOIN")
        self.assertEqual(join.val, "CROSS")
        self.assertEqual(len(join.childs), 2)
        self.assertEqual(join.childs[0].type, "RELATION")
        self.assertEqual(join.childs[0].val, "users")
        self.assertEqual(join.childs[1].type, "RELATION")
        self.assertEqual(join.childs[1].val, "profiles")
    
    def test_comma_separated_three_tables(self):
        """Test FROM table1, table2, table3 creates nested CROSS JOINs"""
        sql = "SELECT * FROM a, b, c;"
        tree = Parser(Tokenizer(sql)).parse()
        join_outer = tree.childs[-1]
        
        # Outer join: (a CROSS b) CROSS c
        self.assertEqual(join_outer.type, "JOIN")
        self.assertEqual(join_outer.val, "CROSS")
        self.assertEqual(len(join_outer.childs), 2)
        
        # Left side should be another CROSS JOIN
        join_inner = join_outer.childs[0]
        self.assertEqual(join_inner.type, "JOIN")
        self.assertEqual(join_inner.val, "CROSS")
        self.assertEqual(join_inner.childs[0].val, "a")
        self.assertEqual(join_inner.childs[1].val, "b")
        
        # Right side should be table c
        self.assertEqual(join_outer.childs[1].type, "RELATION")
        self.assertEqual(join_outer.childs[1].val, "c")
    
    def test_comma_separated_with_aliases(self):
        """Test FROM table1 alias1, table2 alias2"""
        sql = "SELECT * FROM users u, profiles p;"
        tree = Parser(Tokenizer(sql)).parse()
        join = tree.childs[-1]
        
        self.assertEqual(join.type, "JOIN")
        self.assertEqual(join.val, "CROSS")
        
        # Check aliases
        self.assertEqual(join.childs[0].type, "ALIAS")
        self.assertEqual(join.childs[0].val, "u")
        self.assertEqual(join.childs[0].childs[0].val, "users")
        
        self.assertEqual(join.childs[1].type, "ALIAS")
        self.assertEqual(join.childs[1].val, "p")
        self.assertEqual(join.childs[1].childs[0].val, "profiles")
    
    def test_comma_separated_with_where(self):
        """Test FROM table1, table2 WHERE condition (implicit join condition)"""
        sql = "SELECT * FROM employees e, payroll p WHERE e.id = p.employee_id;"
        tree = Parser(Tokenizer(sql)).parse()
        
        # Should have FILTER over CROSS JOIN
        filter_node = tree.childs[-1]
        self.assertEqual(filter_node.type, "FILTER")
        
        join = filter_node.childs[0]
        self.assertEqual(join.type, "JOIN")
        self.assertEqual(join.val, "CROSS")
        self.assertEqual(join.childs[0].type, "ALIAS")
        self.assertEqual(join.childs[1].type, "ALIAS")
        
        # Check WHERE condition
        condition = filter_node.childs[1]
        self.assertEqual(condition.type, "COMPARISON")
        self.assertEqual(condition.val, "=")
    
    def test_mixed_comma_and_explicit_join(self):
        """Test mixing comma-separated tables with explicit JOIN"""
        sql = "SELECT * FROM a, b JOIN c ON b.id = c.bid;"
        tree = Parser(Tokenizer(sql)).parse()
        
        # Outer should be explicit JOIN
        outer_join = tree.childs[-1]
        self.assertEqual(outer_join.type, "JOIN")
        self.assertEqual(outer_join.val, "INNER")
        
        # Left side should be CROSS JOIN (a, b)
        cross_join = outer_join.childs[0]
        self.assertEqual(cross_join.type, "JOIN")
        self.assertEqual(cross_join.val, "CROSS")
        self.assertEqual(cross_join.childs[0].val, "a")
        self.assertEqual(cross_join.childs[1].val, "b")
        
        # Right side should be table c
        self.assertEqual(outer_join.childs[1].val, "c")
        
        # Should have ON condition
        self.assertEqual(len(outer_join.childs), 3)
        self.assertEqual(outer_join.childs[2].type, "COMPARISON")


class TestWhereExpressions(unittest.TestCase):
    def test_in(self):
        sql = "SELECT * FROM users WHERE id IN (1,2,3);"
        filt = Parser(Tokenizer(sql)).parse().childs[-1]
        in_expr = filt.childs[1]
        self.assertEqual(in_expr.type, "IN_EXPR")
        self.assertEqual(in_expr.val, "")
        self.assertEqual(in_expr.childs[0].type, "COLUMN_REF")
        self.assertEqual(in_expr.childs[1].type, "LIST")
        self.assertEqual(len(in_expr.childs[1].childs), 3)

    def test_between_like_isnull(self):
        sql = "SELECT * FROM products WHERE price BETWEEN 10 AND 20 AND desc LIKE '%a%' AND name IS NOT NULL;"
        filt = Parser(Tokenizer(sql)).parse().childs[-1]
        op = filt.childs[1]
        self.assertEqual(op.type, "OPERATOR")
        self.assertEqual(op.val, "AND")
        types = [c.type for c in op.childs]
        self.assertIn("BETWEEN_EXPR", types)
        self.assertIn("LIKE_EXPR", types)
        self.assertIn("IS_NOT_NULL_EXPR", types)

    def test_not_exists(self):
        sql = "SELECT * FROM users WHERE NOT EXISTS (SELECT * FROM profiles);"
        exists_node = Parser(Tokenizer(sql)).parse().childs[-1].childs[1]
        self.assertEqual(exists_node.type, "NOT_EXISTS_EXPR")
        self.assertEqual(exists_node.childs[0].type, "PROJECT")


class TestDDL(unittest.TestCase):
    def test_create_table(self):
        sql = """
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER FOREIGN KEY REFERENCES users(id),
            total FLOAT,
            status VARCHAR(50)
        );
        """
        tree = Parser(Tokenizer(sql)).parse()
        self.assertEqual(tree.type, "CREATE_TABLE")
        self.assertEqual(tree.childs[0].val, "orders")
        col_list = tree.childs[1]
        self.assertEqual(col_list.type, "COLUMN_DEF_LIST")
        self.assertEqual(len(col_list.childs), 4)
        self.assertEqual(col_list.childs[0].childs[2].type, "PRIMARY_KEY")
        fk = col_list.childs[1].childs[2]
        self.assertEqual(fk.type, "FOREIGN_KEY")

    def test_drop_table(self):
        sql = "DROP TABLE users CASCADE;"
        tree = Parser(Tokenizer(sql)).parse()
        self.assertEqual(tree.type, "DROP_TABLE")
        self.assertEqual(tree.val, "CASCADE")
        self.assertEqual(tree.childs[0].val, "users")


class TestDML(unittest.TestCase):
    def test_update(self):
        sql = "UPDATE users SET name = 'John', email='a' WHERE id=1;"
        tree = Parser(Tokenizer(sql)).parse()
        self.assertEqual(tree.type, "UPDATE_QUERY")
        self.assertEqual(len([c for c in tree.childs if c.type == "ASSIGNMENT"]), 2)
        self.assertEqual(tree.childs[-1].type, "FILTER")

    def test_insert(self):
        sql = "INSERT INTO users (id, age) VALUES (1, 25);"
        tree = Parser(Tokenizer(sql)).parse()
        self.assertEqual(tree.type, "INSERT_QUERY")
        self.assertEqual(tree.childs[1].type, "COLUMN_LIST")
        self.assertEqual(tree.childs[2].type, "VALUES_CLAUSE")

    def test_delete(self):
        sql = "DELETE FROM users WHERE id = 1;"
        tree = Parser(Tokenizer(sql)).parse()
        self.assertEqual(tree.type, "DELETE_QUERY")
        self.assertEqual(tree.childs[-1].type, "FILTER")


class TestTransaction(unittest.TestCase):
    def test_begin_transaction(self):
        sql = """
        BEGIN TRANSACTION;
        """
        tree = Parser(Tokenizer(sql)).parse()
        self.assertEqual(tree.type, "BEGIN_TRANSACTION")
    
    def test_commit_transaction(self):
        sql = """
        COMMIT;
        """
        tree = Parser(Tokenizer(sql)).parse()
        self.assertEqual(tree.type, "COMMIT")


class TestPDFExamples(unittest.TestCase):
    def test_example_a_select_join_order_limit(self):
        sql = """
        SELECT u.name, p.city
        FROM users AS u
        INNER JOIN profiles AS p ON u.id = p.user_id
        WHERE u.age > 18
        ORDER BY u.name ASC
        LIMIT 10;
        """
        tree = Parser(Tokenizer(sql)).parse()
        self.assertEqual(tree.type, "PROJECT")
        limit = tree.childs[-1]
        self.assertEqual(limit.type, "LIMIT")
        self.assertEqual(limit.val, "10")
        sort = limit.childs[0]
        self.assertEqual(sort.type, "SORT")
        self.assertEqual(sort.val, "ASC")
        order_expr = sort.childs[0]
        self.assertEqual(order_expr.type, "COLUMN_REF")
        filt = sort.childs[1]
        self.assertEqual(filt.type, "FILTER")
        join = filt.childs[0]
        self.assertEqual(join.type, "JOIN")
        self.assertEqual(join.val, "INNER")
        self.assertEqual(len(join.childs), 3)

    def test_example_b_complex_where(self):
        sql = """
        SELECT *
        FROM products
        WHERE (category IN ('Electronics', 'Books') AND price < 1000)
           OR (stock > 0 AND discount IS NOT NULL);
        """
        tree = Parser(Tokenizer(sql)).parse()
        filt = tree.childs[-1]
        or_node = filt.childs[1]
        self.assertEqual(or_node.type, "OPERATOR")
        self.assertEqual(or_node.val, "OR")
        self.assertEqual(len(or_node.childs), 2)
        for child in or_node.childs:
            self.assertEqual(child.type, "OPERATOR")
            self.assertEqual(child.val, "AND")

    def test_example_c_insert(self):
        sql = """
        INSERT INTO employees (name, salary, department)
        VALUES ('John Doe', 75000, 'Engineering');
        """
        tree = Parser(Tokenizer(sql)).parse()
        self.assertEqual(tree.type, "INSERT_QUERY")
        self.assertEqual(tree.childs[0].val, "employees")
        self.assertEqual(len(tree.childs[1].childs), 3)
        self.assertEqual(len(tree.childs[2].childs), 3)

    def test_example_d_transaction(self):
        sql = """
        BEGIN TRANSACTION;
        """
        tree = Parser(Tokenizer(sql)).parse()
        self.assertEqual(tree.type, "BEGIN_TRANSACTION")
    
    def test_example_d_commit(self):
        sql = """
        COMMIT;
        """
        tree = Parser(Tokenizer(sql)).parse()
        self.assertEqual(tree.type, "COMMIT")
    
    def test_example_d_commit(self):
        sql = """
        ABORT;
        """
        tree = Parser(Tokenizer(sql)).parse()
        self.assertEqual(tree.type, "ABORT")

    def test_example_e_create_table(self):
        sql = """
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER FOREIGN KEY REFERENCES users(id),
            total FLOAT,
            status VARCHAR(50)
        );
        """
        tree = Parser(Tokenizer(sql)).parse()
        self.assertEqual(tree.type, "CREATE_TABLE")
        self.assertEqual(tree.childs[0].val, "orders")
        col_defs = tree.childs[1].childs
        self.assertEqual(len(col_defs), 4)
        self.assertEqual(col_defs[0].childs[2].type, "PRIMARY_KEY")
        self.assertEqual(col_defs[1].childs[2].type, "FOREIGN_KEY")


class TestErrors(unittest.TestCase):
    def test_missing_from(self):
        with self.assertRaises(ParserError):
            Parser(Tokenizer("SELECT id, name;")).parse()

    def test_invalid_join(self):
        with self.assertRaises(ParserError):
            Parser(Tokenizer("SELECT * FROM a JOIN b;")).parse()

    def test_insert_mismatch(self):
        with self.assertRaises(ParserError):
            Parser(Tokenizer("INSERT INTO users (name) VALUES ('a','b');")).parse()

    def test_empty(self):
        with self.assertRaises(ParserError):
            Parser(Tokenizer("")).parse()


if __name__ == "__main__":
    unittest.main()
