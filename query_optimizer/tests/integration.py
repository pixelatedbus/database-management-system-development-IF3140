import unittest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from query_optimizer.optimization_engine import OptimizationEngine, OptimizationError, ParsedQuery
from query_optimizer.query_tree import QueryTree


class TestOptimizationEngineIntegration(unittest.TestCase):    
    def setUp(self):
        engine = OptimizationEngine()
        engine.reset()
    
    # ====================================================================
    # SINGLETON TESTS
    # ====================================================================
    
    def test_singleton_pattern(self):
        engine1 = OptimizationEngine()
        engine2 = OptimizationEngine()
        
        self.assertIs(engine1, engine2, "OptimizationEngine should return same instance")
    
    # ====================================================================
    # PARSE QUERY TESTS - SELECT
    # ====================================================================
    
    def test_parse_simple_select(self):
        engine = OptimizationEngine()
        sql = "SELECT * FROM users"
        
        result = engine.parse_query(sql)
        
        self.assertIsInstance(result, ParsedQuery)
        self.assertIsInstance(result.query_tree, QueryTree)
        self.assertEqual(result.query, sql)
        self.assertEqual(result.query_tree.type, "PROJECT")
    
    def test_parse_select_with_where(self):
        engine = OptimizationEngine()
        sql = "SELECT id, name FROM users WHERE age > 25"
        
        result = engine.parse_query(sql)
        
        self.assertIsInstance(result, ParsedQuery)
        self.assertEqual(result.query_tree.type, "PROJECT")
        
        # Should have FILTER as child
        self.assertEqual(len(result.query_tree.childs), 1)
        filter_node = result.query_tree.childs[0]
        self.assertEqual(filter_node.type, "FILTER")
    
    def test_parse_select_with_join(self):
        engine = OptimizationEngine()
        sql = "SELECT * FROM users JOIN profiles ON users.id = profiles.user_id"
        
        result = engine.parse_query(sql)
        
        self.assertIsInstance(result, ParsedQuery)
        self.assertEqual(result.query_tree.type, "PROJECT")
        
        # Find JOIN node
        join_node = result.query_tree.childs[0]
        self.assertEqual(join_node.type, "JOIN")
        self.assertEqual(len(join_node.childs), 2)
    
    def test_parse_select_with_order_by(self):
        engine = OptimizationEngine()
        sql = "SELECT * FROM users ORDER BY age DESC"
        
        result = engine.parse_query(sql)
        
        self.assertIsInstance(result, ParsedQuery)
        # Should have SORT node
        tree = result.query_tree
        # Navigate down to find SORT
        current = tree
        found_sort = False
        while current and current.childs:
            if current.type == "SORT":
                found_sort = True
                break
            current = current.childs[0] if current.childs else None
        
        self.assertTrue(found_sort, "Should have SORT node")
    
    def test_parse_select_with_in_clause(self):
        engine = OptimizationEngine()
        sql = "SELECT * FROM users WHERE id IN (1, 2, 3)"
        
        result = engine.parse_query(sql)
        
        self.assertIsInstance(result, ParsedQuery)
        # Should have FILTER with IN
        filter_node = result.query_tree.childs[0]
        self.assertEqual(filter_node.type, "FILTER")
        self.assertTrue(filter_node.val.startswith("IN"))
    
    def test_parse_select_with_exists(self):
        engine = OptimizationEngine()
        sql = "SELECT * FROM users WHERE EXISTS (SELECT * FROM profiles WHERE profiles.user_id = users.id)"
        
        result = engine.parse_query(sql)
        
        self.assertIsInstance(result, ParsedQuery)
        # Should have FILTER with EXIST
        filter_node = result.query_tree.childs[0]
        self.assertEqual(filter_node.type, "FILTER")
        self.assertEqual(filter_node.val, "EXIST")
    
    # ====================================================================
    # PARSE QUERY TESTS - DML
    # ====================================================================
    
    def test_parse_insert(self):
        engine = OptimizationEngine()
        sql = "INSERT INTO users (id, name) VALUES (1, 'John')"
        
        result = engine.parse_query(sql)
        
        self.assertIsInstance(result, ParsedQuery)
        self.assertEqual(result.query_tree.type, "INSERT")
    
    def test_parse_update(self):
        engine = OptimizationEngine()
        sql = "UPDATE users SET name = 'Jane' WHERE id = 1"
        
        result = engine.parse_query(sql)
        
        self.assertIsInstance(result, ParsedQuery)
        self.assertEqual(result.query_tree.type, "UPDATE")
        self.assertEqual(result.query_tree.val, "name = 'Jane'")
    
    def test_parse_delete(self):
        engine = OptimizationEngine()
        sql = "DELETE FROM users WHERE id = 1"
        
        result = engine.parse_query(sql)
        
        self.assertIsInstance(result, ParsedQuery)
        self.assertEqual(result.query_tree.type, "DELETE")
    
    # ====================================================================
    # PARSE QUERY TESTS - TRANSACTIONS
    # ====================================================================
    
    def test_parse_transaction(self):
        engine = OptimizationEngine()
        sql = "BEGIN TRANSACTION INSERT INTO users (id, name) VALUES (1, 'John') COMMIT"
        
        result = engine.parse_query(sql)
        
        self.assertIsInstance(result, ParsedQuery)
        self.assertEqual(result.query_tree.type, "BEGIN_TRANSACTION")
        # Should have INSERT as child and COMMIT
        self.assertEqual(len(result.query_tree.childs), 2)
    
    # ====================================================================
    # OPTIMIZE QUERY TESTS
    # ====================================================================
    
    def test_optimize_query_with_parsed_tree(self):
        engine = OptimizationEngine()
        sql = "SELECT * FROM users WHERE age > 25"
        
        parsed = engine.parse_query(sql)
        optimized = engine.optimize_query(parsed)
        
        self.assertIsInstance(optimized, ParsedQuery)
        self.assertIsNotNone(optimized.query_tree)
    
    def test_optimize_query_without_parameter(self):
        engine = OptimizationEngine()
        sql = "SELECT * FROM users WHERE age > 25"
        
        parsed = engine.parse_query(sql)
        engine.query_tree = parsed.query_tree
        
        optimized = engine.optimize_query()
        
        self.assertIsInstance(optimized, ParsedQuery)
        self.assertIsNotNone(optimized.query_tree)
    
    def test_optimize_query_without_parsed_tree_raises_error(self):
        engine = OptimizationEngine()
        engine.reset()
        
        with self.assertRaises(OptimizationError):
            engine.optimize_query()
    
    # ====================================================================
    # FULL WORKFLOW TESTS
    # ====================================================================
    
    def test_full_workflow_parse_optimize_cost(self):
        engine = OptimizationEngine()
        sql = "SELECT * FROM users WHERE age > 25 ORDER BY name"
        
        parsed = engine.parse_query(sql)
        self.assertIsInstance(parsed, ParsedQuery)
        
        optimized = engine.optimize_query(parsed)
        self.assertIsInstance(optimized, ParsedQuery)
        
        cost = engine.get_cost(optimized)
        self.assertIsInstance(cost, int)
        self.assertGreaterEqual(cost, 0)
    
    def test_multiple_queries_in_sequence(self):
        """Test parsing multiple queries in sequence."""
        engine = OptimizationEngine()
        
        sql1 = "SELECT * FROM users"
        result1 = engine.parse_query(sql1)
        self.assertEqual(result1.query, sql1)
        
        # Use 'orders' instead of 'products' (orders is in available tables)
        sql2 = "SELECT * FROM orders"
        result2 = engine.parse_query(sql2)
        self.assertEqual(result2.query, sql2)
        
        self.assertEqual(engine.original_sql, sql2)
    
    # ====================================================================
    # RESET TESTS
    # ====================================================================
    
    def test_reset_clears_state(self):
        engine = OptimizationEngine()
        sql = "SELECT * FROM users"
        
        engine.parse_query(sql)
        self.assertIsNotNone(engine.original_sql)
        
        engine.reset()
        
        self.assertIsNone(engine.query_tree)
        self.assertIsNone(engine.optimized_tree)
        self.assertEqual(engine.original_sql, "")
    
    # ====================================================================
    # ERROR HANDLING TESTS
    # ====================================================================
    
    def test_parse_invalid_sql_syntax(self):
        engine = OptimizationEngine()
        sql = "SELECTT * FROM users"  # Typo
        
        with self.assertRaises(Exception):
            engine.parse_query(sql)
    
    def test_parse_incomplete_sql(self):
        engine = OptimizationEngine()
        sql = "SELECT * FROM"
        
        with self.assertRaises(Exception):
            engine.parse_query(sql)


if __name__ == '__main__':
    unittest.main()
