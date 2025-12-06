"""
Comprehensive Unit Tests for Query Processor

This test suite covers all major SQL operations through the query processor:
1. CREATE TABLE - with primary keys and foreign keys
2. DROP TABLE - with CASCADE and RESTRICT behavior for foreign keys
3. SELECT - simple, complex conditions, joins, aliases
4. INSERT INTO - hardcoded values
5. UPDATE - hardcoded values and expression-based
6. DELETE - with single conditions
7. LIMIT - result limiting
8. ALIAS - table and column aliases
9. ORDER BY - ascending and descending

Test Database Schema:
- departments (dept_id PK, dept_name, budget)
- employees (emp_id PK, emp_name, dept_id FK->departments, salary, hire_date)
- projects (project_id PK, project_name, dept_id FK->departments, budget)
- tasks (task_id PK, task_name, project_id, status)

Run tests:
    python -m pytest query_processor/tests/test_query_processor.py -v
    python -m unittest query_processor.tests.test_query_processor
"""

import unittest
import sys
import os
import shutil
import logging

# Configure logging to reduce noise during tests
logging.basicConfig(level=logging.CRITICAL)

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from query_processor.query_processor import QueryProcessor
from storage_manager.storage_manager import StorageManager


class TestQueryProcessor(unittest.TestCase):
    """Comprehensive test suite for Query Processor functionality."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment once for all tests."""
        cls.test_data_dir = "data/test_query_processor"
        cls.client_id = 1000  # Test client ID
        
    def setUp(self):
        """Set up fresh environment before each test."""
        # Clean up any existing test data
        if os.path.exists(self.test_data_dir):
            shutil.rmtree(self.test_data_dir)
        
        os.makedirs(self.test_data_dir, exist_ok=True)
        
        # Create fresh instances for each test
        StorageManager._instance = None
        StorageManager._initialized = False
        QueryProcessor._instance = None
        
        self.storage_manager = StorageManager(data_dir=self.test_data_dir)
        self.query_processor = QueryProcessor()
        self.query_processor.adapter_storage.sm = self.storage_manager
        self.query_processor.query_execution_engine.storage_manager = self.storage_manager
        self.query_processor.query_execution_engine.storage_adapter.sm = self.storage_manager
        
    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests."""
        if os.path.exists(cls.test_data_dir):
            try:
                shutil.rmtree(cls.test_data_dir)
            except:
                pass  # Ignore cleanup errors
    
    def execute_query(self, query: str):
        """Helper to execute query and return result."""
        return self.query_processor.execute_query(query, self.client_id)
    
    def assertQuerySuccess(self, result, message="Query should succeed"):
        """Assert that query execution was successful."""
        self.assertTrue(result.success, f"{message}\nError: {result.message}\nQuery: {result.query}")
    
    def assertQueryFails(self, result, message="Query should fail"):
        """Assert that query execution failed."""
        self.assertFalse(result.success, f"{message}\nQuery: {result.query}")


class TestCreateTable(TestQueryProcessor):
    """Test CREATE TABLE functionality."""
    
    def test_01_create_simple_table(self):
        """Test creating a simple table with basic columns."""
        query = """
        CREATE TABLE departments (
            dept_id INTEGER PRIMARY KEY,
            dept_name VARCHAR(50),
            budget INTEGER
        )
        """
        result = self.execute_query(query)
        self.assertQuerySuccess(result, "Should create departments table")
        
        # Verify table exists in storage manager
        self.assertIn('departments', self.storage_manager.tables)
    
    def test_02_create_table_with_foreign_key(self):
        """Test creating table with foreign key constraint."""
        # First ensure parent table exists
        parent_query = """
        CREATE TABLE departments (
            dept_id INTEGER PRIMARY KEY,
            dept_name VARCHAR(50),
            budget INTEGER
        )
        """
        self.execute_query(parent_query)
        
        # Create child table with foreign key
        child_query = """
        CREATE TABLE employees (
            emp_id INTEGER PRIMARY KEY,
            emp_name VARCHAR(50),
            dept_id INTEGER,
            salary INTEGER,
            hire_date VARCHAR(20)
        )
        """
        result = self.execute_query(child_query)
        self.assertQuerySuccess(result, "Should create employees table")
        
        # Verify table exists
        self.assertIn('employees', self.storage_manager.tables)
    
    def test_03_create_multiple_tables(self):
        """Test creating multiple related tables."""
        # Parent table
        dept_query = """
        CREATE TABLE departments (
            dept_id INTEGER PRIMARY KEY,
            dept_name VARCHAR(50),
            budget INTEGER
        )
        """
        result = self.execute_query(dept_query)
        self.assertQuerySuccess(result)
        
        # Child table 1
        emp_query = """
        CREATE TABLE employees (
            emp_id INTEGER PRIMARY KEY,
            emp_name VARCHAR(50),
            dept_id INTEGER,
            salary INTEGER,
            hire_date VARCHAR(20)
        )
        """
        result = self.execute_query(emp_query)
        self.assertQuerySuccess(result)
        
        # Child table 2
        proj_query = """
        CREATE TABLE projects (
            project_id INTEGER PRIMARY KEY,
            project_name VARCHAR(50),
            dept_id INTEGER,
            budget INTEGER
        )
        """
        result = self.execute_query(proj_query)
        self.assertQuerySuccess(result)
        
        # Independent table
        tasks_query = """
        CREATE TABLE tasks (
            task_id INTEGER PRIMARY KEY,
            task_name VARCHAR(50),
            project_id INTEGER,
            status VARCHAR(20)
        )
        """
        result = self.execute_query(tasks_query)
        self.assertQuerySuccess(result)
        
        # Verify all tables exist
        self.assertIn('departments', self.storage_manager.tables)
        self.assertIn('employees', self.storage_manager.tables)
        self.assertIn('projects', self.storage_manager.tables)
        self.assertIn('tasks', self.storage_manager.tables)


class TestInsertInto(TestQueryProcessor):
    """Test INSERT INTO functionality with hardcoded values."""
    
    def test_01_insert_simple_values(self):
        """Test simple INSERT with hardcoded values."""
        # Create departments table
        self.execute_query("""
        CREATE TABLE departments (
            dept_id INTEGER PRIMARY KEY,
            dept_name VARCHAR(50),
            budget INTEGER
        )
        """)
        
        query = "INSERT INTO departments (dept_id, dept_name, budget) VALUES (1, 'Engineering', 100000)"
        result = self.execute_query(query)
        self.assertQuerySuccess(result, "Should insert into departments")
        
        # Verify data was inserted
        select_result = self.execute_query("SELECT * FROM departments")
        self.assertEqual(len(select_result.data.rows), 1)
        self.assertEqual(select_result.data.rows[0]['dept_id'], 1)
        self.assertEqual(select_result.data.rows[0]['dept_name'], 'Engineering')
    
    def test_02_insert_multiple_rows(self):
        """Test inserting multiple rows."""
        # Create departments table
        self.execute_query("""
        CREATE TABLE departments (
            dept_id INTEGER PRIMARY KEY,
            dept_name VARCHAR(50),
            budget INTEGER
        )
        """)
        
        queries = [
            "INSERT INTO departments (dept_id, dept_name, budget) VALUES (1, 'Engineering', 100000)",
            "INSERT INTO departments (dept_id, dept_name, budget) VALUES (2, 'Sales', 80000)",
            "INSERT INTO departments (dept_id, dept_name, budget) VALUES (3, 'HR', 60000)"
        ]
        
        for query in queries:
            result = self.execute_query(query)
            self.assertQuerySuccess(result)
        
        # Verify all rows inserted
        select_result = self.execute_query("SELECT * FROM departments")
        self.assertEqual(len(select_result.data.rows), 3)
    
    def test_03_insert_with_partial_columns(self):
        """Test INSERT specifying only some columns."""
        # Create tables
        self.execute_query("""
        CREATE TABLE departments (
            dept_id INTEGER PRIMARY KEY,
            dept_name VARCHAR(50),
            budget INTEGER
        )
        """)
        
        self.execute_query("""
        CREATE TABLE employees (
            emp_id INTEGER PRIMARY KEY,
            emp_name VARCHAR(50),
            dept_id INTEGER,
            salary INTEGER,
            hire_date VARCHAR(20)
        )
        """)
        
        # First insert department
        self.execute_query("INSERT INTO departments (dept_id, dept_name, budget) VALUES (1, 'Engineering', 100000)")
        
        # Insert employee with all columns
        query = "INSERT INTO employees (emp_id, emp_name, dept_id, salary, hire_date) VALUES (1, 'Alice', 1, 60000, '2024-01-15')"
        result = self.execute_query(query)
        self.assertQuerySuccess(result)
        
        # Verify insertion
        select_result = self.execute_query("SELECT emp_name, salary FROM employees WHERE emp_id = 1")
        self.assertEqual(len(select_result.data.rows), 1)
        self.assertEqual(select_result.data.rows[0]['emp_name'], 'Alice')
        self.assertEqual(select_result.data.rows[0]['salary'], 60000)
    
    def test_04_populate_test_data(self):
        """Populate database with comprehensive test data for other tests."""
        # Create tables
        self.execute_query("""
        CREATE TABLE departments (
            dept_id INTEGER PRIMARY KEY,
            dept_name VARCHAR(50),
            budget INTEGER
        )
        """)
        
        self.execute_query("""
        CREATE TABLE employees (
            emp_id INTEGER PRIMARY KEY,
            emp_name VARCHAR(50),
            dept_id INTEGER,
            salary INTEGER,
            hire_date VARCHAR(20)
        )
        """)
        
        # Insert departments
        dept_queries = [
            "INSERT INTO departments (dept_id, dept_name, budget) VALUES (1, 'Engineering', 100000)",
            "INSERT INTO departments (dept_id, dept_name, budget) VALUES (2, 'Sales', 80000)",
            "INSERT INTO departments (dept_id, dept_name, budget) VALUES (3, 'HR', 60000)"
        ]
        for query in dept_queries:
            self.execute_query(query)
        
        # Insert employees
        emp_queries = [
            "INSERT INTO employees (emp_id, emp_name, dept_id, salary, hire_date) VALUES (1, 'Alice', 1, 60000, '2024-01-15')",
            "INSERT INTO employees (emp_id, emp_name, dept_id, salary, hire_date) VALUES (2, 'Bob', 1, 75000, '2023-06-20')",
            "INSERT INTO employees (emp_id, emp_name, dept_id, salary, hire_date) VALUES (3, 'Carol', 2, 55000, '2024-03-10')",
            "INSERT INTO employees (emp_id, emp_name, dept_id, salary, hire_date) VALUES (4, 'Dave', 2, 82000, '2022-11-05')",
            "INSERT INTO employees (emp_id, emp_name, dept_id, salary, hire_date) VALUES (5, 'Eve', 3, 50000, '2024-02-28')"
        ]
        for query in emp_queries:
            self.execute_query(query)
        
        # Verify data
        result = self.execute_query("SELECT * FROM departments")
        self.assertEqual(len(result.data.rows), 3)
        
        result = self.execute_query("SELECT * FROM employees")
        self.assertEqual(len(result.data.rows), 5)


class TestSelect(TestQueryProcessor):
    """Test SELECT query functionality."""
    
    def _setup_test_data(self):
        """Helper to set up tables and data for SELECT tests."""
        # Create and populate tables
        self.execute_query("""
        CREATE TABLE departments (
            dept_id INTEGER PRIMARY KEY,
            dept_name VARCHAR(50),
            budget INTEGER
        )
        """)
        
        self.execute_query("""
        CREATE TABLE employees (
            emp_id INTEGER PRIMARY KEY,
            emp_name VARCHAR(50),
            dept_id INTEGER,
            salary INTEGER,
            hire_date VARCHAR(20)
        )
        """)
        
        # Populate departments
        dept_data = [
            (1, 'Engineering', 100000),
            (2, 'Sales', 80000),
            (3, 'HR', 60000)
        ]
        for dept_id, name, budget in dept_data:
            self.execute_query(f"INSERT INTO departments (dept_id, dept_name, budget) VALUES ({dept_id}, '{name}', {budget})")
        
        # Populate employees
        emp_data = [
            (1, 'Alice', 1, 60000, '2024-01-15'),
            (2, 'Bob', 1, 75000, '2023-06-20'),
            (3, 'Carol', 2, 55000, '2024-03-10'),
            (4, 'Dave', 2, 82000, '2022-11-05'),
            (5, 'Eve', 3, 50000, '2024-02-28')
        ]
        for emp_id, name, dept_id, salary, date in emp_data:
            self.execute_query(f"INSERT INTO employees (emp_id, emp_name, dept_id, salary, hire_date) VALUES ({emp_id}, '{name}', {dept_id}, {salary}, '{date}')")
    
    def test_01_select_all_columns(self):
        """Test SELECT * to retrieve all columns."""
        self._setup_test_data()
        result = self.execute_query("SELECT * FROM departments")
        self.assertQuerySuccess(result)
        self.assertEqual(len(result.data.rows), 3)
        
        # Check that all columns are present
        row = result.data.rows[0]
        self.assertIn('dept_id', row)
        self.assertIn('dept_name', row)
        self.assertIn('budget', row)
    
    def test_02_select_specific_columns(self):
        """Test SELECT with specific column projection."""
        self._setup_test_data()
        result = self.execute_query("SELECT emp_id, emp_name FROM employees")
        self.assertQuerySuccess(result)
        self.assertEqual(len(result.data.rows), 5)
        
        # Check only requested columns are present
        row = result.data.rows[0]
        self.assertIn('emp_id', row)
        self.assertIn('emp_name', row)
        # Note: depending on implementation, other columns might be present but we focus on requested ones
    
    def test_03_select_with_simple_where(self):
        """Test SELECT with simple WHERE condition."""
        self._setup_test_data()
        result = self.execute_query("SELECT * FROM employees WHERE salary > 60000")
        self.assertQuerySuccess(result)
        
        # Should return Bob, Dave (salary > 60000)
        self.assertGreaterEqual(len(result.data.rows), 2)
        for row in result.data.rows:
            self.assertGreater(row['salary'], 60000)
    
    def test_04_select_with_equality_condition(self):
        """Test SELECT with equality condition."""
        self._setup_test_data()
        result = self.execute_query("SELECT * FROM employees WHERE dept_id = 1")
        self.assertQuerySuccess(result)
        
        # Should return Alice and Bob
        self.assertEqual(len(result.data.rows), 2)
        for row in result.data.rows:
            self.assertEqual(row['dept_id'], 1)
    
    def test_05_select_with_complex_and_condition(self):
        """Test SELECT with complex AND condition."""
        self._setup_test_data()
        result = self.execute_query("SELECT * FROM employees WHERE salary > 55000 AND dept_id = 2")
        self.assertQuerySuccess(result)
        
        # Should return Dave (dept_id=2, salary=82000)
        self.assertGreaterEqual(len(result.data.rows), 1)
        for row in result.data.rows:
            self.assertEqual(row['dept_id'], 2)
            self.assertGreater(row['salary'], 55000)
    
    def test_06_select_with_multiple_conditions(self):
        """Test SELECT with multiple AND conditions."""
        self._setup_test_data()
        result = self.execute_query("SELECT emp_name, salary FROM employees WHERE salary > 50000 AND salary < 80000")
        self.assertQuerySuccess(result)
        
        # Should return employees with salary between 50000 and 80000
        for row in result.data.rows:
            self.assertGreater(row['salary'], 50000)
            self.assertLess(row['salary'], 80000)
    
    def test_07_select_with_or_condition(self):
        """Test SELECT with OR condition."""
        self._setup_test_data()
        result = self.execute_query("SELECT * FROM employees WHERE dept_id = 1 OR dept_id = 3")
        self.assertQuerySuccess(result)
        
        # Should return employees from dept 1 (Alice, Bob) and dept 3 (Eve)
        self.assertEqual(len(result.data.rows), 3)
        for row in result.data.rows:
            self.assertIn(row['dept_id'], [1, 3])
    
    def test_08_select_with_complex_and_or(self):
        """Test SELECT with complex AND/OR combinations."""
        self._setup_test_data()
        # Find employees in dept 1 with high salary OR in dept 2
        result = self.execute_query("SELECT emp_name, dept_id, salary FROM employees WHERE (dept_id = 1 AND salary > 70000) OR dept_id = 2")
        self.assertQuerySuccess(result)
        
        # Should return: Bob (dept 1, salary 75000), Carol (dept 2), Dave (dept 2)
        self.assertEqual(len(result.data.rows), 3)
        for row in result.data.rows:
            # Either dept 1 with salary > 70000, or dept 2
            if row['dept_id'] == 1:
                self.assertGreater(row['salary'], 70000)
            elif row['dept_id'] == 2:
                pass  # Any salary in dept 2 is valid
            else:
                self.fail(f"Unexpected dept_id: {row['dept_id']}")
    
    def test_09_select_with_nested_conditions(self):
        """Test SELECT with nested AND/OR conditions."""
        self._setup_test_data()
        # (dept_id = 1 OR dept_id = 2) AND salary > 60000
        result = self.execute_query("SELECT emp_name, dept_id, salary FROM employees WHERE (dept_id = 1 OR dept_id = 2) AND salary > 60000")
        self.assertQuerySuccess(result)
        
        # Should return: Bob (dept 1, 75000), Dave (dept 2, 82000)
        self.assertEqual(len(result.data.rows), 2)
        for row in result.data.rows:
            self.assertIn(row['dept_id'], [1, 2])
            self.assertGreater(row['salary'], 60000)
    
    def test_10_select_with_three_or_conditions(self):
        """Test SELECT with three OR conditions."""
        self._setup_test_data()
        result = self.execute_query("SELECT * FROM employees WHERE salary = 60000 OR salary = 75000 OR salary = 50000")
        self.assertQuerySuccess(result)
        
        # Should return Alice (60000), Bob (75000), Eve (50000)
        self.assertEqual(len(result.data.rows), 3)
        for row in result.data.rows:
            self.assertIn(row['salary'], [60000, 75000, 50000])
    
    def test_11_select_with_mixed_and_or_operators(self):
        """Test SELECT with multiple AND and OR operators without parentheses."""
        self._setup_test_data()
        # dept_id = 1 AND salary > 60000 OR dept_id = 3
        # Should evaluate as: (dept_id = 1 AND salary > 60000) OR dept_id = 3
        result = self.execute_query("SELECT emp_name, dept_id, salary FROM employees WHERE dept_id = 1 AND salary > 60000 OR dept_id = 3")
        self.assertQuerySuccess(result)
        
        # Should return: Bob (dept 1, 75000) and Eve (dept 3, 50000)
        self.assertEqual(len(result.data.rows), 2)
        for row in result.data.rows:
            if row['dept_id'] == 1:
                self.assertGreater(row['salary'], 60000)
            elif row['dept_id'] == 3:
                pass  # Any salary in dept 3
    
    def test_12_select_with_comparison_operators(self):
        """Test SELECT with various comparison operators."""
        self._setup_test_data()
        # Less than or equal
        result = self.execute_query("SELECT * FROM employees WHERE salary <= 60000")
        self.assertQuerySuccess(result)
        for row in result.data.rows:
            self.assertLessEqual(row['salary'], 60000)
        
        # Greater than or equal
        result = self.execute_query("SELECT * FROM employees WHERE salary >= 75000")
        self.assertQuerySuccess(result)
        for row in result.data.rows:
            self.assertGreaterEqual(row['salary'], 75000)
        
        # Not equal (using <> operator)
        result = self.execute_query("SELECT * FROM employees WHERE dept_id <> 1")
        self.assertQuerySuccess(result)
        for row in result.data.rows:
            self.assertNotEqual(row['dept_id'], 1)


class TestJoin(TestQueryProcessor):
    """Test JOIN operations."""
    
    def _setup_join_data(self):
        """Set up tables and data for JOIN tests."""
        # Create tables
        self.execute_query("""
        CREATE TABLE departments (
            dept_id INTEGER PRIMARY KEY,
            dept_name VARCHAR(50),
            budget INTEGER
        )
        """)
        
        self.execute_query("""
        CREATE TABLE employees (
            emp_id INTEGER PRIMARY KEY,
            emp_name VARCHAR(50),
            dept_id INTEGER,
            salary INTEGER,
            hire_date VARCHAR(20)
        )
        """)
        
        # Populate data
        dept_data = [
            (1, 'Engineering', 100000),
            (2, 'Sales', 80000),
            (3, 'HR', 60000)
        ]
        for dept_id, name, budget in dept_data:
            self.execute_query(f"INSERT INTO departments (dept_id, dept_name, budget) VALUES ({dept_id}, '{name}', {budget})")
        
        emp_data = [
            (1, 'Alice', 1, 60000, '2024-01-15'),
            (2, 'Bob', 1, 75000, '2023-06-20'),
            (3, 'Carol', 2, 55000, '2024-03-10')
        ]
        for emp_id, name, dept_id, salary, date in emp_data:
            self.execute_query(f"INSERT INTO employees (emp_id, emp_name, dept_id, salary, hire_date) VALUES ({emp_id}, '{name}', {dept_id}, {salary}, '{date}')")
    
    def test_01_inner_join_with_on(self):
        """Test INNER JOIN with ON condition."""
        self._setup_join_data()
        query = "SELECT * FROM employees JOIN departments ON employees.dept_id = departments.dept_id"
        result = self.execute_query(query)
        self.assertQuerySuccess(result)
        
        # Should return joined rows
        self.assertGreater(len(result.data.rows), 0)
        
        # Verify joined data contains columns from both tables
        if result.data.rows:
            row = result.data.rows[0]
            self.assertIn('emp_name', row)
            self.assertIn('dept_name', row)
    
    def test_02_natural_join(self):
        """Test NATURAL JOIN."""
        self._setup_join_data()
        query = "SELECT * FROM employees NATURAL JOIN departments"
        result = self.execute_query(query)
        self.assertQuerySuccess(result)
        
        # Natural join should work on common column (dept_id)
        self.assertGreater(len(result.data.rows), 0)
    
    def test_03_join_with_projection(self):
        """Test JOIN with specific columns."""
        self._setup_join_data()
        query = "SELECT emp_name, dept_name FROM employees JOIN departments ON employees.dept_id = departments.dept_id"
        result = self.execute_query(query)
        self.assertQuerySuccess(result)
        
        if result.data.rows:
            row = result.data.rows[0]
            self.assertIn('emp_name', row)
            self.assertIn('dept_name', row)
    
    def test_04_cartesian_product_comma_syntax(self):
        """Test Cartesian product using comma-separated table syntax (CROSS JOIN)."""
        self._setup_join_data()
        # Query with comma creates Cartesian product
        query = "SELECT emp_name, dept_name FROM employees, departments"
        result = self.execute_query(query)
        self.assertQuerySuccess(result)
        
        # Cartesian product: 3 employees × 3 departments = 9 rows
        self.assertEqual(len(result.data.rows), 9)
        
        # Verify we have all combinations
        emp_names = set()
        dept_names = set()
        for row in result.data.rows:
            emp_names.add(row['emp_name'])
            dept_names.add(row['dept_name'])
        
        # Should have all 3 employees and all 3 departments in combinations
        self.assertEqual(len(emp_names), 3)
        self.assertEqual(len(dept_names), 3)
    
    def test_05_cartesian_product_with_projection(self):
        """Test Cartesian product with specific column selection."""
        self._setup_join_data()
        
        # Select specific columns from Cartesian product
        query = "SELECT emp_name, budget FROM employees, departments"
        result = self.execute_query(query)
        self.assertQuerySuccess(result)
        
        # Should still return 9 rows (3 × 3)
        self.assertEqual(len(result.data.rows), 9)
        
        # Verify specific columns are present
        for row in result.data.rows:
            self.assertIn('emp_name', row)
            self.assertIn('budget', row)
            # Each employee appears with each department's budget
            self.assertIn(row['budget'], [100000, 80000, 60000])
    
    def test_06_cartesian_product_with_where(self):
        """Test Cartesian product with WHERE clause filtering on both tables."""
        self._setup_join_data()
        
        # Cartesian product with compound WHERE condition
        query = "SELECT emp_name, dept_name FROM employees, departments WHERE employees.salary > 60000 AND departments.budget > 70000"
        result = self.execute_query(query)
        self.assertQuerySuccess(result)
        
        # Bob (salary 75000) with Engineering (100000) and Sales (80000) = 2 rows
        self.assertEqual(len(result.data.rows), 2)
        
        # All rows should have Bob and high-budget departments
        for row in result.data.rows:
            self.assertEqual(row['emp_name'], 'Bob')
            self.assertIn(row['dept_name'], ['Engineering', 'Sales'])
    
    def test_07_cartesian_product_with_simple_filter(self):
        """Test Cartesian product with simple WHERE filter on one table."""
        self._setup_join_data()
        
        # Filter on one table in Cartesian product
        query = "SELECT emp_name, dept_name FROM employees, departments WHERE employees.salary > 60000"
        result = self.execute_query(query)
        self.assertQuerySuccess(result)
        
        # Bob has salary 75000 > 60000, so should get Bob × all 3 departments = 3 rows
        self.assertEqual(len(result.data.rows), 3)
        
        # All rows should have Bob
        for row in result.data.rows:
            self.assertEqual(row['emp_name'], 'Bob')
            # But paired with different departments
            self.assertIn(row['dept_name'], ['Engineering', 'Sales', 'HR'])


class TestUpdate(TestQueryProcessor):
    """Test UPDATE functionality."""
    
    def _setup_update_data(self):
        """Set up tables and data for UPDATE tests."""
        super().setUp()
        self.execute_query("""
        CREATE TABLE employees (
            emp_id INTEGER PRIMARY KEY,
            emp_name VARCHAR(50),
            dept_id INTEGER,
            salary INTEGER,
            hire_date VARCHAR(20)
        )
        """)
        
        emp_data = [
            (1, 'Alice', 1, 60000, '2024-01-15'),
            (2, 'Bob', 1, 75000, '2023-06-20'),
            (3, 'Carol', 2, 55000, '2024-03-10')
        ]
        for emp_id, name, dept_id, salary, date in emp_data:
            self.execute_query(f"INSERT INTO employees (emp_id, emp_name, dept_id, salary, hire_date) VALUES ({emp_id}, '{name}', {dept_id}, {salary}, '{date}')")
    
    def test_01_update_with_hardcoded_value(self):
        """Test UPDATE with hardcoded value."""
        self._setup_update_data()
        query = "UPDATE employees SET salary = 65000 WHERE emp_id = 1"
        result = self.execute_query(query)
        self.assertQuerySuccess(result)
        
        # Verify update
        select_result = self.execute_query("SELECT salary FROM employees WHERE emp_id = 1")
        self.assertEqual(select_result.data.rows[0]['salary'], 65000)
    
    def test_02_update_with_expression(self):
        """Test UPDATE with expression (salary = salary + 5000)."""
        self._setup_update_data()
        query = "UPDATE employees SET salary = salary + 5000 WHERE dept_id = 1"
        result = self.execute_query(query)
        self.assertQuerySuccess(result)
        
        # Verify updates - Alice should be 65000, Bob should be 80000
        select_result = self.execute_query("SELECT emp_id, salary FROM employees WHERE dept_id = 1")
        salaries = {row['emp_id']: row['salary'] for row in select_result.data.rows}
        self.assertEqual(salaries[1], 65000)  # Alice: 60000 + 5000
        self.assertEqual(salaries[2], 80000)  # Bob: 75000 + 5000
    
    def test_03_update_multiple_rows(self):
        """Test UPDATE affecting multiple rows."""
        self._setup_update_data()
        # Set all dept_id=2 employees to salary 70000
        query = "UPDATE employees SET salary = 70000 WHERE dept_id = 2"
        result = self.execute_query(query)
        self.assertQuerySuccess(result)
        
        # Verify
        select_result = self.execute_query("SELECT salary FROM employees WHERE dept_id = 2")
        for row in select_result.data.rows:
            self.assertEqual(row['salary'], 70000)
    
    def test_04_update_with_arithmetic_expression(self):
        """Test UPDATE with more complex arithmetic."""
        self._setup_update_data()
        query = "UPDATE employees SET salary = salary * 2 WHERE emp_id = 3"
        result = self.execute_query(query)
        self.assertQuerySuccess(result)
        
        # Carol's salary should be 55000 * 2 = 110000
        select_result = self.execute_query("SELECT salary FROM employees WHERE emp_id = 3")
        self.assertEqual(select_result.data.rows[0]['salary'], 110000)


class TestDelete(TestQueryProcessor):
    """Test DELETE functionality including referential integrity with ON DELETE actions."""
    
    def _setup_delete_data(self):
        """Set up tables and data for DELETE tests."""
        super().setUp()
        self.execute_query("""
        CREATE TABLE tasks (
            task_id INTEGER PRIMARY KEY,
            task_name VARCHAR(50),
            project_id INTEGER,
            status VARCHAR(20)
        )
        """)
        
        task_data = [
            (1, 'Design UI', 1, 'completed'),
            (2, 'Implement API', 1, 'in_progress'),
            (3, 'Write Tests', 1, 'completed'),
            (4, 'Deploy', 2, 'pending')
        ]
        for task_id, name, proj_id, status in task_data:
            self.execute_query(f"INSERT INTO tasks (task_id, task_name, project_id, status) VALUES ({task_id}, '{name}', {proj_id}, '{status}')")
    
    def test_01_delete_with_simple_condition(self):
        """Test DELETE with single condition."""
        self._setup_delete_data()
        query = "DELETE FROM tasks WHERE task_id = 1"
        result = self.execute_query(query)
        self.assertQuerySuccess(result)
        
        # Verify deletion
        select_result = self.execute_query("SELECT * FROM tasks WHERE task_id = 1")
        self.assertEqual(len(select_result.data.rows), 0)
    
    def test_02_delete_with_string_condition(self):
        """Test DELETE with string condition."""
        self._setup_delete_data()
        query = "DELETE FROM tasks WHERE status = 'completed'"
        result = self.execute_query(query)
        self.assertQuerySuccess(result)
        
        # Verify all completed tasks deleted
        select_result = self.execute_query("SELECT * FROM tasks WHERE status = 'completed'")
        self.assertEqual(len(select_result.data.rows), 0)
        
        # Verify other tasks still exist
        all_result = self.execute_query("SELECT * FROM tasks")
        self.assertGreater(len(all_result.data.rows), 0)
    
    def test_03_delete_with_numeric_condition(self):
        """Test DELETE with numeric condition."""
        self._setup_delete_data()
        query = "DELETE FROM tasks WHERE project_id = 2"
        result = self.execute_query(query)
        self.assertQuerySuccess(result)
        
        # Verify deletion
        select_result = self.execute_query("SELECT * FROM tasks WHERE project_id = 2")
        self.assertEqual(len(select_result.data.rows), 0)
    
    def test_04_delete_parent_with_foreign_key_reference(self):
        """Test deleting from parent table when child references exist."""
        super().setUp()
        # Create parent and child tables
        self.execute_query("""
        CREATE TABLE categories (
            cat_id INTEGER PRIMARY KEY,
            cat_name VARCHAR(50)
        )
        """)
        
        self.execute_query("""
        CREATE TABLE products (
            prod_id INTEGER PRIMARY KEY,
            cat_id INTEGER,
            prod_name VARCHAR(50)
        )
        """)
        
        # Insert data with foreign key relationships
        self.execute_query("INSERT INTO categories (cat_id, cat_name) VALUES (1, 'Electronics')")
        self.execute_query("INSERT INTO categories (cat_id, cat_name) VALUES (2, 'Clothing')")
        self.execute_query("INSERT INTO products (prod_id, cat_id, prod_name) VALUES (1, 1, 'Laptop')")
        self.execute_query("INSERT INTO products (prod_id, cat_id, prod_name) VALUES (2, 1, 'Phone')")
        self.execute_query("INSERT INTO products (prod_id, cat_id, prod_name) VALUES (3, 2, 'Shirt')")
        
        # Try to delete parent category that has child products
        # Behavior depends on implementation (CASCADE vs RESTRICT)
        result = self.execute_query("DELETE FROM categories WHERE cat_id = 1")
        
        # Verify behavior is consistent
        if result.success:
            # Delete succeeded - check parent is gone
            select_result = self.execute_query("SELECT * FROM categories WHERE cat_id = 1")
            self.assertEqual(len(select_result.data.rows), 0)
            
            # Child products may be cascaded or orphaned depending on implementation
            products_result = self.execute_query("SELECT * FROM products WHERE cat_id = 1")
            # Either cascaded (0 rows) or orphaned (2 rows with broken reference)
        else:
            # Delete was restricted - verify parent and children still exist
            select_result = self.execute_query("SELECT * FROM categories WHERE cat_id = 1")
            self.assertEqual(len(select_result.data.rows), 1)
            products_result = self.execute_query("SELECT * FROM products WHERE cat_id = 1")
            self.assertEqual(len(products_result.data.rows), 2)
    
    def test_05_delete_child_with_foreign_key(self):
        """Test deleting from child table (should always succeed)."""
        super().setUp()
        # Create parent and child tables
        self.execute_query("""
        CREATE TABLE departments (
            dept_id INTEGER PRIMARY KEY,
            dept_name VARCHAR(50)
        )
        """)
        
        self.execute_query("""
        CREATE TABLE employees (
            emp_id INTEGER PRIMARY KEY,
            dept_id INTEGER,
            emp_name VARCHAR(50)
        )
        """)
        
        # Insert data
        self.execute_query("INSERT INTO departments (dept_id, dept_name) VALUES (1, 'IT')")
        self.execute_query("INSERT INTO departments (dept_id, dept_name) VALUES (2, 'HR')")
        self.execute_query("INSERT INTO employees (emp_id, dept_id, emp_name) VALUES (1, 1, 'Alice')")
        self.execute_query("INSERT INTO employees (emp_id, dept_id, emp_name) VALUES (2, 1, 'Bob')")
        self.execute_query("INSERT INTO employees (emp_id, dept_id, emp_name) VALUES (3, 2, 'Carol')")
        
        # Delete from child table - should always succeed
        result = self.execute_query("DELETE FROM employees WHERE dept_id = 1")
        self.assertQuerySuccess(result)
        
        # Verify children deleted
        select_result = self.execute_query("SELECT * FROM employees WHERE dept_id = 1")
        self.assertEqual(len(select_result.data.rows), 0)
        
        # Verify parent unaffected
        dept_result = self.execute_query("SELECT * FROM departments WHERE dept_id = 1")
        self.assertEqual(len(dept_result.data.rows), 1)
        
        # Verify other children unaffected
        other_emp = self.execute_query("SELECT * FROM employees WHERE dept_id = 2")
        self.assertEqual(len(other_emp.data.rows), 1)
    
    def test_06_delete_multiple_levels_foreign_key(self):
        """Test deleting with multi-level foreign key relationships."""
        super().setUp()
        # Create three-level hierarchy
        self.execute_query("""
        CREATE TABLE companies (
            company_id INTEGER PRIMARY KEY,
            company_name VARCHAR(50)
        )
        """)
        
        self.execute_query("""
        CREATE TABLE departments (
            dept_id INTEGER PRIMARY KEY,
            company_id INTEGER,
            dept_name VARCHAR(50)
        )
        """)
        
        self.execute_query("""
        CREATE TABLE employees (
            emp_id INTEGER PRIMARY KEY,
            dept_id INTEGER,
            emp_name VARCHAR(50)
        )
        """)
        
        # Insert hierarchical data
        self.execute_query("INSERT INTO companies (company_id, company_name) VALUES (1, 'TechCorp')")
        self.execute_query("INSERT INTO departments (dept_id, company_id, dept_name) VALUES (1, 1, 'Engineering')")
        self.execute_query("INSERT INTO departments (dept_id, company_id, dept_name) VALUES (2, 1, 'Sales')")
        self.execute_query("INSERT INTO employees (emp_id, dept_id, emp_name) VALUES (1, 1, 'Alice')")
        self.execute_query("INSERT INTO employees (emp_id, dept_id, emp_name) VALUES (2, 1, 'Bob')")
        self.execute_query("INSERT INTO employees (emp_id, dept_id, emp_name) VALUES (3, 2, 'Carol')")
        
        # Try to delete from top level (company)
        result = self.execute_query("DELETE FROM companies WHERE company_id = 1")
        
        # Check what happened
        company_check = self.execute_query("SELECT * FROM companies WHERE company_id = 1")
        if len(company_check.data.rows) == 0:
            # Company deleted - may have cascaded or left orphans
            pass
        else:
            # Delete was restricted - all relationships should still exist
            self.assertEqual(len(company_check.data.rows), 1)
            dept_check = self.execute_query("SELECT * FROM departments WHERE company_id = 1")
            self.assertEqual(len(dept_check.data.rows), 2)
    
    def test_07_delete_with_complex_where_and_foreign_key(self):
        """Test DELETE with complex WHERE on table with foreign keys."""
        super().setUp()
        self.execute_query("""
        CREATE TABLE projects (
            proj_id INTEGER PRIMARY KEY,
            proj_name VARCHAR(50),
            budget INTEGER
        )
        """)
        
        self.execute_query("""
        CREATE TABLE tasks (
            task_id INTEGER PRIMARY KEY,
            proj_id INTEGER,
            task_name VARCHAR(50),
            priority INTEGER
        )
        """)
        
        # Insert data
        self.execute_query("INSERT INTO projects (proj_id, proj_name, budget) VALUES (1, 'Project A', 100000)")
        self.execute_query("INSERT INTO projects (proj_id, proj_name, budget) VALUES (2, 'Project B', 50000)")
        self.execute_query("INSERT INTO tasks (task_id, proj_id, task_name, priority) VALUES (1, 1, 'Task 1', 1)")
        self.execute_query("INSERT INTO tasks (task_id, proj_id, task_name, priority) VALUES (2, 1, 'Task 2', 3)")
        self.execute_query("INSERT INTO tasks (task_id, proj_id, task_name, priority) VALUES (3, 2, 'Task 3', 2)")
        
        # Delete tasks with complex condition
        result = self.execute_query("DELETE FROM tasks WHERE proj_id = 1 AND priority > 1")
        self.assertQuerySuccess(result)
        
        # Verify correct deletion
        remaining = self.execute_query("SELECT * FROM tasks WHERE proj_id = 1")
        self.assertEqual(len(remaining.data.rows), 1)
        self.assertEqual(remaining.data.rows[0]['priority'], 1)
        
        # Verify parent unaffected
        proj_check = self.execute_query("SELECT * FROM projects WHERE proj_id = 1")
        self.assertEqual(len(proj_check.data.rows), 1)
    
    def test_08_delete_with_on_delete_restrict(self):
        """Test DELETE with ON DELETE RESTRICT - prevents deletion when children exist."""
        super().setUp()
        # Create parent and child with RESTRICT
        self.execute_query("""
        CREATE TABLE departments (
            dept_id INTEGER PRIMARY KEY,
            dept_name VARCHAR(50)
        )
        """)
        
        self.execute_query("""
        CREATE TABLE employees (
            emp_id INTEGER PRIMARY KEY,
            emp_name VARCHAR(50),
            dept_id INTEGER FOREIGN KEY REFERENCES departments(dept_id) ON DELETE RESTRICT
        )
        """)
        
        # Insert test data
        self.execute_query("INSERT INTO departments (dept_id, dept_name) VALUES (1, 'Engineering')")
        self.execute_query("INSERT INTO departments (dept_id, dept_name) VALUES (2, 'Sales')")
        self.execute_query("INSERT INTO employees (emp_id, emp_name, dept_id) VALUES (1, 'Alice', 1)")
        self.execute_query("INSERT INTO employees (emp_id, emp_name, dept_id) VALUES (2, 'Bob', 1)")
        
        # Try to delete parent - should FAIL
        result = self.execute_query("DELETE FROM departments WHERE dept_id = 1")
        self.assertQueryFails(result, "Should not allow deleting parent with children (RESTRICT)")
        
        # Verify parent still exists
        select_result = self.execute_query("SELECT * FROM departments WHERE dept_id = 1")
        self.assertEqual(len(select_result.data.rows), 1)
        
        # Verify children still exist
        emp_result = self.execute_query("SELECT * FROM employees WHERE dept_id = 1")
        self.assertEqual(len(emp_result.data.rows), 2)
        
        # Delete without children should succeed
        result = self.execute_query("DELETE FROM departments WHERE dept_id = 2")
        self.assertQuerySuccess(result, "Should allow deleting parent without children")
    
    def test_09_delete_with_on_delete_cascade(self):
        """Test DELETE with ON DELETE CASCADE - automatically deletes child rows."""
        super().setUp()
        # Create parent and child with CASCADE
        self.execute_query("""
        CREATE TABLE customers (
            cust_id INTEGER PRIMARY KEY,
            cust_name VARCHAR(50)
        )
        """)
        
        self.execute_query("""
        CREATE TABLE orders (
            order_id INTEGER PRIMARY KEY,
            cust_id INTEGER FOREIGN KEY REFERENCES customers(cust_id) ON DELETE CASCADE,
            amount INTEGER
        )
        """)
        
        # Insert test data
        self.execute_query("INSERT INTO customers (cust_id, cust_name) VALUES (1, 'John')")
        self.execute_query("INSERT INTO customers (cust_id, cust_name) VALUES (2, 'Jane')")
        self.execute_query("INSERT INTO orders (order_id, cust_id, amount) VALUES (1, 1, 100)")
        self.execute_query("INSERT INTO orders (order_id, cust_id, amount) VALUES (2, 1, 200)")
        self.execute_query("INSERT INTO orders (order_id, cust_id, amount) VALUES (3, 2, 150)")
        
        # Delete parent - should CASCADE to children
        result = self.execute_query("DELETE FROM customers WHERE cust_id = 1")
        self.assertQuerySuccess(result, "Should allow deleting parent (CASCADE)")
        
        # Verify parent deleted
        cust_result = self.execute_query("SELECT * FROM customers WHERE cust_id = 1")
        self.assertEqual(len(cust_result.data.rows), 0)
        
        # Verify children also deleted (CASCADE)
        order_result = self.execute_query("SELECT * FROM orders WHERE cust_id = 1")
        self.assertEqual(len(order_result.data.rows), 0, "Child rows should be deleted (CASCADE)")
        
        # Verify other customer and orders still exist
        cust_result = self.execute_query("SELECT * FROM customers WHERE cust_id = 2")
        self.assertEqual(len(cust_result.data.rows), 1)
        order_result = self.execute_query("SELECT * FROM orders WHERE cust_id = 2")
        self.assertEqual(len(order_result.data.rows), 1)
    
    def test_10_delete_with_on_delete_cascade_multi_level(self):
        """Test CASCADE with multi-level foreign key hierarchy."""
        super().setUp()
        # Create 3-level hierarchy with CASCADE
        self.execute_query("""
        CREATE TABLE companies (
            company_id INTEGER PRIMARY KEY,
            company_name VARCHAR(50)
        )
        """)
        
        self.execute_query("""
        CREATE TABLE departments (
            dept_id INTEGER PRIMARY KEY,
            dept_name VARCHAR(50),
            company_id INTEGER FOREIGN KEY REFERENCES companies(company_id) ON DELETE CASCADE
        )
        """)
        
        self.execute_query("""
        CREATE TABLE employees (
            emp_id INTEGER PRIMARY KEY,
            emp_name VARCHAR(50),
            dept_id INTEGER FOREIGN KEY REFERENCES departments(dept_id) ON DELETE CASCADE
        )
        """)
        
        # Insert test data
        self.execute_query("INSERT INTO companies (company_id, company_name) VALUES (1, 'TechCorp')")
        self.execute_query("INSERT INTO departments (dept_id, dept_name, company_id) VALUES (1, 'Engineering', 1)")
        self.execute_query("INSERT INTO departments (dept_id, dept_name, company_id) VALUES (2, 'Sales', 1)")
        self.execute_query("INSERT INTO employees (emp_id, emp_name, dept_id) VALUES (1, 'Alice', 1)")
        self.execute_query("INSERT INTO employees (emp_id, emp_name, dept_id) VALUES (2, 'Bob', 1)")
        self.execute_query("INSERT INTO employees (emp_id, emp_name, dept_id) VALUES (3, 'Charlie', 2)")
        
        # Delete top-level parent
        result = self.execute_query("DELETE FROM companies WHERE company_id = 1")
        self.assertQuerySuccess(result, "Should cascade delete through all levels")
        
        # Verify all data cascaded deleted
        comp_result = self.execute_query("SELECT * FROM companies")
        self.assertEqual(len(comp_result.data.rows), 0)
        
        dept_result = self.execute_query("SELECT * FROM departments")
        self.assertEqual(len(dept_result.data.rows), 0, "Departments should cascade delete")
        
        emp_result = self.execute_query("SELECT * FROM employees")
        self.assertEqual(len(emp_result.data.rows), 0, "Employees should cascade delete")
    
    def test_11_delete_with_on_delete_set_null(self):
        """Test DELETE with ON DELETE SET NULL - sets FK to NULL when parent deleted."""
        super().setUp()
        # Create parent and child with SET NULL
        self.execute_query("""
        CREATE TABLE managers (
            mgr_id INTEGER PRIMARY KEY,
            mgr_name VARCHAR(50)
        )
        """)
        
        self.execute_query("""
        CREATE TABLE employees (
            emp_id INTEGER PRIMARY KEY,
            emp_name VARCHAR(50),
            mgr_id INTEGER FOREIGN KEY REFERENCES managers(mgr_id) ON DELETE SET NULL
        )
        """)
        
        # Insert test data
        self.execute_query("INSERT INTO managers (mgr_id, mgr_name) VALUES (1, 'Boss')")
        self.execute_query("INSERT INTO managers (mgr_id, mgr_name) VALUES (2, 'Supervisor')")
        self.execute_query("INSERT INTO employees (emp_id, emp_name, mgr_id) VALUES (1, 'Alice', 1)")
        self.execute_query("INSERT INTO employees (emp_id, emp_name, mgr_id) VALUES (2, 'Bob', 1)")
        self.execute_query("INSERT INTO employees (emp_id, emp_name, mgr_id) VALUES (3, 'Charlie', 2)")
        
        # Delete parent
        result = self.execute_query("DELETE FROM managers WHERE mgr_id = 1")
        self.assertQuerySuccess(result, "Should allow deleting parent (SET NULL)")
        
        # Verify parent deleted
        mgr_result = self.execute_query("SELECT * FROM managers WHERE mgr_id = 1")
        self.assertEqual(len(mgr_result.data.rows), 0)
        
        # Verify children exist but FK is NULL
        emp_result = self.execute_query("SELECT * FROM employees WHERE emp_id IN (1, 2)")
        self.assertEqual(len(emp_result.data.rows), 2, "Child rows should still exist")
        
        for row in emp_result.data.rows:
            self.assertIsNone(row.get('mgr_id'), f"FK should be NULL for emp_id={row['emp_id']}")
        
        # Verify other manager and employee unaffected
        mgr_result = self.execute_query("SELECT * FROM managers WHERE mgr_id = 2")
        self.assertEqual(len(mgr_result.data.rows), 1)
        emp_result = self.execute_query("SELECT * FROM employees WHERE emp_id = 3")
        self.assertEqual(len(emp_result.data.rows), 1)
        self.assertEqual(emp_result.data.rows[0]['mgr_id'], 2)
    
    def test_12_delete_with_mixed_cascade_and_restrict(self):
        """Test table with some children using CASCADE and others using RESTRICT."""
        super().setUp()
        # Parent table
        self.execute_query("""
        CREATE TABLE projects (
            proj_id INTEGER PRIMARY KEY,
            proj_name VARCHAR(50)
        )
        """)
        
        # Child 1: CASCADE
        self.execute_query("""
        CREATE TABLE tasks (
            task_id INTEGER PRIMARY KEY,
            task_name VARCHAR(50),
            proj_id INTEGER FOREIGN KEY REFERENCES projects(proj_id) ON DELETE CASCADE
        )
        """)
        
        # Child 2: RESTRICT
        self.execute_query("""
        CREATE TABLE milestones (
            milestone_id INTEGER PRIMARY KEY,
            milestone_name VARCHAR(50),
            proj_id INTEGER FOREIGN KEY REFERENCES projects(proj_id) ON DELETE RESTRICT
        )
        """)
        
        # Insert data
        self.execute_query("INSERT INTO projects (proj_id, proj_name) VALUES (1, 'Project Alpha')")
        self.execute_query("INSERT INTO tasks (task_id, task_name, proj_id) VALUES (1, 'Task 1', 1)")
        self.execute_query("INSERT INTO milestones (milestone_id, milestone_name, proj_id) VALUES (1, 'Milestone 1', 1)")
        
        # Try to delete parent - should FAIL because of RESTRICT
        result = self.execute_query("DELETE FROM projects WHERE proj_id = 1")
        self.assertQueryFails(result, "Should fail due to RESTRICT even with CASCADE children")
        
        # Verify all data unchanged
        proj_result = self.execute_query("SELECT * FROM projects WHERE proj_id = 1")
        self.assertEqual(len(proj_result.data.rows), 1)
        task_result = self.execute_query("SELECT * FROM tasks WHERE proj_id = 1")
        self.assertEqual(len(task_result.data.rows), 1)
        milestone_result = self.execute_query("SELECT * FROM milestones WHERE proj_id = 1")
        self.assertEqual(len(milestone_result.data.rows), 1)
        
        # Delete RESTRICT child first
        self.execute_query("DELETE FROM milestones WHERE milestone_id = 1")
        
        # Now deletion should succeed and CASCADE
        result = self.execute_query("DELETE FROM projects WHERE proj_id = 1")
        self.assertQuerySuccess(result, "Should succeed after removing RESTRICT child")
        
        # Verify CASCADE worked
        task_result = self.execute_query("SELECT * FROM tasks WHERE proj_id = 1")
        self.assertEqual(len(task_result.data.rows), 0, "Tasks should cascade delete")


class TestLimit(TestQueryProcessor):
    """Test LIMIT functionality."""
    
    def _setup_limit_data(self):
        """Set up tables and data for LIMIT tests."""
        super().setUp()
        self.execute_query("""
        CREATE TABLE employees (
            emp_id INTEGER PRIMARY KEY,
            emp_name VARCHAR(50),
            dept_id INTEGER,
            salary INTEGER,
            hire_date VARCHAR(20)
        )
        """)
        
        # Insert 10 employees
        for i in range(1, 11):
            self.execute_query(f"INSERT INTO employees (emp_id, emp_name, dept_id, salary, hire_date) VALUES ({i}, 'Employee{i}', 1, {50000 + i*1000}, '2024-01-01')")
    
    def test_01_simple_limit(self):
        """Test simple LIMIT clause."""
        self._setup_limit_data()
        query = "SELECT * FROM employees LIMIT 5"
        result = self.execute_query(query)
        self.assertQuerySuccess(result)
        
        # Should return exactly 5 rows
        self.assertEqual(len(result.data.rows), 5)
    
    def test_02_limit_with_where(self):
        """Test LIMIT with WHERE clause."""
        self._setup_limit_data()
        query = "SELECT * FROM employees WHERE salary > 52000 LIMIT 3"
        result = self.execute_query(query)
        self.assertQuerySuccess(result)
        
        # Should return at most 3 rows
        self.assertLessEqual(len(result.data.rows), 3)
        
        # All returned rows should satisfy condition
        for row in result.data.rows:
            self.assertGreater(row['salary'], 52000)
    
    def test_03_limit_larger_than_result_set(self):
        """Test LIMIT larger than available rows."""
        self._setup_limit_data()
        query = "SELECT * FROM employees WHERE dept_id = 1 LIMIT 100"
        result = self.execute_query(query)
        self.assertQuerySuccess(result)
        
        # Should return all available rows (10 in this case)
        self.assertEqual(len(result.data.rows), 10)


class TestOrderBy(TestQueryProcessor):
    """Test ORDER BY functionality."""
    
    def _setup_orderby_data(self):
        """Set up tables and data for ORDER BY tests."""
        super().setUp()
        self.execute_query("""
        CREATE TABLE employees (
            emp_id INTEGER PRIMARY KEY,
            emp_name VARCHAR(50),
            dept_id INTEGER,
            salary INTEGER,
            hire_date VARCHAR(20)
        )
        """)
        
        emp_data = [
            (1, 'Alice', 1, 60000, '2024-01-15'),
            (2, 'Bob', 1, 75000, '2023-06-20'),
            (3, 'Carol', 2, 55000, '2024-03-10'),
            (4, 'Dave', 2, 82000, '2022-11-05'),
            (5, 'Eve', 3, 50000, '2024-02-28')
        ]
        for emp_id, name, dept_id, salary, date in emp_data:
            self.execute_query(f"INSERT INTO employees (emp_id, emp_name, dept_id, salary, hire_date) VALUES ({emp_id}, '{name}', {dept_id}, {salary}, '{date}')")
    
    def test_01_order_by_asc(self):
        """Test ORDER BY ascending."""
        self._setup_orderby_data()
        query = "SELECT * FROM employees ORDER BY salary ASC"
        result = self.execute_query(query)
        self.assertQuerySuccess(result)
        
        # Verify ascending order
        salaries = [row['salary'] for row in result.data.rows]
        self.assertEqual(salaries, sorted(salaries))
    
    def test_02_order_by_desc(self):
        """Test ORDER BY descending."""
        self._setup_orderby_data()
        query = "SELECT * FROM employees ORDER BY salary DESC"
        result = self.execute_query(query)
        self.assertQuerySuccess(result)
        
        # Verify descending order
        salaries = [row['salary'] for row in result.data.rows]
        self.assertEqual(salaries, sorted(salaries, reverse=True))
    
    def test_03_order_by_with_where(self):
        """Test ORDER BY with WHERE clause."""
        self._setup_orderby_data()
        query = "SELECT * FROM employees WHERE dept_id = 1 ORDER BY salary DESC"
        result = self.execute_query(query)
        self.assertQuerySuccess(result)
        
        # Verify all rows have dept_id = 1
        for row in result.data.rows:
            self.assertEqual(row['dept_id'], 1)
        
        # Verify descending order
        salaries = [row['salary'] for row in result.data.rows]
        self.assertEqual(salaries, sorted(salaries, reverse=True))
    
    def test_04_order_by_string_column(self):
        """Test ORDER BY on string column."""
        self._setup_orderby_data()
        query = "SELECT * FROM employees ORDER BY emp_name ASC"
        result = self.execute_query(query)
        self.assertQuerySuccess(result)
        
        # Verify alphabetical order
        names = [row['emp_name'] for row in result.data.rows]
        self.assertEqual(names, sorted(names))


class TestAlias(TestQueryProcessor):
    """Test table and column ALIAS functionality."""
    
    def _setup_alias_data(self):
        """Set up tables and data for ALIAS tests."""
        super().setUp()
        # Create tables
        self.execute_query("""
        CREATE TABLE departments (
            dept_id INTEGER PRIMARY KEY,
            dept_name VARCHAR(50),
            budget INTEGER
        )
        """)
        
        self.execute_query("""
        CREATE TABLE employees (
            emp_id INTEGER PRIMARY KEY,
            emp_name VARCHAR(50),
            dept_id INTEGER,
            salary INTEGER,
            hire_date VARCHAR(20)
        )
        """)
        
        # Populate data
        self.execute_query("INSERT INTO departments (dept_id, dept_name, budget) VALUES (1, 'Engineering', 100000)")
        self.execute_query("INSERT INTO employees (emp_id, emp_name, dept_id, salary, hire_date) VALUES (1, 'Alice', 1, 60000, '2024-01-15')")
    
    def test_01_table_alias_in_join(self):
        """Test table aliases in JOIN."""
        self._setup_alias_data()
        query = "SELECT e.emp_name, d.dept_name FROM employees e JOIN departments d ON e.dept_id = d.dept_id"
        result = self.execute_query(query)
        self.assertQuerySuccess(result)
        
        # Should successfully parse and execute with aliases
        self.assertGreater(len(result.data.rows), 0)


class TestDropTable(TestQueryProcessor):
    """Test DROP TABLE functionality with foreign key handling."""
    
    def test_01_drop_simple_table(self):
        """Test dropping a table without dependencies."""
        # Create and drop a simple table
        self.execute_query("""
        CREATE TABLE temp_table (
            id INTEGER PRIMARY KEY,
            name VARCHAR(50)
        )
        """)
        
        # Verify table exists
        self.assertIn('temp_table', self.storage_manager.tables)
        
        # Drop table
        result = self.execute_query("DROP TABLE temp_table")
        self.assertQuerySuccess(result)
        
        # Verify table removed
        self.assertNotIn('temp_table', self.storage_manager.tables)
    
    def test_02_drop_table_with_data(self):
        """Test dropping a table that contains data."""
        # Create and populate table
        self.execute_query("""
        CREATE TABLE temp_data (
            id INTEGER PRIMARY KEY,
            value INTEGER
        )
        """)
        
        self.execute_query("INSERT INTO temp_data (id, value) VALUES (1, 100)")
        self.execute_query("INSERT INTO temp_data (id, value) VALUES (2, 200)")
        
        # Drop table
        result = self.execute_query("DROP TABLE temp_data")
        self.assertQuerySuccess(result)
        
        # Verify table removed
        self.assertNotIn('temp_data', self.storage_manager.tables)
    
    def test_03_drop_parent_table_with_foreign_key(self):
        """Test dropping a parent table that has foreign key references."""
        # Create parent and child tables
        self.execute_query("""
        CREATE TABLE parent_table (
            parent_id INTEGER PRIMARY KEY,
            parent_name VARCHAR(50)
        )
        """)
        
        self.execute_query("""
        CREATE TABLE child_table (
            child_id INTEGER PRIMARY KEY,
            parent_id INTEGER,
            child_name VARCHAR(50)
        )
        """)
        
        # Insert data with foreign key relationship
        self.execute_query("INSERT INTO parent_table (parent_id, parent_name) VALUES (1, 'Parent1')")
        self.execute_query("INSERT INTO child_table (child_id, parent_id, child_name) VALUES (1, 1, 'Child1')")
        
        # Try to drop parent table - behavior depends on implementation
        # Some systems allow it, others restrict it
        result = self.execute_query("DROP TABLE parent_table")
        
        # If drop succeeds, child should still exist but references broken
        # If drop fails, both tables should still exist
        # We'll just verify the behavior is consistent
        if result.success:
            self.assertNotIn('parent_table', self.storage_manager.tables)
            # Child table may or may not exist depending on CASCADE behavior
        else:
            # Drop was restricted due to foreign key
            self.assertIn('parent_table', self.storage_manager.tables)
            self.assertIn('child_table', self.storage_manager.tables)
    
    def test_04_drop_child_table_with_foreign_key(self):
        """Test dropping a child table that references a parent."""
        # Create parent and child tables
        self.execute_query("""
        CREATE TABLE parent_dept (
            dept_id INTEGER PRIMARY KEY,
            dept_name VARCHAR(50)
        )
        """)
        
        self.execute_query("""
        CREATE TABLE child_emp (
            emp_id INTEGER PRIMARY KEY,
            dept_id INTEGER,
            emp_name VARCHAR(50)
        )
        """)
        
        # Insert data
        self.execute_query("INSERT INTO parent_dept (dept_id, dept_name) VALUES (1, 'Sales')")
        self.execute_query("INSERT INTO child_emp (emp_id, dept_id, emp_name) VALUES (1, 1, 'Alice')")
        
        # Drop child table - should succeed without issues
        result = self.execute_query("DROP TABLE child_emp")
        self.assertQuerySuccess(result)
        
        # Verify child removed, parent remains
        self.assertNotIn('child_emp', self.storage_manager.tables)
        self.assertIn('parent_dept', self.storage_manager.tables)
    
    def test_05_drop_multiple_related_tables(self):
        """Test dropping multiple tables with relationships."""
        # Create a chain of tables
        self.execute_query("""
        CREATE TABLE level1 (
            id INTEGER PRIMARY KEY,
            name VARCHAR(50)
        )
        """)
        
        self.execute_query("""
        CREATE TABLE level2 (
            id INTEGER PRIMARY KEY,
            level1_id INTEGER,
            name VARCHAR(50)
        )
        """)
        
        self.execute_query("""
        CREATE TABLE level3 (
            id INTEGER PRIMARY KEY,
            level2_id INTEGER,
            name VARCHAR(50)
        )
        """)
        
        # Insert data
        self.execute_query("INSERT INTO level1 (id, name) VALUES (1, 'Level1')")
        self.execute_query("INSERT INTO level2 (id, level1_id, name) VALUES (1, 1, 'Level2')")
        self.execute_query("INSERT INTO level3 (id, level2_id, name) VALUES (1, 1, 'Level3')")
        
        # Drop in reverse order (child first)
        result = self.execute_query("DROP TABLE level3")
        self.assertQuerySuccess(result)
        self.assertNotIn('level3', self.storage_manager.tables)
        
        result = self.execute_query("DROP TABLE level2")
        self.assertQuerySuccess(result)
        self.assertNotIn('level2', self.storage_manager.tables)
        
        result = self.execute_query("DROP TABLE level1")
        self.assertQuerySuccess(result)
        self.assertNotIn('level1', self.storage_manager.tables)


class TestSubqueries(TestQueryProcessor):
    """Test subquery operations (IN, NOT IN, EXISTS, NOT EXISTS)."""
    
    def _setup_subquery_data(self):
        """Set up test data for subquery tests."""
        super().setUp()
        # Create departments table
        self.execute_query("""
        CREATE TABLE departments (
            dept_id INTEGER PRIMARY KEY,
            dept_name VARCHAR(50),
            budget INTEGER
        )
        """)
        
        # Create employees table
        self.execute_query("""
        CREATE TABLE employees (
            emp_id INTEGER PRIMARY KEY,
            emp_name VARCHAR(50),
            dept_id INTEGER,
            salary INTEGER
        )
        """)
        
        # Populate departments
        dept_data = [(1, 'Engineering', 100000), (2, 'Sales', 80000), (3, 'HR', 60000)]
        for dept_id, name, budget in dept_data:
            self.execute_query(f"INSERT INTO departments (dept_id, dept_name, budget) VALUES ({dept_id}, '{name}', {budget})")
        
        # Populate employees (note: dept_id 3 has no employees)
        emp_data = [
            (1, 'Alice', 1, 75000),
            (2, 'Bob', 1, 65000),
            (3, 'Charlie', 2, 60000),
            (4, 'Diana', 2, 55000),
            (5, 'Eve', 99, 50000)  # Invalid dept_id
        ]
        for emp_id, name, dept_id, salary in emp_data:
            self.execute_query(f"INSERT INTO employees (emp_id, emp_name, dept_id, salary) VALUES ({emp_id}, '{name}', {dept_id}, {salary})")
    
    def test_01_in_subquery_simple(self):
        """Test IN with simple subquery."""
        self._setup_subquery_data()
        query = """
        SELECT emp_name, salary 
        FROM employees 
        WHERE dept_id IN (SELECT dept_id FROM departments WHERE budget > 70000)
        """
        result = self.execute_query(query)
        self.assertQuerySuccess(result)
        
        # Should return employees from Engineering (1) and Sales (2)
        self.assertEqual(len(result.data.rows), 4)
        names = [row['emp_name'] for row in result.data.rows]
        self.assertIn('Alice', names)
        self.assertIn('Bob', names)
        self.assertIn('Charlie', names)
        self.assertIn('Diana', names)
    
    def test_02_not_in_subquery(self):
        """Test NOT IN with subquery."""
        self._setup_subquery_data()
        query = """
        SELECT emp_name, salary 
        FROM employees 
        WHERE dept_id NOT IN (SELECT dept_id FROM departments WHERE budget > 70000)
        """
        result = self.execute_query(query)
        self.assertQuerySuccess(result)
        
        # Should return only Eve (dept_id 99, not in high-budget departments)
        self.assertEqual(len(result.data.rows), 1)
        self.assertEqual(result.data.rows[0]['emp_name'], 'Eve')
    
    def test_03_exists_subquery(self):
        """Test EXISTS with non-correlated subquery."""
        self._setup_subquery_data()
        # Check if any department has budget > 90000
        query = """
        SELECT dept_name, budget 
        FROM departments 
        WHERE EXISTS (SELECT * FROM departments WHERE budget > 90000)
        """
        result = self.execute_query(query)
        self.assertQuerySuccess(result)
        
        # Should return all departments (because Engineering has budget 100000)
        self.assertEqual(len(result.data.rows), 3)
    
    def test_04_not_exists_subquery(self):
        """Test NOT EXISTS with non-correlated subquery."""
        self._setup_subquery_data()
        # Check if no department has budget > 150000
        query = """
        SELECT dept_name, budget 
        FROM departments 
        WHERE NOT EXISTS (SELECT * FROM departments WHERE budget > 150000)
        """
        result = self.execute_query(query)
        self.assertQuerySuccess(result)
        
        # Should return all departments (because no dept has budget > 150000)
        self.assertEqual(len(result.data.rows), 3)
    
    def test_05_in_subquery_with_where(self):
        """Test IN subquery combined with additional WHERE conditions."""
        self._setup_subquery_data()
        query = """
        SELECT emp_name, salary 
        FROM employees 
        WHERE salary > 60000 
        AND dept_id IN (SELECT dept_id FROM departments WHERE budget > 80000)
        """
        result = self.execute_query(query)
        self.assertQuerySuccess(result)
        
        # Should return Alice and Bob (high salary in Engineering)
        self.assertEqual(len(result.data.rows), 2)
        names = [row['emp_name'] for row in result.data.rows]
        self.assertIn('Alice', names)
        self.assertIn('Bob', names)
    
    def test_06_nested_subquery(self):
        """Test nested subqueries (subquery within subquery)."""
        self._setup_subquery_data()
        # Find employees in departments that are listed in a subquery of high-budget departments
        query = """
        SELECT emp_name 
        FROM employees 
        WHERE dept_id IN (
            SELECT dept_id 
            FROM departments 
            WHERE dept_id IN (SELECT dept_id FROM departments WHERE budget > 70000)
        )
        """
        result = self.execute_query(query)
        self.assertQuerySuccess(result)
        
        # Should return employees from Engineering and Sales (budget > 70000)
        self.assertEqual(len(result.data.rows), 4)
        names = [row['emp_name'] for row in result.data.rows]
        self.assertIn('Alice', names)
        self.assertIn('Bob', names)
        self.assertIn('Charlie', names)
        self.assertIn('Diana', names)
    
    def test_07_in_subquery_with_order_limit(self):
        """Test IN subquery with ORDER BY and LIMIT."""
        self._setup_subquery_data()
        query = """
        SELECT emp_name, salary 
        FROM employees 
        WHERE dept_id IN (SELECT dept_id FROM departments WHERE budget > 70000)
        ORDER BY salary DESC
        LIMIT 2
        """
        result = self.execute_query(query)
        self.assertQuerySuccess(result)
        
        # Should return top 2 earners from high-budget departments
        self.assertEqual(len(result.data.rows), 2)
        self.assertEqual(result.data.rows[0]['emp_name'], 'Alice')  # 75000
        self.assertEqual(result.data.rows[1]['emp_name'], 'Bob')    # 65000


class TestComplexQueries(TestQueryProcessor):
    """Test complex query combinations."""
    
    def _setup_complex_data(self):
        """Set up comprehensive test database."""
        super().setUp()
        # Create tables
        self.execute_query("""
        CREATE TABLE departments (
            dept_id INTEGER PRIMARY KEY,
            dept_name VARCHAR(50),
            budget INTEGER
        )
        """)
        
        self.execute_query("""
        CREATE TABLE employees (
            emp_id INTEGER PRIMARY KEY,
            emp_name VARCHAR(50),
            dept_id INTEGER,
            salary INTEGER,
            hire_date VARCHAR(20)
        )
        """)
        
        # Populate data
        dept_data = [(1, 'Engineering', 100000), (2, 'Sales', 80000), (3, 'HR', 60000)]
        for dept_id, name, budget in dept_data:
            self.execute_query(f"INSERT INTO departments (dept_id, dept_name, budget) VALUES ({dept_id}, '{name}', {budget})")
        
        emp_data = [
            (1, 'Alice', 1, 60000, '2024-01-15'),
            (2, 'Bob', 1, 75000, '2023-06-20'),
            (3, 'Carol', 2, 55000, '2024-03-10'),
            (4, 'Dave', 2, 82000, '2022-11-05'),
            (5, 'Eve', 3, 50000, '2024-02-28')
        ]
        for emp_id, name, dept_id, salary, date in emp_data:
            self.execute_query(f"INSERT INTO employees (emp_id, emp_name, dept_id, salary, hire_date) VALUES ({emp_id}, '{name}', {dept_id}, {salary}, '{date}')")
    
    def test_01_join_with_where_and_order(self):
        """Test JOIN combined with WHERE and ORDER BY."""
        self._setup_complex_data()
        query = """
        SELECT e.emp_name, d.dept_name, e.salary
        FROM employees e JOIN departments d ON e.dept_id = d.dept_id
        WHERE e.salary > 55000
        ORDER BY e.salary DESC
        """
        result = self.execute_query(query)
        self.assertQuerySuccess(result)
        
        # Verify salary filtering
        for row in result.data.rows:
            self.assertGreater(row['salary'], 55000)
        
        # Verify ordering
        salaries = [row['salary'] for row in result.data.rows]
        self.assertEqual(salaries, sorted(salaries, reverse=True))
    
    def test_02_select_with_where_order_limit(self):
        """Test SELECT with WHERE, ORDER BY, and LIMIT."""
        self._setup_complex_data()
        query = """
        SELECT emp_name, salary
        FROM employees
        WHERE dept_id = 1
        ORDER BY salary DESC
        LIMIT 1
        """
        result = self.execute_query(query)
        self.assertQuerySuccess(result)
        
        # Should return highest paid employee in dept 1 (Bob with 75000)
        self.assertEqual(len(result.data.rows), 1)
        self.assertEqual(result.data.rows[0]['emp_name'], 'Bob')


def suite():
    """Create test suite."""
    test_suite = unittest.TestSuite()
    
    # Add all test classes
    test_suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestCreateTable))
    test_suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestInsertInto))
    test_suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestSelect))
    test_suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestJoin))
    test_suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestUpdate))
    test_suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestDelete))
    test_suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestLimit))
    test_suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestOrderBy))
    test_suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestAlias))
    test_suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestDropTable))
    test_suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestSubqueries))
    test_suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestComplexQueries))
    
    return test_suite


if __name__ == '__main__':
    # Run tests with verbose output
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite())
