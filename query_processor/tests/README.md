# Query Processor Test Suite

Comprehensive unit tests for the Query Processor module covering all major SQL operations.

## Test Coverage

### 1. CREATE TABLE Tests (`TestCreateTable`)
Tests table creation with various column types and constraints:
- Simple table creation with INTEGER and VARCHAR columns
- Tables with PRIMARY KEY constraints
- Tables with FOREIGN KEY constraints
- Multiple related tables

**Example Queries:**
```sql
CREATE TABLE departments (
    dept_id INTEGER PRIMARY KEY,
    dept_name VARCHAR(50),
    budget INTEGER
)
```

### 2. INSERT INTO Tests (`TestInsertInto`)
Tests data insertion with hardcoded values:
- Simple INSERT with all columns
- INSERT multiple rows
- INSERT with partial column specification
- Comprehensive data population

**Example Queries:**
```sql
INSERT INTO departments (dept_id, dept_name, budget) VALUES (1, 'Engineering', 100000)
INSERT INTO employees (emp_id, emp_name, dept_id, salary, hire_date) 
    VALUES (1, 'Alice', 1, 60000, '2024-01-15')
```

### 3. SELECT Tests (`TestSelect`)
Tests various SELECT query patterns:
- `SELECT *` (all columns)
- SELECT with specific column projection
- SELECT with simple WHERE conditions (>, <, =)
- SELECT with complex WHERE (AND, multiple conditions)
- SELECT with equality and inequality operators

**Example Queries:**
```sql
SELECT * FROM departments
SELECT emp_id, emp_name FROM employees
SELECT * FROM employees WHERE salary > 60000
SELECT * FROM employees WHERE salary > 55000 AND dept_id = 2
```

### 4. JOIN Tests (`TestJoin`)
Tests JOIN operations between tables:
- INNER JOIN with ON condition
- NATURAL JOIN
- JOIN with column projection
- Multi-table joins

**Example Queries:**
```sql
SELECT * FROM employees JOIN departments ON employees.dept_id = departments.dept_id
SELECT * FROM employees NATURAL JOIN departments
SELECT emp_name, dept_name FROM employees e JOIN departments d ON e.dept_id = d.dept_id
```

### 5. UPDATE Tests (`TestUpdate`)
Tests UPDATE operations with both hardcoded and expression-based values:
- UPDATE with hardcoded value
- UPDATE with arithmetic expression (`salary = salary + 5000`)
- UPDATE affecting multiple rows
- UPDATE with complex arithmetic (`salary = salary * 2`)

**Example Queries:**
```sql
UPDATE employees SET salary = 65000 WHERE emp_id = 1
UPDATE employees SET salary = salary + 5000 WHERE dept_id = 1
UPDATE employees SET salary = salary * 2 WHERE emp_id = 3
```

### 6. DELETE Tests (`TestDelete`)
Tests DELETE operations with single conditions:
- DELETE with numeric condition
- DELETE with string condition
- DELETE affecting multiple rows

**Example Queries:**
```sql
DELETE FROM tasks WHERE task_id = 1
DELETE FROM tasks WHERE status = 'completed'
DELETE FROM tasks WHERE project_id = 2
```

### 7. LIMIT Tests (`TestLimit`)
Tests result limiting functionality:
- Simple LIMIT clause
- LIMIT with WHERE clause
- LIMIT larger than result set

**Example Queries:**
```sql
SELECT * FROM employees LIMIT 5
SELECT * FROM employees WHERE salary > 52000 LIMIT 3
SELECT * FROM employees WHERE dept_id = 1 LIMIT 100
```

### 8. ORDER BY Tests (`TestOrderBy`)
Tests sorting functionality:
- ORDER BY ASC (ascending)
- ORDER BY DESC (descending)
- ORDER BY with WHERE clause
- ORDER BY on string columns

**Example Queries:**
```sql
SELECT * FROM employees ORDER BY salary ASC
SELECT * FROM employees ORDER BY salary DESC
SELECT * FROM employees WHERE dept_id = 1 ORDER BY salary DESC
SELECT * FROM employees ORDER BY emp_name ASC
```

### 9. ALIAS Tests (`TestAlias`)
Tests table and column aliasing:
- Table aliases in JOIN operations
- Column aliases (implicit through projection)

**Example Queries:**
```sql
SELECT e.emp_name, d.dept_name FROM employees e JOIN departments d ON e.dept_id = d.dept_id
```

### 10. DROP TABLE Tests (`TestDropTable`)
Tests table deletion:
- Drop simple table without dependencies
- Drop table containing data
- Foreign key handling (RESTRICT/CASCADE)

**Example Queries:**
```sql
DROP TABLE temp_table
DROP TABLE temp_data
```

### 11. Subquery Tests (`TestSubqueries`)
Tests subquery operations for filtering and existence checking:
- IN with subquery
- NOT IN with subquery
- EXISTS with non-correlated subquery
- NOT EXISTS with non-correlated subquery
- IN with additional WHERE conditions
- Nested subqueries (subquery within subquery)
- IN with ORDER BY and LIMIT

**Example Queries:**
```sql
-- IN subquery
SELECT emp_name, salary 
FROM employees 
WHERE dept_id IN (SELECT dept_id FROM departments WHERE budget > 70000)

-- NOT IN subquery
SELECT emp_name, salary 
FROM employees 
WHERE dept_id NOT IN (SELECT dept_id FROM departments WHERE budget > 70000)

-- EXISTS subquery
SELECT dept_name, budget 
FROM departments 
WHERE EXISTS (SELECT * FROM departments WHERE budget > 90000)

-- Nested subquery
SELECT emp_name 
FROM employees 
WHERE dept_id IN (
    SELECT dept_id 
    FROM departments 
    WHERE dept_id IN (SELECT dept_id FROM departments WHERE budget > 70000)
)

-- IN with ORDER BY and LIMIT
SELECT emp_name, salary 
FROM employees 
WHERE dept_id IN (SELECT dept_id FROM departments WHERE budget > 70000)
ORDER BY salary DESC
LIMIT 2
```

### 12. Complex Queries (`TestComplexQueries`)
Tests combinations of multiple SQL features:
- JOIN + WHERE + ORDER BY
- SELECT + WHERE + ORDER BY + LIMIT

**Example Queries:**
```sql
SELECT e.emp_name, d.dept_name, e.salary
FROM employees e JOIN departments d ON e.dept_id = d.dept_id
WHERE e.salary > 55000
ORDER BY e.salary DESC

SELECT emp_name, salary
FROM employees
WHERE dept_id = 1
ORDER BY salary DESC
LIMIT 1
```

## Test Database Schema

The tests use the following database schema:

```
departments
├── dept_id (INTEGER, PRIMARY KEY)
├── dept_name (VARCHAR(50))
└── budget (INTEGER)

employees
├── emp_id (INTEGER, PRIMARY KEY)
├── emp_name (VARCHAR(50))
├── dept_id (INTEGER, FOREIGN KEY -> departments.dept_id)
├── salary (INTEGER)
└── hire_date (VARCHAR(20))

projects
├── project_id (INTEGER, PRIMARY KEY)
├── project_name (VARCHAR(50))
├── dept_id (INTEGER, FOREIGN KEY -> departments.dept_id)
└── budget (INTEGER)

tasks
├── task_id (INTEGER, PRIMARY KEY)
├── task_name (VARCHAR(50))
├── project_id (INTEGER)
└── status (VARCHAR(20))
```

## Sample Test Data

### Departments
| dept_id | dept_name   | budget |
|---------|-------------|--------|
| 1       | Engineering | 100000 |
| 2       | Sales       | 80000  |
| 3       | HR          | 60000  |

### Employees
| emp_id | emp_name | dept_id | salary | hire_date  |
|--------|----------|---------|--------|------------|
| 1      | Alice    | 1       | 60000  | 2024-01-15 |
| 2      | Bob      | 1       | 75000  | 2023-06-20 |
| 3      | Carol    | 2       | 55000  | 2024-03-10 |
| 4      | Dave     | 2       | 82000  | 2022-11-05 |
| 5      | Eve      | 3       | 50000  | 2024-02-28 |

## Running Tests

### Run all tests:
```bash
# Using unittest
python -m unittest query_processor.tests.test_query_processor

# Using pytest (if installed)
pytest query_processor/tests/test_query_processor.py -v
```

### Run specific test class:
```bash
python -m unittest query_processor.tests.test_query_processor.TestSelect
python -m unittest query_processor.tests.test_query_processor.TestJoin
```

### Run specific test method:
```bash
python -m unittest query_processor.tests.test_query_processor.TestSelect.test_01_select_all_columns
```

### Run with verbose output:
```bash
python -m unittest query_processor.tests.test_query_processor -v
```

## Test Structure

Each test class follows this pattern:

1. **`setUpClass()`**: Initialize test environment once (create storage manager, query processor)
2. **`setUp()`**: Set up test-specific tables and data before each test
3. **`test_XX_description()`**: Individual test methods (numbered for execution order)
4. **`tearDownClass()`**: Clean up test data directory

## Assertion Helpers

- `assertQuerySuccess(result)`: Verify query executed successfully
- `assertQueryFails(result)`: Verify query failed as expected
- Standard unittest assertions: `assertEqual`, `assertIn`, `assertGreater`, etc.

## Notes

- Tests use auto-commit mode (no explicit transactions)
- Test data is isolated in `data/test_query_processor/` directory
- Each test class creates its own tables to ensure isolation
- Tests are numbered to suggest execution order but can run independently
- Foreign key CASCADE/RESTRICT behavior depends on storage manager implementation

## Contributing

When adding new tests:
1. Create a new test class inheriting from `TestQueryProcessor`
2. Implement `setUp()` to create necessary tables and data
3. Name test methods with `test_XX_description` pattern
4. Use assertion helpers for consistent error messages
5. Document the SQL features being tested
