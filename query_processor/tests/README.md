# Query Processor Test Suite

This folder contains comprehensive tests for the query processor.

## Test Files

### Individual Test Suites

1. **test_select.py** - Basic SELECT queries
   - SELECT * (all columns)
   - SELECT specific columns
   - WHERE with simple conditions
   - WHERE with comparisons (>, <, =, etc.)
   - WHERE with AND conditions
   - LIMIT clause
   - Combined WHERE and LIMIT

2. **test_join.py** - JOIN queries
   - CROSS JOIN (cartesian product)
   - INNER JOIN with ON condition
   - JOIN with table aliases
   - JOIN with column projection
   - JOIN with WHERE filters

3. **test_orderby.py** - Sorting queries
   - ORDER BY ASC/DESC
   - ORDER BY with different columns
   - ORDER BY with WHERE
   - ORDER BY with LIMIT
   - Combined ORDER BY, WHERE, and LIMIT

4. **test_dml.py** - Data manipulation
   - INSERT INTO with VALUES
   - UPDATE with WHERE
   - UPDATE with expressions
   - DELETE with WHERE
   - Verification queries for each operation

### Running Tests

#### Run individual test suite:
```bash
cd query_processor/tests
py test_select.py
py test_join.py
py test_orderby.py
py test_dml.py
```

#### Run all tests at once:
```bash
cd query_processor/tests
py run_all_tests.py
```

## Features

- ✅ Uses real storage manager (not mocks)
- ✅ Uses SQL parser to convert queries to trees
- ✅ Clean test data for each suite (isolated)
- ✅ Clear output with test results
- ✅ Transaction-aware execution
- ✅ Automatic cleanup of old test data

## Test Data

Each test suite creates its own isolated data directory:
- `data/test_select/` - Products table
- `data/test_join/` - Customers and Orders tables
- `data/test_orderby/` - Students table
- `data/test_dml/` - Inventory table

Test data is automatically cleaned up before each run to ensure consistent results.
