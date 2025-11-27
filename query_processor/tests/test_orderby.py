"""
Test suite for ORDER BY and sorting queries
Tests: ORDER BY ASC, ORDER BY DESC, combined with WHERE and LIMIT
"""
import sys
import os

# Add parent directories to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from query_execution import QueryExecution
from adapter_optimizer import AdapterOptimizer
from storage_manager import StorageManager, ColumnDefinition, DataWrite

def setup_test_data():
    """Create test table with sample data"""
    data_dir = os.path.join(os.path.dirname(__file__), 'data', 'test_orderby')
    
    # Clean up old data
    import shutil
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
    os.makedirs(data_dir, exist_ok=True)
    
    storage_manager = StorageManager(data_dir)
    
    # Create students table
    storage_manager.create_table(
        table_name='students',
        columns=[
            ColumnDefinition(name='id', data_type='INTEGER', is_primary_key=True),
            ColumnDefinition(name='name', data_type='VARCHAR', size=100),
            ColumnDefinition(name='grade', data_type='INTEGER'),
            ColumnDefinition(name='age', data_type='INTEGER'),
        ],
        primary_keys=['id']
    )
    
    # Insert test data (unsorted)
    students = [
        (5, 'Eve', 85, 20),
        (2, 'Bob', 92, 21),
        (8, 'Helen', 78, 19),
        (1, 'Alice', 95, 22),
        (4, 'Diana', 88, 20),
        (7, 'George', 82, 21),
        (3, 'Charlie', 90, 19),
        (6, 'Frank', 87, 22),
    ]
    
    for student in students:
        data_write = DataWrite(
            table='students',
            column=['id', 'name', 'grade', 'age'],
            new_value=list(student),
            conditions=[]
        )
        storage_manager.write_block(data_write)
    
    return storage_manager

def run_query(storage_manager, sql, test_name):
    """Execute a query and display results"""
    print(f"\n{'='*70}")
    print(f"TEST: {test_name}")
    print(f"{'='*70}")
    print(f"SQL: {sql}\n")
    
    executor = QueryExecution(storage_manager=storage_manager)
    optimizer = AdapterOptimizer()
    
    try:
        parsed = optimizer.parse_optimized_query(sql)
        result = executor.execute_node(parsed.query_tree, transaction_id=1)
        
        print(f"\n{'='*70}")
        print(f"RESULT: {len(result)} rows")
        print(f"{'='*70}")
        for i, row in enumerate(result, 1):
            print(f"{i}. {row}")
        
        print(f"\n[OK] {test_name} PASSED\n")
        return True
    except Exception as e:
        print(f"\n[FAILED] {test_name}")
        print(f"Error: {e}\n")
        import traceback
        traceback.print_exc()
        return False

def main():
    print("="*70)
    print("ORDER BY QUERIES TEST SUITE")
    print("="*70)
    
    storage_manager = setup_test_data()
    results = []
    
    # Test 1: ORDER BY ASC (default)
    results.append(run_query(
        storage_manager,
        "SELECT * FROM students ORDER BY grade",
        "ORDER BY grade (ascending)"
    ))
    
    # Test 2: ORDER BY DESC
    results.append(run_query(
        storage_manager,
        "SELECT * FROM students ORDER BY grade DESC",
        "ORDER BY grade (descending)"
    ))
    
    # Test 3: ORDER BY name
    results.append(run_query(
        storage_manager,
        "SELECT name, grade FROM students ORDER BY name",
        "ORDER BY name (alphabetical)"
    ))
    
    # Test 4: ORDER BY with WHERE
    results.append(run_query(
        storage_manager,
        "SELECT * FROM students WHERE age >= 20 ORDER BY grade DESC",
        "ORDER BY with WHERE filter"
    ))
    
    # Test 5: ORDER BY with LIMIT
    results.append(run_query(
        storage_manager,
        "SELECT name, grade FROM students ORDER BY grade DESC LIMIT 3",
        "ORDER BY with LIMIT (top 3 grades)"
    ))
    
    # Test 6: ORDER BY with WHERE and LIMIT
    results.append(run_query(
        storage_manager,
        "SELECT * FROM students WHERE grade >= 85 ORDER BY grade LIMIT 4",
        "ORDER BY with WHERE and LIMIT"
    ))
    
    # Test 7: ORDER BY age
    results.append(run_query(
        storage_manager,
        "SELECT name, age, grade FROM students ORDER BY age DESC",
        "ORDER BY age (descending)"
    ))
    
    # Summary
    print("\n" + "="*70)
    print("TEST SUMMARY")
    print("="*70)
    passed = sum(results)
    total = len(results)
    print(f"Tests passed: {passed}/{total}")
    
    if passed == total:
        print("\n[OK] ALL TESTS PASSED!")
    else:
        print(f"\n[FAILED] {total - passed} test(s) failed")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
