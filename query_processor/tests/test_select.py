"""
Test suite for basic SELECT queries
Tests: SELECT *, SELECT columns, WHERE conditions, LIMIT
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
    data_dir = os.path.join(os.path.dirname(__file__), 'data', 'test_select')
    
    # Clean up old data
    import shutil
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
    os.makedirs(data_dir, exist_ok=True)
    
    storage_manager = StorageManager(data_dir)
    
    # Create products table
    storage_manager.create_table(
        table_name='products',
        columns=[
            ColumnDefinition(name='id', data_type='INTEGER', is_primary_key=True),
            ColumnDefinition(name='name', data_type='VARCHAR', size=100),
            ColumnDefinition(name='category', data_type='VARCHAR', size=50),
            ColumnDefinition(name='price', data_type='INTEGER'),
            ColumnDefinition(name='stock', data_type='INTEGER'),
        ],
        primary_keys=['id']
    )
    
    # Insert test data
    products = [
        (1, 'Laptop', 'Electronics', 15000, 10),
        (2, 'Mouse', 'Electronics', 250, 50),
        (3, 'Keyboard', 'Electronics', 500, 30),
        (4, 'Desk', 'Furniture', 2000, 15),
        (5, 'Chair', 'Furniture', 1500, 20),
        (6, 'Monitor', 'Electronics', 3000, 12),
        (7, 'Book', 'Stationery', 150, 100),
        (8, 'Pen', 'Stationery', 20, 500),
    ]
    
    for prod in products:
        data_write = DataWrite(
            table='products',
            column=['id', 'name', 'category', 'price', 'stock'],
            new_value=list(prod),
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
    print("SELECT QUERIES TEST SUITE")
    print("="*70)
    
    storage_manager = setup_test_data()
    results = []
    
    # Test 1: SELECT all columns
    results.append(run_query(
        storage_manager,
        "SELECT * FROM products",
        "SELECT * - All columns, all rows"
    ))
    
    # Test 2: SELECT specific columns
    results.append(run_query(
        storage_manager,
        "SELECT name, price FROM products",
        "SELECT specific columns"
    ))
    
    # Test 3: WHERE with simple condition
    results.append(run_query(
        storage_manager,
        "SELECT * FROM products WHERE category = 'Electronics'",
        "WHERE with equality condition"
    ))
    
    # Test 4: WHERE with comparison
    results.append(run_query(
        storage_manager,
        "SELECT name, price FROM products WHERE price > 1000",
        "WHERE with comparison (>)"
    ))
    
    # Test 5: WHERE with AND
    results.append(run_query(
        storage_manager,
        "SELECT * FROM products WHERE category = 'Electronics' AND price < 1000",
        "WHERE with AND condition"
    ))
    
    # Test 6: LIMIT
    results.append(run_query(
        storage_manager,
        "SELECT * FROM products LIMIT 3",
        "LIMIT clause"
    ))
    
    # Test 7: WHERE with LIMIT
    results.append(run_query(
        storage_manager,
        "SELECT name, price FROM products WHERE stock > 20 LIMIT 5",
        "WHERE with LIMIT"
    ))
    
    # Test 8: Complex WHERE with multiple AND
    results.append(run_query(
        storage_manager,
        "SELECT * FROM products WHERE price >= 500 AND price <= 2000 AND stock > 10",
        "Complex WHERE with multiple AND"
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
