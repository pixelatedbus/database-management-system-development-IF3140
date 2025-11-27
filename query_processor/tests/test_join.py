"""
Test suite for JOIN queries
Tests: INNER JOIN, CROSS JOIN, JOIN with aliases, JOIN with conditions
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
    """Create test tables with sample data"""
    data_dir = os.path.join(os.path.dirname(__file__), 'data', 'test_join')
    
    # Clean up old data
    import shutil
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
    os.makedirs(data_dir, exist_ok=True)
    
    storage_manager = StorageManager(data_dir)
    
    # Create customers table
    storage_manager.create_table(
        table_name='customers',
        columns=[
            ColumnDefinition(name='customer_id', data_type='INTEGER', is_primary_key=True),
            ColumnDefinition(name='name', data_type='VARCHAR', size=100),
            ColumnDefinition(name='city', data_type='VARCHAR', size=50),
        ],
        primary_keys=['customer_id']
    )
    
    # Create orders table
    storage_manager.create_table(
        table_name='orders',
        columns=[
            ColumnDefinition(name='order_id', data_type='INTEGER', is_primary_key=True),
            ColumnDefinition(name='customer_id', data_type='INTEGER'),
            ColumnDefinition(name='total', data_type='INTEGER'),
        ],
        primary_keys=['order_id']
    )
    
    # Insert customers
    customers = [
        (1, 'Alice', 'Jakarta'),
        (2, 'Bob', 'Bandung'),
        (3, 'Charlie', 'Jakarta'),
        (4, 'Diana', 'Surabaya'),
    ]
    
    for cust in customers:
        data_write = DataWrite(
            table='customers',
            column=['customer_id', 'name', 'city'],
            new_value=list(cust),
            conditions=[]
        )
        storage_manager.write_block(data_write)
    
    # Insert orders
    orders = [
        (101, 1, 5000),
        (102, 1, 3000),
        (103, 2, 7000),
        (104, 3, 2000),
        (105, 3, 4000),
    ]
    
    for order in orders:
        data_write = DataWrite(
            table='orders',
            column=['order_id', 'customer_id', 'total'],
            new_value=list(order),
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
    print("JOIN QUERIES TEST SUITE")
    print("="*70)
    
    storage_manager = setup_test_data()
    results = []
    
    # Test 1: CROSS JOIN
    results.append(run_query(
        storage_manager,
        "SELECT * FROM customers, orders",
        "CROSS JOIN (cartesian product)"
    ))
    
    # Test 2: INNER JOIN with ON
    results.append(run_query(
        storage_manager,
        "SELECT * FROM customers INNER JOIN orders ON customers.customer_id = orders.customer_id",
        "INNER JOIN with ON condition"
    ))
    
    # Test 3: JOIN with aliases
    results.append(run_query(
        storage_manager,
        "SELECT * FROM customers AS c INNER JOIN orders AS o ON c.customer_id = o.customer_id",
        "JOIN with table aliases"
    ))
    
    # Test 4: JOIN with specific columns
    results.append(run_query(
        storage_manager,
        "SELECT name, total FROM customers AS c INNER JOIN orders AS o ON c.customer_id = o.customer_id",
        "JOIN with column projection"
    ))
    
    # Test 5: JOIN with WHERE
    results.append(run_query(
        storage_manager,
        "SELECT * FROM customers AS c INNER JOIN orders AS o ON c.customer_id = o.customer_id WHERE o.total > 3000",
        "JOIN with WHERE filter"
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
