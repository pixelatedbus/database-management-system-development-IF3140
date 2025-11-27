"""
Test suite for DML queries (INSERT, UPDATE, DELETE)
Tests: INSERT values, UPDATE with WHERE, DELETE with WHERE
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
    """Create test table with initial data"""
    data_dir = os.path.join(os.path.dirname(__file__), 'data', 'test_dml')
    
    # Clean up old data
    import shutil
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
    os.makedirs(data_dir, exist_ok=True)
    
    storage_manager = StorageManager(data_dir)
    
    # Create inventory table
    storage_manager.create_table(
        table_name='inventory',
        columns=[
            ColumnDefinition(name='id', data_type='INTEGER', is_primary_key=True),
            ColumnDefinition(name='item', data_type='VARCHAR', size=100),
            ColumnDefinition(name='quantity', data_type='INTEGER'),
            ColumnDefinition(name='price', data_type='INTEGER'),
        ],
        primary_keys=['id']
    )
    
    # Insert initial data
    items = [
        (1, 'Widget', 100, 50),
        (2, 'Gadget', 75, 120),
        (3, 'Doohickey', 50, 80),
    ]
    
    for item in items:
        data_write = DataWrite(
            table='inventory',
            column=['id', 'item', 'quantity', 'price'],
            new_value=list(item),
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
        
        if result is not None and isinstance(result, list):
            print(f"\n{'='*70}")
            print(f"RESULT: {len(result)} rows")
            print(f"{'='*70}")
            for i, row in enumerate(result, 1):
                print(f"{i}. {row}")
        else:
            print(f"\nAffected rows: {result if result else 'N/A'}")
        
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
    print("DML QUERIES TEST SUITE (INSERT, UPDATE, DELETE)")
    print("="*70)
    
    storage_manager = setup_test_data()
    results = []
    
    # Test 1: View initial data
    results.append(run_query(
        storage_manager,
        "SELECT * FROM inventory",
        "Initial data (before modifications)"
    ))
    
    # Test 2: INSERT new row
    results.append(run_query(
        storage_manager,
        "INSERT INTO inventory (id, item, quantity, price) VALUES (4, 'Thingamajig', 30, 95)",
        "INSERT new row"
    ))
    
    # Test 3: Verify INSERT
    results.append(run_query(
        storage_manager,
        "SELECT * FROM inventory WHERE id = 4",
        "Verify INSERT (SELECT new row)"
    ))
    
    # Test 4: UPDATE with WHERE
    results.append(run_query(
        storage_manager,
        "UPDATE inventory SET quantity = 150 WHERE id = 1",
        "UPDATE single row"
    ))
    
    # Test 5: Verify UPDATE
    results.append(run_query(
        storage_manager,
        "SELECT * FROM inventory WHERE id = 1",
        "Verify UPDATE (SELECT updated row)"
    ))
    
    # Test 6: UPDATE multiple rows
    results.append(run_query(
        storage_manager,
        "UPDATE inventory SET price = price + 10 WHERE quantity < 100",
        "UPDATE with expression (price increase)"
    ))
    
    # Test 7: View all after updates
    results.append(run_query(
        storage_manager,
        "SELECT * FROM inventory",
        "View all rows after updates"
    ))
    
    # Test 8: DELETE with WHERE
    results.append(run_query(
        storage_manager,
        "DELETE FROM inventory WHERE id = 2",
        "DELETE single row"
    ))
    
    # Test 9: Verify DELETE
    results.append(run_query(
        storage_manager,
        "SELECT * FROM inventory",
        "Verify DELETE (view remaining rows)"
    ))
    
    # Test 10: DELETE multiple rows
    results.append(run_query(
        storage_manager,
        "DELETE FROM inventory WHERE quantity < 50",
        "DELETE multiple rows with condition"
    ))
    
    # Test 11: Final state
    results.append(run_query(
        storage_manager,
        "SELECT * FROM inventory",
        "Final state (after all modifications)"
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
