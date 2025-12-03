"""
Test batch update functionality during COMMIT.

This test verifies that:
1. Multiple UPDATEs to the same row in a transaction are handled correctly
2. The final committed state reflects all updates applied sequentially
3. The batch_update_data method works correctly with old/new data matching
"""

import sys
import os
import io

# Fix Unicode encoding issues on Windows
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from query_processor.query_processor import QueryProcessor
from storage_manager.storage_manager import StorageManager
from storage_manager.models import ColumnDefinition


def setup_test_table():
    """Create a test table with sample data"""
    sm = StorageManager(data_dir="query_processor/tests/data/test_batch_update")
    
    # Drop table if exists
    try:
        sm.drop_table("users")
    except:
        pass
    
    # Create table
    columns = [
        ColumnDefinition(name="id", data_type="INTEGER", is_primary_key=True),
        ColumnDefinition(name="name", data_type="VARCHAR", size=50),
        ColumnDefinition(name="score", data_type="INTEGER"),
        ColumnDefinition(name="status", data_type="VARCHAR", size=20)
    ]
    sm.create_table("users", columns, primary_keys=["id"])
    
    # Insert test data
    test_rows = [
        {"id": 1, "name": "Alice", "score": 100, "status": "active"},
        {"id": 2, "name": "Bob", "score": 200, "status": "active"},
        {"id": 3, "name": "Charlie", "score": 300, "status": "inactive"}
    ]
    sm.insert_rows("users", test_rows)
    
    print("OK Test table 'users' created with 3 rows")
    return sm


def test_multiple_updates_same_row():
    """Test multiple UPDATEs to the same row within a transaction"""
    print("\n" + "="*70)
    print("TEST 1: Multiple UPDATEs to same row in one transaction")
    print("="*70)
    
    sm = setup_test_table()
    qp = QueryProcessor()
    
    # Override storage manager in query processor
    qp.adapter_storage.sm = sm
    qp.query_execution_engine.storage_adapter.sm = sm
    qp.query_execution_engine.storage_manager = sm
    
    client_id = 1
    
    # BEGIN transaction
    result = qp.execute_query("BEGIN TRANSACTION;", client_id)
    print(f"\n{result.message}")
    t_id = result.transaction_id
    
    # Update 1: score 100 -> 150
    print("\n--- UPDATE 1: score 100 -> 150 ---")
    result = qp.execute_query("UPDATE users SET score = 150 WHERE id = 1;", client_id)
    print(f"{result.message}")
    
    # Update 2: score 150 -> 200
    print("\n--- UPDATE 2: score 150 -> 200 ---")
    result = qp.execute_query("UPDATE users SET score = 200 WHERE id = 1;", client_id)
    print(f"{result.message}")
    
    # Update 3: status active -> premium
    print("\n--- UPDATE 3: status active -> premium ---")
    result = qp.execute_query("UPDATE users SET status = 'premium' WHERE id = 1;", client_id)
    print(f"{result.message}")
    
    # SELECT before COMMIT (should see buffered changes)
    print("\n--- SELECT before COMMIT (should see all buffered updates) ---")
    result = qp.execute_query("SELECT * FROM users WHERE id = 1;", client_id)
    if result.data.rows:
        row = result.data.rows[0]
        print(f"  Row: {row}")
        assert row["score"] == 200, f"Expected score=200, got {row['score']}"
        assert row["status"] == "premium", f"Expected status='premium', got {row['status']}"
        print("  OK Buffered updates visible to transaction")
    
    # COMMIT
    print("\n--- COMMIT (batch update should execute) ---")
    result = qp.execute_query("COMMIT;", client_id)
    print(f"{result.message}")
    
    # Verify final state in storage
    print("\n--- Verify final state in storage ---")
    from storage_manager.models import DataRetrieval
    final_data = sm.read_block(DataRetrieval(table="users", conditions=[]))
    alice = next((r for r in final_data if r["id"] == 1), None)
    
    print(f"  Final row: {alice}")
    assert alice is not None, "Row with id=1 not found"
    assert alice["score"] == 200, f"Expected final score=200, got {alice['score']}"
    assert alice["status"] == "premium", f"Expected final status='premium', got {alice['status']}"
    
    print("\nOK TEST PASSED: Multiple updates correctly applied in batch")
    return True


def test_multiple_rows_updated():
    """Test updating multiple different rows in one transaction"""
    print("\n" + "="*70)
    print("TEST 2: Update multiple different rows in one transaction")
    print("="*70)
    
    sm = setup_test_table()
    qp = QueryProcessor()
    
    # Override storage manager
    qp.adapter_storage.sm = sm
    qp.query_execution_engine.storage_adapter.sm = sm
    qp.query_execution_engine.storage_manager = sm
    
    client_id = 2
    
    # BEGIN transaction
    result = qp.execute_query("BEGIN TRANSACTION;", client_id)
    print(f"\n{result.message}")
    
    # Update row 1
    print("\n--- UPDATE row 1: score -> 111 ---")
    qp.execute_query("UPDATE users SET score = 111 WHERE id = 1;", client_id)
    
    # Update row 2
    print("\n--- UPDATE row 2: score -> 222 ---")
    qp.execute_query("UPDATE users SET score = 222 WHERE id = 2;", client_id)
    
    # Update row 3
    print("\n--- UPDATE row 3: score -> 333 ---")
    qp.execute_query("UPDATE users SET score = 333 WHERE id = 3;", client_id)
    
    # COMMIT
    print("\n--- COMMIT (batch update all 3 rows) ---")
    result = qp.execute_query("COMMIT;", client_id)
    print(f"{result.message}")
    
    # Verify all rows updated
    print("\n--- Verify all rows updated ---")
    from storage_manager.models import DataRetrieval
    final_data = sm.read_block(DataRetrieval(table="users", conditions=[]))
    
    for row in final_data:
        expected_score = row["id"] * 111
        print(f"  Row {row['id']}: score={row['score']} (expected {expected_score})")
        assert row["score"] == expected_score, f"Row {row['id']} score mismatch"
    
    print("\nOK TEST PASSED: Multiple rows updated correctly in batch")
    return True


def test_auto_commit_update():
    """Test UPDATE in auto-commit mode (no explicit transaction)"""
    print("\n" + "="*70)
    print("TEST 3: UPDATE in auto-commit mode")
    print("="*70)
    
    sm = setup_test_table()
    qp = QueryProcessor()
    
    # Override storage manager
    qp.adapter_storage.sm = sm
    qp.query_execution_engine.storage_adapter.sm = sm
    qp.query_execution_engine.storage_manager = sm
    
    client_id = 3
    
    # Execute UPDATE without transaction (auto-commit)
    print("\n--- UPDATE without BEGIN TRANSACTION (auto-commit) ---")
    result = qp.execute_query("UPDATE users SET status = 'verified' WHERE id = 2;", client_id)
    print(f"{result.message}")
    
    # Verify immediately committed
    print("\n--- Verify immediately committed ---")
    from storage_manager.models import DataRetrieval, Condition
    final_data = sm.read_block(DataRetrieval(
        table="users", 
        conditions=[Condition(column="id", operation="=", operand=2)]
    ))
    
    bob = final_data[0] if final_data else None
    print(f"  Row: {bob}")
    assert bob is not None, "Row with id=2 not found"
    assert bob["status"] == "verified", f"Expected status='verified', got {bob['status']}"
    
    print("\nOK TEST PASSED: Auto-commit UPDATE works correctly")
    return True


def test_update_with_expression():
    """Test UPDATE with expression (SET score = score + 50)"""
    print("\n" + "="*70)
    print("TEST 4: UPDATE with expression (score = score + 50)")
    print("="*70)
    
    sm = setup_test_table()
    qp = QueryProcessor()
    
    # Override storage manager
    qp.adapter_storage.sm = sm
    qp.query_execution_engine.storage_adapter.sm = sm
    qp.query_execution_engine.storage_manager = sm
    
    client_id = 4
    
    # BEGIN transaction
    result = qp.execute_query("BEGIN TRANSACTION;", client_id)
    print(f"\n{result.message}")
    
    # Update with expression (twice)
    print("\n--- UPDATE 1: score = score + 50 ---")
    qp.execute_query("UPDATE users SET score = score + 50 WHERE id = 1;", client_id)
    
    print("\n--- UPDATE 2: score = score + 50 (again) ---")
    qp.execute_query("UPDATE users SET score = score + 50 WHERE id = 1;", client_id)
    
    # COMMIT
    print("\n--- COMMIT ---")
    result = qp.execute_query("COMMIT;", client_id)
    print(f"{result.message}")
    
    # Verify: original 100 + 50 + 50 = 200
    print("\n--- Verify final score ---")
    from storage_manager.models import DataRetrieval, Condition
    final_data = sm.read_block(DataRetrieval(
        table="users",
        conditions=[Condition(column="id", operation="=", operand=1)]
    ))
    
    alice = final_data[0] if final_data else None
    print(f"  Final row: {alice}")
    assert alice is not None, "Row with id=1 not found"
    assert alice["score"] == 200, f"Expected score=200 (100+50+50), got {alice['score']}"
    
    print("\nOK TEST PASSED: Expression-based UPDATEs applied correctly")
    return True


def main():
    """Run all batch update tests"""
    print("\n" + "="*70)
    print("BATCH UPDATE TEST SUITE")
    print("="*70)
    
    tests = [
        test_multiple_updates_same_row,
        test_multiple_rows_updated,
        test_auto_commit_update,
        test_update_with_expression
    ]
    
    passed = 0
    failed = 0
    
    for test_func in tests:
        try:
            if test_func():
                passed += 1
        except Exception as e:
            print(f"\nX TEST FAILED: {e}")
            import traceback
            traceback.print_exc()
            failed += 1
    
    print("\n" + "="*70)
    print(f"TEST RESULTS: {passed} passed, {failed} failed")
    print("="*70)
    
    return failed == 0


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
