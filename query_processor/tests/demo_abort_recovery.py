"""
Demo: Transaction Abort Recovery After Checkpoint

This demonstrates that when a transaction aborts after checkpoint has flushed
operations to storage, the FRM properly recovers (undoes) those operations.

Key Points:
- Checkpoint flushes buffer operations to storage
- ABORT must undo both: buffered operations + already-flushed operations
- FRM's recover_transaction() handles the flushed operations
"""

import sys
import os
import shutil

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from query_processor.query_processor import QueryProcessor
from storage_manager.storage_manager import StorageManager
from failure_recovery_manager.failure_recovery_manager import FailureRecovery


def main():
    print("\n" + "="*80)
    print("DEMO: Transaction Abort Recovery After Checkpoint")
    print("="*80 + "\n")
    
    # Setup
    test_dir = "demo_abort_data"
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)
    os.makedirs(test_dir)
    
    # Reset singletons
    StorageManager._instance = None
    StorageManager._initialized = False
    QueryProcessor._instance = None
    FailureRecovery._instance = None
    
    # Create instances with LOW wal_size
    sm = StorageManager(data_dir=test_dir)
    qp = QueryProcessor()
    qp.adapter_frm.frm.wal_size = 3  # Very low threshold for demo
    
    # Connect storage
    qp.adapter_storage.sm = sm
    qp.query_execution_engine.storage_manager = sm
    qp.query_execution_engine.storage_adapter.sm = sm
    
    client_id = 1
    
    # Create table
    print("[1] Creating table...")
    qp.execute_query("CREATE TABLE products (id INTEGER PRIMARY KEY, name VARCHAR(50), price INTEGER)", client_id)
    print("    Table 'products' created\n")
    
    # Begin transaction
    print("[2] Starting transaction...")
    result = qp.execute_query("BEGIN TRANSACTION", client_id)
    t_id = result.transaction_id
    print(f"    Transaction {t_id} started")
    print(f"    WAL threshold: {qp.adapter_frm.frm.wal_size}\n")
    
    # Insert multiple rows to trigger checkpoints
    print("[3] Inserting products (will trigger checkpoints)...")
    products = [
        (1, "Laptop", 15000),
        (2, "Mouse", 500),
        (3, "Keyboard", 1200),
        (4, "Monitor", 3000),
        (5, "Headset", 800),
        (6, "Webcam", 1500),
        (7, "Cable", 200),
    ]
    
    for prod_id, name, price in products:
        qp.execute_query(f"INSERT INTO products (id, name, price) VALUES ({prod_id}, '{name}', {price})", client_id)
        wal_size = len(qp.adapter_frm.frm.mem_wal)
        print(f"    Inserted: {name:12s} (price: {price:5d}) | WAL size: {wal_size}")
    
    # Check checkpoints
    logs = qp.adapter_frm.frm.logFile.get_logs()
    checkpoints = sum(1 for log in logs if log.action == 4)
    print(f"\n[4] Checkpoints created: {checkpoints}")
    print(f"    Total logs: {len(logs)}")
    
    # Check storage
    rows_in_storage = len(sm.tables.get('products', {}).get('data', []))
    print(f"    Rows flushed to storage: {rows_in_storage}")
    print(f"    Rows in buffer: {len(qp.query_execution_engine.transaction_buffer.get_buffered_operations(t_id))}\n")
    
    # ABORT!
    print("[5] ABORTING transaction...")
    print(f"    This will:")
    print(f"      - Discard buffered operations (not yet in storage)")
    print(f"      - Undo flushed operations (already in storage via checkpoint)")
    qp.execute_query("ABORT", client_id)
    print(f"    Transaction {t_id} aborted\n")
    
    # Verify rollback
    print("[6] Verifying rollback...")
    result = qp.execute_query("SELECT * FROM products", client_id)
    print(f"    Rows in table after abort: {len(result.data.rows)}")
    
    if len(result.data.rows) == 0:
        print("\n" + "="*80)
        print("SUCCESS! All operations rolled back correctly!")
        print("="*80)
        print("\nWhat happened:")
        print(f"  - {len(products)} rows were inserted")
        print(f"  - {checkpoints} checkpoint(s) flushed {rows_in_storage} row(s) to storage")
        print(f"  - ABORT triggered FRM recovery")
        print(f"  - Recovery undid all {rows_in_storage} flushed row(s)")
        print(f"  - Query processor discarded remaining buffered row(s)")
        print(f"  - Result: Table is empty (complete rollback)")
        print("="*80 + "\n")
    else:
        print(f"\n!!! FAILED: {len(result.data.rows)} rows still in table\n")
    
    # Cleanup
    shutil.rmtree(test_dir)


if __name__ == '__main__':
    main()
