"""
Test script for System Crash Recovery

This script demonstrates the crash recovery mechanism:
1. Start transactions and perform operations
2. Simulate a crash (abrupt exit without COMMIT/ABORT)
3. Restart system and verify recovery

Run this test in two phases:
Phase 1: python test_crash_recovery.py setup
Phase 2: python test_crash_recovery.py recover
"""

import sys
import os
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(name)s: %(message)s"
)

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from query_processor.query_processor import QueryProcessor
from query_processor.adapter_ccm import AlgorithmType

def cleanup():
    """Clean up test data"""
    import shutil
    parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
    test_dir = os.path.join(parent_dir, 'data')
    log_dir = os.path.join(parent_dir, 'failure_recovery_manager', 'log')
    
    # Keep data directory but clean specific test tables
    if os.path.exists(test_dir):
        test_files = ['crash_test_users.dat', 'crash_test_orders.dat']
        for f in test_files:
            fpath = os.path.join(test_dir, f)
            if os.path.exists(fpath):
                os.remove(fpath)
                print(f"Removed {f}")

def phase1_setup_and_crash():
    """
    Phase 1: Setup tables, start transactions, insert data, then crash without commit
    """
    print("=" * 70)
    print("PHASE 1: Setup and Simulate Crash")
    print("=" * 70)
    
    processor = QueryProcessor()
    
    # Create test tables
    print("\n[1] Creating test tables...")
    result = processor.execute_query("""
        CREATE TABLE crash_test_users (
            user_id INTEGER PRIMARY KEY,
            username VARCHAR(50),
            balance INTEGER
        )
    """, client_id=1)
    print(f"   Result: {result.message}")
    
    result = processor.execute_query("""
        CREATE TABLE crash_test_orders (
            order_id INTEGER PRIMARY KEY,
            user_id INTEGER,
            amount INTEGER
        )
    """, client_id=1)
    print(f"   Result: {result.message}")
    
    # Transaction 1: Committed (should persist after crash)
    print("\n[2] Transaction 1 (COMMITTED - should survive crash):")
    result = processor.execute_query("BEGIN TRANSACTION", client_id=1)
    print(f"   {result.message}")
    
    result = processor.execute_query(
        "INSERT INTO crash_test_users (user_id, username, balance) VALUES (1, 'Alice', 1000)",
        client_id=1
    )
    print(f"   Inserted Alice: {result.message}")
    
    result = processor.execute_query(
        "INSERT INTO crash_test_users (user_id, username, balance) VALUES (2, 'Bob', 2000)",
        client_id=1
    )
    print(f"   Inserted Bob: {result.message}")
    
    result = processor.execute_query("COMMIT", client_id=1)
    print(f"   {result.message}")
    
    # Transaction 2: NOT committed (should be undone after crash)
    print("\n[3] Transaction 2 (NOT COMMITTED - should be undone):")
    result = processor.execute_query("BEGIN TRANSACTION", client_id=2)
    print(f"   {result.message}")
    
    result = processor.execute_query(
        "INSERT INTO crash_test_users (user_id, username, balance) VALUES (3, 'Charlie', 3000)",
        client_id=2
    )
    print(f"   Inserted Charlie: {result.message}")
    
    result = processor.execute_query(
        "INSERT INTO crash_test_orders (order_id, user_id, amount) VALUES (1, 3, 500)",
        client_id=2
    )
    print(f"   Inserted order: {result.message}")
    
    # Transaction 3: NOT committed (should be undone after crash)
    print("\n[4] Transaction 3 (NOT COMMITTED - should be undone):")
    result = processor.execute_query("BEGIN TRANSACTION", client_id=3)
    print(f"   {result.message}")
    
    result = processor.execute_query(
        "INSERT INTO crash_test_users (user_id, username, balance) VALUES (4, 'Dave', 4000)",
        client_id=3
    )
    print(f"   Inserted Dave: {result.message}")
    
    # Check what's in buffer before crash
    print("\n[5] Current state (before crash):")
    result = processor.execute_query("SELECT * FROM crash_test_users", client_id=10)
    if result.success and result.data:
        print(f"   Users in storage: {len(result.data.rows)} rows")
        for row in result.data.rows:
            print(f"     - {row}")
    
    # Show transaction buffer state
    print("\n[6] Transaction buffer state:")
    for tid, ops in processor.query_execution_engine.transaction_buffer.buffers.items():
        print(f"   Transaction {tid}: {len(ops)} buffered operation(s)")
        for op in ops:
            print(f"     - {op.operation_type} on {op.table_name}")
    
    print("\n" + "=" * 70)
    print("SIMULATING CRASH...")
    print("Exiting without COMMIT for transactions 2 and 3")
    print("=" * 70)
    
    # Force flush FRM logs to disk before crash
    processor.adapter_frm._flush_to_disk()
    print("\n[INFO] WAL flushed to disk before crash")
    
    # Exit without committing - this simulates a crash!
    sys.exit(0)

def phase2_recover():
    """
    Phase 2: Restart system and let crash recovery mechanism work
    """
    print("=" * 70)
    print("PHASE 2: System Restart and Recovery")
    print("=" * 70)
    
    print("\n[1] Initializing QueryProcessor (will trigger recovery)...")
    processor = QueryProcessor()
    
    print("\n[2] Recovery completed! Checking final state...")
    
    # Query the data after recovery
    result = processor.execute_query("SELECT * FROM crash_test_users", client_id=1)
    
    print("\n[3] Final state of crash_test_users:")
    if result.success and result.data:
        print(f"   Total rows: {len(result.data.rows)}")
        for row in result.data.rows:
            print(f"     - User {row['user_id']}: {row['username']} (balance: {row['balance']})")
        
        # Verify expectations
        print("\n[4] Verification:")
        user_ids = [row['user_id'] for row in result.data.rows]
        
        if 1 in user_ids and 2 in user_ids:
            print("   ✓ Transaction 1 (COMMITTED): Alice and Bob present")
        else:
            print("   ✗ Transaction 1 (COMMITTED): Missing Alice or Bob!")
        
        if 3 not in user_ids:
            print("   ✓ Transaction 2 (UNCOMMITTED): Charlie was undone")
        else:
            print("   ✗ Transaction 2 (UNCOMMITTED): Charlie should have been undone!")
        
        if 4 not in user_ids:
            print("   ✓ Transaction 3 (UNCOMMITTED): Dave was undone")
        else:
            print("   ✗ Transaction 3 (UNCOMMITTED): Dave should have been undone!")
    else:
        print(f"   Error querying data: {result.message}")
    
    # Check orders table
    result = processor.execute_query("SELECT * FROM crash_test_orders", client_id=1)
    print("\n[5] Final state of crash_test_orders:")
    if result.success and result.data:
        print(f"   Total rows: {len(result.data.rows)}")
        if len(result.data.rows) == 0:
            print("   ✓ Order from uncommitted transaction was undone")
        else:
            print("   ✗ Orders should be empty!")
            for row in result.data.rows:
                print(f"     - Order {row['order_id']}: User {row['user_id']}, Amount {row['amount']}")
    
    print("\n" + "=" * 70)
    print("Recovery test completed!")
    print("=" * 70)

def main():
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python test_crash_recovery.py setup    # Phase 1: Setup and simulate crash")
        print("  python test_crash_recovery.py recover  # Phase 2: Recover from crash")
        print("  python test_crash_recovery.py cleanup  # Clean up test data")
        sys.exit(1)
    
    command = sys.argv[1].lower()
    
    if command == "setup":
        phase1_setup_and_crash()
    elif command == "recover":
        phase2_recover()
    elif command == "cleanup":
        cleanup()
        print("Cleanup completed!")
    else:
        print(f"Unknown command: {command}")
        print("Use 'setup', 'recover', or 'cleanup'")
        sys.exit(1)

if __name__ == "__main__":
    main()
