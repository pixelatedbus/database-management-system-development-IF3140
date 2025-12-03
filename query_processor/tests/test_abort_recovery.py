"""
Test Transaction Abort Recovery After Checkpoint

This test verifies that when a transaction aborts after a checkpoint has been created,
all operations that were flushed to storage during the checkpoint are properly rolled back.

Test Scenario:
1. Create a table
2. Start a transaction with LOW wal_size to trigger checkpoints frequently
3. Perform MANY INSERT operations (more than wal_size) to trigger checkpoint mid-transaction
4. Verify checkpoint was created and some data was flushed to storage
5. Abort the transaction
6. Verify ALL data inserted by the transaction is rolled back (both buffered and flushed)
7. Verify table is empty after abort
"""

import sys
import os
import unittest
import shutil

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from query_processor.query_processor import QueryProcessor
from storage_manager.storage_manager import StorageManager
from failure_recovery_manager.failure_recovery_manager import FailureRecovery


class TestAbortRecovery(unittest.TestCase):
    """Test transaction abort recovery after checkpoint"""
    
    @classmethod
    def setUpClass(cls):
        cls.test_data_dir = os.path.join(os.path.dirname(__file__), "test_abort_recovery_data")
        cls.client_id = 1
    
    def setUp(self):
        """Set up fresh environment before each test."""
        # Clean up any existing test data
        if os.path.exists(self.test_data_dir):
            shutil.rmtree(self.test_data_dir)
        
        os.makedirs(self.test_data_dir, exist_ok=True)
        
        # Reset singletons
        StorageManager._instance = None
        StorageManager._initialized = False
        QueryProcessor._instance = None
        FailureRecovery._instance = None
        
        # Create fresh instances with LOW wal_size to trigger frequent checkpoints
        self.storage_manager = StorageManager(data_dir=self.test_data_dir)
        self.query_processor = QueryProcessor()
        
        # Set LOW WAL size to trigger checkpoint after just 5 operations
        self.query_processor.adapter_frm.frm.wal_size = 5
        
        # Connect storage manager
        self.query_processor.adapter_storage.sm = self.storage_manager
        self.query_processor.query_execution_engine.storage_manager = self.storage_manager
        self.query_processor.query_execution_engine.storage_adapter.sm = self.storage_manager
    
    def tearDown(self):
        """Clean up after test."""
        if os.path.exists(self.test_data_dir):
            shutil.rmtree(self.test_data_dir)
    
    def execute_query(self, query: str):
        """Helper to execute query and return result."""
        return self.query_processor.execute_query(query, self.client_id)
    
    def test_abort_recovery_after_checkpoint(self):
        """Test that abort properly recovers data after checkpoint."""
        print("\n" + "="*80)
        print("TEST: Abort Recovery After Checkpoint")
        print("="*80)
        
        # Step 1: Create table
        print("\n[STEP 1] Creating test table...")
        create_result = self.execute_query("""
            CREATE TABLE employees (
                emp_id INTEGER PRIMARY KEY,
                emp_name VARCHAR(50),
                salary INTEGER
            )
        """)
        self.assertTrue(create_result.success, f"Table creation failed: {create_result.message}")
        print(f"[STEP 1] OK - Table created successfully")
        
        # Step 2: Begin transaction
        print("\n[STEP 2] Starting transaction...")
        begin_result = self.execute_query("BEGIN TRANSACTION")
        self.assertTrue(begin_result.success, f"BEGIN failed: {begin_result.message}")
        t_id = begin_result.transaction_id
        print(f"[STEP 2] OK - Transaction {t_id} started")
        print(f"[STEP 2]   WAL size threshold: {self.query_processor.adapter_frm.frm.wal_size}")
        
        # Step 3: Insert many rows to trigger checkpoint
        print("\n[STEP 3] Inserting rows to trigger checkpoint...")
        num_inserts = 15  # WAL size is 5, so this will trigger ~3 checkpoints
        
        for i in range(num_inserts):
            insert_result = self.execute_query(
                f"INSERT INTO employees (emp_id, emp_name, salary) VALUES ({i}, 'Employee_{i}', {50000 + i * 1000})"
            )
            self.assertTrue(insert_result.success, f"Insert {i} failed: {insert_result.message}")
            
            wal_size = len(self.query_processor.adapter_frm.frm.mem_wal)
            print(f"[STEP 3]   Insert {i:2d}: emp_id={i}, WAL size after: {wal_size}")
        
        print(f"[STEP 3] OK - Inserted {num_inserts} rows")
        
        # Step 4: Check if checkpoint was triggered
        print("\n[STEP 4] Checking for checkpoints...")
        frm = self.query_processor.adapter_frm.frm
        logs = frm.logFile.get_logs()
        
        checkpoint_count = sum(1 for log in logs if log.action == 4)  # actiontype.checkpoint = 4
        print(f"[STEP 4]   Total logs: {len(logs)}")
        print(f"[STEP 4]   Checkpoints found: {checkpoint_count}")
        
        self.assertGreater(checkpoint_count, 0, "At least one checkpoint should have been created")
        print(f"[STEP 4] OK - Checkpoint(s) created during transaction")
        
        # Step 5: ABORT the transaction
        print("\n[STEP 5] ABORTING transaction...")
        print(f"[STEP 5]   This should undo ALL {num_inserts} inserts (buffered + flushed)")
        abort_result = self.execute_query("ABORT")
        self.assertTrue(abort_result.success, f"ABORT failed: {abort_result.message}")
        print(f"[STEP 5] OK - Transaction {t_id} aborted")
        
        # Step 6: Verify ALL data was rolled back
        print("\n[STEP 6] Verifying data rollback...")
        
        # Check via SELECT (should be empty)
        select_result = self.execute_query("SELECT * FROM employees")
        self.assertTrue(select_result.success, f"SELECT failed: {select_result.message}")
        
        rows_after_abort = len(select_result.data.rows)
        print(f"[STEP 6]   Rows visible via SELECT: {rows_after_abort}")
        
        # Check storage directly
        table_data = self.storage_manager.tables.get('employees', {}).get('data', [])
        rows_in_storage = len(table_data)
        print(f"[STEP 6]   Rows in storage: {rows_in_storage}")
        
        # Verify table is completely empty
        self.assertEqual(rows_after_abort, 0, 
                        f"Table should be empty after abort, but has {rows_after_abort} rows")
        self.assertEqual(rows_in_storage, 0,
                        f"Storage should be empty after abort, but has {rows_in_storage} rows")
        
        print(f"[STEP 6] OK - All data successfully rolled back!")
        
        # Step 7: Verify abort log was written
        print("\n[STEP 7] Verifying abort log...")
        logs = frm.logFile.get_logs()
        abort_logs = [log for log in logs if log.action == 3 and log.transaction_id == t_id]  # actiontype.abort = 3
        
        self.assertGreater(len(abort_logs), 0, "Abort log should be written")
        print(f"[STEP 7]   Abort logs found: {len(abort_logs)}")
        print(f"[STEP 7] OK - Abort properly logged")
        
        print("\n" + "="*80)
        print("TEST PASSED: Abort recovery after checkpoint works correctly!")
        print("="*80 + "\n")


if __name__ == '__main__':
    # Run test
    print("\n" + "="*80)
    print("TRANSACTION ABORT RECOVERY TEST SUITE")
    print("="*80)
    print("\nThis test verifies that transaction abort properly recovers")
    print("data that was flushed to storage during checkpoint.\n")
    
    unittest.main(verbosity=2)
