import unittest
import sys
import os
from datetime import datetime

# Setup Python path - add src directory
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.dirname(current_dir)
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

# Import directly from src modules
from ..cc_manager import CCManager
from ..enums import AlgorithmType, ActionType, TransactionStatus
from ..row import Row
from ..transaction import Transaction
from .mvcc_tester import MVCCTester


class TestCCManagerBasic(unittest.TestCase):
    """Basic tests for CCManager functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.cc_manager = CCManager(AlgorithmType.LockBased, log_file="test_cc_log.txt")
    
    def tearDown(self):
        """Clean up test files"""
        if os.path.exists("test_cc_log.txt"):
            os.remove("test_cc_log.txt")
    
    def test_begin_transaction(self):
        """Test beginning a new transaction"""
        tid = self.cc_manager.begin_transaction()
        self.assertEqual(tid, 1)
        self.assertIn(tid, self.cc_manager.transactions)
        
        # Transaction should be active
        status = self.cc_manager.get_transaction_status(tid)
        self.assertEqual(status, TransactionStatus.Active)
    
    def test_multiple_transactions(self):
        """Test creating multiple transactions"""
        tid1 = self.cc_manager.begin_transaction()
        tid2 = self.cc_manager.begin_transaction()
        tid3 = self.cc_manager.begin_transaction()
        
        self.assertEqual(tid1, 1)
        self.assertEqual(tid2, 2)
        self.assertEqual(tid3, 3)
        
        # All should be active
        self.assertEqual(self.cc_manager.get_transaction_status(tid1), TransactionStatus.Active)
        self.assertEqual(self.cc_manager.get_transaction_status(tid2), TransactionStatus.Active)
        self.assertEqual(self.cc_manager.get_transaction_status(tid3), TransactionStatus.Active)
    
    def test_log_object(self):
        """Test logging object access"""
        tid = self.cc_manager.begin_transaction()
        row = Row(object_id="X", table_name="test_table", data={"value": 100})
        
        # Should not raise exception
        self.cc_manager.log_object(row, tid)
    
    def test_validate_object(self):
        """Test validating object access"""
        tid = self.cc_manager.begin_transaction()
        row = Row(object_id="X", table_name="test_table", data={"value": 100})
        
        response = self.cc_manager.validate_object(row, tid, ActionType.READ)
        self.assertIsNotNone(response)
        self.assertTrue(hasattr(response, 'allowed'))
    
    def test_end_transaction_commit(self):
        """Test ending transaction with commit"""
        tid = self.cc_manager.begin_transaction()
        row = Row(object_id="X", table_name="test_table", data={"value": 100})
        
        # Perform some operations
        self.cc_manager.validate_object(row, tid, ActionType.READ)
        
        # End transaction
        self.cc_manager.end_transaction(tid)
        
        # Should be committed
        status = self.cc_manager.get_transaction_status(tid)
        self.assertEqual(status, TransactionStatus.Terminated)
    
    def test_abort_transaction(self):
        """Test aborting a transaction"""
        tid = self.cc_manager.begin_transaction()
        
        # Abort the transaction
        self.cc_manager.abort_transaction(tid)
        
        # Should be aborted
        status = self.cc_manager.get_transaction_status(tid)
        self.assertEqual(status, TransactionStatus.Aborted)
    
    def test_set_algorithm(self):
        """Test changing concurrency control algorithm"""
        # Initially LockBased
        self.assertEqual(self.cc_manager.algorithm, AlgorithmType.LockBased)
        
        # Change to TimestampBased (no active transactions)
        self.cc_manager.set_algorithm(AlgorithmType.TimestampBased)
        self.assertEqual(self.cc_manager.algorithm, AlgorithmType.TimestampBased)
    
    def test_set_algorithm_with_active_transactions(self):
        """Test that algorithm change fails with active transactions"""
        tid = self.cc_manager.begin_transaction()
        
        # Should raise RuntimeError
        with self.assertRaises(RuntimeError):
            self.cc_manager.set_algorithm(AlgorithmType.TimestampBased)
        
        # Clean up
        self.cc_manager.end_transaction(tid)
    
    def test_get_active_transactions(self):
        """Test getting active transactions"""
        tid1 = self.cc_manager.begin_transaction()
        tid2 = self.cc_manager.begin_transaction()
        tid3 = self.cc_manager.begin_transaction()
        
        active = self.cc_manager.get_active_transactions()
        self.assertEqual(len(active), 3)
        self.assertIn(tid1, active)
        self.assertIn(tid2, active)
        self.assertIn(tid3, active)
        
        # End one transaction
        self.cc_manager.end_transaction(tid1)
        
        active = self.cc_manager.get_active_transactions()
        self.assertEqual(len(active), 2)
        self.assertNotIn(tid1, active)
    
    def test_clear_completed_transactions(self):
        """Test clearing completed transactions"""
        tid1 = self.cc_manager.begin_transaction()
        tid2 = self.cc_manager.begin_transaction()
        
        # End tid1
        self.cc_manager.end_transaction(tid1)
        
        # Clear completed
        self.cc_manager.clear_completed_transactions()
        
        # tid1 should be removed, tid2 should remain
        self.assertNotIn(tid1, self.cc_manager.transactions)
        self.assertIn(tid2, self.cc_manager.transactions)


class TestLockBasedAlgorithm(unittest.TestCase):
    """Tests for Lock-Based concurrency control"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.cc_manager = CCManager(AlgorithmType.LockBased, log_file="test_lock_log.txt")
    
    def tearDown(self):
        """Clean up test files"""
        if os.path.exists("test_lock_log.txt"):
            os.remove("test_lock_log.txt")
    
    def test_simple_read_lock(self):
        """Test acquiring read lock"""
        tid = self.cc_manager.begin_transaction()
        row = Row(object_id="X", table_name="test_table", data={"value": 100})
        
        response = self.cc_manager.validate_object(row, tid, ActionType.READ)
        self.assertTrue(response.allowed)
        
        self.cc_manager.end_transaction(tid)
    
    def test_simple_write_lock(self):
        """Test acquiring write lock"""
        tid = self.cc_manager.begin_transaction()
        row = Row(object_id="X", table_name="test_table", data={"value": 100})
        
        response = self.cc_manager.validate_object(row, tid, ActionType.WRITE)
        self.assertTrue(response.allowed)
        
        self.cc_manager.end_transaction(tid)
    
    def test_concurrent_reads(self):
        """Test multiple transactions can read the same object"""
        tid1 = self.cc_manager.begin_transaction()
        tid2 = self.cc_manager.begin_transaction()
        
        row = Row(object_id="X", table_name="test_table", data={"value": 100})
        
        # Both should get read locks
        response1 = self.cc_manager.validate_object(row, tid1, ActionType.READ)
        response2 = self.cc_manager.validate_object(row, tid2, ActionType.READ)
        
        self.assertTrue(response1.allowed)
        self.assertTrue(response2.allowed)
        
        self.cc_manager.end_transaction(tid1)
        self.cc_manager.end_transaction(tid2)
    
    def test_write_blocks_read(self):
        """Test write lock blocks read lock"""
        tid1 = self.cc_manager.begin_transaction()
        tid2 = self.cc_manager.begin_transaction()
        
        row = Row(object_id="X", table_name="test_table", data={"value": 100})
        
        # T1 gets write lock
        response1 = self.cc_manager.validate_object(row, tid1, ActionType.WRITE)
        self.assertTrue(response1.allowed)
        
        # T2 should be blocked
        response2 = self.cc_manager.validate_object(row, tid2, ActionType.READ)
        self.assertFalse(response2.allowed)
        
        # Commit T1
        self.cc_manager.end_transaction(tid1)
        print(f"--- TID 1 Status: {self.cc_manager.get_transaction_status(tid1)} ---")
        # Terminated but doesnt reset the locktable
        # Now T2 should succeed
        response3 = self.cc_manager.validate_object(row, tid2, ActionType.READ)
        self.assertTrue(response3.allowed)
        
        self.cc_manager.end_transaction(tid2)
    
    def test_read_blocks_write(self):
        """Test read lock blocks write lock"""
        tid1 = self.cc_manager.begin_transaction()
        tid2 = self.cc_manager.begin_transaction()
        
        row = Row(object_id="X", table_name="test_table", data={"value": 100})
        
        # T1 gets read lock
        response1 = self.cc_manager.validate_object(row, tid1, ActionType.READ)
        self.assertTrue(response1.allowed)
        
        # T2 should be blocked for write
        response2 = self.cc_manager.validate_object(row, tid2, ActionType.WRITE)
        self.assertFalse(response2.allowed)
        
        self.cc_manager.end_transaction(tid1)
        self.cc_manager.end_transaction(tid2)
    
    def test_write_blocks_write(self):
        """Test write lock blocks another write lock"""
        tid1 = self.cc_manager.begin_transaction()
        tid2 = self.cc_manager.begin_transaction()
        
        row = Row(object_id="X", table_name="test_table", data={"value": 100})
        
        # T1 gets write lock
        response1 = self.cc_manager.validate_object(row, tid1, ActionType.WRITE)
        self.assertTrue(response1.allowed)
        
        # T2 should be blocked
        response2 = self.cc_manager.validate_object(row, tid2, ActionType.WRITE)
        self.assertFalse(response2.allowed)
        
        self.cc_manager.end_transaction(tid1)
        self.cc_manager.end_transaction(tid2)
    
    def test_lock_upgrade(self):
        """Test upgrading read lock to write lock"""
        tid = self.cc_manager.begin_transaction()
        row = Row(object_id="X", table_name="test_table", data={"value": 100})
        
        # Get read lock
        response1 = self.cc_manager.validate_object(row, tid, ActionType.READ)
        self.assertTrue(response1.allowed)
        
        # Upgrade to write lock
        response2 = self.cc_manager.validate_object(row, tid, ActionType.WRITE)
        self.assertTrue(response2.allowed)
        
        self.cc_manager.end_transaction(tid)
    
    def test_multiple_objects(self):
        """Test locking multiple objects"""
        tid1 = self.cc_manager.begin_transaction()
        tid2 = self.cc_manager.begin_transaction()
        
        rowX = Row(object_id="X", table_name="test_table", data={"value": 100})
        rowY = Row(object_id="Y", table_name="test_table", data={"value": 200})
        
        # T1 locks X, T2 locks Y - should both succeed
        response1 = self.cc_manager.validate_object(rowX, tid1, ActionType.WRITE)
        response2 = self.cc_manager.validate_object(rowY, tid2, ActionType.WRITE)
        
        self.assertTrue(response1.allowed)
        self.assertTrue(response2.allowed)
        
        self.cc_manager.end_transaction(tid1)
        self.cc_manager.end_transaction(tid2)


class TestTimestampBasedAlgorithm(unittest.TestCase):
    """Tests for Timestamp-Based concurrency control"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.cc_manager = CCManager(AlgorithmType.TimestampBased, log_file="test_ts_log.txt")
    
    def tearDown(self):
        """Clean up test files"""
        if os.path.exists("test_ts_log.txt"):
            os.remove("test_ts_log.txt")
    
    def test_simple_read(self):
        """Test simple read operation"""
        tid = self.cc_manager.begin_transaction()
        row = Row(object_id="X", table_name="test_table", data={"value": 100})
        
        response = self.cc_manager.validate_object(row, tid, ActionType.READ)
        self.assertTrue(response.allowed)
        
        self.cc_manager.end_transaction(tid)
    
    def test_simple_write(self):
        """Test simple write operation"""
        tid = self.cc_manager.begin_transaction()
        row = Row(object_id="X", table_name="test_table", data={"value": 100})
        
        response = self.cc_manager.validate_object(row, tid, ActionType.WRITE)
        self.assertTrue(response.allowed)
        
        self.cc_manager.end_transaction(tid)
    
    def test_timestamp_ordering(self):
        """Test timestamp ordering is enforced"""
        tid1 = self.cc_manager.begin_transaction()
        tid2 = self.cc_manager.begin_transaction()
        
        # tid1 has lower timestamp than tid2
        self.assertLess(tid1, tid2)
        
        row = Row(object_id="X", table_name="test_table", data={"value": 100})
        
        # T2 writes first
        response1 = self.cc_manager.validate_object(row, tid2, ActionType.WRITE)
        
        # T1 tries to read - depends on implementation
        response2 = self.cc_manager.validate_object(row, tid1, ActionType.READ)
        
        # At least one should succeed
        self.assertTrue(response1.allowed or response2.allowed)
        
        # Clean up
        if self.cc_manager.get_transaction_status(tid1) == TransactionStatus.Active:
            self.cc_manager.end_transaction(tid1)
        if self.cc_manager.get_transaction_status(tid2) == TransactionStatus.Active:
            self.cc_manager.end_transaction(tid2)


class TestValidationBasedAlgorithm(unittest.TestCase):
    """Tests for Validation-Based (Optimistic) concurrency control"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.cc_manager = CCManager(AlgorithmType.ValidationBased, log_file="test_val_log.txt")
    
    def tearDown(self):
        """Clean up test files"""
        if os.path.exists("test_val_log.txt"):
            os.remove("test_val_log.txt")
    
    def test_simple_read(self):
        """Test simple read operation"""
        tid = self.cc_manager.begin_transaction()
        row = Row(object_id="X", table_name="test_table", data={"value": 100})
        
        response = self.cc_manager.validate_object(row, tid, ActionType.READ)
        self.assertTrue(response.allowed)
        
        self.cc_manager.end_transaction(tid)
    
    def test_simple_write(self):
        """Test simple write operation"""
        tid = self.cc_manager.begin_transaction()
        row = Row(object_id="X", table_name="test_table", data={"value": 100})
        
        response = self.cc_manager.validate_object(row, tid, ActionType.WRITE)
        self.assertTrue(response.allowed)
        
        self.cc_manager.end_transaction(tid)
    
    def test_optimistic_execution(self):
        """Test optimistic execution allows operations"""
        tid1 = self.cc_manager.begin_transaction()
        tid2 = self.cc_manager.begin_transaction()
        
        row = Row(object_id="X", table_name="test_table", data={"value": 100})
        
        # Both should be allowed during execution
        response1 = self.cc_manager.validate_object(row, tid1, ActionType.WRITE)
        response2 = self.cc_manager.validate_object(row, tid2, ActionType.WRITE)
        
        self.assertTrue(response1.allowed)
        self.assertTrue(response2.allowed)
        
        # Validation happens at commit
        # Clean up
        try:
            self.cc_manager.end_transaction(tid1)
        except:
            pass
        try:
            self.cc_manager.end_transaction(tid2)
        except:
            pass


class TestMVCCAlgorithm(unittest.TestCase):
    """Tests for MVCC (Multi-Version Concurrency Control)"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.cc_manager = CCManager(AlgorithmType.MVCC, log_file="test_mvcc_log.txt")
    
    def tearDown(self):
        """Clean up test files"""
        if os.path.exists("test_mvcc_log.txt"):
            os.remove("test_mvcc_log.txt")
    
    def test_simple_read(self):
        """Test simple read operation"""
        tid = self.cc_manager.begin_transaction()
        row = Row(object_id="X", table_name="test_table", data={"value": 100})
        
        response = self.cc_manager.validate_object(row, tid, ActionType.READ)
        self.assertTrue(response.allowed)
        
        self.cc_manager.end_transaction(tid)
    
    def test_simple_write(self):
        """Test simple write operation"""
        tid = self.cc_manager.begin_transaction()
        row = Row(object_id="X", table_name="test_table", data={"value": 100})
        
        response = self.cc_manager.validate_object(row, tid, ActionType.WRITE)
        self.assertTrue(response.allowed)
        
        self.cc_manager.end_transaction(tid)
    
    def test_concurrent_reads(self):
        """Test multiple transactions can read concurrently"""
        tid1 = self.cc_manager.begin_transaction()
        tid2 = self.cc_manager.begin_transaction()
        
        row = Row(object_id="X", table_name="test_table", data={"value": 100})
        
        # Both should be able to read
        response1 = self.cc_manager.validate_object(row, tid1, ActionType.READ)
        response2 = self.cc_manager.validate_object(row, tid2, ActionType.READ)
        
        self.assertTrue(response1.allowed)
        self.assertTrue(response2.allowed)
        
        self.cc_manager.end_transaction(tid1)
        self.cc_manager.end_transaction(tid2)
    
    def test_multiversion_reads(self):
        """Test reading different versions"""
        tid1 = self.cc_manager.begin_transaction()
        row = Row(object_id="X", table_name="test_table", data={"value": 100})
        
        # T1 writes
        self.cc_manager.validate_object(row, tid1, ActionType.WRITE)
        
        # Start T2 before T1 commits
        tid2 = self.cc_manager.begin_transaction()
        
        # T2 should read old version
        response2 = self.cc_manager.validate_object(row, tid2, ActionType.READ)
        self.assertTrue(response2.allowed)
        
        # Commit T1
        self.cc_manager.end_transaction(tid1)
        
        # Start T3 after T1 commits
        tid3 = self.cc_manager.begin_transaction()
        
        # T3 should read new version
        response3 = self.cc_manager.validate_object(row, tid3, ActionType.READ)
        self.assertTrue(response3.allowed)
        
        self.cc_manager.end_transaction(tid2)
        self.cc_manager.end_transaction(tid3)


class TestIntegrationScenarios(unittest.TestCase):
    """Integration tests with realistic scenarios"""
    
    def test_bank_transfer_scenario(self):
        """Test bank account transfer scenario"""
        cc_manager = CCManager(AlgorithmType.LockBased, log_file="test_bank_log.txt")
        
        # Account A: $1000, Account B: $500
        accountA = Row(object_id="AccountA", table_name="accounts", data={"balance": 1000})
        accountB = Row(object_id="AccountB", table_name="accounts", data={"balance": 500})
        
        # Transaction: Transfer $100 from A to B
        tid = cc_manager.begin_transaction()
        
        # Read A
        response1 = cc_manager.validate_object(accountA, tid, ActionType.READ)
        self.assertTrue(response1.allowed)
        
        # Write A (deduct 100)
        accountA.data["balance"] = 900
        response2 = cc_manager.validate_object(accountA, tid, ActionType.WRITE)
        self.assertTrue(response2.allowed)
        
        # Read B
        response3 = cc_manager.validate_object(accountB, tid, ActionType.READ)
        self.assertTrue(response3.allowed)
        
        # Write B (add 100)
        accountB.data["balance"] = 600
        response4 = cc_manager.validate_object(accountB, tid, ActionType.WRITE)
        self.assertTrue(response4.allowed)
        
        # Commit
        cc_manager.end_transaction(tid)
        
        self.assertEqual(cc_manager.get_transaction_status(tid), TransactionStatus.Terminated)
        
        # Clean up
        if os.path.exists("test_bank_log.txt"):
            os.remove("test_bank_log.txt")
    
    def test_concurrent_bank_transfers(self):
        """Test concurrent bank transfers with conflict"""
        cc_manager = CCManager(AlgorithmType.LockBased, log_file="test_concurrent_log.txt")
        
        accountA = Row(object_id="AccountA", table_name="accounts", data={"balance": 1000})
        accountB = Row(object_id="AccountB", table_name="accounts", data={"balance": 500})
        accountC = Row(object_id="AccountC", table_name="accounts", data={"balance": 750})
        
        # T1: A -> B ($100)
        tid1 = cc_manager.begin_transaction()
        cc_manager.validate_object(accountA, tid1, ActionType.WRITE)
        
        # T2: A -> C ($200) - should be blocked
        tid2 = cc_manager.begin_transaction()
        response = cc_manager.validate_object(accountA, tid2, ActionType.WRITE)
        self.assertFalse(response.allowed)  # Blocked by T1
        
        # Commit T1
        cc_manager.end_transaction(tid1)
        print(f"--- TID 1 Status: {cc_manager.get_transaction_status(tid1)} ---")
        # Terminated but doesnt reset the locktable so when it wants to validate it still said that T1 have the lock.
        

        # Now T2 should be able to proceed
        response2 = cc_manager.validate_object(accountA, tid2, ActionType.WRITE)
        self.assertTrue(response2.allowed)
        
        cc_manager.end_transaction(tid2)
        
        # Clean up
        if os.path.exists("test_concurrent_log.txt"):
            os.remove("test_concurrent_log.txt")


class TestMVTO(unittest.TestCase):
    """Tests for MVTO (Multi-Version Timestamp Ordering) Algorithm"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.tester = MVCCTester("MVTO", verbose=False)
    
    def test_mvto_case1_cascading_rollback(self):
        """MVTO Test Case 1: Cascading Rollback with Auto Re-execution"""
        # Initialize transactions
        for i in range(1, 8):
            self.tester.create_transaction(i)
        
        # Phase 1: Execute operations
        self.tester.read(7, 'A')
        self.tester.read(3, 'B')
        self.tester.write(5, 'C', 50)
        self.tester.read(1, 'D')
        self.tester.write(6, 'A', 100)
        
        # Verify operations executed
        self.assertGreater(len(self.tester.execution_trace), 0)
    
    def test_mvto_case2_multiple_rollbacks(self):
        """MVTO Test Case 2: Multiple Rollbacks with Immediate Re-execution"""
        for i in range(1, 8):
            self.tester.create_transaction(i)
        
        # Setup for first abort
        self.tester.read(5, 'P')
        self.tester.read(7, 'Q')
        self.tester.write(3, 'R', 30)
        
        # Verify transactions created
        self.assertEqual(len(self.tester.transactions), 7)
    
    def test_mvto_case3_read_write_conflicts(self):
        """MVTO Test Case 3: Read-Write Conflicts"""
        for i in range(1, 6):
            self.tester.create_transaction(i)
        
        # Reads establish high R-TS
        self.tester.read(5, 'X')
        self.tester.read(4, 'Y')
        self.tester.read(3, 'Z')
        
        # Older transactions try to write (should conflict)
        response1 = self.tester.write(1, 'X', 100)
        
        # Verify conflict handling
        self.assertIsNotNone(response1)
    
    def test_mvto_case4_complex_cascading_chain(self):
        """MVTO Test Case 4: Complex Cascading Chain"""
        for i in range(1, 7):
            self.tester.create_transaction(i)
        
        # Build dependency chain
        self.tester.write(1, 'A', 10)
        self.tester.read(2, 'A')
        self.tester.write(2, 'B', 20)
        self.tester.read(3, 'B')
        
        # Verify chain created
        self.assertGreater(len(self.tester.execution_trace), 0)
    
    def test_mvto_case5_interleaved_pattern(self):
        """MVTO Test Case 5: Interleaved Read-Write Pattern"""
        for i in range(1, 8):
            self.tester.create_transaction(i)
        
        # Initial writes create versions
        self.tester.write(1, 'A', 10)
        self.tester.write(2, 'B', 20)
        self.tester.write(3, 'C', 30)
        
        # Verify versions created
        self.assertGreater(len(self.tester.execution_trace), 0)


class TestMV2PL(unittest.TestCase):
    """Tests for MV2PL (Multi-Version Two-Phase Locking) Algorithm"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.tester = MVCCTester("MV2PL", verbose=False)
    
    def test_mv2pl_case1_lock_conflicts(self):
        """MV2PL Test Case 1: Lock Conflicts with Queue Management"""
        for i in range(1, 6):
            self.tester.create_transaction(i)
        
        # Concurrent reads (shared locks)
        self.tester.read(1, 'X')
        self.tester.read(2, 'X')
        self.tester.read(3, 'X')
        
        # Write attempt (exclusive lock conflict)
        response = self.tester.write(4, 'X', 100)
        
        # Verify lock mechanism working
        self.assertIsNotNone(response)
    
    def test_mv2pl_case2_wound_wait_deadlock(self):
        """MV2PL Test Case 2: Deadlock Prevention (Wound-Wait)"""
        for i in range(1, 6):
            self.tester.create_transaction(i)
        
        # Setup for wound-wait scenario
        self.tester.write(1, 'X', 10)
        self.tester.write(2, 'Y', 20)
        
        # Cross-access to trigger wound-wait
        self.tester.write(1, 'Y', 15)
        self.tester.write(2, 'X', 25)
        
        # Verify wound-wait mechanism
        self.assertGreater(len(self.tester.execution_trace), 0)
    
    def test_mv2pl_case3_multiple_items_locking(self):
        """MV2PL Test Case 3: Multiple Items Locking"""
        for i in range(1, 6):
            self.tester.create_transaction(i)
        
        # Lock multiple items
        self.tester.write(1, 'A', 10)
        self.tester.write(1, 'B', 11)
        self.tester.write(2, 'C', 20)
        
        # Verify multiple locks
        self.assertGreater(len(self.tester.execution_trace), 0)
    
    def test_mv2pl_case4_lock_escalation(self):
        """MV2PL Test Case 4: Lock Escalation"""
        for i in range(1, 6):
            self.tester.create_transaction(i)
        
        # Read then write (lock upgrade)
        self.tester.read(1, 'X')
        self.tester.write(1, 'X', 100)
        
        # Verify lock escalation
        self.assertGreater(len(self.tester.execution_trace), 0)
    
    def test_mv2pl_case5_high_contention(self):
        """MV2PL Test Case 5: High Contention"""
        for i in range(1, 9):
            self.tester.create_transaction(i)
        
        # All try to access same item
        for i in range(1, 9):
            self.tester.write(i, 'HOT', i * 10)
        
        # Verify contention handling
        self.assertGreater(len(self.tester.execution_trace), 0)


class TestSnapshotFUW(unittest.TestCase):
    """Tests for Snapshot Isolation with First-Updater-Wins"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.tester = MVCCTester("Snapshot_FUW", verbose=False)
    
    def test_snapshot_fuw_case1_basic_fuw(self):
        """SI-FUW Test Case 1: Basic First-Updater-Wins"""
        for i in range(1, 6):
            self.tester.create_transaction(i)
        
        # Concurrent reads (no conflicts)
        self.tester.read(1, 'X')
        self.tester.read(2, 'X')
        
        # First writes (acquire exclusive locks)
        response1 = self.tester.write(1, 'X', 10)
        response2 = self.tester.write(2, 'X', 20)
        
        # Verify first-updater-wins
        self.assertTrue(response1.allowed)
        self.assertFalse(response2.allowed)
    
    def test_snapshot_fuw_case2_multiple_conflicts(self):
        """SI-FUW Test Case 2: Multiple Conflicts"""
        for i in range(1, 6):
            self.tester.create_transaction(i)
        
        # Multiple concurrent writes
        self.tester.write(1, 'A', 10)
        self.tester.write(2, 'A', 20)
        self.tester.write(3, 'B', 30)
        
        # Verify conflict detection
        self.assertGreater(len(self.tester.execution_trace), 0)
    
    def test_snapshot_fuw_case3_mixed_operations(self):
        """SI-FUW Test Case 3: Mixed Operations"""
        for i in range(1, 7):
            self.tester.create_transaction(i)
        
        # Mix of reads and writes
        self.tester.read(1, 'P')
        self.tester.write(2, 'P', 20)
        self.tester.read(3, 'Q')
        
        # Verify snapshot isolation
        self.assertGreater(len(self.tester.execution_trace), 0)
    
    def test_snapshot_fuw_case4_interleaved_updates(self):
        """SI-FUW Test Case 4: Interleaved Updates"""
        for i in range(1, 7):
            self.tester.create_transaction(i)
        
        # Alternating locks
        self.tester.write(1, 'X', 10)
        self.tester.write(2, 'Y', 20)
        self.tester.write(3, 'Z', 30)
        
        # Verify interleaved handling
        self.assertGreater(len(self.tester.execution_trace), 0)
    
    def test_snapshot_fuw_case5_high_contention(self):
        """SI-FUW Test Case 5: High Contention"""
        for i in range(1, 11):
            self.tester.create_transaction(i)
        
        # First transaction gets lock
        response1 = self.tester.write(1, 'HOT', 10)
        
        # Avalanche of conflicts
        for i in range(2, 11):
            response = self.tester.write(i, 'HOT', i * 10)
            if i > 1:
                self.assertFalse(response.allowed)
        
        # Verify first wins
        self.assertTrue(response1.allowed)


class TestSnapshotFCW(unittest.TestCase):
    """Tests for Snapshot Isolation with First-Committer-Wins"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.tester = MVCCTester("Snapshot_FCW", verbose=False)
    
    def test_snapshot_fcw_case1_basic_fcw(self):
        """SI-FCW Test Case 1: Basic First-Committer-Wins"""
        for i in range(1, 6):
            self.tester.create_transaction(i)
        
        # Concurrent reads
        self.tester.read(1, 'X')
        self.tester.read(2, 'X')
        
        # Concurrent writes (buffered)
        self.tester.write(1, 'X', 10)
        self.tester.write(2, 'X', 20)
        
        # Commit phase - first committer wins
        result1 = self.tester.commit(1)
        result2 = self.tester.commit(2)
        
        self.assertTrue(result1.allowed)
        self.assertFalse(result2.allowed)
    
    def test_snapshot_fcw_case2_multiple_items(self):
        """SI-FCW Test Case 2: Multiple Item Conflicts"""
        for i in range(1, 6):
            self.tester.create_transaction(i)
        
        # Multiple items
        self.tester.write(1, 'A', 10)
        self.tester.write(1, 'B', 11)
        self.tester.write(2, 'A', 20)
        
        # Verify buffered writes
        self.assertGreater(len(self.tester.execution_trace), 0)
    
    def test_snapshot_fcw_case3_long_running(self):
        """SI-FCW Test Case 3: Long-Running Transactions"""
        for i in range(1, 7):
            self.tester.create_transaction(i)
        
        # T1 starts early
        self.tester.read(1, 'P')
        
        # Others commit changes
        self.tester.write(2, 'P', 20)
        self.tester.commit(2)
        
        # T1 continues with stale snapshot
        self.tester.write(1, 'P', 10)
        result = self.tester.commit(1)
        
        # Should detect conflict
        self.assertFalse(result.allowed)
    
    def test_snapshot_fcw_case4_buffered_writes(self):
        """SI-FCW Test Case 4: Buffered Writes Validation"""
        # Create all transactions first (before any commits)
        for i in range(1, 5):
            self.tester.create_transaction(i)
        
        # T1 writes and commits - establishes baseline
        self.tester.write(1, 'X', 10)
        self.tester.write(1, 'Y', 11)
        self.tester.commit(1)
        
        # T2 and T3 read same snapshot (after T1 commits, but they started before)
        self.tester.read(2, 'X')
        self.tester.read(3, 'X')
        self.tester.read(2, 'Y')
        self.tester.read(3, 'Y')
        
        # Buffer multiple writes
        self.tester.write(2, 'X', 20)
        self.tester.write(2, 'Z', 21)
        self.tester.write(3, 'X', 30)
        self.tester.write(3, 'W', 31)
        
        # T4 independent write
        self.tester.write(4, 'V', 40)
        
        # Commit and validate
        result2 = self.tester.commit(2)
        result3 = self.tester.commit(3)
        result4 = self.tester.commit(4)
        
        # Both T2 and T3 abort due to write-write conflict with T1
        self.assertFalse(result2.allowed)
        self.assertFalse(result3.allowed)
        self.assertTrue(result4.allowed)
        
        # Restart T3 (as in original test)
        if not result3.allowed:
            self.tester.create_transaction(3)
            self.tester.read(3, 'X')
            self.tester.read(3, 'Y')
            self.tester.write(3, 'X', 30)
            self.tester.write(3, 'W', 31)
            result3_retry = self.tester.commit(3)
            self.assertTrue(result3_retry.allowed)
    
    def test_snapshot_fcw_case5_high_contention(self):
        """SI-FCW Test Case 5: High Contention Scenario"""
        for i in range(1, 9):
            self.tester.create_transaction(i)
        
        # All read same item
        for i in range(1, 9):
            self.tester.read(i, 'HOT')
        
        # All try to write same item
        for i in range(1, 9):
            self.tester.write(i, 'HOT', i * 10)
        
        # Commit race - only first wins
        result1 = self.tester.commit(1)
        self.assertTrue(result1.allowed)
        
        for i in range(2, 9):
            result = self.tester.commit(i)
            self.assertFalse(result.allowed)


def run_tests():
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add all test classes
    suite.addTests(loader.loadTestsFromTestCase(TestCCManagerBasic))
    suite.addTests(loader.loadTestsFromTestCase(TestLockBasedAlgorithm))
    suite.addTests(loader.loadTestsFromTestCase(TestTimestampBasedAlgorithm))
    suite.addTests(loader.loadTestsFromTestCase(TestValidationBasedAlgorithm))
    suite.addTests(loader.loadTestsFromTestCase(TestMVCCAlgorithm))
    suite.addTests(loader.loadTestsFromTestCase(TestIntegrationScenarios))
    suite.addTests(loader.loadTestsFromTestCase(TestMVTO))
    suite.addTests(loader.loadTestsFromTestCase(TestMV2PL))
    suite.addTests(loader.loadTestsFromTestCase(TestSnapshotFUW))
    suite.addTests(loader.loadTestsFromTestCase(TestSnapshotFCW))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    print("\n" + "="*80)
    print("TEST SUMMARY")
    print("="*80)
    print(f"Tests run: {result.testsRun}")
    print(f"Successes: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print("="*80)
    
    return result


if __name__ == "__main__":
    result = run_tests()
    sys.exit(0 if result.wasSuccessful() else 1)
