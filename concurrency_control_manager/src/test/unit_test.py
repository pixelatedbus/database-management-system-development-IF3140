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
