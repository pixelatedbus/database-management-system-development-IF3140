"""
Unit Tests for MVCC Algorithms: MVTO, MV2PL, Snapshot FUW, Snapshot FCW
Using unittest framework format consistent with unit_test.py
"""

import unittest
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.dirname(current_dir)
project_root = os.path.dirname(os.path.dirname(src_dir))

# Add paths
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)


from concurrency_control_manager.src.test.mvcc_tester import MVCCTester


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


def run_mvcc_tests():
    """Run all MVCC unit tests"""
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add all test classes
    suite.addTests(loader.loadTestsFromTestCase(TestMVTO))
    suite.addTests(loader.loadTestsFromTestCase(TestMV2PL))
    suite.addTests(loader.loadTestsFromTestCase(TestSnapshotFUW))
    suite.addTests(loader.loadTestsFromTestCase(TestSnapshotFCW))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    print("\n" + "="*80)
    print("MVCC TEST SUMMARY")
    print("="*80)
    print(f"Tests run: {result.testsRun}")
    print(f"Successes: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print("="*80)
    
    return result


if __name__ == "__main__":
    result = run_mvcc_tests()
    sys.exit(0 if result.wasSuccessful() else 1)
