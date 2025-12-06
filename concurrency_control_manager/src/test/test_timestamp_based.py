import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.dirname(current_dir)
project_root = os.path.dirname(os.path.dirname(src_dir))

if src_dir not in sys.path:
    sys.path.insert(0, src_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import unittest
from datetime import datetime, timedelta

from concurrency_control_manager.src.algorithms.timestamp_based import TimestampBasedAlgorithm, ObjectTimestamp
from concurrency_control_manager.src.transaction import Transaction
from concurrency_control_manager.src.row import Row
from concurrency_control_manager.src.enums import ActionType


class TestObjectTimestamp(unittest.TestCase):
    """Test cases for ObjectTimestamp class"""

    def setUp(self):
        """Set up test fixtures"""
        self.obj_ts = ObjectTimestamp("obj1")
        self.ts1 = datetime.now()
        self.ts2 = self.ts1 + timedelta(seconds=1)
        self.ts3 = self.ts1 + timedelta(seconds=2)

    def test_initial_state(self):
        """Test initial state of ObjectTimestamp"""
        self.assertIsNone(self.obj_ts.read_timestamp)
        self.assertIsNone(self.obj_ts.write_timestamp)

    def test_update_read_timestamp(self):
        """Test updating read timestamp"""
        self.obj_ts.update_read_timestamp(self.ts1)
        self.assertEqual(self.obj_ts.read_timestamp, self.ts1)

        # Update with newer timestamp
        self.obj_ts.update_read_timestamp(self.ts2)
        self.assertEqual(self.obj_ts.read_timestamp, self.ts2)

        # Try to update with older timestamp (should not update)
        self.obj_ts.update_read_timestamp(self.ts1)
        self.assertEqual(self.obj_ts.read_timestamp, self.ts2)

    def test_update_write_timestamp(self):
        """Test updating write timestamp"""
        self.obj_ts.update_write_timestamp(self.ts1)
        self.assertEqual(self.obj_ts.write_timestamp, self.ts1)

        self.obj_ts.update_write_timestamp(self.ts2)
        self.assertEqual(self.obj_ts.write_timestamp, self.ts2)

    def test_is_read_valid(self):
        """Test read validation"""
        # No write timestamp - should be valid
        self.assertTrue(self.obj_ts.is_read_valid(self.ts1))

        # Set write timestamp
        self.obj_ts.update_write_timestamp(self.ts2)

        # Transaction timestamp >= write timestamp - valid
        self.assertTrue(self.obj_ts.is_read_valid(self.ts2))
        self.assertTrue(self.obj_ts.is_read_valid(self.ts3))

        # Transaction timestamp < write timestamp - invalid
        self.assertFalse(self.obj_ts.is_read_valid(self.ts1))

    def test_is_write_valid(self):
        """Test write validation"""
        # No timestamps - should be valid
        self.assertTrue(self.obj_ts.is_write_valid(self.ts1))

        # Set read timestamp
        self.obj_ts.update_read_timestamp(self.ts2)

        # Transaction timestamp >= read timestamp - valid
        self.assertTrue(self.obj_ts.is_write_valid(self.ts2))
        self.assertTrue(self.obj_ts.is_write_valid(self.ts3))

        # Transaction timestamp < read timestamp - invalid
        self.assertFalse(self.obj_ts.is_write_valid(self.ts1))

        # Set write timestamp
        self.obj_ts.update_write_timestamp(self.ts2)

        # Transaction timestamp < write timestamp - invalid
        self.assertFalse(self.obj_ts.is_write_valid(self.ts1))


class TestTimestampBasedAlgorithm(unittest.TestCase):
    """Test cases for TimestampBasedAlgorithm class"""

    def setUp(self):
        """Set up test fixtures"""
        self.algorithm = TimestampBasedAlgorithm()

        # Create transactions with different timestamps
        self.t1 = Transaction(1)
        self.t1.start_timestamp = datetime.now()

        self.t2 = Transaction(2)
        self.t2.start_timestamp = self.t1.start_timestamp + timedelta(seconds=1)

        self.t3 = Transaction(3)
        self.t3.start_timestamp = self.t1.start_timestamp + timedelta(seconds=2)

        # Create test objects
        self.obj1 = Row("obj1", "table1", {"id": 1, "value": "A"})
        self.obj2 = Row("obj2", "table1", {"id": 2, "value": "B"})

    def test_read_on_new_object(self):
        """Test read on object that has never been accessed"""
        response = self.algorithm.check_permission(self.t1, self.obj1, ActionType.READ)
        self.assertTrue(response.allowed)

    def test_write_on_new_object(self):
        """Test write on object that has never been accessed"""
        response = self.algorithm.check_permission(self.t1, self.obj1, ActionType.WRITE)
        self.assertTrue(response.allowed)

    def test_read_after_write_same_transaction(self):
        """Test read after write by newer transaction"""
        # T1 writes
        response1 = self.algorithm.check_permission(self.t1, self.obj1, ActionType.WRITE)
        self.assertTrue(response1.allowed)

        # T2 (newer) reads - should be allowed
        response2 = self.algorithm.check_permission(self.t2, self.obj1, ActionType.READ)
        self.assertTrue(response2.allowed)

    def test_read_after_write_older_transaction(self):
        """Test read by older transaction after write by newer transaction"""
        # T2 writes
        response1 = self.algorithm.check_permission(self.t2, self.obj1, ActionType.WRITE)
        self.assertTrue(response1.allowed)

        # T1 (older) tries to read - should be denied
        response2 = self.algorithm.check_permission(self.t1, self.obj1, ActionType.READ)
        self.assertFalse(response2.allowed)

    def test_write_after_read_newer_transaction(self):
        """Test write by newer transaction after read"""
        # T1 reads
        response1 = self.algorithm.check_permission(self.t1, self.obj1, ActionType.READ)
        self.assertTrue(response1.allowed)

        # T2 (newer) writes - should be allowed
        response2 = self.algorithm.check_permission(self.t2, self.obj1, ActionType.WRITE)
        self.assertTrue(response2.allowed)

    def test_write_after_read_older_transaction(self):
        """Test write by older transaction after read by newer transaction"""
        # T2 reads
        response1 = self.algorithm.check_permission(self.t2, self.obj1, ActionType.READ)
        self.assertTrue(response1.allowed)

        # T1 (older) tries to write - should be denied
        response2 = self.algorithm.check_permission(self.t1, self.obj1, ActionType.WRITE)
        self.assertFalse(response2.allowed)

    def test_write_after_write_newer_transaction(self):
        """Test write by newer transaction after write"""
        # T1 writes
        response1 = self.algorithm.check_permission(self.t1, self.obj1, ActionType.WRITE)
        self.assertTrue(response1.allowed)

        # T2 (newer) writes - should be allowed
        response2 = self.algorithm.check_permission(self.t2, self.obj1, ActionType.WRITE)
        self.assertTrue(response2.allowed)

    def test_write_after_write_older_transaction(self):
        """Test write by older transaction after write by newer transaction"""
        # T2 writes
        response1 = self.algorithm.check_permission(self.t2, self.obj1, ActionType.WRITE)
        self.assertTrue(response1.allowed)

        # T1 (older) tries to write - should be denied
        response2 = self.algorithm.check_permission(self.t1, self.obj1, ActionType.WRITE)
        self.assertFalse(response2.allowed)

    def test_serializable_schedule(self):
        """Test a serializable schedule: T1 -> T2"""
        # T1: R(A), W(A)
        response1 = self.algorithm.check_permission(self.t1, self.obj1, ActionType.READ)
        self.assertTrue(response1.allowed)

        response2 = self.algorithm.check_permission(self.t1, self.obj1, ActionType.WRITE)
        self.assertTrue(response2.allowed)

        # T2: R(A), W(A)
        response3 = self.algorithm.check_permission(self.t2, self.obj1, ActionType.READ)
        self.assertTrue(response3.allowed)

        response4 = self.algorithm.check_permission(self.t2, self.obj1, ActionType.WRITE)
        self.assertTrue(response4.allowed)

    def test_non_serializable_schedule_detection(self):
        """Test detection of non-serializable schedule"""
        # T1: R(A)
        response1 = self.algorithm.check_permission(self.t1, self.obj1, ActionType.READ)
        self.assertTrue(response1.allowed)

        # T2: W(A) - should be allowed (T2 is newer)
        response2 = self.algorithm.check_permission(self.t2, self.obj1, ActionType.WRITE)
        self.assertTrue(response2.allowed)

        # T1: W(A) - should be denied (T1 timestamp < T2's write timestamp)
        response3 = self.algorithm.check_permission(self.t1, self.obj1, ActionType.WRITE)
        self.assertFalse(response3.allowed)

    def test_commit_transaction(self):
        """Test commit transaction"""
        # Perform some operations
        self.algorithm.check_permission(self.t1, self.obj1, ActionType.WRITE)

        # Commit should not raise error
        try:
            self.algorithm.commit_transaction(self.t1)
        except Exception as e:
            self.fail(f"commit_transaction raised exception: {e}")

    def test_abort_transaction(self):
        """Test abort transaction"""
        # Perform some operations
        self.algorithm.check_permission(self.t1, self.obj1, ActionType.WRITE)

        # Abort should not raise error
        try:
            self.algorithm.abort_transaction(self.t1)
        except Exception as e:
            self.fail(f"abort_transaction raised exception: {e}")

    def test_multiple_objects(self):
        """Test operations on multiple objects"""
        # T1: R(A), W(B)
        response1 = self.algorithm.check_permission(self.t1, self.obj1, ActionType.READ)
        self.assertTrue(response1.allowed)

        response2 = self.algorithm.check_permission(self.t1, self.obj2, ActionType.WRITE)
        self.assertTrue(response2.allowed)

        # T2: W(A), R(B)
        response3 = self.algorithm.check_permission(self.t2, self.obj1, ActionType.WRITE)
        self.assertTrue(response3.allowed)

        response4 = self.algorithm.check_permission(self.t2, self.obj2, ActionType.READ)
        self.assertTrue(response4.allowed)


if __name__ == '__main__':
    unittest.main()
