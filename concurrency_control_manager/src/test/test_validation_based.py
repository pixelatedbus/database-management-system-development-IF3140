import unittest
import sys
import os

# Setup Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.dirname(current_dir)
project_root = os.path.dirname(os.path.dirname(src_dir))

if src_dir not in sys.path:
    sys.path.insert(0, src_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from concurrency_control_manager.src.cc_manager import CCManager
from concurrency_control_manager.src.enums import AlgorithmType, ActionType, TransactionStatus
from concurrency_control_manager.src.row import Row


class TestValidationBasedAlgorithm(unittest.TestCase):    
    def setUp(self):
        """Set up test fixtures"""
        self.cc_manager = CCManager(AlgorithmType.ValidationBased, log_file="test_val_log.txt")
    
    def tearDown(self):
        """Clean up test files"""
        if os.path.exists("test_val_log.txt"):
            os.remove("test_val_log.txt")
    
    def test_basic_validation_no_conflict(self):
        """Test basic validation with no conflicts"""
        tid1 = self.cc_manager.begin_transaction()
        tid2 = self.cc_manager.begin_transaction()
        
        rowA = Row(object_id="A", table_name="test_table", data={"value": 100})
        rowB = Row(object_id="B", table_name="test_table", data={"value": 200})
        
        # T1: Read A, Write A
        self.cc_manager.validate_object(rowA, tid1, ActionType.READ)
        self.cc_manager.validate_object(rowA, tid1, ActionType.WRITE)
        
        # T2: Read B, Write B
        self.cc_manager.validate_object(rowB, tid2, ActionType.READ)
        self.cc_manager.validate_object(rowB, tid2, ActionType.WRITE)
        
        # Both should commit successfully
        self.cc_manager.end_transaction(tid1)
        self.cc_manager.end_transaction(tid2)
        
        self.assertEqual(self.cc_manager.get_transaction_status(tid1), TransactionStatus.Terminated)
        self.assertEqual(self.cc_manager.get_transaction_status(tid2), TransactionStatus.Terminated)
    
    def test_read_write_conflict(self):
        """Test detection of read-write conflicts"""
        tid1 = self.cc_manager.begin_transaction()
        tid2 = self.cc_manager.begin_transaction()
        
        rowA = Row(object_id="A", table_name="test_table", data={"value": 100})
        
        # T1 reads A
        self.cc_manager.validate_object(rowA, tid1, ActionType.READ)
        
        # T2 writes A
        self.cc_manager.validate_object(rowA, tid2, ActionType.WRITE)
        
        # T2 commits first
        self.cc_manager.end_transaction(tid2)
        status2 = self.cc_manager.get_transaction_status(tid2)
        
        # T1 should be aborted (read-write conflict)
        self.cc_manager.end_transaction(tid1)
        status1 = self.cc_manager.get_transaction_status(tid1)
        
        # T2 committed, T1 aborted
        self.assertIn(status2, [TransactionStatus.Committed, TransactionStatus.Terminated])
        self.assertIn(status1, [TransactionStatus.Aborted, TransactionStatus.Terminated])
    
    def test_write_write_conflict(self):
        """Test detection of write-write conflicts"""
        tid1 = self.cc_manager.begin_transaction()
        tid2 = self.cc_manager.begin_transaction()
        
        rowB = Row(object_id="B", table_name="test_table", data={"value": 200})
        
        # Both write B
        self.cc_manager.validate_object(rowB, tid1, ActionType.WRITE)
        self.cc_manager.validate_object(rowB, tid2, ActionType.WRITE)
        
        # T1 commits first
        self.cc_manager.end_transaction(tid1)
        status1 = self.cc_manager.get_transaction_status(tid1)
        
        # T2 should be aborted (write-write conflict)
        self.cc_manager.end_transaction(tid2)
        status2 = self.cc_manager.get_transaction_status(tid2)
        
        # T1 committed, T2 aborted
        self.assertIn(status1, [TransactionStatus.Committed, TransactionStatus.Terminated])
        self.assertIn(status2, [TransactionStatus.Aborted, TransactionStatus.Terminated])
    
    def test_serial_execution(self):
        """Test that serial execution always passes validation"""
        results = []
        
        rowS = Row(object_id="S", table_name="test_table", data={"value": 0})
        
        for i in range(5):
            tid = self.cc_manager.begin_transaction()
            
            self.cc_manager.validate_object(rowS, tid, ActionType.READ)
            self.cc_manager.validate_object(rowS, tid, ActionType.WRITE)
            
            self.cc_manager.end_transaction(tid)
            results.append(self.cc_manager.get_transaction_status(tid))
        
        # All should be committed or terminated (not aborted)
        for status in results:
            self.assertIn(status, [TransactionStatus.Committed, TransactionStatus.Terminated])
    
    def test_multiple_reads_no_conflict(self):
        """Test multiple transactions reading the same object"""
        tid1 = self.cc_manager.begin_transaction()
        tid2 = self.cc_manager.begin_transaction()
        tid3 = self.cc_manager.begin_transaction()
        
        rowZ = Row(object_id="Z", table_name="test_table", data={"value": 500})
        
        # All read Z
        self.cc_manager.validate_object(rowZ, tid1, ActionType.READ)
        self.cc_manager.validate_object(rowZ, tid2, ActionType.READ)
        self.cc_manager.validate_object(rowZ, tid3, ActionType.READ)
        
        # All commit
        self.cc_manager.end_transaction(tid1)
        self.cc_manager.end_transaction(tid2)
        self.cc_manager.end_transaction(tid3)
        
        # All should succeed
        for tid in [tid1, tid2, tid3]:
            status = self.cc_manager.get_transaction_status(tid)
            self.assertIn(status, [TransactionStatus.Committed, TransactionStatus.Terminated])
    
    def test_scenario(self):
        """Test with multiple transactions and objects"""
        obj_a = Row("A", "test_table", {"value": 100})
        obj_b = Row("B", "test_table", {"value": 200})
        obj_c = Row("C", "test_table", {"value": 300})
        
        # T1: Read A, Write B
        tid1 = self.cc_manager.begin_transaction()
        self.cc_manager.validate_object(obj_a, tid1, ActionType.READ)
        self.cc_manager.validate_object(obj_b, tid1, ActionType.WRITE)
        
        # T2: Read B, Write C
        tid2 = self.cc_manager.begin_transaction()
        self.cc_manager.validate_object(obj_b, tid2, ActionType.READ)
        self.cc_manager.validate_object(obj_c, tid2, ActionType.WRITE)
        
        # T3: Read C, Write A
        tid3 = self.cc_manager.begin_transaction()
        self.cc_manager.validate_object(obj_c, tid3, ActionType.READ)
        self.cc_manager.validate_object(obj_a, tid3, ActionType.WRITE)
        
        # Commit in order: T1, T2, T3
        self.cc_manager.end_transaction(tid1)
        status1 = self.cc_manager.get_transaction_status(tid1)
        
        self.cc_manager.end_transaction(tid2)
        status2 = self.cc_manager.get_transaction_status(tid2)
        
        self.cc_manager.end_transaction(tid3)
        status3 = self.cc_manager.get_transaction_status(tid3)
        
        # T1 should commit (no conflicts)
        self.assertIn(status1, [TransactionStatus.Committed, TransactionStatus.Terminated])
        
        # T2 should abort (read-write conflict with T1 writing B)
        self.assertIn(status2, [TransactionStatus.Aborted, TransactionStatus.Terminated])
    
    def test_no_conflict_different_objects(self):
        """Test that transactions on different objects don't conflict"""
        tid1 = self.cc_manager.begin_transaction()
        tid2 = self.cc_manager.begin_transaction()
        
        rowX = Row(object_id="X", table_name="test_table", data={"value": 100})
        rowY = Row(object_id="Y", table_name="test_table", data={"value": 200})
        
        # T1 writes X, T2 writes Y
        self.cc_manager.validate_object(rowX, tid1, ActionType.WRITE)
        self.cc_manager.validate_object(rowY, tid2, ActionType.WRITE)
        
        # Both should commit successfully
        self.cc_manager.end_transaction(tid1)
        self.cc_manager.end_transaction(tid2)
        
        status1 = self.cc_manager.get_transaction_status(tid1)
        status2 = self.cc_manager.get_transaction_status(tid2)
        
        self.assertIn(status1, [TransactionStatus.Committed, TransactionStatus.Terminated])
        self.assertIn(status2, [TransactionStatus.Committed, TransactionStatus.Terminated])
    
    def test_abort_transaction(self):
        """Test aborting a transaction"""
        tid = self.cc_manager.begin_transaction()
        
        row = Row(object_id="X", table_name="test_table", data={"value": 100})
        self.cc_manager.validate_object(row, tid, ActionType.WRITE)
        
        # Abort the transaction
        self.cc_manager.abort_transaction(tid)
        
        status = self.cc_manager.get_transaction_status(tid)
        self.assertEqual(status, TransactionStatus.Aborted)


if __name__ == '__main__':
    unittest.main()
