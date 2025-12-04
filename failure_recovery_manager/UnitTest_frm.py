import unittest
import sys
from datetime import datetime
from unittest.mock import MagicMock
from typing import List


# Cara Run python3 -m failure_recovery_manager.UnitTest_frm

# ==========================================
# 1. MOCK OBJECTS & DATA STRUCTURES
# ==========================================
class ExecutionResult:
    def __init__(self, transaction_id, action, timestamp, old_data=None, new_data=None, table_name="T", query=""):
        self.transaction_id = transaction_id
        self.action = action
        self.timestamp = timestamp
        self.old_data = old_data if old_data is not None else []
        self.new_data = new_data if new_data is not None else []
        self.table_name = table_name
        self.query = query
    
    def __repr__(self): 
        return f"ExecRes(id={self.transaction_id}, act={self.action})"

# Mock dependencies
sys.modules["storage_manager.models"] = MagicMock()
sys.modules["failure_recovery_manager.fake_exec_result"] = MagicMock()
sys.modules["failure_recovery_manager.fake_exec_result"].ExecutionResult = ExecutionResult

from failure_recovery_manager.failure_recovery_manager import FailureRecovery
from failure_recovery_manager.log import actiontype, log

class TestFailureRecoveryManager(unittest.TestCase):

    def setUp(self):
        FailureRecovery._instance = None
        FailureRecovery._initialized = False
        self.frm = FailureRecovery(wal_size=3)
        
        # Mock LogFile
        self.mock_logfile = MagicMock()
        self.frm.logFile = self.mock_logfile
        self.virtual_disk = []

        def fake_write(entry):
            self.virtual_disk.append(entry)
        def fake_get_logs():
            return self.virtual_disk

        self.mock_logfile.write_log_execRes.side_effect = fake_write
        self.mock_logfile.get_logs.side_effect = fake_get_logs

    # SPESIFIKASI 1: BUFFERING
    def test_write_log_memory_buffering(self):
        self.frm.write_log(ExecutionResult(1, actiontype.write, datetime.now()))
        self.assertEqual(len(self.frm.mem_wal), 1)
        self.assertEqual(len(self.virtual_disk), 0)

    # SPESIFIKASI 2: FLUSH ON COMMIT
    def test_write_log_flush_on_commit(self):
        self.frm.write_log(ExecutionResult(1, actiontype.write, datetime.now()))
        self.frm.write_log(ExecutionResult(1, actiontype.commit, datetime.now()))
        self.assertEqual(len(self.frm.mem_wal), 0)
        self.assertEqual(len(self.virtual_disk), 2)

    # SPESIFIKASI 3: FLUSH ON LIMIT
    def test_wal_size_limit_trigger_flush(self):
        self.frm.wal_size = 2
        self.frm.write_log(ExecutionResult(1, actiontype.write, datetime.now()))
        self.assertEqual(len(self.virtual_disk), 0)
        self.frm.write_log(ExecutionResult(1, actiontype.write, datetime.now()))
        self.assertEqual(len(self.virtual_disk), 2)
        self.assertEqual(len(self.frm.mem_wal), 0)

    # SPESIFIKASI 4: ATOMICITY (Crash Recovery)
    def test_recover_uncommitted_transaction(self):
        tx_id = 500
        self.virtual_disk = [
            log(tx_id, actiontype.start, datetime.now()),
            log(tx_id, actiontype.write, datetime.now())
        ]
        self.frm.recover_system_crash()
        last_log = self.virtual_disk[-1]
        self.assertEqual(last_log.action, actiontype.abort)
        self.assertEqual(last_log.transaction_id, tx_id)

    # SPESIFIKASI 5: DURABILITY
    def test_durability_committed_transaction(self):
        tx_id = 600
        self.virtual_disk = [
            log(tx_id, actiontype.start, datetime.now()),
            log(tx_id, actiontype.write, datetime.now()),
            log(tx_id, actiontype.commit, datetime.now())
        ]
        initial_count = len(self.virtual_disk)
        self.frm.recover_system_crash()
        self.assertEqual(len(self.virtual_disk), initial_count)

    # SPESIFIKASI 6: MIXED SCENARIO
    def test_mixed_transaction_scenario(self):
        self.virtual_disk = [
            log(1, actiontype.start, datetime.now()),
            log(1, actiontype.commit, datetime.now()),
            log(2, actiontype.start, datetime.now()),
            log(2, actiontype.write, datetime.now())
        ]
        self.frm.recover_system_crash()
        last_log = self.virtual_disk[-1]
        self.assertEqual(last_log.transaction_id, 2)
        self.assertEqual(last_log.action, actiontype.abort)
        # Pastikan T1 tidak di-abort
        aborts = [l.transaction_id for l in self.virtual_disk if l.action == actiontype.abort]
        self.assertNotIn(1, aborts)

    # SPESIFIKASI 7: IDEMPOTENCY
    def test_recovery_idempotency(self):
        self.virtual_disk = [
            log(20, actiontype.start, datetime.now()),
            log(20, actiontype.write, datetime.now())
        ]
        self.frm.recover_system_crash()
        count_1 = len(self.virtual_disk)
        self.assertEqual(self.virtual_disk[-1].action, actiontype.abort)
        
        self.frm.recover_system_crash()
        count_2 = len(self.virtual_disk)
        self.assertEqual(count_1, count_2)

    # SPESIFIKASI 8: CHECKPOINTING
    def test_save_checkpoint(self):
        self.frm.write_log(ExecutionResult(99, actiontype.write, datetime.now()))
        self.frm._save_checkpoint()
        self.assertEqual(len(self.frm.mem_wal), 0) # RAM harus bersih
        self.assertEqual(self.virtual_disk[-1].action, actiontype.checkpoint)

    # SPESIFIKASI 9: RUNTIME ROLLBACK
    def test_recover_specific_transaction(self):
        self.virtual_disk = [
            log(1, actiontype.start, datetime.now()), # T1
            log(2, actiontype.start, datetime.now())  # T2
        ]
        self.frm._recover_transaction(1) # Undo T1 saja
        
        last_log = self.virtual_disk[-1]
        self.assertEqual(last_log.transaction_id, 1)
        self.assertEqual(last_log.action, actiontype.abort)
        
        # Cek T2 tidak kena dampaknya
        t2_aborts = [l for l in self.virtual_disk if l.transaction_id == 2 and l.action == actiontype.abort]
        self.assertEqual(len(t2_aborts), 0)

if __name__ == '__main__':
    unittest.main()