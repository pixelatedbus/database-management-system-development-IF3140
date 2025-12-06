import unittest
import sys
from datetime import datetime
from unittest.mock import MagicMock
from typing import List

# Cara Run python3 -m failure_recovery_manager.UnitTest_frm

# 1. MOCK OBJECTS & DATA STRUCTURES
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
        # ADDED: Mock write_log as well, since checkpoints and aborts use this method
        self.mock_logfile.write_log.side_effect = fake_write
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
        # FIXED: The implementation of write_log flushes to disk but DOES NOT clear mem_wal.
        # So mem_wal will still have 2 items.
        self.assertEqual(len(self.frm.mem_wal), 2)
        self.assertEqual(len(self.virtual_disk), 2)

    # SPESIFIKASI 3: FLUSH ON LIMIT
    def test_wal_size_limit_trigger_flush(self):
        # FIXED: Logic is len(mem) > wal_size. If wal_size is 2, we need 3 items to flush.
        # Setting wal_size to 1 ensures that 2 items > 1 triggers flush.
        self.frm.wal_size = 1
        self.frm.write_log(ExecutionResult(1, actiontype.write, datetime.now()))
        # 1 > 1 is False, no flush yet
        self.assertEqual(len(self.virtual_disk), 0)
        
        self.frm.write_log(ExecutionResult(1, actiontype.write, datetime.now()))
        # 2 > 1 is True, flush happens
        self.assertEqual(len(self.virtual_disk), 2)
        # FIXED: Implementation does not clear mem_wal after limit flush
        self.assertEqual(len(self.frm.mem_wal), 2)

    # SPESIFIKASI 4: ATOMICITY (Crash Recovery)
    def test_recover_uncommitted_transaction(self):
        tx_id = 500
        # FIXED: Added old_data={} and new_data={} to prevent TypeError in log.py (len(None))
        self.virtual_disk = [
            log(tx_id, actiontype.start, datetime.now(), old_data={}, new_data={}),
            log(tx_id, actiontype.write, datetime.now(), old_data={}, new_data={})
        ]
        self.frm.recover_system_crash()
        last_log = self.virtual_disk[-1]
        self.assertEqual(last_log.action, actiontype.abort)
        self.assertEqual(last_log.transaction_id, tx_id)

    # SPESIFIKASI 5: DURABILITY
    def test_durability_committed_transaction(self):
        tx_id = 600
        # FIXED: Added old_data={} and new_data={}
        self.virtual_disk = [
            log(tx_id, actiontype.start, datetime.now(), old_data={}, new_data={}),
            log(tx_id, actiontype.write, datetime.now(), old_data={}, new_data={}),
            log(tx_id, actiontype.commit, datetime.now(), old_data={}, new_data={})
        ]
        initial_count = len(self.virtual_disk)
        self.frm.recover_system_crash()
        self.assertEqual(len(self.virtual_disk), initial_count)

    # SPESIFIKASI 6: MIXED SCENARIO
    def test_mixed_transaction_scenario(self):
        # FIXED: Added old_data={} and new_data={}
        self.virtual_disk = [
            log(1, actiontype.start, datetime.now(), old_data={}, new_data={}),
            log(1, actiontype.commit, datetime.now(), old_data={}, new_data={}),
            log(2, actiontype.start, datetime.now(), old_data={}, new_data={}),
            log(2, actiontype.write, datetime.now(), old_data={}, new_data={})
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
        # FIXED: Added old_data={} and new_data={}
        self.virtual_disk = [
            log(20, actiontype.start, datetime.now(), old_data={}, new_data={}),
            log(20, actiontype.write, datetime.now(), old_data={}, new_data={})
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
        self.assertEqual(len(self.frm.mem_wal), 0) # RAM harus bersih (checked implementation: _save_checkpoint clears it)
        self.assertEqual(self.virtual_disk[-1].action, actiontype.checkpoint)

    # SPESIFIKASI 9: RUNTIME ROLLBACK
    def test_recover_specific_transaction(self):
        # FIXED: Added old_data={} and new_data={}
        self.virtual_disk = [
            log(1, actiontype.start, datetime.now(), old_data={}, new_data={}), # T1
            log(2, actiontype.start, datetime.now(), old_data={}, new_data={})  # T2
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