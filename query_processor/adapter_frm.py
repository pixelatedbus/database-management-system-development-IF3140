import sys
import os
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from failure_recovery_manager.failure_recovery_manager import FailureRecovery
from failure_recovery_manager.fake_exec_result import ExecutionResult
import logging
logger = logging.getLogger(__name__)

class AdapterFRM:
    """
    Adapter for Failure Recovery Manager
    Provides interface between Query Processor and FRM for write-ahead logging
    """
    
    def __init__(self, wal_size=50):
        """
        Initialize adapter with FRM singleton instance
        
        Args:
            wal_size (int): Size of write-ahead log before flushing to disk
        """
        self.frm = FailureRecovery(wal_size=wal_size)
    
    def log_operation(self, transaction_id: int, query: str, action: str, 
                     table_name: str, old_data=None, new_data=None) -> None:
        exec_result = ExecutionResult()
        exec_result.transaction_id = transaction_id
        exec_result.timestamp = datetime.now()
        exec_result.query = query
        exec_result.action = action.lower()
        exec_result.table_name = table_name
        exec_result.old_data = old_data if old_data is not None else []
        exec_result.new_data = new_data if new_data is not None else []
        exec_result.message = f"Transaction {transaction_id}: {action} on {table_name}"
        
        self.frm.write_log(exec_result)
        logger.info(f"[FRM] Logged {action} for transaction {transaction_id} on table '{table_name}'")
    
    def log_begin(self, transaction_id: int) -> None:
        """Log transaction start"""
        self.log_operation(
            transaction_id=transaction_id,
            query="BEGIN TRANSACTION",
            action="start",
            table_name="",
            old_data=None,
            new_data=None
        )
    
    def log_write(self, transaction_id: int, query: str, table_name: str, 
                  old_data=None, new_data=None) -> None:
        """Log a write operation (INSERT/UPDATE/DELETE)"""
        self.log_operation(
            transaction_id=transaction_id,
            query=query,
            action="write",
            table_name=table_name,
            old_data=old_data,
            new_data=new_data
        )
    
    def log_commit(self, transaction_id: int) -> None:
        """Log transaction commit - this flushes WAL to disk"""
        self.log_operation(
            transaction_id=transaction_id,
            query="COMMIT",
            action="commit",
            table_name="",
            old_data=None,
            new_data=None
        )
    
    def log_abort(self, transaction_id: int) -> None:
        """Log transaction abort"""
        self.log_operation(
            transaction_id=transaction_id,
            query="ABORT",
            action="abort",
            table_name="",
            old_data=None,
            new_data=None
        )
    
    def get_undo_list(self):
        """Get list of transactions that need to be undone"""
        return self.frm.undo_list
    
    def recover(self, criteria=None):
        """Trigger recovery process"""
        return self.frm.recover(criteria)
