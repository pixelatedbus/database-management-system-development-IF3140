import sys
import os
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from failure_recovery_manager.failure_recovery_manager import FailureRecovery
from failure_recovery_manager.fake_exec_result import ExecutionResult
from failure_recovery_manager.log import actiontype
import logging
logger = logging.getLogger(__name__)

# Map string actions to actiontype enum values
ACTION_MAP = {
    "start": 0,  # actiontype.start
    "write": 1,  # actiontype.write
    "commit": 2,  # actiontype.commit
    "abort": 3,  # actiontype.abort
    "checkpoint": 4  # actiontype.checkpoint
}

class AdapterFRM:
    def __init__(self, wal_size=50):
        """
        Initialize adapter with FRM singleton instance
        
        Args:
            wal_size (int): Size of write-ahead log before flushing to disk
        """
        self.frm = FailureRecovery(wal_size=wal_size)
        self.query_processor = None  # Will be set by QueryProcessor
    
    def log_operation(self, transaction_id: int, query: str, action: str, 
                     table_name: str, old_data=None, new_data=None) -> None:
        exec_result = ExecutionResult()
        exec_result.transaction_id = transaction_id
        exec_result.timestamp = datetime.now()
        exec_result.query = query
        # Convert string action to numeric actiontype value
        action_lower = action.lower()
        exec_result.action = ACTION_MAP.get(action_lower, action_lower)
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
        # Note: WAL is kept in memory, will be flushed on checkpoint or commit
    
    def log_write(self, transaction_id: int, query: str, table_name: str, 
                  old_data=None, new_data=None) -> None:
        """Log a write operation (INSERT/UPDATE/DELETE)"""
        # Log the operation first
        self.log_operation(
            transaction_id=transaction_id,
            query=query,
            action="write",
            table_name=table_name,
            old_data=old_data,
            new_data=new_data
        )
        
        # Check if we need to checkpoint AFTER adding new log
        if len(self.frm.mem_wal) >= self.frm.wal_size:
            logger.info(f"[FRM] WAL size ({len(self.frm.mem_wal)}) reached threshold ({self.frm.wal_size}). Creating checkpoint...")
            self._create_checkpoint()
    
    def _flush_to_disk(self) -> None:
        """Force immediate flush of WAL to disk"""
        if self.frm.mem_wal:
            for info in self.frm.mem_wal:
                self.frm.logFile.write_log_execRes(info)
            self.frm.mem_wal = []
    
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
    
    def recover_transaction(self, transaction_id: int) -> None:
        """
        Recover (undo) a transaction by reverting all changes that were flushed to storage.
        This is called when a transaction aborts after checkpoint has flushed some operations.
        
        Args:
            transaction_id: ID of transaction to recover
        """
        logger.info(f"[FRM] Recovering transaction {transaction_id}...")
        
        # Get undo operations from FRM
        data_execs = self.frm._recover_transaction(transaction_id)
        
        if not data_execs:
            logger.info(f"[FRM] No storage operations to undo for transaction {transaction_id}")
            return
        
        # Apply undo operations to storage
        adapter_storage = self.query_processor.adapter_storage
        
        logger.info(f"[FRM] Applying {len(data_execs)} undo operation(s) to storage")
        
        for data_exec in data_execs:
            # Get table name - DataDeletion uses 'table' attribute, not 'table_name'
            if hasattr(data_exec, 'table_name'):
                table_name = data_exec.table_name
            elif hasattr(data_exec, 'table'):
                table_name = data_exec.table
            else:
                print(f"[FRM RECOVERY]     ERROR: No table attribute found!")
                continue
            
            # Determine operation type and execute
            from storage_manager.models import DataWrite, DataUpdate, DataDeletion
            
            if isinstance(data_exec, DataWrite):
                # Undo INSERT: DELETE the inserted row
                columns = list(data_exec.data.keys())
                values = [data_exec.data[col] for col in columns]
                # Create condition to match exactly this row
                conditions = [(col, "=", val) for col, val in zip(columns, values)]
                adapter_storage.delete_data(
                    table_name=table_name,
                    conditions=conditions,
                    transaction_id=transaction_id
                )
                logger.info(f"[FRM RECOVERY] Undid INSERT into '{table_name}': {data_exec.data}")
            
            elif isinstance(data_exec, DataUpdate):
                # Undo UPDATE: restore old values
                adapter_storage.update_data(
                    table_name=table_name,
                    old_data=data_exec.new_data,  # Current state in storage
                    new_data=data_exec.old_data,  # Restore to old state
                    transaction_id=transaction_id
                )
                logger.info(f"[FRM RECOVERY] Undid UPDATE in '{table_name}': {data_exec.new_data} -> {data_exec.old_data}")
            
            elif isinstance(data_exec, DataDeletion):
                # DataDeletion from to_data_undo() means: delete this row to undo an INSERT
                # The conditions specify which row to delete
                adapter_storage.delete_data(
                    table_name=table_name,
                    conditions=data_exec.conditions,
                    transaction_id=transaction_id
                )
                logger.info(f"[FRM RECOVERY] Undid INSERT by deleting from '{table_name}'")
        
        logger.info(f"[FRM] Recovery completed for transaction {transaction_id}: {len(data_execs)} operation(s) undone")
    
    def get_undo_list(self):
        """Get list of transactions that need to be undone"""
        return self.frm.undo_list
    
    def recover(self, criteria=None):
        """Trigger recovery process"""
        return self.frm.recover(criteria)
    
    def _create_checkpoint(self) -> None:
        """Create a checkpoint: flush all buffered transactions and write checkpoint log"""
        # Flush all current WAL entries to disk first
        self._flush_to_disk()
        
        # Commit all buffered transactions to storage
        if self.query_processor:
            self._commit_all_buffered_transactions()
        
        # Create checkpoint log entry
        self.frm._save_checkpoint()
        
        logger.info(f"[FRM] Checkpoint created. Undo list: {self.frm.undo_list}")
    
    def _commit_all_buffered_transactions(self) -> None:
        """Commit all transactions that have buffered operations to storage"""
        transaction_buffer = self.query_processor.query_execution_engine.transaction_buffer
        
        # Get all active transaction IDs with buffered operations
        active_transactions = list(transaction_buffer.buffers.keys())
        
        if not active_transactions:
            logger.info("[FRM] No buffered transactions to commit for checkpoint")
            return
        
        logger.info(f"[FRM] Committing {len(active_transactions)} buffered transaction(s) for checkpoint: {active_transactions}")
        
        from collections import defaultdict
        adapter_storage = self.query_processor.adapter_storage
        
        for t_id in active_transactions:
            buffered_ops = transaction_buffer.get_buffered_operations(t_id)
            
            if not buffered_ops:
                continue
            
            # Group operations by table and type
            ops_by_table_type = defaultdict(list)
            for op in buffered_ops:
                key = (op.table_name, op.operation_type)
                ops_by_table_type[key].append(op)
            
            # Execute all buffered operations
            for (table_name, op_type), ops in ops_by_table_type.items():
                if op_type == "INSERT":
                    for op in ops:
                        columns = list(op.data.keys())
                        values = [op.data[col] for col in columns]
                        adapter_storage.write_data(
                            table_name=op.table_name,
                            columns=columns,
                            values=values,
                            conditions=[],
                            transaction_id=t_id
                        )
                    logger.info(f"[FRM CHECKPOINT]   Flushed {len(ops)} INSERT(s) into '{table_name}' for T{t_id}")
                
                elif op_type == "UPDATE":
                    # Collapse chained updates
                    state_map = {}
                    for op in ops:
                        if op.old_data:
                            pk_field = list(op.old_data.keys())[0]
                            pk_value = op.old_data[pk_field]
                            row_id = (pk_field, pk_value)
                        else:
                            row_id = tuple(sorted(op.old_data.items()))
                        
                        if row_id not in state_map:
                            state_map[row_id] = (op.old_data, op.data)
                        else:
                            first_old, _ = state_map[row_id]
                            state_map[row_id] = (first_old, op.data)
                    
                    old_data_list = [old for old, _ in state_map.values()]
                    new_data_list = [new for _, new in state_map.values()]
                    
                    rows_updated = adapter_storage.batch_update_data(
                        table_name=table_name,
                        old_data_list=old_data_list,
                        new_data_list=new_data_list,
                        transaction_id=t_id
                    )
                    logger.info(f"[FRM CHECKPOINT]   Flushed {rows_updated} UPDATE(s) in '{table_name}' for T{t_id}")
                
                elif op_type == "DELETE":
                    for op in ops:
                        adapter_storage.delete_data(
                            table_name=op.table_name,
                            conditions=op.conditions,
                            transaction_id=t_id
                        )
                    logger.info(f"[FRM CHECKPOINT]   Flushed {len(ops)} DELETE(s) from '{table_name}' for T{t_id}")
            
            # Clear buffer for this transaction after flushing to storage
            transaction_buffer.clear_transaction(t_id)
            logger.info(f"[FRM CHECKPOINT] Cleared buffer for transaction {t_id}")
