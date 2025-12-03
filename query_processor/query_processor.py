import sys
import os
import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from storage_manager import Rows
from .adapter_ccm import AdapterCCM, AlgorithmType
from .adapter_storage import AdapterStorage
from .adapter_optimizer import AdapterOptimizer
from .adapter_frm import AdapterFRM
from .query_execution import QueryExecution, TransactionAbortedException

class ExecutionResult:
    def __init__(self, message: str, success: bool = True, data: Rows = Rows([]), transaction_id: int = -1, query: str = ""):
        self.transaction_id: int = transaction_id
        self.data: Rows = data
        self.timestamp: datetime.datetime = datetime.datetime.now()
        self.message: str = message
        self.query: str = query
        self.success = success
        
class QueryProcessor:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(QueryProcessor, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        # Initialize Failure Recovery Manager
        self.adapter_frm = AdapterFRM(wal_size=50)
        self.adapter_ccm = AdapterCCM(algorithm=AlgorithmType.LockBased)
        self.adapter_storage = AdapterStorage()
        self.query_execution_engine = QueryExecution(self.adapter_storage, self.adapter_ccm, frm_adapter=self.adapter_frm)

        self.adapter_optimizer = AdapterOptimizer() 
        
        self.active_transactions = {}
        

    def execute_query(self, query: str, client_id: int):
        try:
            parsed_result = self._get_query_tree(query)
            query_tree = parsed_result.query_tree
            query_type = query_tree.type
        except Exception as e:
            return ExecutionResult(f"Error parsing/validating query: {e}", success=False, query=query)
        
        t_id = self.active_transactions.get(client_id)
        
        if query_type == "BEGIN_TRANSACTION":
            if t_id:
                return ExecutionResult("Transaction already active for this client.", success=False, transaction_id=t_id, query=query)
            new_t_id = self.adapter_ccm.begin_transaction()
            self.active_transactions[client_id] = new_t_id
            
            # Initialize transaction buffer
            self.query_execution_engine.transaction_buffer.start_transaction(new_t_id)
            
            # Log transaction start to FRM
            self.adapter_frm.log_begin(new_t_id)
            
            return ExecutionResult(
                message=f"Transaction started. ID={new_t_id}", 
                transaction_id=new_t_id,
                query=query
            )
        
        elif query_type in ["COMMIT", "ABORT"]:
            if not t_id:
                return ExecutionResult("No active transaction to commit/abort.", success=False, query=query)
            
            if query_type == "COMMIT":
                # Flush buffered operations to storage
                print(f"\n[COMMIT] Flushing buffered operations for transaction {t_id}...")
                buffered_ops = self.query_execution_engine.transaction_buffer.get_buffered_operations(t_id)
                print(f"[COMMIT] Found {len(buffered_ops)} buffered operation(s)")
                
                # Group operations by table and type for batch processing
                from collections import defaultdict
                ops_by_table_type = defaultdict(list)
                for op in buffered_ops:
                    key = (op.table_name, op.operation_type)
                    ops_by_table_type[key].append(op)
                
                # Process operations by type
                for (table_name, op_type), ops in ops_by_table_type.items():
                    if op_type == "INSERT":
                        # Batch INSERT all rows at once
                        for op in ops:
                            columns = list(op.data.keys())
                            values = [op.data[col] for col in columns]
                            self.adapter_storage.write_data(
                                table_name=op.table_name,
                                columns=columns,
                                values=values,
                                conditions=[],
                                transaction_id=t_id
                            )
                        print(f"[COMMIT]   Executed {len(ops)} INSERT(s) into '{table_name}'")
                    
                    elif op_type == "UPDATE":
                        # Collapse chained updates: old1->new1, new1->new2 becomes old1->new2
                        # Simple approach: iterate sequentially, track each unique starting point
                        
                        # Build map of: final_state_key -> (original_old_data, final_new_data)
                        # where we track transformations by following chains
                        
                        state_map = {}  # Maps row identity -> (first_old_data, latest_new_data)
                        
                        for op in ops:
                            # Use primary key (first field) as row identity
                            if op.old_data:
                                pk_field = list(op.old_data.keys())[0]
                                pk_value = op.old_data[pk_field]
                                row_id = (pk_field, pk_value)
                            else:
                                row_id = tuple(sorted(op.old_data.items()))
                            
                            if row_id not in state_map:
                                # First update to this row
                                state_map[row_id] = (op.old_data, op.data)
                                print(f"[COMMIT]     New row update: {row_id} (old->new)")
                            else:
                                # Subsequent update to same row - keep first old, update to latest new
                                first_old, _ = state_map[row_id]
                                state_map[row_id] = (first_old, op.data)
                                print(f"[COMMIT]     Chained update: {row_id} (keeping original old, updating to latest new)")
                        
                        old_data_list = [old for old, _ in state_map.values()]
                        new_data_list = [new for _, new in state_map.values()]
                        
                        rows_updated = self.adapter_storage.batch_update_data(
                            table_name=table_name,
                            old_data_list=old_data_list,
                            new_data_list=new_data_list,
                            transaction_id=t_id
                        )
                        print(f"[COMMIT]   Batch updated {rows_updated} row(s) in '{table_name}' "
                              f"(collapsed {len(ops)} operations -> {len(state_map)} unique rows)")
                    
                    elif op_type == "DELETE":
                        # Execute DELETE operations
                        for op in ops:
                            self.adapter_storage.delete_data(
                                table_name=op.table_name,
                                conditions=op.conditions,
                                transaction_id=t_id
                            )
                        print(f"[COMMIT]   Executed {len(ops)} DELETE(s) from '{table_name}'")
                
                # Clear buffer for this transaction
                self.query_execution_engine.transaction_buffer.clear_transaction(t_id)
                
                # Log commit to FRM (flushes WAL to disk)
                self.adapter_frm.log_commit(t_id)
                self.adapter_ccm.commit_transaction(t_id)
            else:
                # ABORT - discard buffered operations
                print(f"\n[ABORT] Discarding buffered operations for transaction {t_id}...")
                buffered_ops = self.query_execution_engine.transaction_buffer.get_buffered_operations(t_id)
                print(f"[ABORT] Discarded {len(buffered_ops)} buffered operation(s)")
                
                # Clear buffer for this transaction
                self.query_execution_engine.transaction_buffer.clear_transaction(t_id)
                
                # Log abort to FRM
                self.adapter_frm.log_abort(t_id)
                self.adapter_ccm.abort_transaction(t_id)
                
            del self.active_transactions[client_id]
            return ExecutionResult(
                message=f"Transaction {t_id} {query_type}ed successfully.",
                transaction_id=t_id,
                query=query
            )

        else:            
            # TODO: Implementasi Optimisasi Query di sini
            
            # Check if we're in an explicit transaction or auto-commit mode
            in_explicit_transaction = t_id is not None
            
            if not in_explicit_transaction:
                # AUTO-COMMIT MODE: Create transaction just for this query
                t_id = self.adapter_ccm.begin_transaction()
                self.query_execution_engine.transaction_buffer.start_transaction(t_id)
                self.adapter_frm.log_begin(t_id)
                print(f"\n[AUTO-COMMIT] Created transaction {t_id} for single query")
            
            try:
                result_rows = self.query_execution_engine.execute_node(query_tree, t_id)
                
                # TODO: Logging ke FRM harus dilakukan di sini setelah berhasil (sebelum COMMIT)
                
                # Wrap result in Rows if it's a list
                if isinstance(result_rows, list):
                    result_data = Rows(result_rows)
                else:
                    result_data = result_rows or Rows([])
                
                # AUTO-COMMIT: Immediately commit single-query transaction
                if not in_explicit_transaction:
                    print(f"\n[AUTO-COMMIT] Committing transaction {t_id}...")
                    
                    # Flush buffered operations to storage
                    buffered_ops = self.query_execution_engine.transaction_buffer.get_buffered_operations(t_id)
                    print(f"[AUTO-COMMIT] Found {len(buffered_ops)} buffered operation(s)")
                    
                    # Group operations by table and type for batch processing
                    from collections import defaultdict
                    ops_by_table_type = defaultdict(list)
                    for op in buffered_ops:
                        key = (op.table_name, op.operation_type)
                        ops_by_table_type[key].append(op)
                    
                    # Process operations by type
                    for (table_name, op_type), ops in ops_by_table_type.items():
                        if op_type == "INSERT":
                            # Batch INSERT
                            for op in ops:
                                columns = list(op.data.keys())
                                values = [op.data[col] for col in columns]
                                self.adapter_storage.write_data(
                                    table_name=op.table_name,
                                    columns=columns,
                                    values=values,
                                    conditions=[],
                                    transaction_id=t_id
                                )
                            print(f"[AUTO-COMMIT]   Executed {len(ops)} INSERT(s) into '{table_name}'")
                        
                        elif op_type == "UPDATE":
                            # Collapse sequential updates with chain detection
                            collapsed_updates = {}  # original_state_key -> (first_old_data, last_new_data)
                            
                            for op in ops:
                                original_key = tuple(sorted(op.old_data.items()))
                                
                                if original_key not in collapsed_updates:
                                    collapsed_updates[original_key] = (op.old_data, op.data)
                                else:
                                    first_old, _ = collapsed_updates[original_key]
                                    previous_new_key = tuple(sorted(op.old_data.items()))
                                    
                                    # Check for chained updates
                                    found_chain = False
                                    for orig_key, (orig_old, prev_new) in collapsed_updates.items():
                                        prev_new_key = tuple(sorted(prev_new.items()))
                                        if prev_new_key == previous_new_key:
                                            collapsed_updates[orig_key] = (orig_old, op.data)
                                            found_chain = True
                                            break
                                    
                                    if not found_chain:
                                        collapsed_updates[original_key] = (first_old, op.data)
                            
                            old_data_list = [old for old, _ in collapsed_updates.values()]
                            new_data_list = [new for _, new in collapsed_updates.values()]
                            
                            rows_updated = self.adapter_storage.batch_update_data(
                                table_name=table_name,
                                old_data_list=old_data_list,
                                new_data_list=new_data_list,
                                transaction_id=t_id
                            )
                            print(f"[AUTO-COMMIT]   Batch updated {rows_updated} row(s) in '{table_name}' "
                                  f"(collapsed {len(ops)} operations -> {len(collapsed_updates)} unique rows)")
                        
                        elif op_type == "DELETE":
                            # Execute DELETE operations
                            for op in ops:
                                self.adapter_storage.delete_data(
                                    table_name=op.table_name,
                                    conditions=op.conditions,
                                    transaction_id=t_id
                                )
                            print(f"[AUTO-COMMIT]   Executed {len(ops)} DELETE(s) from '{table_name}'")
                    
                    # Clear buffer and commit
                    self.query_execution_engine.transaction_buffer.clear_transaction(t_id)
                    self.adapter_frm.log_commit(t_id)
                    self.adapter_ccm.commit_transaction(t_id)
                    print(f"[AUTO-COMMIT] Transaction {t_id} committed successfully")
                
                return ExecutionResult(
                    message="Query executed successfully.", 
                    data=result_data, 
                    transaction_id=t_id,
                    query=query
                )
            except TransactionAbortedException as e:
                # Transaction was aborted by CCM (deadlock, etc.)
                print(f"\n[ABORT] Transaction {t_id} aborted by CCM: {e.reason}")
                
                # Clear buffer for aborted transaction
                self.query_execution_engine.transaction_buffer.clear_transaction(t_id)
                
                # Log abort to FRM
                self.adapter_frm.log_abort(t_id)
                
                # Remove from active transactions if it was explicit
                if in_explicit_transaction and client_id in self.active_transactions:
                    del self.active_transactions[client_id]
                
                return ExecutionResult(
                    message=f"Transaction {t_id} aborted: {e.reason}", 
                    success=False,
                    transaction_id=t_id,
                    query=query
                )
            except Exception as e:
                # Other errors - abort transaction
                print(f"\n[ERROR] Execution failed for transaction {t_id}: {e}")
                self.adapter_ccm.abort_transaction(t_id)
                
                # Clear buffer
                self.query_execution_engine.transaction_buffer.clear_transaction(t_id)
                
                # Remove from active transactions if it was explicit
                if in_explicit_transaction and client_id in self.active_transactions:
                    del self.active_transactions[client_id]
                
                return ExecutionResult(
                    message=f"Execution failed, Transaction {t_id} aborted: {e}", 
                    success=False,
                    transaction_id=t_id,
                    query=query
                )

    # ---------------------------- private methods ----------------------------
    def _get_query_tree(self, query:str):
        return self.adapter_optimizer.parse_optimized_query(query)