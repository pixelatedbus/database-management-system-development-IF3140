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
                
                for op in buffered_ops:
                    if op.operation_type == "INSERT":
                        # Execute buffered INSERT
                        columns = list(op.data.keys())
                        values = [op.data[col] for col in columns]
                        self.adapter_storage.write_data(
                            table_name=op.table_name,
                            columns=columns,
                            values=values,
                            conditions=[],
                            transaction_id=t_id
                        )
                        print(f"[COMMIT]   Executed INSERT into '{op.table_name}'")
                    
                    elif op.operation_type == "UPDATE":
                        # Execute buffered UPDATE
                        columns = list(op.data.keys())
                        values = [op.data[col] for col in columns]
                        self.adapter_storage.write_data(
                            table_name=op.table_name,
                            columns=columns,
                            values=values,
                            conditions=op.conditions,
                            transaction_id=t_id
                        )
                        print(f"[COMMIT]   Executed UPDATE on '{op.table_name}'")
                    
                    elif op.operation_type == "DELETE":
                        # Execute buffered DELETE
                        self.adapter_storage.delete_data(
                            table_name=op.table_name,
                            conditions=op.conditions,
                            transaction_id=t_id
                        )
                        print(f"[COMMIT]   Executed DELETE from '{op.table_name}'")
                
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
                    
                    for op in buffered_ops:
                        if op.operation_type == "INSERT":
                            columns = list(op.data.keys())
                            values = [op.data[col] for col in columns]
                            self.adapter_storage.write_data(
                                table_name=op.table_name,
                                columns=columns,
                                values=values,
                                conditions=[],
                                transaction_id=t_id
                            )
                            print(f"[AUTO-COMMIT]   Executed INSERT into '{op.table_name}'")
                        
                        elif op.operation_type == "UPDATE":
                            columns = list(op.data.keys())
                            values = [op.data[col] for col in columns]
                            self.adapter_storage.write_data(
                                table_name=op.table_name,
                                columns=columns,
                                values=values,
                                conditions=op.conditions,
                                transaction_id=t_id
                            )
                            print(f"[AUTO-COMMIT]   Executed UPDATE on '{op.table_name}'")
                        
                        elif op.operation_type == "DELETE":
                            self.adapter_storage.delete_data(
                                table_name=op.table_name,
                                conditions=op.conditions,
                                transaction_id=t_id
                            )
                            print(f"[AUTO-COMMIT]   Executed DELETE from '{op.table_name}'")
                    
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