import sys
import os
import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from misc import *
from query_optimizer import *
from .adapter_ccm import AdapterCCM, AlgorithmType
from .adapter_storage import AdapterStorage
from .adapter_optimizer import AdapterOptimizer
from .query_execution import QueryExecution

class Rows:
    def __init__(self, data: list, rows_count: int = 0):
        self.data: list = data
        self.rows_count: int = rows_count or len(data)

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
        # TODO: Inisialisasi Failure Recovery Manager (FRM) di sini
        self.adapter_ccm = AdapterCCM(algorithm=AlgorithmType.LockBased)
        self.adapter_storage = AdapterStorage()
        self.query_execution_engine = QueryExecution(self.adapter_storage, self.adapter_ccm)

        self.adapter_optimizer = AdapterOptimizer() 
        
        self.active_transactions = {}
        

    def execute_query(self, query: str, client_id: int):
        try:
            query_tree = self._get_query_tree(query)
            query_type = query_tree.type
        except Exception as e:
            return ExecutionResult(f"Error parsing/validating query: {e}", success=False, query=query)
        
        t_id = self.active_transactions.get(client_id)
        
        if query_type == "BEGIN_TRANSACTION":
            if t_id:
                return ExecutionResult("Transaction already active for this client.", success=False, transaction_id=t_id, query=query)
            new_t_id = self.adapter_ccm.begin_transaction()
            self.active_transactions[client_id] = new_t_id
            return ExecutionResult(
                message=f"Transaction started. ID={new_t_id}", 
                transaction_id=new_t_id,
                query=query
            )
        
        elif query_type in ["COMMIT", "ABORT"]:
            if not t_id:
                return ExecutionResult("No active transaction to commit/abort.", success=False, query=query)
            
            if query_type == "COMMIT":
                self.adapter_ccm.commit_transaction(t_id)
            else:
                self.adapter_ccm.abort_transaction(t_id)
                
            del self.active_transactions[client_id]
            # TODO: Interaksi dengan Failure Recovery Manager (FRM) harus dilakukan di sini
            return ExecutionResult(
                message=f"Transaction {t_id} {query_type}ed successfully.",
                transaction_id=t_id,
                query=query
            )

        else:
            if not t_id:
                return ExecutionResult("Query requires an active transaction. Use BEGIN_TRANSACTION.", success=False, query=query)
            
            # TODO: Implementasi Optimisasi Query di sini
            
            try:
                result_rows = self.query_execution_engine.execute_node(query_tree, t_id)
                
                # TODO: Logging ke FRM harus dilakukan di sini setelah berhasil (sebelum COMMIT)
                
                return ExecutionResult(
                    message="Query executed successfully.", 
                    data=result_rows or Rows([]), 
                    transaction_id=t_id,
                    query=query
                )
            except Exception as e:
                self.adapter_ccm.abort_transaction(t_id)
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