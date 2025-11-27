"""
Transaction Buffer for Write-Ahead Logging
Buffers write operations per transaction until COMMIT
"""

from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field


@dataclass
class BufferedOperation:
    operation_type: str  # "INSERT", "UPDATE", "DELETE"
    table_name: str
    data: Dict[str, Any]  # Row data
    conditions: List[Any] = field(default_factory=list)  # For UPDATE/DELETE
    old_data: Optional[Dict[str, Any]] = None  # For UPDATE rollback


class TransactionBuffer:

    def __init__(self):
        self.buffers: Dict[int, List[BufferedOperation]] = {}
        self.uncommitted_data: Dict[int, Dict[str, List[Dict[str, Any]]]] = {}
    
    def start_transaction(self, transaction_id: int):
        self.buffers[transaction_id] = []
        self.uncommitted_data[transaction_id] = {}
        print(f"[BUFFER] Started buffer for transaction {transaction_id}")
    
    def buffer_insert(self, transaction_id: int, table_name: str, row_data: Dict[str, Any]):
        if transaction_id not in self.buffers:
            self.start_transaction(transaction_id)
        
        operation = BufferedOperation(
            operation_type="INSERT",
            table_name=table_name,
            data=row_data
        )
        self.buffers[transaction_id].append(operation)
        
        if table_name not in self.uncommitted_data[transaction_id]:
            self.uncommitted_data[transaction_id][table_name] = []
        self.uncommitted_data[transaction_id][table_name].append(row_data.copy())
        
        print(f"[BUFFER] Buffered INSERT into '{table_name}' for transaction {transaction_id}")
    
    def buffer_update(self, transaction_id: int, table_name: str, 
                     old_data: Dict[str, Any], new_data: Dict[str, Any], 
                     conditions: List[Any]):
        if transaction_id not in self.buffers:
            self.start_transaction(transaction_id)
        
        operation = BufferedOperation(
            operation_type="UPDATE",
            table_name=table_name,
            data=new_data,
            conditions=conditions,
            old_data=old_data
        )
        self.buffers[transaction_id].append(operation)
        
        if table_name not in self.uncommitted_data[transaction_id]:
            self.uncommitted_data[transaction_id][table_name] = []
        
        uncommitted_rows = self.uncommitted_data[transaction_id][table_name]
        for i, row in enumerate(uncommitted_rows):
            if self._matches_conditions(row, old_data):
                uncommitted_rows[i] = new_data.copy()
        
        print(f"[BUFFER] Buffered UPDATE on '{table_name}' for transaction {transaction_id}")
    
    def buffer_delete(self, transaction_id: int, table_name: str, 
                     row_data: Dict[str, Any], conditions: List[Any]):
        if transaction_id not in self.buffers:
            self.start_transaction(transaction_id)
        
        operation = BufferedOperation(
            operation_type="DELETE",
            table_name=table_name,
            data=row_data,
            conditions=conditions
        )
        self.buffers[transaction_id].append(operation)
        
        if table_name in self.uncommitted_data[transaction_id]:
            uncommitted_rows = self.uncommitted_data[transaction_id][table_name]
            self.uncommitted_data[transaction_id][table_name] = [
                row for row in uncommitted_rows 
                if not self._matches_conditions(row, row_data)
            ]
        
        print(f"[BUFFER] Buffered DELETE from '{table_name}' for transaction {transaction_id}")
    
    def get_buffered_operations(self, transaction_id: int) -> List[BufferedOperation]:
        """Get all buffered operations for a transaction"""
        return self.buffers.get(transaction_id, [])
    
    def get_uncommitted_data(self, transaction_id: int, table_name: str) -> List[Dict[str, Any]]:
        """Get uncommitted data for a table in this transaction"""
        if transaction_id not in self.uncommitted_data:
            return []
        return self.uncommitted_data[transaction_id].get(table_name, [])
    
    def clear_transaction(self, transaction_id: int):
        """Clear buffer for a transaction (on COMMIT or ABORT)"""
        if transaction_id in self.buffers:
            del self.buffers[transaction_id]
        if transaction_id in self.uncommitted_data:
            del self.uncommitted_data[transaction_id]
        print(f"[BUFFER] Cleared buffer for transaction {transaction_id}")
    
    def _matches_conditions(self, row: Dict[str, Any], target: Dict[str, Any]) -> bool:
        """Check if row matches target data (for finding rows to update/delete)"""
        for key, value in target.items():
            if key in row and row[key] != value:
                return False
        return True
