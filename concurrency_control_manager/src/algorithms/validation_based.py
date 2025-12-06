"""
Validation-based (Optimistic) concurrency control algorithm
"""

from typing import Set, Dict
from .base import ConcurrencyAlgorithm
from ..transaction import Transaction
from ..row import Row
from ..enums import ActionType, TransactionStatus
from ..response import AlgorithmResponse


class Validator:
    """Validates transactions for optimistic concurrency control"""
    
    def __init__(self):
        self.active_transactions: Set[int] = set()
        self.read_sets: Dict[int, Set[str]] = {}
        self.write_sets: Dict[int, Set[str]] = {}
    
    def validate_transaction(self, transaction: Transaction) -> bool:
        """Validate a transaction before commit"""
        transaction_id = transaction.transaction_id
        
        # Check for conflicts with all other active transactions
        for other_id in self.active_transactions:
            if other_id != transaction_id:
                if self.detect_conflict(transaction_id, other_id):
                    return False
        
        return True
    
    def record_read(self, transaction_id: int, object_id: str) -> None:
        """Record a read operation"""
        if transaction_id not in self.read_sets:
            self.read_sets[transaction_id] = set()
        self.read_sets[transaction_id].add(object_id)
        
        self.active_transactions.add(transaction_id)
    
    def record_write(self, transaction_id: int, object_id: str) -> None:
        """Record a write operation"""
        if transaction_id not in self.write_sets:
            self.write_sets[transaction_id] = set()
        self.write_sets[transaction_id].add(object_id)
        
        self.active_transactions.add(transaction_id)
    
    def detect_conflict(self, transaction_id: int, other_id: int) -> bool:
        """Detect conflict between two transactions"""
        if self.has_read_write_conflict(transaction_id, other_id):
            return True
        if self.has_read_write_conflict(other_id, transaction_id):
            return True
        
        if self.has_write_write_conflict(transaction_id, other_id):
            return True
        
        return False
    
    def has_read_write_conflict(self, t1: int, t2: int) -> bool:
        """Check for read-write conflict (t1 reads what t2 writes)"""
        if t1 not in self.read_sets or t2 not in self.write_sets:
            return False
        
        # Conflict if t1's read set intersects with t2's write set
        return bool(self.read_sets[t1] & self.write_sets[t2])
    
    def has_write_write_conflict(self, t1: int, t2: int) -> bool:
        """Check for write-write conflict"""
        if t1 not in self.write_sets or t2 not in self.write_sets:
            return False
        
        return bool(self.write_sets[t1] & self.write_sets[t2])
    
    def clear_transaction(self, transaction_id: int) -> None:
        """Clear transaction data from validator"""
        self.active_transactions.discard(transaction_id)
        self.read_sets.pop(transaction_id, None)
        self.write_sets.pop(transaction_id, None)


class ValidationBasedAlgorithm(ConcurrencyAlgorithm):
    """Validation-based (Optimistic) concurrency control algorithm implementation"""
    
    def __init__(self):
        self.validator: Validator = Validator()
    
    def check_permission(self, t: Transaction, obj: Row, 
                        action: ActionType) -> AlgorithmResponse:
        """Check permission (always allowed in read/write phase)"""
        transaction_id = t.transaction_id
        object_id = obj.object_id
        
        # Conflicts are detected only during validation phase
        if action == ActionType.READ:
            self.validator.record_read(transaction_id, object_id)
            return AlgorithmResponse(
                allowed=True,
                message=f"T{transaction_id} reads {object_id} - allowed (optimistic)"
            )
        elif action == ActionType.WRITE:
            self.validator.record_read(transaction_id, object_id)
            self.validator.record_write(transaction_id, object_id)
            return AlgorithmResponse(
                allowed=True,
                message=f"T{transaction_id} writes {object_id} - allowed (optimistic)"
            )
        else:
            return AlgorithmResponse(
                allowed=False,
                message=f"Unknown action type: {action}"
            )
    
    def commit_transaction(self, t: Transaction) -> None:
        """Commit transaction after validation"""
        from datetime import datetime
        
        t.validation_timestamp = datetime.now()
        
        # Perform validation
        if self.validation_phase(t):
            # commit n clear the transaction data if success
            t.set_status(TransactionStatus.Committed)
            self.validator.clear_transaction(t.transaction_id)
        else:
            t.set_status(TransactionStatus.Aborted)
            self.abort_transaction(t)
    
    def abort_transaction(self, t: Transaction) -> None:
        """Abort transaction"""
        
        t.set_status(TransactionStatus.Aborted)
        self.validator.clear_transaction(t.transaction_id)
    
    def validation_phase(self, t: Transaction) -> bool:
        """Perform validation phase before commit"""
        return self.validator.validate_transaction(t)
