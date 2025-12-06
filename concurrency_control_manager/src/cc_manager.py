"""
Concurrency Control Manager - Main interface
"""

from typing import Dict, Optional
from .enums import AlgorithmType, ActionType, TransactionStatus
from .transaction import Transaction
from .log_handler import LogHandler
from .row import Row
from .response import Response
from .algorithms.base import ConcurrencyAlgorithm
from .algorithms.lock_based import LockBasedAlgorithm
from .algorithms.timestamp_based import TimestampBasedAlgorithm
from .algorithms.validation_based import ValidationBasedAlgorithm
from .algorithms.mvcc import MVCCAlgorithm


class CCManager:
    """Main concurrency control manager"""
    
    def __init__(self, algorithm: AlgorithmType, log_file: str = "cc_log.txt"):
        self.algorithm: AlgorithmType = algorithm
        self.transactions: Dict[int, Transaction] = {}
        self.log_handler: Optional[LogHandler] = LogHandler(log_file)
        self.concurrency_algorithm: Optional[ConcurrencyAlgorithm] = None
        self.next_transaction_id: int = 1
        
        # Initialize the appropriate algorithm
        self._initialize_algorithm(algorithm)
    
    def _initialize_algorithm(self, algorithm: AlgorithmType) -> None:
        """Initialize the concurrency control algorithm based on type"""
        if algorithm == AlgorithmType.LockBased:
            self.concurrency_algorithm = LockBasedAlgorithm()
        elif algorithm == AlgorithmType.TimestampBased:
            self.concurrency_algorithm = TimestampBasedAlgorithm()
        elif algorithm == AlgorithmType.ValidationBased:
            self.concurrency_algorithm = ValidationBasedAlgorithm()
        elif algorithm == AlgorithmType.MVCC:
            self.concurrency_algorithm = MVCCAlgorithm()
        else:
            raise ValueError(f"Unsupported algorithm type: {algorithm}")
    
    def begin_transaction(self) -> int:
        """Begin a new transaction and return its ID"""
        # Create new transaction
        transaction = self._create_transaction()
        transaction_id = transaction.transaction_id
        
        # Store transaction
        self.transactions[transaction_id] = transaction
        
        # Log the event
        self.log_handler.log_transaction_event(transaction_id, "BEGIN_TRANSACTION")
        
        return transaction_id
    
    def log_object(self, obj: Row, transaction_id: int) -> None:
        """Log access to an object"""
        # Get the transaction
        transaction = self._get_transaction(transaction_id)
        
        if transaction is None:
            raise ValueError(f"Transaction {transaction_id} does not exist")
        
        # Log the object access
        self.log_handler.log_access(
            transaction_id=transaction_id,
            object_id=obj.object_id,
            action=ActionType.READ,  # Default to READ for logging
            status="LOGGED"
        )
    
    def validate_object(self, obj: Row, transaction_id: int, 
                       action: ActionType) -> Response:
        """Validate access to an object"""
        # Get the transaction
        transaction = self._get_transaction(transaction_id)
        
        if transaction is None:
            raise ValueError(f"Transaction {transaction_id} does not exist")
        
        # Check if transaction is in valid state
        if transaction.get_status() not in [TransactionStatus.Active, TransactionStatus.PartiallyCommitted]:
            return type('Response', (), {
                'allowed': False,
                'message': f"Transaction {transaction_id} is not in active state"
            })()
        
        # Use the concurrency algorithm to check permission
        if self.concurrency_algorithm is None:
            raise RuntimeError("Concurrency algorithm not initialized")
        
        response = self.concurrency_algorithm.check_permission(transaction, obj, action)
        
        # Log the validation result
        status = "ALLOWED" if response.allowed else "DENIED"
        self.log_handler.log_access(
            transaction_id=transaction_id,
            object_id=obj.object_id,
            action=action,
            status=status
        )
        
        return response
    
    def end_transaction(self, transaction_id: int) -> None:
        """End a transaction"""
        # Get the transaction
        transaction = self._get_transaction(transaction_id)
        
        if transaction is None:
            raise ValueError(f"Transaction {transaction_id} does not exist")
        
        # Check current status
        current_status = transaction.get_status()
        
        if current_status == TransactionStatus.Active:
            # Transition to PartiallyCommitted
            transaction.set_status(TransactionStatus.PartiallyCommitted)
            self.log_handler.log_transaction_event(transaction_id, "PARTIALLY_COMMITTED")
            
            # Try to commit
            try:
                if self.concurrency_algorithm:
                    self.concurrency_algorithm.commit_transaction(transaction)
                transaction.set_status(TransactionStatus.Committed)
                self.log_handler.log_transaction_event(transaction_id, "COMMITTED")
            except Exception as e:
                # If commit fails, abort
                transaction.set_status(TransactionStatus.Failed)
                self.log_handler.log_transaction_event(transaction_id, "FAILED")
                if self.concurrency_algorithm:
                    self.concurrency_algorithm.abort_transaction(transaction)
                transaction.set_status(TransactionStatus.Aborted)
                self.log_handler.log_transaction_event(transaction_id, "ABORTED")
                raise e
        elif current_status == TransactionStatus.Failed:
            # Abort the transaction
            if self.concurrency_algorithm:
                self.concurrency_algorithm.abort_transaction(transaction)
            transaction.set_status(TransactionStatus.Aborted)
            self.log_handler.log_transaction_event(transaction_id, "ABORTED")
        
        # Mark as terminated
        transaction.set_status(TransactionStatus.Terminated)
        self.log_handler.log_transaction_event(transaction_id, "TERMINATED")
        
    def set_algorithm(self, algorithm: AlgorithmType) -> None:
        """Set the concurrency control algorithm"""
        if algorithm == self.algorithm:
            return  # Already using this algorithm
        
        # Check if there are active transactions
        active_transactions = [
            tid for tid, t in self.transactions.items() 
            if t.get_status() == TransactionStatus.Active
        ]
        
        if active_transactions:
            raise RuntimeError(
                f"Cannot change algorithm while transactions are active: {active_transactions}"
            )
        
        # Change the algorithm
        self.algorithm = algorithm
        self._initialize_algorithm(algorithm)
        self.log_handler.log_transaction_event(0, f"ALGORITHM_CHANGED: {algorithm.value}")
    
    def _get_transaction(self, transaction_id: int) -> Optional[Transaction]:
        """Get transaction by ID"""
        return self.transactions.get(transaction_id)
    
    def _create_transaction(self) -> Transaction:
        """Create a new transaction"""
        transaction = Transaction(self.next_transaction_id)
        self.next_transaction_id += 1
        return transaction
    
    def abort_transaction(self, transaction_id: int) -> None:
        """Abort a transaction"""
        transaction = self._get_transaction(transaction_id)
        
        if transaction is None:
            raise ValueError(f"Transaction {transaction_id} does not exist")
        
        # Set status to failed
        transaction.set_status(TransactionStatus.Failed)
        self.log_handler.log_transaction_event(transaction_id, "FAILED")
        
        # Use algorithm to abort
        if self.concurrency_algorithm:
            self.concurrency_algorithm.abort_transaction(transaction)
        
        # Set status to aborted
        transaction.set_status(TransactionStatus.Aborted)
        self.log_handler.log_transaction_event(transaction_id, "ABORTED")

    def get_transaction_status(self, transaction_id: int) -> Optional[TransactionStatus]:
        """Get the status of a transaction"""
        transaction = self._get_transaction(transaction_id)
        return transaction.get_status() if transaction else None
    
    def get_active_transactions(self) -> Dict[int, Transaction]:
        """Get all active transactions"""
        return {
            tid: t for tid, t in self.transactions.items()
            if t.get_status() == TransactionStatus.Active
        }
    
    def clear_completed_transactions(self) -> None:
        """Clear transactions that are terminated"""
        terminated_ids = [
            tid for tid, t in self.transactions.items()
            if t.get_status() == TransactionStatus.Terminated
        ]
        for tid in terminated_ids:
            del self.transactions[tid]
