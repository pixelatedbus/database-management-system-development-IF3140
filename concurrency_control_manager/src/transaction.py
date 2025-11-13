from datetime import datetime
from typing import List, Optional
from .enums import TransactionStatus
from .action import Action


class Transaction:
    """Represents a database transaction"""
    
    def __init__(self, transaction_id: int):
        self.transaction_id: int = transaction_id
        self.status: TransactionStatus = TransactionStatus.Active
        self.start_timestamp: datetime = datetime.now()
        self.validation_timestamp: Optional[datetime] = None
        self.finish_timestamp: Optional[datetime] = None
        self.actions: List[Action] = []
        self.wait_time: float = 0.0

    def __repr__(self) -> str:
        return (f"Transaction(transaction_id={self.transaction_id}, status={self.status.name}, "
                f"start_timestamp={self.start_timestamp}, finish_timestamp={self.finish_timestamp}, "
                f"validation_timestamp={self.validation_timestamp}, wait_time={self.wait_time}, "
                f"actions={self.actions})")
    
    def add_action(self, action: Action) -> None:
        """Add an action to the transaction"""
        if action.transaction_id != self.transaction_id:
            raise ValueError("Action transaction_id does not match Transaction transaction_id")
        self.actions.append(action)
    
    def set_status(self, status: TransactionStatus) -> None:
        """Set the transaction status"""
        self.status = status
        if status in [TransactionStatus.Committed, TransactionStatus.Aborted, TransactionStatus.Terminated]:
            if self.finish_timestamp is None:
                self.finish_timestamp = datetime.now()
    
    def get_status(self) -> TransactionStatus:
        """Get the transaction status"""
        return self.status
    
    def get_age(self) -> float:
        """Get the age of the transaction"""
        end_time = self.finish_timestamp if self.finish_timestamp else datetime.now()
        return (end_time - self.start_timestamp).total_seconds()
    
    def get_action_count(self) -> int:
        """Get the number of actions in the transaction"""
        return len(self.actions)