"""
Transaction class representing a database transaction
"""

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
    
    def add_action(self, action: Action) -> None:
        """Add an action to the transaction"""
        pass
    
    def set_status(self, status: TransactionStatus) -> None:
        """Set the transaction status"""
        pass
    
    def get_status(self) -> TransactionStatus:
        """Get the transaction status"""
        pass
    
    def get_age(self) -> float:
        """Get the age of the transaction"""
        pass
    
    def get_action_count(self) -> int:
        """Get the number of actions in the transaction"""
        pass
