"""
Action class representing a transaction operation
"""

from datetime import datetime
from typing import Optional
from .enums import ActionType, ActionStatus


class Action:
    """Represents an action within a transaction"""
    
    def __init__(self, action_id: int, transaction_id: int, object_id: str, 
                 action_type: ActionType, timestamp: datetime):
        self.action_id: int = action_id
        self.transaction_id: int = transaction_id
        self.object_id: str = object_id
        self.type: ActionType = action_type
        self.timestamp: datetime = timestamp
        self.status: ActionStatus = ActionStatus.Pending
        self.retry_count: int = 0
        self.blocked_timestamp: Optional[datetime] = None
    
    def mark_executed(self) -> None:
        """Mark action as executed"""
        pass
    
    def mark_denied(self) -> None:
        """Mark action as denied"""
        pass
    
    def mark_blocked(self) -> None:
        """Mark action as blocked"""
        pass
    
    def increment_retry(self) -> None:
        """Increment retry count"""
        pass
    
    def get_wait_time(self) -> float:
        """Get wait time for blocked action"""
        pass
    
    def should_abort(self) -> bool:
        """Check if action should be aborted"""
        pass
