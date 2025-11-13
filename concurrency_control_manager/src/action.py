"""
Action class representing a transaction operation
"""

from datetime import datetime
from typing import Optional
from .enums import ActionType, ActionStatus

MAX_RETRY = 3
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
    
    def __repr__(self):
        return (f"Action(action_id={self.action_id}, transaction_id={self.transaction_id}, "
                f"object_id='{self.object_id}', action_type={self.type.name}, "
                f"status={self.status.name}, timestamp={self.timestamp}, "
                f"retry_count={self.retry_count})")
    
    def mark_executed(self) -> None:
        """Mark action as executed"""
        self.status = ActionStatus.Executed
        self.blocked_timestamp = None
    
    def mark_denied(self) -> None:
        """Mark action as denied"""
        self.status = ActionStatus.Denied
    
    def mark_blocked(self) -> None:
        """Mark action as blocked"""
        self.status = ActionStatus.Blocked
        self.blocked_timestamp = datetime.now()
    
    def increment_retry(self) -> None:
        """Increment retry count"""
        self.retry_count += 1
    
    def get_wait_time(self) -> float:
        """Get wait time for blocked action"""
        if self.status == ActionStatus.Blocked and self.blocked_timestamp:
            return (datetime.now() - self.blocked_timestamp).total_seconds()
        return 0
    
    def should_abort(self) -> bool:
        """Check if action should be aborted"""
        return self.retry_count >= MAX_RETRY