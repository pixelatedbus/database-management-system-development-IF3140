"""
Timestamp-based concurrency control algorithm
"""

from typing import Dict, Optional
from datetime import datetime
from .base import ConcurrencyAlgorithm
from ..transaction import Transaction
from ..row import Row
from ..enums import ActionType
from ..response import Response


class ObjectTimestamp:
    """Tracks read and write timestamps for an object"""
    
    def __init__(self, object_id: str):
        self.object_id: str = object_id
        self.read_timestamp: Optional[datetime] = None
        self.write_timestamp: Optional[datetime] = None
    
    def update_read_timestamp(self, timestamp: datetime) -> None:
        """Update read timestamp"""
        pass
    
    def update_write_timestamp(self, timestamp: datetime) -> None:
        """Update write timestamp"""
        pass
    
    def is_read_valid(self, transaction_timestamp: datetime) -> bool:
        """Check if read is valid for given transaction timestamp"""
        pass
    
    def is_write_valid(self, transaction_timestamp: datetime) -> bool:
        """Check if write is valid for given transaction timestamp"""
        pass


class TimestampBasedAlgorithm(ConcurrencyAlgorithm):
    """Timestamp-based concurrency control algorithm implementation"""
    
    def __init__(self):
        self.object_timestamps: Dict[str, ObjectTimestamp] = {}
    
    def check_permission(self, t: Transaction, obj: Row, 
                        action: ActionType) -> Response:
        """Check permission using timestamps"""
        pass
    
    def commit_transaction(self, t: Transaction) -> None:
        """Commit transaction"""
        pass
    
    def abort_transaction(self, t: Transaction) -> None:
        """Abort transaction"""
        pass
    
    def _compare_timestamps(self, t1_timestamp: datetime, 
                           t2_timestamp: datetime) -> int:
        """Compare two timestamps"""
        pass
    
    def _get_read_timestamp(self, object_id: str) -> Optional[datetime]:
        """Get read timestamp for an object"""
        pass
    
    def _get_write_timestamp(self, object_id: str) -> Optional[datetime]:
        """Get write timestamp for an object"""
        pass
    
    def _update_timestamps(self, object_id: str, t: Transaction, 
                          action: ActionType) -> None:
        """Update timestamps after successful operation"""
        pass
