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
        """Update read timestamp to max of current and new timestamp"""
        if self.read_timestamp is None or timestamp > self.read_timestamp:
            self.read_timestamp = timestamp

    def update_write_timestamp(self, timestamp: datetime) -> None:
        """Update write timestamp"""
        self.write_timestamp = timestamp

    def is_read_valid(self, transaction_timestamp: datetime) -> bool:
        """Check if read is valid for given transaction timestamp

        Read is valid if:
        - TS(T) >= W-timestamp(X)
        (Transaction can read if it's newer than the last write)
        """
        if self.write_timestamp is None:
            return True
        return transaction_timestamp >= self.write_timestamp

    def is_write_valid(self, transaction_timestamp: datetime) -> bool:
        """Check if write is valid for given transaction timestamp

        Write is valid if:
        - TS(T) >= R-timestamp(X) (newer than last read)
        - TS(T) >= W-timestamp(X) (newer than last write)
        """
        # Check against read timestamp
        if self.read_timestamp is not None and transaction_timestamp < self.read_timestamp:
            return False

        # Check against write timestamp
        if self.write_timestamp is not None and transaction_timestamp < self.write_timestamp:
            return False

        return True


class TimestampBasedAlgorithm(ConcurrencyAlgorithm):
    """Timestamp-based concurrency control algorithm implementation"""

    def __init__(self):
        self.object_timestamps: Dict[str, ObjectTimestamp] = {}

    def check_permission(self, t: Transaction, obj: Row,
                        action: ActionType) -> Response:
        """Check permission using timestamps

        For READ operations:
        - If TS(T) < W-timestamp(X), reject (reading obsolete data)
        - Otherwise, allow and update R-timestamp(X) = max(R-timestamp(X), TS(T))

        For WRITE operations:
        - If TS(T) < R-timestamp(X), reject (overwriting data that was already read by newer transaction)
        - If TS(T) < W-timestamp(X), reject (overwriting data written by newer transaction)
        - Otherwise, allow and update W-timestamp(X) = TS(T)
        """
        object_id = obj.object_id
        transaction_timestamp = t.start_timestamp

        # Get or create object timestamp
        if object_id not in self.object_timestamps:
            self.object_timestamps[object_id] = ObjectTimestamp(object_id)

        obj_ts = self.object_timestamps[object_id]

        # Check permission based on action type
        if action == ActionType.READ:
            if obj_ts.is_read_valid(transaction_timestamp):
                # Update read timestamp
                obj_ts.update_read_timestamp(transaction_timestamp)
                return self._create_response(
                    allowed=True,
                    message=f"Read allowed for transaction {t.transaction_id} on object {object_id}"
                )
            else:
                return self._create_response(
                    allowed=False,
                    message=f"Read denied for transaction {t.transaction_id} on object {object_id}: "
                            f"Transaction timestamp {transaction_timestamp} < Write timestamp {obj_ts.write_timestamp}"
                )

        elif action == ActionType.WRITE:
            if obj_ts.is_write_valid(transaction_timestamp):
                # Update write timestamp
                obj_ts.update_write_timestamp(transaction_timestamp)
                return self._create_response(
                    allowed=True,
                    message=f"Write allowed for transaction {t.transaction_id} on object {object_id}"
                )
            else:
                # Determine reason for rejection
                if obj_ts.read_timestamp and transaction_timestamp < obj_ts.read_timestamp:
                    reason = f"Transaction timestamp {transaction_timestamp} < Read timestamp {obj_ts.read_timestamp}"
                else:
                    reason = f"Transaction timestamp {transaction_timestamp} < Write timestamp {obj_ts.write_timestamp}"

                return self._create_response(
                    allowed=False,
                    message=f"Write denied for transaction {t.transaction_id} on object {object_id}: {reason}"
                )

        else:
            return self._create_response(
                allowed=False,
                message=f"Unknown action type: {action}"
            )

    def commit_transaction(self, t: Transaction) -> None:
        """Commit transaction

        For timestamp-based algorithm, no special action needed on commit
        as timestamps are already updated during operation validation
        """
        # No special cleanup needed for timestamp-based algorithm
        # Timestamps remain for future validations
        pass

    def abort_transaction(self, t: Transaction) -> None:
        """Abort transaction

        For timestamp-based algorithm, we don't rollback timestamps
        as the transaction never completed. The timestamps of objects
        remain as they were set by successfully committed transactions.
        """
        # No rollback needed for timestamp-based algorithm
        # The timestamps represent the last successfully committed operation
        pass

    def _compare_timestamps(self, t1_timestamp: datetime,
                           t2_timestamp: datetime) -> int:
        if t1_timestamp < t2_timestamp:
            return -1
        elif t1_timestamp > t2_timestamp:
            return 1
        else:
            return 0

    def _get_read_timestamp(self, object_id: str) -> Optional[datetime]:
        """Get read timestamp for an object"""
        if object_id in self.object_timestamps:
            return self.object_timestamps[object_id].read_timestamp
        return None

    def _get_write_timestamp(self, object_id: str) -> Optional[datetime]:
        """Get write timestamp for an object"""
        if object_id in self.object_timestamps:
            return self.object_timestamps[object_id].write_timestamp
        return None

    def _update_timestamps(self, object_id: str, t: Transaction,
                          action: ActionType) -> None:
        """Update timestamps after successful operation"""
        if object_id not in self.object_timestamps:
            self.object_timestamps[object_id] = ObjectTimestamp(object_id)

        obj_ts = self.object_timestamps[object_id]

        if action == ActionType.READ:
            obj_ts.update_read_timestamp(t.start_timestamp)
        elif action == ActionType.WRITE:
            obj_ts.update_write_timestamp(t.start_timestamp)

    def _create_response(self, allowed: bool, message: str) -> Response:
        """Create a Response object"""
        return type('Response', (), {
            'allowed': allowed,
            'message': message
        })()
