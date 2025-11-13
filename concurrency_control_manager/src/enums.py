"""
Enumerations for Concurrency Control Manager
"""

from enum import Enum


class TransactionStatus(Enum):
    """Status of a transaction in its lifecycle"""
    Active = "Active"
    PartiallyCommitted = "PartiallyCommitted"
    Failed = "Failed"
    Committed = "Committed"
    Aborted = "Aborted"
    Terminated = "Terminated"


class ActionType(Enum):
    """Type of action performed on a database object"""
    READ = "READ"
    WRITE = "WRITE"


class ActionStatus(Enum):
    """Status of an action in the schedule"""
    Pending = "Pending"
    Executed = "Executed"
    Denied = "Denied"
    Blocked = "Blocked"


class LockType(Enum):
    """Type of lock for lock-based concurrency control"""
    READ_LOCK = "READ_LOCK"
    WRITE_LOCK = "WRITE_LOCK"


class AlgorithmType(Enum):
    """Type of concurrency control algorithm"""
    LockBased = "LockBased"
    TimestampBased = "TimestampBased"
    ValidationBased = "ValidationBased"
    MVCC = "MVCC"
