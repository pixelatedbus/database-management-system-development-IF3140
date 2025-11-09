"""
Module ini mengekspor StorageManager dan dataclass terkait.
"""
from .storage_manager import (
    Condition,
    DataRetrieval,
    DataWrite,
    DataDeletion,
    Statistic,
    StorageManager,
)

__all__ = [
    "Condition",
    "DataRetrieval",
    "DataWrite",
    "DataDeletion",
    "Statistic",
    "StorageManager",
]
