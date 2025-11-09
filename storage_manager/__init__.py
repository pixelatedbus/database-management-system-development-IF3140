"""Package storage manager untuk mini relational DBMS.

Module ini mengekspor StorageManager dan dataclass terkait.
"""
from .models import (
    Condition,
    DataRetrieval,
    DataWrite,
    DataDeletion,
    Statistic,
)
from .storage_manager import StorageManager
from .utils import (
    evaluate_condition,
    project_columns,
    validate_table_name,
)

__all__ = [
    # Models
    "Condition",
    "DataRetrieval",
    "DataWrite",
    "DataDeletion",
    "Statistic",
    # Manager
    "StorageManager",
    # Utils
    "evaluate_condition",
    "project_columns",
    "validate_table_name",
]
