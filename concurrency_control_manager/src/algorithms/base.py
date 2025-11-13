"""
Base abstract class for concurrency control algorithms
"""

from abc import ABC, abstractmethod
from ..transaction import Transaction
from ..row import Row
from ..enums import ActionType
from ..response import Response


class ConcurrencyAlgorithm(ABC):
    """Abstract base class for concurrency control algorithms"""
    
    @abstractmethod
    def check_permission(self, t: Transaction, obj: Row, 
                        action: ActionType) -> Response:
        """Check if transaction has permission to perform action on object"""
        pass
    
    @abstractmethod
    def commit_transaction(self, t: Transaction) -> None:
        """Commit a transaction"""
        pass
    
    @abstractmethod
    def abort_transaction(self, t: Transaction) -> None:
        """Abort a transaction"""
        pass
