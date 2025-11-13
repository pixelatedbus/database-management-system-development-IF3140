"""
Lock-based concurrency control algorithm
"""

from typing import Dict, List, Set, Optional
from datetime import datetime
from .base import ConcurrencyAlgorithm
from ..transaction import Transaction
from ..row import Row
from ..enums import ActionType, LockType
from ..response import Response


class LockEntry:
    """Represents a lock entry in the lock table"""
    
    def __init__(self, object_id: str, transaction_id: int, lock_type: LockType):
        self.object_id: str = object_id
        self.transaction_id: int = transaction_id
        self.lock_type: LockType = lock_type
        self.granted: bool = False
        self.timestamp: datetime = datetime.now()
        self.wait_start: Optional[datetime] = None
    
    def is_expired(self, timeout: float) -> bool:
        """Check if lock request has expired (waited too long)
            timeout in seconds"""
        
        if self.granted:
            return False
            
        if self.wait_start is None:
            return False
            
        waiting_time = (datetime.now() - self.wait_start).total_seconds()
        return waiting_time > timeout


class LockManager:
    """Manages locks for lock-based concurrency control"""
    
    def __init__(self):
        self.lock_table: Dict[str, List[LockEntry]] = {}
        self.wait_for_graph: Dict[int, Set[int]] = {}
        self.timeout_seconds: float = 30.0
    
    def acquire_lock(self, object_id: str, transaction_id: int, lock_type: LockType) -> bool:
        """Attempt to acquire a lock for a transaction on a database object"""
        
        if object_id not in self.lock_table:
            self.lock_table[object_id] = []
        lock_list = self.lock_table[object_id]

        # Existing locks acquired by the same transaction
        existing_locks : LockEntry = None
        for lock in lock_list:
            if lock.transaction_id == transaction_id:
                existing_locks = lock
                break
        
        # Transaction (id) which already holds the requested lock
        conflicting_transactions : Set[int] = set()
        for lock in lock_list:
            if lock.transaction_id != transaction_id and lock.granted == True:
                if self._is_compatible(lock.lock_type, lock_type) == False:
                    conflicting_transactions.add(lock.transaction_id)
        
        # Logic to acquire lock
        if existing_locks:
            if existing_locks.granted:
                if existing_locks.lock_type == lock_type or existing_locks.lock_type == LockType.WRITE_LOCK:
                    return True
                if existing_locks.lock_type == LockType.READ_LOCK and lock_type == LockType.WRITE_LOCK:
                    if conflicting_transactions:
                        existing_locks.granted = False
                        existing_locks.wait_start = datetime.now()
                        self.wait_for_graph.setdefault(transaction_id, set()).update(conflicting_transactions)
                        return False
                    else:
                        existing_locks.lock_type = LockType.WRITE_LOCK
                        existing_locks.timestamp = datetime.now()
                        return True
            else:
                return False
        
        new_lock_entry = LockEntry(object_id, transaction_id, lock_type)

        if conflicting_transactions:
            new_lock_entry.granted = False
            new_lock_entry.wait_start = datetime.now()
            lock_list.append(new_lock_entry)
            self.wait_for_graph.setdefault(transaction_id, set()).update(conflicting_transactions)
            return False
        else:
            new_lock_entry.granted = True
            lock_list.append(new_lock_entry)
            return True
    
    def release_locks(self, transaction_id: int) -> List[str]:
        """Release all locks held by a transaction"""
        released_object_id = []

        object_id_to_check = [self.lock_table.keys()]

        for object_id in object_id_to_check:
            lock_list = self.lock_table.get(object_id)
            if not lock_list:
                continue

            lock_to_remove: LockEntry = None
            for lock in lock_list:
                if lock.transaction_id == transaction_id:
                    lock_to_remove = lock
                    break

            if lock_to_remove:
                released_object_id.append(object_id)
                lock_list.remove(lock_to_remove)
                
                self._grant_waiting_locks(object_id)
                
                if not lock_list:
                    del self.lock_table[object_id]
        
        self.wait_for_graph.pop(transaction_id, None)
                
        return released_object_id
    
    def release_lock(self, object_id: str, transaction_id: int) -> bool:
        """Release a specific lock"""
        lock_list = self.lock_table.get(object_id)
        
        if not lock_list:
            return False

        lock_to_remove: LockEntry = None
        for lock in lock_list:
            if lock.transaction_id == transaction_id:
                lock_to_remove = lock
                break

        if lock_to_remove is None:
            return False

        lock_list.remove(lock_to_remove)
        self._grant_waiting_locks(object_id)
        if not lock_list:
            del self.lock_table[object_id]
        return True
    
    def _grant_waiting_locks(self, object_id: str):
        lock_list = self.lock_table.get(object_id)
        if not lock_list:
            return

        current_granted_locks = [l for l in lock_list if l.granted]

        waiting_locks = sorted(
            (l for l in lock_list if not l.granted), 
            key=lambda l: l.wait_start
        )

        for lock in waiting_locks:
            is_conflicted = False
            for granted_lock in current_granted_locks:
                if not self._is_compatible(lock.lock_type, granted_lock.lock_type):
                    is_conflicted = True
                    break
            
            if not is_conflicted:
                lock.granted = True
                lock.wait_start = None 
                
                current_granted_locks.append(lock)
                
                self.wait_for_graph.pop(lock.transaction_id, None)
                
                if lock.lock_type == LockType.READ_LOCK:
                    continue
                else:
                    break
    
    def check_conflict(self, object_id: str, transaction_id: int, 
                      lock_type: LockType) -> bool:
        """Check if lock request conflicts with existing locks"""
        pass
    
    def detect_deadlock(self) -> bool:
        """Detect if there is a deadlock"""
        pass
    
    def get_deadlock_victim(self) -> int:
        """Select a transaction to abort in case of deadlock"""
        pass
    
    def upgrade_lock(self, object_id: str, transaction_id: int) -> bool:
        """Upgrade a read lock to a write lock"""
        return self.acquire_lock(object_id, transaction_id, LockType.WRITE_LOCK)
    
    def _build_wait_for_graph(self) -> None:
        """Build the wait-for graph for deadlock detection"""
        pass
    
    def _has_cycle(self) -> bool:
        """Check if wait-for graph has a cycle"""
        pass
    
    def _is_compatible(self, lock_type1: LockType, lock_type2: LockType) -> bool:
        """Check if two lock types are compatible"""
        if lock_type1 == LockType.READ_LOCK and lock_type2 == LockType.READ_LOCK:
            return True
        else:
            return False

class LockBasedAlgorithm(ConcurrencyAlgorithm):
    """Lock-based concurrency control algorithm implementation"""
    
    def __init__(self):
        self.lock_manager: LockManager = LockManager()
    
    def check_permission(self, t: Transaction, obj: Row, 
                        action: ActionType) -> Response:
        """Check permission using locks"""
        pass
    
    def commit_transaction(self, t: Transaction) -> None:
        """Commit transaction and release locks"""
        pass
    
    def abort_transaction(self, t: Transaction) -> None:
        """Abort transaction and release locks"""
        pass
