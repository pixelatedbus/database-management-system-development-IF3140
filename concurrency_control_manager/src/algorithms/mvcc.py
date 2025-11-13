from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
from enum import Enum
from .base import ConcurrencyAlgorithm
from ..transaction import Transaction
from ..row import Row
from ..action import Action
from ..schedule import Schedule
from ..log_handler import LogHandler
from ..response import Response
from ..enums import TransactionStatus, ActionType, ActionStatus
class MVCCVariant(Enum):
    MVTO = "MVTO"                               # Multi-Version Timestamp Ordering
    MV2PL = "MV2PL"                             # Multi-Version Two-Phase Locking
    SNAPSHOT_ISOLATION = "SNAPSHOT_ISOLATION"   # Snapshot Isolation
class IsolationPolicy(Enum):
    FIRST_COMMITTER_WIN = "FIRST_COMMITTER_WIN"
    FIRST_UPDATER_WIN = "FIRST_UPDATER_WIN"
class TransactionType(Enum):
    """Type of transaction for MV2PL"""
    UPDATE = "UPDATE"
    READ_ONLY = "READ_ONLY"
class DataVersion:
    def __init__(self, value: Any, version_id: int, read_ts: int, write_ts: int, creator_ts: Optional[int] = None, commit_ts: Optional[int] = None):
        self.value: Any = value                     # Nilai data
        self.version_id: int = version_id           # ID versi data (k dalam Q_k)
        self.read_ts: int = read_ts                 # Read Timestamp [R-TS(Q_k)]
        self.write_ts: int = write_ts               # Write Timestamp [W-TS(Q_k)]
        self.creator_ts: Optional[int] = creator_ts # Creator timestamp (untuk SI)
        self.commit_ts: Optional[int] = commit_ts   # Commit timestamp (untuk SI dan MV2PL)
    
    def __repr__(self):
        if self.creator_ts is not None:
            return f"<V{self.version_id}: {self.value}, creator={self.creator_ts}, commit={self.commit_ts}>"
        return f"<V{self.version_id}: {self.value}, R-TS={self.read_ts}, W-TS={self.write_ts}>"
class TransactionInfo:
    """Extended transaction information for MVCC"""
    
    def __init__(self, transaction: Transaction, timestamp: int, 
                 transaction_type: TransactionType = TransactionType.UPDATE):
        self.transaction: Transaction = transaction                 # Reference ke Transaction object
        self.timestamp: int = timestamp                             # Timestamp untuk MVCC
        self.transaction_type: TransactionType = transaction_type
        self.read_set: set = set()                                  # Set object_id yang dibaca
        self.write_set: set = set()                                 # Set object_id yang ditulis
        self.locks_held: Dict[str, str] = {}                        # Lock yang dipegang (MV2PL)
        self.has_exclusive_lock: bool = False                       # Exclusive lock flag (SI)
        self.buffered_writes: Dict[str, Any] = {}                   # Buffered writes (SI)
        self.rollback_count: int = 0                                # Jumlah rollback
class MVCCResponse:
    def __init__(self, allowed: bool, message: str, value: Any = None):
        self.allowed: bool = allowed
        self.message: str = message
        self.value: Any = value 
class MVCCAlgorithm(ConcurrencyAlgorithm):
    def __init__(self, variant: str = "MVTO", isolation_policy: str = "FIRST_COMMITTER_WIN", log_handler: Optional[LogHandler] = None):
        # Convert string to enum
        if isinstance(variant, str):
            self.variant = MVCCVariant[variant]
        else:
            self.variant = variant
            
        if isinstance(isolation_policy, str):
            self.isolation_policy = IsolationPolicy[isolation_policy]
        else:
            self.isolation_policy = isolation_policy
        
        self.data_versions: Dict[str, List[DataVersion]] = {}       # Data store: menyimpan list versi untuk setiap object_id
        self.transaction_info: Dict[int, TransactionInfo] = {}      # Extended transaction info
        self.schedule: Schedule = Schedule()                        # Schedule untuk manage blocked actions
        self.log_handler: Optional[LogHandler] = log_handler        # LogHandler untuk logging
        self.ts_counter: int = 0                                    # Global timestamp counter (untuk MV2PL dan SI)
        self.operation_count: int = 0                               # Operation counter (untuk MVTO rollback)
        self.next_action_id: int = 1                                # Action counter untuk generate action_id
        self.lock_queue: Dict[str, List[Tuple[int, str]]] = {}      # Lock queue (untuk MV2PL)
    
    def begin_transaction(self, transaction: Transaction) -> Response:
        # Tentukan timestamp berdasarkan variant
        if self.variant == MVCCVariant.MVTO:
            timestamp = transaction.transaction_id  # TS(Ti) = i
        else:
            timestamp = self.ts_counter             # Gunakan ts_counter
        
        # Buat TransactionInfo
        trans_info = TransactionInfo(
            transaction=transaction,
            timestamp=timestamp,
            transaction_type=TransactionType.UPDATE  # Default
        )
        self.transaction_info[transaction.transaction_id] = trans_info
        
        # Set transaction status
        transaction.set_status(TransactionStatus.Active)
        
        # Log event
        if self.log_handler:
            self.log_handler.log_transaction_event(
                transaction.transaction_id,
                f"BEGIN (TS={timestamp}, variant={self.variant.value})"
            )
        
        return MVCCResponse(
            allowed=True,
            message=f"Transaction T{transaction.transaction_id} started with TS={timestamp}"
        )
    
    def validate_read(self, transaction: Transaction, row: Row) -> Response:
        # Buat Action object
        action = Action(
            action_id=self._generate_action_id(),
            transaction_id=transaction.transaction_id,
            object_id=row.object_id,
            action_type=ActionType.READ,
            timestamp=datetime.now()
        )
        
        # Tambahkan action ke transaction
        transaction.add_action(action)
        
        # Inisialisasi data jika belum ada
        if row.object_id not in self.data_versions:
            self._initialize_data(row.object_id, row.data.get('value', 0))
        
        # Increment operation counter
        self.operation_count += 1
        
        # Eksekusi read berdasarkan variant
        if self.variant == MVCCVariant.MVTO:
            success, value, message = self._read_mvto(transaction, row, action)
        elif self.variant == MVCCVariant.MV2PL:
            success, value, message = self._read_mv2pl(transaction, row, action)
        else:  # SNAPSHOT_ISOLATION
            success, value, message = self._read_snapshot(transaction, row, action)
        
        # Update action status
        if success:
            action.mark_executed()
            self.schedule.mark_ready(action)
        else:
            action.mark_blocked()
            self.schedule.mark_blocked(action)
        
        # Log access
        if self.log_handler:
            status = "SUCCESS" if success else "BLOCKED"
            self.log_handler.log_access(
                transaction.transaction_id,
                row.object_id,
                ActionType.READ,
                status
            )
        
        return MVCCResponse(allowed=success, message=message, value=value)
    
    def validate_write(self, transaction: Transaction, row: Row) -> Response:
        # Buat Action object
        action = Action(
            action_id=self._generate_action_id(),
            transaction_id=transaction.transaction_id,
            object_id=row.object_id,
            action_type=ActionType.WRITE,
            timestamp=datetime.now()
        )
        
        # Tambahkan action ke transaction
        transaction.add_action(action)
        
        # Inisialisasi data jika belum ada
        if row.object_id not in self.data_versions:
            self._initialize_data(row.object_id, 0)
        
        # Increment operation counter
        self.operation_count += 1
        
        # Eksekusi write berdasarkan variant
        if self.variant == MVCCVariant.MVTO:
            success, message = self._write_mvto(transaction, row, action)
        elif self.variant == MVCCVariant.MV2PL:
            success, message = self._write_mv2pl(transaction, row, action)
        else:  # SNAPSHOT_ISOLATION
            success, message = self._write_snapshot(transaction, row, action)
        
        # Update action status
        if success:
            action.mark_executed()
            self.schedule.mark_ready(action)
        else:
            if "ROLLBACK" in message or "ABORTED" in message:
                action.mark_denied()
            else:
                action.mark_blocked()
                self.schedule.mark_blocked(action)
        
        # Log access
        if self.log_handler:
            status = "SUCCESS" if success else "DENIED/BLOCKED"
            self.log_handler.log_access(
                transaction.transaction_id,
                row.object_id,
                ActionType.WRITE,
                status
            )
        
        return MVCCResponse(allowed=success, message=message)
    
    def commit_transaction(self, transaction: Transaction) -> Response:
        if transaction.transaction_id not in self.transaction_info:
            return MVCCResponse(False, f"Transaction T{transaction.transaction_id} not found")
        
        trans_info = self.transaction_info[transaction.transaction_id]
        
        # Cek apakah sudah aborted
        if transaction.status == TransactionStatus.Aborted:
            return MVCCResponse(False, f"Transaction T{transaction.transaction_id} already aborted")
        
        # Commit berdasarkan variant
        if self.variant == MVCCVariant.MVTO:
            success, message = self._commit_mvto(transaction, trans_info)
        elif self.variant == MVCCVariant.MV2PL:
            success, message = self._commit_mv2pl(transaction, trans_info)
        else:  # SNAPSHOT_ISOLATION
            success, message = self._commit_snapshot(transaction, trans_info)
        
        # Update transaction status
        if success:
            transaction.set_status(TransactionStatus.Committed)
        else:
            transaction.set_status(TransactionStatus.Aborted)
        
        # Log event
        if self.log_handler:
            event = "COMMIT" if success else "ABORT"
            self.log_handler.log_transaction_event(transaction.transaction_id, event)
        
        return MVCCResponse(allowed=success, message=message)
    
    def abort_transaction(self, transaction: Transaction) -> Response:
        if transaction.transaction_id not in self.transaction_info:
            return MVCCResponse(True, f"Transaction T{transaction.transaction_id} not found")
        
        trans_info = self.transaction_info[transaction.transaction_id]
        trans_info.rollback_count += 1
        
        # Set transaction status
        transaction.set_status(TransactionStatus.Aborted)
        
        # Cleanup berdasarkan variant
        if self.variant == MVCCVariant.MV2PL:
            self._release_all_locks(transaction.transaction_id)
        
        # Remove actions dari schedule
        self.schedule.remove_transaction_actions(transaction.transaction_id)
        
        # Log event
        if self.log_handler:
            self.log_handler.log_transaction_event(
                transaction.transaction_id,
                f"ABORT (rollback #{trans_info.rollback_count})"
            )
        
        return MVCCResponse(
            allowed=True,
            message=f"Transaction T{transaction.transaction_id} aborted"
        )
    
    def _read_mvto(self, transaction: Transaction, row: Row, action: Action) -> Tuple[bool, Any, str]:
        object_id = row.object_id
        trans_info = self.transaction_info[transaction.transaction_id]
        trans_ts = trans_info.timestamp
        
        versions = self.data_versions[object_id]
        
        # Cari versi yang tepat: W-TS(Qk) < TS(Ti)
        selected_version = None
        for version in reversed(versions):
            if version.write_ts < trans_ts:
                selected_version = version
                break
        
        if selected_version is None:
            selected_version = versions[0]  # Fallback
        
        # Update R-TS jika perlu update
        if selected_version.read_ts < trans_ts:
            selected_version.read_ts = trans_ts
            message = f"T{transaction.transaction_id} reads {object_id}{selected_version.version_id}, R-TS updated to {trans_ts}"
        else:
            message = f"T{transaction.transaction_id} reads {object_id}{selected_version.version_id}, R-TS not updated"
        
        # Tambahkan ke read set
        trans_info.read_set.add(object_id)
        
        return True, selected_version.value, message
    
    def _write_mvto(self, transaction: Transaction, row: Row, action: Action) -> Tuple[bool, str]:
        object_id = row.object_id
        trans_info = self.transaction_info[transaction.transaction_id]
        trans_ts = trans_info.timestamp
        
        versions = self.data_versions[object_id]
        latest_version = versions[-1]
        
        # Kondisi 1: TS(Ti) < R-TS(Qk) -> Rollback
        if trans_ts < latest_version.read_ts:
            self._rollback_mvto(transaction)
            return False, f"T{transaction.transaction_id} ROLLBACK: write conflict"
        
        # Kondisi 2: TS(Ti) = W-TS(Qk) -> Overwrite
        if trans_ts == latest_version.write_ts:
            latest_version.value = row.data.get('value', latest_version.value)
            latest_version.read_ts = trans_ts
            message = f"T{transaction.transaction_id} overwrites {object_id}{latest_version.version_id}"
        else:
            # Kondisi 3: Buat versi baru
            new_version = DataVersion(
                value=row.data.get('value', 0),
                version_id=latest_version.version_id + 1,
                read_ts=trans_ts,
                write_ts=trans_ts
            )
            versions.append(new_version)
            message = f"T{transaction.transaction_id} creates {object_id}{new_version.version_id}"
        
        # Tambahkan ke write set
        trans_info.write_set.add(object_id)
        
        return True, message
    
    def _rollback_mvto(self, transaction: Transaction) -> None:
        trans_info = self.transaction_info[transaction.transaction_id]
        
        # Hitung timestamp baru = max(operation_count, max_concurrent_ts + 1)
        max_concurrent_ts = max(
            (info.timestamp for tid, info in self.transaction_info.items()
             if tid != transaction.transaction_id and 
                info.transaction.status != TransactionStatus.Aborted),
            default=0
        )
        new_timestamp = max(self.operation_count, max_concurrent_ts + 1)
        trans_info.timestamp = new_timestamp
        trans_info.rollback_count += 1
        
        # Set status
        transaction.set_status(TransactionStatus.Aborted)
        
        # Cascading rollback
        for tid, other_info in self.transaction_info.items():
            if tid == transaction.transaction_id:
                continue
            if other_info.transaction.status == TransactionStatus.Aborted:
                continue
            
            # Cek apakah membaca data yang ditulis transaksi ini
            if other_info.read_set & trans_info.write_set:
                self._rollback_mvto(other_info.transaction)
    
    def _commit_mvto(self, transaction: Transaction, trans_info: TransactionInfo) -> Tuple[bool, str]:
        return True, f"T{transaction.transaction_id} COMMIT"
    
    def _read_mv2pl(self, transaction: Transaction, row: Row, action: Action) -> Tuple[bool, Any, str]:
        object_id = row.object_id
        trans_info = self.transaction_info[transaction.transaction_id]
        
        if trans_info.transaction_type == TransactionType.READ_ONLY:
            # Read-only: baca snapshot
            versions = self.data_versions[object_id]
            selected_version = versions[0]
            
            for version in reversed(versions):
                version_ts = version.write_ts if version.write_ts != float('inf') else 0
                if version_ts <= self.ts_counter:
                    selected_version = version
                    break
            
            trans_info.read_set.add(object_id)
            return True, selected_version.value, f"T{transaction.transaction_id} (read-only) reads {object_id}"
        
        else:
            # Update transaction: acquire shared lock
            if not self._acquire_lock(transaction.transaction_id, object_id, "S"):
                return False, None, f"T{transaction.transaction_id} blocked waiting for lock-S({object_id})"
            
            versions = self.data_versions[object_id]
            latest_version = versions[-1]
            
            trans_info.read_set.add(object_id)
            return True, latest_version.value, f"T{transaction.transaction_id} reads {object_id} (lock-S acquired)"
    
    def _write_mv2pl(self, transaction: Transaction, row: Row, action: Action) -> Tuple[bool, str]:
        object_id = row.object_id
        trans_info = self.transaction_info[transaction.transaction_id]
        
        # Acquire exclusive lock
        if not self._acquire_lock(transaction.transaction_id, object_id, "X"):
            return False, f"T{transaction.transaction_id} blocked waiting for lock-X({object_id})"
        
        # Buat versi dengan infinity
        versions = self.data_versions[object_id]
        new_version = DataVersion(
            value=row.data.get('value', 0),
            version_id=len(versions),
            read_ts=float('inf'),
            write_ts=float('inf')
        )
        versions.append(new_version)
        
        trans_info.write_set.add(object_id)
        return True, f"T{transaction.transaction_id} writes {object_id}infinite (lock-X acquired)"
    
    def _commit_mv2pl(self, transaction: Transaction, trans_info: TransactionInfo) -> Tuple[bool, str]:
        if trans_info.transaction_type == TransactionType.READ_ONLY:
            return True, f"T{transaction.transaction_id} (read-only) COMMIT"
        
        # Update timestamp
        new_ts = self.ts_counter + 1
        trans_info.timestamp = new_ts
        
        # Update semua versi infinity
        for object_id in trans_info.write_set:
            versions = self.data_versions[object_id]
            for version in versions:
                if version.write_ts == float('inf'):
                    version.read_ts = new_ts
                    version.write_ts = new_ts
                    version.commit_ts = new_ts
        
        # Increment ts-counter
        self.ts_counter += 1
        
        # Release locks
        self._release_all_locks(transaction.transaction_id)
        
        return True, f"T{transaction.transaction_id} COMMIT (TS={new_ts})"
    
    def _acquire_lock(self, transaction_id: int, object_id: str, lock_type: str) -> bool:
        trans_info = self.transaction_info[transaction_id]
        
        # Cek conflict
        for tid, other_info in self.transaction_info.items():
            if tid == transaction_id:
                continue
            
            if object_id in other_info.locks_held:
                held_lock = other_info.locks_held[object_id]
                if lock_type == "X" or held_lock == "X":
                    # Add to queue
                    if object_id not in self.lock_queue:
                        self.lock_queue[object_id] = []
                    self.lock_queue[object_id].append((transaction_id, lock_type))
                    return False
        
        # Grant lock
        trans_info.locks_held[object_id] = lock_type
        return True
    
    def _release_all_locks(self, transaction_id: int) -> None:
        trans_info = self.transaction_info[transaction_id]
        released = list(trans_info.locks_held.keys())
        trans_info.locks_held.clear()
        
        # Process queue
        for object_id in released:
            if object_id in self.lock_queue and self.lock_queue[object_id]:
                waiting_tid, lock_type = self.lock_queue[object_id].pop(0)
                self._acquire_lock(waiting_tid, object_id, lock_type)
    
    def _read_snapshot(self, transaction: Transaction, row: Row, action: Action) -> Tuple[bool, Any, str]:
        """Read operation untuk Snapshot Isolation"""
        object_id = row.object_id
        trans_info = self.transaction_info[transaction.transaction_id]
        versions = self.data_versions[object_id]
        
        # Baca snapshot
        selected_version = versions[0]
        for version in reversed(versions):
            if version.commit_ts is not None and version.commit_ts <= trans_info.timestamp:
                selected_version = version
                break
        
        trans_info.read_set.add(object_id)
        return True, selected_version.value, f"T{transaction.transaction_id} reads {object_id} (snapshot)"
    
    def _write_snapshot(self, transaction: Transaction, row: Row, action: Action) -> Tuple[bool, str]:
        object_id = row.object_id
        trans_info = self.transaction_info[transaction.transaction_id]
        
        if self.isolation_policy == IsolationPolicy.FIRST_UPDATER_WIN:
            # Cek exclusive lock
            for tid, other_info in self.transaction_info.items():
                if tid == transaction.transaction_id:
                    continue
                if other_info.transaction.status in [TransactionStatus.Committed, TransactionStatus.Aborted]:
                    continue
                
                if other_info.has_exclusive_lock:
                    transaction.set_status(TransactionStatus.Aborted)
                    return False, f"T{transaction.transaction_id} ABORTED: exclusive lock conflict"
            
            trans_info.has_exclusive_lock = True
        
        # Buffer write
        trans_info.write_set.add(object_id)
        trans_info.buffered_writes[object_id] = row.data.get('value', 0)
        
        return True, f"T{transaction.transaction_id} writes {object_id} (buffered)"
    
    def _commit_snapshot(self, transaction: Transaction, trans_info: TransactionInfo) -> Tuple[bool, str]:
        # Read-only
        if not trans_info.write_set:
            return True, f"T{transaction.transaction_id} COMMIT (read-only)"
        
        # Validasi write-write conflict (First-Committer Win)
        if self.isolation_policy == IsolationPolicy.FIRST_COMMITTER_WIN:
            for object_id in trans_info.write_set:
                versions = self.data_versions[object_id]
                for version in versions:
                    if (version.commit_ts is not None and
                        version.commit_ts > trans_info.timestamp and
                        version.creator_ts != transaction.transaction_id):
                        return False, f"T{transaction.transaction_id} ABORTED: write-write conflict"
        
        # Finalisasi writes
        self.ts_counter += 1
        
        for object_id, value in trans_info.buffered_writes.items():
            versions = self.data_versions[object_id]
            new_version = DataVersion(
                value=value,
                version_id=len(versions),
                read_ts=self.ts_counter,
                write_ts=self.ts_counter,
                creator_ts=transaction.transaction_id,
                commit_ts=self.ts_counter
            )
            versions.append(new_version)
        
        return True, f"T{transaction.transaction_id} COMMIT (ts-counter={self.ts_counter})"
    
    def _initialize_data(self, object_id: str, initial_value: Any) -> None:
        if object_id not in self.data_versions:
            if self.variant == MVCCVariant.SNAPSHOT_ISOLATION:
                initial_version = DataVersion(
                    value=initial_value,
                    version_id=0,
                    read_ts=0,
                    write_ts=0,
                    creator_ts=-1,
                    commit_ts=0
                )
            else:
                initial_version = DataVersion(
                    value=initial_value,
                    version_id=0,
                    read_ts=0,
                    write_ts=0
                )
            self.data_versions[object_id] = [initial_version]
    
    def _generate_action_id(self) -> int:
        action_id = self.next_action_id
        self.next_action_id += 1
        return action_id
    
    def set_transaction_type(self, transaction_id: int, trans_type: str) -> None:
        if transaction_id in self.transaction_info:
            if trans_type == "READ_ONLY":
                self.transaction_info[transaction_id].transaction_type = TransactionType.READ_ONLY
            else:
                self.transaction_info[transaction_id].transaction_type = TransactionType.UPDATE
    
    def get_data_versions(self, object_id: str) -> List[DataVersion]:
        return self.data_versions.get(object_id, [])