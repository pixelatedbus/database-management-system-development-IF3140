from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
from enum import Enum
from .base import ConcurrencyAlgorithm
from ..transaction import Transaction
from ..row import Row
from ..action import Action
from ..log_handler import LogHandler
from ..response import AlgorithmResponse
from ..enums import TransactionStatus, ActionType, ActionStatus

class MVTOResponse(AlgorithmResponse):
    """Response khusus untuk MVTO yang menyimpan informasi cascaded transactions"""
    def __init__(self, allowed: bool, message: str, value: any = None, cascaded_tids: Optional[List[int]] = None):
        super().__init__(allowed, message, value)
        self.cascaded_tids = cascaded_tids

class MVCCVariant(Enum):
    MVTO = "MVTO"                               # Multi-Version Timestamp Ordering
    MV2PL = "MV2PL"                             # Multi-Version Two-Phase Locking
    SNAPSHOT_ISOLATION = "SNAPSHOT_ISOLATION"   # Snapshot Isolation
class IsolationPolicy(Enum):
    FIRST_COMMITTER_WIN = "FIRST_COMMITTER_WIN"
    FIRST_UPDATER_WIN = "FIRST_UPDATER_WIN"
class TransactionType(Enum):
    UPDATE = "UPDATE"
    READ_ONLY = "READ_ONLY"
class RowVersion:
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
    def __init__(self, transaction: Transaction, timestamp: int, transaction_type: TransactionType = TransactionType.UPDATE):
        self.transaction: Transaction = transaction                 # Reference ke Transaction object
        self.timestamp: int = timestamp                             # Timestamp untuk MVCC
        self.transaction_type: TransactionType = transaction_type
        self.read_set: set = set()                                  # Set object_id yang dibaca
        self.write_set: set = set()                                 # Set object_id yang ditulis
        self.write_intents: set = set()                             # Objek yang akan ditulis (untuk keputusan lock MV2PL)
        self.read_versions: Dict[str, int] = {}                     # Lacak write_ts dari versi yang dibaca untuk setiap object_id (untuk cascading rollback)
        self.locks_held: Dict[str, str] = {}                        # Lock yang dipegang (MV2PL)
        self.has_exclusive_lock: bool = False                       # Flag exclusive lock (SI)
        self.buffered_writes: Dict[str, Any] = {}                   # Buffered writes (SI)
        self.rollback_count: int = 0                                # Jumlah rollback
        self.locks_from_queue: set = set()                          # Lacak lock yang didapat dari queue (MV2PL)
class MVCCAlgorithm(ConcurrencyAlgorithm):
    def __init__(self, variant: str = "MVTO", isolation_policy: str = "FIRST_COMMITTER_WIN", log_handler: Optional[LogHandler] = None):
        if isinstance(variant, str):
            self.variant = MVCCVariant[variant]
        else:
            self.variant = variant
            
        if isinstance(isolation_policy, str):
            self.isolation_policy = IsolationPolicy[isolation_policy]
        else:
            self.isolation_policy = isolation_policy
        
        self.data_versions: Dict[str, List[RowVersion]] = {}        # Penyimpanan data: menyimpan list versi untuk setiap object_id
        self.transaction_info: Dict[int, TransactionInfo] = {}      # Informasi transaksi yang diperluas
        self.log_handler: Optional[LogHandler] = log_handler        # LogHandler untuk logging
        self.ts_counter: int = 0                                    # Penghitung timestamp global (untuk MV2PL dan SI)
        self.operation_count: int = 0                               # Penghitung operasi (untuk rollback MVTO)
        self.next_action_id: int = 1                                # Penghitung action untuk generate action_id
        self.operation_queue: List[Dict] = []                       # Queue untuk semua operasi yang di-block termasuk commit
        self.blocked_transactions: set = set()                      # Set ID transaksi yang di-block (untuk MV2PL)
    
    def begin_transaction(self, transaction: Transaction) -> AlgorithmResponse:
        if self.variant == MVCCVariant.MVTO:
            timestamp = transaction.transaction_id
        else:
            timestamp = self.ts_counter
        
        trans_info = TransactionInfo(
            transaction=transaction,
            timestamp=timestamp,
            transaction_type=TransactionType.UPDATE
        )
        self.transaction_info[transaction.transaction_id] = trans_info
        
        transaction.set_status(TransactionStatus.Active)
        
        if self.log_handler:
            self.log_handler.log_transaction_event(
                transaction.transaction_id,
                f"BEGIN (TS={timestamp}, variant={self.variant.value})"
            )
        
        return AlgorithmResponse(
            allowed=True,
            message=f"Transaction T{transaction.transaction_id} started with TS={timestamp}"
        )
    
    def validate_read(self, transaction: Transaction, row: Row) -> AlgorithmResponse:
        if transaction.transaction_id not in self.transaction_info:
            self.begin_transaction(transaction)
        
        action = Action(
            action_id=self._generate_action_id(),
            transaction_id=transaction.transaction_id,
            object_id=row.object_id,
            action_type=ActionType.READ,
            timestamp=datetime.now()
        )
        
        transaction.add_action(action)
        
        if row.object_id not in self.data_versions:
            self._initialize_data(row.object_id, row.data.get('value', 0))
        
        self.operation_count += 1
        
        if self.variant == MVCCVariant.MVTO:
            success, value, message = self._read_mvto(transaction, row, action)
        elif self.variant == MVCCVariant.MV2PL:
            success, value, message = self._read_mv2pl(transaction, row, action)
        else:
            success, value, message = self._read_snapshot(transaction, row, action)
        
        if success:
            action.mark_executed()
        else:
            action.mark_blocked()
        
        if self.log_handler:
            status = "SUCCESS" if success else "BLOCKED"
            self.log_handler.log_access(
                transaction.transaction_id,
                row.object_id,
                ActionType.READ,
                status
            )
        
        return AlgorithmResponse(allowed=success, message=message, value=value)
    
    def validate_write(self, transaction: Transaction, row: Row, auto_reexecute: bool = True) -> AlgorithmResponse:
        """Validasi operasi write.
        
        Untuk MVTO dengan auto_reexecute=True, ini akan secara otomatis menangani rollback dan re-execution.
        Untuk MVTO, mengembalikan MVTOResponse yang berisi cascaded_tids jika ada rollback.
        """
        if transaction.transaction_id not in self.transaction_info:
            return AlgorithmResponse(False, f"Transaction T{transaction.transaction_id} not started")
        
        object_id = row.object_id
        if object_id not in self.data_versions:
            self.data_versions[object_id] = [
                RowVersion(value=0, version_id=1, read_ts=0, write_ts=0)
            ]
        
        if self.variant == MVCCVariant.MVTO:
            return self.validate_write_mvto(transaction, row, auto_reexecute)
        
        action = Action(
            action_id=self.next_action_id,
            transaction_id=transaction.transaction_id,
            action_type=ActionType.WRITE,
            object_id=object_id,
            timestamp=datetime.now()
        )
        self.next_action_id += 1
        
        if self.variant == MVCCVariant.MV2PL:
            success, message = self._write_mv2pl(transaction, row, action)
        else:
            success, message = self._write_snapshot(transaction, row, action)
        
        return AlgorithmResponse(allowed=success, message=message)
    
    def commit_transaction(self, transaction: Transaction) -> AlgorithmResponse:
        if transaction.transaction_id not in self.transaction_info:
            return AlgorithmResponse(False, f"Transaction T{transaction.transaction_id} not found")
        
        trans_info = self.transaction_info[transaction.transaction_id]
        
        # Cek apakah transaksi sedang di-block (hanya MV2PL) - tidak bisa commit jika masih menunggu lock
        if self.variant == MVCCVariant.MV2PL:
            # Cek apakah transaksi masih punya operasi pending di queue
            has_pending_ops = any(item['tid'] == transaction.transaction_id and item['type'] != 'commit' 
                                 for item in self.operation_queue)
            
            if has_pending_ops:
                # Tambahkan commit ke queue
                if not any(item['tid'] == transaction.transaction_id and item['type'] == 'commit' 
                          for item in self.operation_queue):
                    self.operation_queue.append({
                        'type': 'commit',
                        'tid': transaction.transaction_id,
                        'transaction': transaction
                    })
                
                queue_str = self._get_queue_string()
                return AlgorithmResponse(
                    allowed=False,
                    message=f"T{transaction.transaction_id} is blocked, cannot commit. {queue_str}"
                )
        
        if transaction.status == TransactionStatus.Aborted:
            return AlgorithmResponse(False, f"Transaction T{transaction.transaction_id} already aborted")
        
        if self.variant == MVCCVariant.MVTO:
            success, message = self._commit_mvto(transaction, trans_info)
        elif self.variant == MVCCVariant.MV2PL:
            result = self._commit_mv2pl(transaction, trans_info)
            if len(result) == 3:
                success, message, executed_ops = result
                # Simpan executed_ops untuk tampilan test jika diperlukan
                if hasattr(self, '_last_executed_ops'):
                    self._last_executed_ops = executed_ops
            else:
                success, message = result
        else:
            success, message = self._commit_snapshot(transaction, trans_info)
        
        if success:
            transaction.set_status(TransactionStatus.Committed)
        else:
            transaction.set_status(TransactionStatus.Aborted)
        
        if self.log_handler:
            event = "COMMIT" if success else "ABORT"
            self.log_handler.log_transaction_event(transaction.transaction_id, event)
        
        return AlgorithmResponse(allowed=success, message=message)
    
    def abort_transaction(self, transaction: Transaction) -> AlgorithmResponse:
        if transaction.transaction_id not in self.transaction_info:
            return AlgorithmResponse(True, f"Transaction T{transaction.transaction_id} not found")
        
        trans_info = self.transaction_info[transaction.transaction_id]
        trans_info.rollback_count += 1
        
        transaction.set_status(TransactionStatus.Aborted)
        
        if self.variant == MVCCVariant.MV2PL:
            self._release_all_locks(transaction.transaction_id)
        
        
        if self.log_handler:
            self.log_handler.log_transaction_event(
                transaction.transaction_id,
                f"ABORT (rollback #{trans_info.rollback_count})"
            )
        
        return AlgorithmResponse(
            allowed=True,
            message=f"Transaction T{transaction.transaction_id} aborted"
        )
    
    def check_permission(self, t: Transaction, obj: Row, action: ActionType) -> AlgorithmResponse:
        # Lazy initialization: jika transaksi belum ada di transaction_info, inisialisasi dulu
        if t.transaction_id not in self.transaction_info:
            self.begin_transaction(t)
        
        if action == ActionType.READ:
            return self.validate_read(t, obj)
        elif action == ActionType.WRITE:
            return self.validate_write(t, obj)
        else:
            return AlgorithmResponse(
                allowed=False,
                message=f"Unknown action type: {action}"
            )
    
    def _read_mvto(self, transaction: Transaction, row: Row, action: Action) -> Tuple[bool, Any, str]:
        object_id = row.object_id
        trans_info = self.transaction_info[transaction.transaction_id]
        trans_ts = trans_info.timestamp
        
        versions = self.data_versions[object_id]
        
        selected_version = None
        for version in reversed(versions):
            if version.write_ts < trans_ts:
                selected_version = version
                break
        
        if selected_version is None:
            selected_version = versions[0]
        
        if selected_version.read_ts < trans_ts:
            selected_version.read_ts = trans_ts
            message = f"T{transaction.transaction_id} reads {object_id}{selected_version.version_id}, R-TS updated to {trans_ts}"
        else:
            message = f"T{transaction.transaction_id} reads {object_id}{selected_version.version_id}, R-TS not updated"
        
        trans_info.read_set.add(object_id)
        # Lacak versi mana (write_ts) yang dibaca untuk cascading rollback
        trans_info.read_versions[object_id] = selected_version.write_ts
        
        return True, selected_version.value, message
    
    def _write_mvto(self, transaction: Transaction, row: Row, action: Action) -> Tuple[bool, str]:
        object_id = row.object_id
        trans_info = self.transaction_info[transaction.transaction_id]
        trans_ts = trans_info.timestamp
        
        versions = self.data_versions[object_id]
        latest_version = versions[-1]
        
        # Cek konflik write-write: TS(T) < RTS(X)
        if trans_ts < latest_version.read_ts:
            # Return ABORT - transaksi perlu di-rollback dan re-execute
            transaction.set_status(TransactionStatus.Aborted)
            return False, f"T{transaction.transaction_id} ABORTED: TS({trans_ts}) < RTS({latest_version.read_ts}) on {object_id} - needs ROLLBACK"
        
        # Tidak ada konflik - lanjutkan write
        if trans_ts == latest_version.write_ts:
            latest_version.value = row.data.get('value', latest_version.value)
            latest_version.read_ts = trans_ts
            message = f"T{transaction.transaction_id} overwrites {object_id}{latest_version.version_id}"
        else:
            new_version = RowVersion(
                value=row.data.get('value', 0),
                version_id=latest_version.version_id + 1,
                read_ts=trans_ts,
                write_ts=trans_ts
            )
            versions.append(new_version)
            
            if trans_info.rollback_count > 0:
                message = f"T{transaction.transaction_id} creates {object_id}{new_version.version_id} (after rollback #{trans_info.rollback_count}, TS={trans_ts})"
            else:
                message = f"T{transaction.transaction_id} creates {object_id}{new_version.version_id}"
        
        trans_info.write_set.add(object_id)
        
        return True, message
    
    def validate_write_mvto(self, transaction: Transaction, row: Row, auto_reexecute: bool = True) -> MVTOResponse:
        """Validasi write untuk MVTO dengan rollback dan re-execution otomatis.
        
        Returns:
            MVTOResponse yang berisi:
            - allowed: apakah operasi berhasil
            - message: pesan hasil operasi
            - cascaded_tids: List ID transaksi yang di-cascade (None jika tidak ada rollback)
        """
        action = Action(
            action_id=self.next_action_id,
            transaction_id=transaction.transaction_id,
            action_type=ActionType.WRITE,
            object_id=row.object_id,
            timestamp=datetime.now()
        )
        self.next_action_id += 1
        
        # Coba operasi write
        success, message = self._write_mvto(transaction, row, action)
        
        # Jika gagal (ABORT) dan auto_reexecute enabled, rollback dan retry
        if not success and auto_reexecute:
            # Lakukan rollback dan dapatkan transaksi yang di-cascade
            new_ts, cascaded_tids = self.rollback_mvto_transaction(transaction)
            
            # Re-execute write dengan timestamp baru
            success, message = self._write_mvto(transaction, row, action)
            
            return MVTOResponse(allowed=success, message=message, cascaded_tids=cascaded_tids)
        
        # Tidak perlu rollback
        return MVTOResponse(allowed=success, message=message, cascaded_tids=None)
    
    def rollback_mvto_transaction(self, transaction: Transaction) -> Tuple[int, List[int]]:
        """Rollback transaksi di MVTO dan tangani cascading rollback.
        Returns (new_timestamp, cascaded_transaction_ids).
        """
        trans_info = self.transaction_info[transaction.transaction_id]
        
        # Simpan timestamp lama sebelum update (untuk menghapus versi)
        old_timestamp = trans_info.timestamp
        
        # Hitung timestamp baru (lebih besar dari semua transaksi aktif)
        max_concurrent_ts = max(
            (info.timestamp for tid, info in self.transaction_info.items()
                if tid != transaction.transaction_id),
            default=0
        )
        new_timestamp = max(self.operation_count, max_concurrent_ts + 1)
        self.operation_count = new_timestamp
        
        # Increment counter rollback
        trans_info.rollback_count += 1
        
        # Simpan write set lama untuk pengecekan cascading rollback
        old_write_set = trans_info.write_set.copy()
        
        # Bersihkan state transaksi untuk re-execution
        trans_info.write_set.clear()
        trans_info.read_set.clear()
        trans_info.timestamp = new_timestamp
        trans_info.locks_held.clear()
        trans_info.buffered_writes.clear()
        
        # Hapus versi yang dibuat oleh transaksi ini (menggunakan old_timestamp)
        for obj_id in old_write_set:
            if obj_id in self.data_versions:
                # Hapus versi yang ditulis oleh transaksi ini dengan timestamp lama
                self.data_versions[obj_id] = [
                    v for v in self.data_versions[obj_id]
                    if v.write_ts != old_timestamp
                ]
        
        # Set status transaksi ke active untuk re-execution
        transaction.set_status(TransactionStatus.Active)
        
        # Cascading rollback: abort transaksi yang membaca VERSI yang ditulis oleh transaksi ini
        # Hanya abort jika mereka membaca versi spesifik dengan write_ts = old_timestamp
        cascaded_transactions = []
        for tid, other_info in self.transaction_info.items():
            if tid == transaction.transaction_id:
                continue
            if other_info.transaction.status == TransactionStatus.Aborted:
                continue
            if other_info.transaction.status == TransactionStatus.Committed:
                continue
            
            # Cek apakah transaksi lain membaca VERSI yang ditulis oleh transaksi yang di-rollback
            # Transaksi harus di-cascade hanya jika membaca versi dengan write_ts = old_timestamp
            should_cascade = False
            for obj_id in old_write_set:
                # Cek apakah transaksi lain membaca objek ini DAN membaca versi yang ditulis oleh tx yang di-rollback
                if obj_id in other_info.read_versions:
                    if other_info.read_versions[obj_id] == old_timestamp:
                        should_cascade = True
                        break
            
            if should_cascade:
                other_info.transaction.set_status(TransactionStatus.Aborted)
                cascaded_transactions.append(tid)
        
        return new_timestamp, cascaded_transactions
    
    def _rollback_mvto(self, transaction: Transaction) -> int:
        """Method legacy - memanggil rollback_mvto_transaction"""
        new_ts, _ = self.rollback_mvto_transaction(transaction)
        return new_ts
    
    def _commit_mvto(self, transaction: Transaction, trans_info: TransactionInfo) -> Tuple[bool, str]:
        return True, f"T{transaction.transaction_id} COMMIT"
    
    def _read_mv2pl(self, transaction: Transaction, row: Row, trans_info: TransactionInfo) -> Tuple[bool, Any, str]:
        object_id = row.object_id
        trans_info = self.transaction_info[transaction.transaction_id]
        
        # Cek apakah ini benar-benar read-only untuk objek spesifik ini
        is_read_only_for_object = (trans_info.transaction_type == TransactionType.READ_ONLY or 
                                   object_id not in trans_info.write_intents)
        
        if is_read_only_for_object:
            # Tidak perlu lock - baca versi committed
            versions = self.data_versions[object_id]
            selected_version = versions[0]
            
            # Read-only: baca versi COMMITTED terbaru
            for version in reversed(versions):
                # Skip versi uncommitted (W-TS = inf atau commit_ts adalah None)
                if version.write_ts == float('inf') or version.commit_ts is None:
                    continue
                # Gunakan versi committed dengan commit_ts <= ts_counter saat ini
                if version.commit_ts <= self.ts_counter:
                    selected_version = version
                    break
            
            trans_info.read_set.add(object_id)
            return True, selected_version.value, f"T{transaction.transaction_id} (read-only) reads {object_id}"
        
        else:
            # Akan menulis objek ini - perlu lock
            if not self._acquire_lock(transaction.transaction_id, object_id, "S"):
                # Tambahkan ke queue
                self.operation_queue.append({
                    'type': 'read',
                    'tid': transaction.transaction_id,
                    'object_id': object_id,
                    'row': row
                })
                queue_str = self._get_queue_string()
                return False, None, f"T{transaction.transaction_id} blocked waiting for lock-S({object_id}) {queue_str}"
            
            versions = self.data_versions[object_id]
            latest_version = versions[-1]
            
            trans_info.read_set.add(object_id)
            
            # Cek apakah lock ini didapat dari queue
            if object_id in trans_info.locks_from_queue:
                trans_info.locks_from_queue.remove(object_id)  # Hapus flag setelah digunakan
                return True, latest_version.value, f"T{transaction.transaction_id} reads {object_id} (lock-S acquired from queue)"
            return True, latest_version.value, f"T{transaction.transaction_id} reads {object_id} (lock-S acquired)"
    
    def _write_mv2pl(self, transaction: Transaction, row: Row, action: Action) -> Tuple[bool, str]:
        object_id = row.object_id
        trans_info = self.transaction_info[transaction.transaction_id]
        
        # Tandai write intent
        trans_info.write_intents.add(object_id)
        
        if not self._acquire_lock(transaction.transaction_id, object_id, "X"):
            # Tambahkan ke queue
            self.operation_queue.append({
                'type': 'write',
                'tid': transaction.transaction_id,
                'object_id': object_id,
                'row': row
            })
            queue_str = self._get_queue_string()
            return False, f"T{transaction.transaction_id} blocked waiting for lock-X({object_id}) {queue_str}"
        
        versions = self.data_versions[object_id]
        new_version = RowVersion(
            value=row.data.get('value', 0),
            version_id=len(versions),
            read_ts=float('inf'),
            write_ts=float('inf')
        )
        versions.append(new_version)
        
        trans_info.write_set.add(object_id)
        
        # Cek apakah lock ini didapat dari queue
        if object_id in trans_info.locks_from_queue:
            trans_info.locks_from_queue.remove(object_id)  # Hapus flag setelah digunakan
            return True, f"T{transaction.transaction_id} writes {object_id}∞ (lock-X acquired from queue)"
        return True, f"T{transaction.transaction_id} writes {object_id}∞ (lock-X acquired)"
    
    def _commit_mv2pl(self, transaction: Transaction, trans_info: TransactionInfo) -> Tuple[bool, str]:
        if trans_info.transaction_type == TransactionType.READ_ONLY:
            return True, f"T{transaction.transaction_id} (read-only) COMMIT"
        
        new_ts = self.ts_counter + 1
        trans_info.timestamp = new_ts
        
        for object_id in trans_info.write_set:
            versions = self.data_versions[object_id]
            for version in versions:
                if version.write_ts == float('inf'):
                    version.read_ts = new_ts
                    version.write_ts = new_ts
                    version.commit_ts = new_ts
        
        self.ts_counter += 1
        
        # Proses queue dan dapatkan operasi yang dieksekusi
        executed_ops = self._release_all_locks_and_process_queue(transaction.transaction_id)
        
        queue_str = self._get_queue_string()
        
        base_message = f"T{transaction.transaction_id} COMMIT (TS={new_ts})"
        
        if executed_ops:
            # Tambahkan info tentang pemrosesan queue
            if queue_str:
                return True, f"{base_message}, process queue {queue_str}", executed_ops
            return True, f"{base_message}, process queue", executed_ops
        
        if queue_str:
            return True, f"{base_message} {queue_str}"
        return True, base_message
    
    def _acquire_lock(self, transaction_id: int, object_id: str, lock_type: str) -> bool:
        trans_info = self.transaction_info[transaction_id]
        
        # Cek apakah transaksi sudah punya lock ini
        if object_id in trans_info.locks_held:
            held_lock = trans_info.locks_held[object_id]
            # Jika sudah punya X lock, bisa melakukan apapun
            if held_lock == "X":
                return True
            # Jika punya S lock dan mau S lock, OK
            if held_lock == "S" and lock_type == "S":
                return True
            # Jika punya S lock dan mau X lock, perlu upgrade (cek tidak ada S lock lain)
            if held_lock == "S" and lock_type == "X":
                # Cek apakah ada transaksi lain yang memegang S lock pada objek yang sama
                for tid, other_info in self.transaction_info.items():
                    if tid == transaction_id:
                        continue
                    if object_id in other_info.locks_held and other_info.locks_held[object_id] == "S":
                        # Tidak bisa upgrade, harus menunggu
                        self.blocked_transactions.add(transaction_id)
                        return False
                # Bisa upgrade
                trans_info.locks_held[object_id] = "X"
                return True
        
        # Cek apakah lock bisa didapat
        can_acquire = True
        for tid, other_info in self.transaction_info.items():
            if tid == transaction_id:
                continue
            
            if object_id in other_info.locks_held:
                held_lock = other_info.locks_held[object_id]
                # Kasus konflik:
                # 1. Mau X lock - konflik dengan lock apapun (S atau X)
                # 2. Mau S lock - konflik hanya dengan X lock
                if lock_type == "X" or held_lock == "X":
                    can_acquire = False
                    break
        
        if not can_acquire:
            # Tandai transaksi sebagai blocked
            self.blocked_transactions.add(transaction_id)
            return False
        
        # Berikan lock
        trans_info.locks_held[object_id] = lock_type
        return True
    
    def _release_all_locks(self, transaction_id: int) -> None:
        trans_info = self.transaction_info[transaction_id]
        trans_info.locks_held.clear()
    
    def _release_all_locks_and_process_queue(self, transaction_id: int) -> List[Tuple[str, int, str, Any]]:
        """Lepaskan lock dan proses queue. Returns list dari (op_type, tid, message, value) untuk operasi yang dieksekusi."""
        trans_info = self.transaction_info[transaction_id]
        trans_info.locks_held.clear()
        
        executed_ops = []
        
        # Proses queue operasi sampai tidak ada lagi progress
        progress = True
        while progress:
            progress = False
            remaining_queue = []
            
            for op in self.operation_queue:
                op_tid = op['tid']
                op_type = op['type']
                
                if op_type == 'read':
                    object_id = op['object_id']
                    row = op['row']
                    
                    # Coba untuk mendapat lock
                    if self._acquire_lock(op_tid, object_id, "S"):
                        # Eksekusi read
                        versions = self.data_versions[object_id]
                        latest_version = versions[-1]
                        trans_info_op = self.transaction_info[op_tid]
                        trans_info_op.read_set.add(object_id)
                        trans_info_op.locks_from_queue.add(object_id)
                        
                        message = f"T{op_tid} reads {object_id} (lock-S acquired from queue)"
                        executed_ops.append(('read', op_tid, message, latest_version.value))
                        progress = True
                    else:
                        remaining_queue.append(op)
                
                elif op_type == 'write':
                    object_id = op['object_id']
                    row = op['row']
                    
                    # Coba untuk mendapat lock
                    if self._acquire_lock(op_tid, object_id, "X"):
                        # Eksekusi write
                        versions = self.data_versions[object_id]
                        new_version = RowVersion(
                            value=row.data.get('value', 0),
                            version_id=len(versions),
                            read_ts=float('inf'),
                            write_ts=float('inf')
                        )
                        versions.append(new_version)
                        
                        trans_info_op = self.transaction_info[op_tid]
                        trans_info_op.write_set.add(object_id)
                        trans_info_op.locks_from_queue.add(object_id)
                        
                        message = f"T{op_tid} writes {object_id}∞ (lock-X acquired from queue)"
                        executed_ops.append(('write', op_tid, message, None))
                        progress = True
                    else:
                        remaining_queue.append(op)
                
                elif op_type == 'commit':
                    # Cek apakah transaksi masih punya operasi non-commit yang pending
                    has_pending = any(item['tid'] == op_tid and item['type'] != 'commit' 
                                    for item in self.operation_queue if item != op)
                    
                    if not has_pending:
                        # Bisa commit sekarang
                        transaction = op['transaction']
                        trans_info_commit = self.transaction_info[op_tid]
                        
                        if trans_info_commit.transaction_type != TransactionType.READ_ONLY:
                            new_ts = self.ts_counter + 1
                            trans_info_commit.timestamp = new_ts
                            
                            for obj_id in trans_info_commit.write_set:
                                versions = self.data_versions[obj_id]
                                for version in versions:
                                    if version.write_ts == float('inf'):
                                        version.read_ts = new_ts
                                        version.write_ts = new_ts
                                        version.commit_ts = new_ts
                            
                            self.ts_counter += 1
                            message = f"T{op_tid} COMMIT (TS={new_ts}) (from queue)"
                        else:
                            message = f"T{op_tid} (read-only) COMMIT (from queue)"
                        
                        transaction.set_status(TransactionStatus.Committed)
                        executed_ops.append(('commit', op_tid, message, None))
                        progress = True
                        
                        # Hapus dari blocked
                        if op_tid in self.blocked_transactions:
                            self.blocked_transactions.remove(op_tid)
                    else:
                        remaining_queue.append(op)
            
            self.operation_queue = remaining_queue
            
            # Unblock transaksi yang tidak punya lagi operasi pending
            for tid in list(self.blocked_transactions):
                if not any(item['tid'] == tid for item in self.operation_queue):
                    self.blocked_transactions.remove(tid)
        
        return executed_ops
    
    def _read_snapshot(self, transaction: Transaction, row: Row, action: Action) -> Tuple[bool, Any, str]:
        object_id = row.object_id
        trans_info = self.transaction_info[transaction.transaction_id]
        versions = self.data_versions[object_id]
        
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
            for tid, other_info in self.transaction_info.items():
                if tid == transaction.transaction_id:
                    continue
                if other_info.transaction.status in [TransactionStatus.Committed, TransactionStatus.Aborted]:
                    continue
                
                if other_info.has_exclusive_lock:
                    transaction.set_status(TransactionStatus.Aborted)
                    return False, f"T{transaction.transaction_id} ABORTED: exclusive lock conflict"
            
            trans_info.has_exclusive_lock = True
        
        trans_info.write_set.add(object_id)
        trans_info.buffered_writes[object_id] = row.data.get('value', 0)
        
        return True, f"T{transaction.transaction_id} writes {object_id} (buffered)"
    
    def _commit_snapshot(self, transaction: Transaction, trans_info: TransactionInfo) -> Tuple[bool, str]:
        if not trans_info.write_set:
            return True, f"T{transaction.transaction_id} COMMIT (read-only)"
        
        if self.isolation_policy == IsolationPolicy.FIRST_COMMITTER_WIN:
            for object_id in trans_info.write_set:
                versions = self.data_versions[object_id]
                for version in versions:
                    if (version.commit_ts is not None and
                        version.commit_ts > trans_info.timestamp and
                        version.creator_ts != transaction.transaction_id):
                        return False, f"T{transaction.transaction_id} ABORTED: write-write conflict"
        
        self.ts_counter += 1
        
        for object_id, value in trans_info.buffered_writes.items():
            versions = self.data_versions[object_id]
            new_version = RowVersion(
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
                initial_version = RowVersion(
                    value=initial_value,
                    version_id=0,
                    read_ts=0,
                    write_ts=0,
                    creator_ts=-1,
                    commit_ts=0
                )
            else:
                initial_version = RowVersion(
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
    
    def _get_queue_string(self) -> str:
        """Generate string tampilan queue dalam format: Q = [W3(B), W5(E), C3]"""
        if not self.operation_queue:
            return ""
        
        queue_items = []
        for op in self.operation_queue:
            tid = op['tid']
            op_type = op['type']
            
            if op_type == 'read':
                object_id = op['object_id']
                queue_items.append(f"R{tid}({object_id})")
            elif op_type == 'write':
                object_id = op['object_id']
                queue_items.append(f"W{tid}({object_id})")
            elif op_type == 'commit':
                queue_items.append(f"C{tid}")
        
        if queue_items:
            return f"Q = [{', '.join(queue_items)}]"
        return ""
    
    def set_transaction_type(self, transaction_id: int, trans_type: str) -> None:
        if transaction_id in self.transaction_info:
            if trans_type == "READ_ONLY":
                self.transaction_info[transaction_id].transaction_type = TransactionType.READ_ONLY
            else:
                self.transaction_info[transaction_id].transaction_type = TransactionType.UPDATE
    
    def set_write_intent(self, transaction_id: int, object_id: str) -> None:
        """Tandai bahwa transaksi akan menulis ke objek ini (untuk keputusan lock MV2PL)"""
        if transaction_id in self.transaction_info:
            self.transaction_info[transaction_id].write_intents.add(object_id)
    
    def get_data_versions(self, object_id: str) -> List[RowVersion]:
        return self.data_versions.get(object_id, [])