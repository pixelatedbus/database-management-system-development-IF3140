from .buffer import buffer, table
from .log import actiontype, log
from .recovery_criteria import RecoveryCriteria
from .logFile import logFile
import threading
from typing import List, Set
from datetime import datetime

# replace with the correct one later
from .fake_exec_result import ExecutionResult 

# TODO: MAKE THIS SINGLETON BRUH
class FailureRecovery:

    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                # Double-checked locking to ensure thread safety
                if not cls._instance:
                    cls._instance = super(FailureRecovery, cls).__new__(cls)
        return cls._instance

    def __init__(self, wal_size=50):
        
        # singleton check if init or not
        if getattr(self, "_initialized", False):
            return

        self.lock = threading.RLock()
        self.buffer: buffer = []
        self.mem_wal: List[ExecutionResult] = [] # List of Execution Results, this is the write-ahead log
        self.logFile: logFile = logFile()
        self.wal_size: int = wal_size
        self.undo_list: List[int] = []

        self.current_transaction_id: int = 0
        self.current_table_name: str = ""

        # singleton class attr
        self._initialized = True
    
    def _flush_mem_wal(self):
        with self.lock:
            # [MODIFIED] Menggunakan 'self.logFile' (instance) bukan 'logFile' (class)
            # Dan menambahkan pengecekan agar tidak error jika kosong
            if not self.mem_wal:
                return

            for infos in self.mem_wal:
                # [MODIFIED] Pastikan method ini ada di logFile.py Anda (write_log atau write_log_execRes)
                # Saya ikuti pola kode teman Anda: write_log_execRes
                self.logFile.write_log_execRes(infos)
            
            # [MODIFIED] CRITICAL FIX: Mengosongkan buffer memori setelah ditulis ke disk
            # Ini memperbaiki bug "Test Flush Failed"
            self.mem_wal = []

    def write_log(self, info: ExecutionResult):
        '''
        Important notice: this only updates the memory's write-ahead log, maybe some adjustment is needed in the future? idk
        for now lets say the following:
        
        - if a commit is to occur, write from mem_wal to .log file via logFile
        '''

        # Maybe set the current transaction id here?
        self.current_transaction_id = info.transaction_id
        self.current_table_name = info.table_name

        try:
            with self.lock:
                self.mem_wal.append(info)

                # [MODIFIED] Logika Flush disatukan untuk menangani bug Memory Leak & Auto-Flush
                # FIX ERROR: 'log' object has no attribute 'query'. Kita pakai getattr atau check hasattr.
                is_commit = False
                
                # Cek actiontype enum
                if info.action == actiontype.commit:
                    is_commit = True
                # Cek string action
                elif str(info.action).lower() == "commit":
                    is_commit = True
                # Cek query string (Safe Check)
                elif hasattr(info, 'query') and "commit" in str(info.query).lower():
                    is_commit = True

                # Deteksi Buffer Penuh
                is_buffer_full = len(self.mem_wal) >= self.wal_size

                # Jika salah satu kondisi terpenuhi, lakukan flush
                if is_commit or is_buffer_full:
                    self._flush_mem_wal()
                
                # to be more efficient, we can also insert the transaction ID into the undo list
                if not is_commit: # [MODIFIED] Simplified logic
                    if info.transaction_id not in self.undo_list:
                        self.undo_list.append(info.transaction_id)

        except Exception as e:
            print(f"Error in Failure Recovery Manager;write_log: {e}")
        
        pass

    def _save_checkpoint(self):

        checkpoint_log = log(
            transaction_id=self.current_transaction_id, 
            action=actiontype.checkpoint,
            timestamp=datetime.now(),
            old_data=self.undo_list,
            new_data=None,
            table_name=self.current_table_name
        )

        # Dalam method ini, semua entri dalam write-ahead log sejak checkpoint terakhir akan digunakan untuk memperbarui data di physical storage, agar data tetap sinkron.
        # Assume this is the part where we renew the .dat file

        # [MODIFIED] Tulis checkpoint ke buffer dulu, lalu paksa flush
        # Agar checkpoint tercatat urut di disk
        self.write_log(checkpoint_log)
        self._flush_mem_wal() 

        pass

    def _recover_transaction(self, transaction_id: int) -> None:
        self._flush_mem_wal()

        l_list = self.logFile.get_logs()

        # 

        for i in range(len(l_list) - 1, -1, -1):
            print(i, end=" ")
            l = l_list[i]
            if l.transaction_id == transaction_id:
                if l.action == actiontype.write:
                    undo_log = log(
                        transaction_id=l.transaction_id,
                        action=actiontype.write,
                        timestamp=datetime.now(), # TODO: check if this is correct?
                        old_data=l.new_data,
                        new_data=l.old_data,
                        table_name=l.table_name
                    )
                    # [MODIFIED] Ubah ke write_log_execRes agar konsisten dengan flush & ditangkap Mock Test
                    self.logFile.write_log_execRes(undo_log)
                    # TODO: DO THE UNDO TO THE BUFFER HERE
                else: # start, commit, abort.
                    break
        abort_log = log(
            transaction_id=transaction_id,
            action=actiontype.abort,
            timestamp=datetime.now(), # TODO: check if this is correct?
            old_data={},
            new_data={}
        )
        # [MODIFIED] Ubah ke write_log_execRes
        self.logFile.write_log_execRes(abort_log)
    
    # [MODIFIED] Menambahkan method recover_system_crash untuk memenuhi kebutuhan Unit Test (Idempotency)
    # Method ini menangani restart setelah mati listrik/crash
    def recover_system_crash(self):
        
        
        # 1. Pastikan semua yang di memori tertulis (jika masih ada sisa)
        self._flush_mem_wal()
        
        # 2. Baca semua log
        logs = self.logFile.get_logs()
        if not logs:
            return

        committed_tx_ids = set()
        aborted_tx_ids = set()

        # Phase Analysis: Cari winner & loser
        for entry in logs:
            if entry.action == actiontype.commit:
                committed_tx_ids.add(entry.transaction_id)
            elif entry.action == actiontype.abort:
                aborted_tx_ids.add(entry.transaction_id)

        # Transaksi yang sudah selesai (Commit/Abort) tidak perlu diapa-apakan
        processed_tx_ids = committed_tx_ids.union(aborted_tx_ids)

        # Phase Undo: Mundur dari belakang
        for i in range(len(logs) - 1, -1, -1):
            entry = logs[i]
            tx_id = entry.transaction_id

            # Skip jika transaksi sistem (ID 0) atau sudah selesai
            if tx_id == 0 or tx_id in processed_tx_ids:
                continue

            # Jika ketemu START dari transaksi yang belum selesai (Loser)
            if entry.action == actiontype.start:
                # Tulis log ABORT agar recovery berikutnya tahu ini sudah selesai (Idempotency)
                abort_log = log(
                    transaction_id=tx_id,
                    action=actiontype.abort,
                    timestamp=datetime.now(),
                    old_data={},
                    new_data={}
                )
                # [MODIFIED] Gunakan write_log_execRes agar tertangkap oleh Unit Test Mock
                self.logFile.write_log_execRes(abort_log)
                processed_tx_ids.add(tx_id)
                continue

            # Jika ketemu WRITE, lakukan UNDO
            if entry.action == actiontype.write:
                # Logic undo: swap new_data dengan old_data
                undo_log = log(
                    transaction_id=tx_id,
                    action=actiontype.write,
                    timestamp=datetime.now(),
                    old_data=entry.new_data, # Swap
                    new_data=entry.old_data, # Swap
                    table_name=entry.table_name
                )
                # [MODIFIED] Gunakan write_log_execRes agar tertangkap oleh Unit Test Mock
                self.logFile.write_log_execRes(undo_log)
                # Note: Actual data restore logic should be connected to Storage Manager here

    def recover(self, criteria: RecoveryCriteria = None) -> None:
        '''
        Implemented so far: transactional recovery
        TODO: add recovery for system crash (?)
        '''
        
        # [MODIFIED] Memanggil method system crash jika criteria meminta (atau default jika None)
        # Sesuai TODO teman Anda
        if criteria is None:
             # Asumsi jika tidak ada kriteria spesifik, lakukan crash recovery full
             self.recover_system_crash()
             return

        if criteria == None:
            raise("Recovery Criteria cannot be None")
        
        # For now we assume its transactional
        self._recover_transaction(criteria.transaction_id)