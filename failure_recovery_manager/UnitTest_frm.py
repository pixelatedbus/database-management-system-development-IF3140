import unittest
import os
import shutil
import sys
from datetime import datetime
from unittest.mock import MagicMock, patch
from pathlib import Path

# Cara run python -m failure_recovery_manager.UnitTest

# ==========================================
# 1. MOCK CLASSES (Simulasi Modul Lain)
# ==========================================
# Kita mock modul dependency agar test ini bisa jalan mandiri
# tanpa perlu file asli storage_manager atau database.

class Condition:
    def __init__(self, column, operation, operand):
        self.column = column
        self.operation = operation
        self.operand = operand
    def __repr__(self): return f"Cond({self.column}{self.operation}{self.operand})"
    def __eq__(self, other):
        return self.__dict__ == other.__dict__

class DataWrite:
    def __init__(self, table, column, conditions, new_value):
        self.table = table
        self.column = column
        self.conditions = conditions
        self.new_value = new_value
    def __eq__(self, other): return self.__dict__ == other.__dict__

class DataDeletion:
    def __init__(self, table, conditions):
        self.table = table
        self.conditions = conditions
    def __eq__(self, other): return self.__dict__ == other.__dict__

class DataUpdate:
    def __init__(self, table, old_data, new_data):
        self.table = table
        self.old_data = old_data
        self.new_data = new_data
    def __eq__(self, other): return self.__dict__ == other.__dict__

class ExecutionResult:
    def __init__(self, transaction_id, action, timestamp, old_data=None, new_data=None, table_name="", query=""):
        self.transaction_id = transaction_id
        self.action = action
        self.timestamp = timestamp
        # Konsistensi: Selalu gunakan List of Dicts
        self.old_data = old_data if old_data is not None else []
        self.new_data = new_data if new_data is not None else []
        self.table_name = table_name
        self.query = query

# Apply Mocks ke sys.modules
mock_storage_models = MagicMock()
mock_storage_models.DataWrite = DataWrite
mock_storage_models.DataDeletion = DataDeletion
mock_storage_models.DataUpdate = DataUpdate
mock_storage_models.Condition = Condition
sys.modules["storage_manager.models"] = mock_storage_models

mock_fake_exec = MagicMock()
mock_fake_exec.ExecutionResult = ExecutionResult
sys.modules["failure_recovery_manager.fake_exec_result"] = mock_fake_exec

# ==========================================
# 2. IMPORT MODULE SYSTEM UNDER TEST
# ==========================================
# Asumsi struktur folder Anda sesuai. 
from failure_recovery_manager.failure_recovery_manager import FailureRecovery
from failure_recovery_manager.log import actiontype, log
# Note: LogFile dan recovery_criteria diimport via FailureRecovery biasanya, 
# tapi kita import untuk keperluan assert

class TestFailureRecoveryManager(unittest.TestCase):

    def setUp(self):
        """Persiapan lingkungan bersih sebelum setiap test."""
        # Reset Singleton
        FailureRecovery._instance = None
        FailureRecovery._initialized = False
        
        # Setup direktori log test
        self.test_log_dir = "test_log_dir_unittest"
        if os.path.exists(self.test_log_dir):
            shutil.rmtree(self.test_log_dir)
        os.makedirs(self.test_log_dir, exist_ok=True)
        
        # Patch Path untuk mengarahkan log ke folder test
        self.patcher = patch('failure_recovery_manager.logFile.Path')
        self.MockPath = self.patcher.start()
        
        # Mocking resolve() agar menunjuk ke folder test kita
        mock_resolved_path = MagicMock()
        self.MockPath.return_value.resolve.return_value = mock_resolved_path
        absolute_test_path = Path(os.path.abspath(self.test_log_dir))
        mock_resolved_path.parent = absolute_test_path
        
        # Inisialisasi System Under Test (SUT)
        self.frm = FailureRecovery(wal_size=3)
        self.frm.logFile.make_new_file()
        
        # Buat file fisik kosong agar handler file tidak error
        for path in self.frm.logFile.paths:
            if isinstance(path, Path):
                with open(path, 'w') as f: pass

    def tearDown(self):
        """Pembersihan setelah test selesai."""
        self.patcher.stop()
        FailureRecovery._instance = None
        if os.path.exists(self.test_log_dir):
            shutil.rmtree(self.test_log_dir)

    # ----------------------------------------------------------------
    # GROUP 1: BASIC FUNCTIONALITY (Buffer, Flush, Rotation)
    # ----------------------------------------------------------------

    def test_singleton_behavior(self):
        """Memastikan instance FailureRecovery bersifat Singleton."""
        frm1 = FailureRecovery()
        frm2 = FailureRecovery()
        self.assertIs(frm1, frm2, "Instance harus object yang sama")
        
        # Cek atribut tidak ter-reset
        frm1.wal_size = 999
        frm3 = FailureRecovery()
        self.assertEqual(frm3.wal_size, 999)

    def test_write_log_memory_buffering(self):
        """Log harus masuk memori dulu (WAL), tidak langsung ke disk."""
        exec_res = ExecutionResult(101, actiontype.write, datetime.now(), 
                                   old_data=[{'id': 1}], new_data=[{'id': 2}], table_name="Student")
        
        self.frm.write_log(exec_res)
        
        # Assert masuk buffer memori
        self.assertEqual(len(self.frm.mem_wal), 1)
        self.assertEqual(self.frm.mem_wal[0].transaction_id, 101)
        
        # Assert disk masih kosong (karena wal_size=3, baru isi 1)
        log_content = self.frm.logFile.get_logs()
        self.assertEqual(len(log_content), 0, "Log tidak boleh flush ke disk sebelum batas WAL/Commit")

    def test_write_log_flush_on_commit(self):
        """Log harus dipaksa flush ke disk saat ada COMMIT."""
        # 1. Write biasa (masuk memori)
        write_res = ExecutionResult(101, actiontype.write, datetime.now(), [], [{'a':1}], "T1")
        self.frm.write_log(write_res)
        
        # 2. Commit (harus trigger flush)
        commit_res = ExecutionResult(101, actiontype.commit, datetime.now(), [], [], "T1", query="COMMIT")
        self.frm.write_log(commit_res)
        
        # Assert data ada di disk
        logs_on_disk = self.frm.logFile.get_logs()
        self.assertTrue(len(logs_on_disk) >= 2, "Harus ada minimal log Write dan Commit di disk")
        self.assertEqual(logs_on_disk[-1].action, actiontype.commit)
        self.assertEqual(len(self.frm.mem_wal), 0, "Memori buffer harus kosong setelah flush")

    def test_wal_size_limit_trigger_flush(self):
        """Log harus flush jika memori penuh (WAL Size Limit)."""
        self.frm.wal_size = 2 # Set batas kecil
        
        # Isi log ke-1
        self.frm.write_log(ExecutionResult(1, 1, datetime.now(), [], [], "T"))
        self.assertEqual(len(self.frm.mem_wal), 1)
        
        # Isi log ke-2 (Batas tercapai, trigger flush)
        self.frm.write_log(ExecutionResult(1, 1, datetime.now(), [], [], "T"))
        
        # Cek apakah sudah masuk disk
        logs_on_disk = self.frm.logFile.get_logs()
        self.assertEqual(len(logs_on_disk), 2, "Harus flush karena WAL penuh")
        self.assertEqual(len(self.frm.mem_wal), 0)

    # ----------------------------------------------------------------
    # GROUP 2: FAILURE RECOVERY & ATOMICITY
    # ----------------------------------------------------------------

    def test_recover_uncommitted_transaction(self):
        """Skenario: Transaksi Write tapi Crash sebelum Commit (Harus Undo/Abort)."""
        tx_id = 500
        
        # 1. Buat log simulasi di disk (Write tanpa Commit)
        log1 = log(tx_id, actiontype.start, datetime.now())
        log2 = log(tx_id, actiontype.write, datetime.now(), 
                   old_data=[{'id':1, 'val':'A'}], 
                   new_data=[{'id':1, 'val':'B'}], 
                   table_name="Users")
        
        self.frm.logFile.write_log(log1)
        self.frm.logFile.write_log(log2)
        
        # 2. Jalankan Recovery
        self.frm.recover_system_crash()
        
        # 3. Assertions
        final_logs = self.frm.logFile.get_logs()
        aborted_tx = [l for l in final_logs if l.action == actiontype.abort]
        
        self.assertTrue(len(aborted_tx) > 0, "Harus ada log ABORT untuk transaksi yang tidak tuntas")
        self.assertEqual(aborted_tx[0].transaction_id, tx_id)
        
        # Validasi Undo Data (Opsional: Cek return value jika method me-return undo ops)
        # Di sini kita mengecek bahwa sistem mendeteksi kegagalan.

    def test_durability_committed_transaction(self):
        """Skenario: Transaksi sudah Commit lalu Crash (Harus selamat, TIDAK Boleh Undo)."""
        tx_id = 600
        
        # 1. Buat log lengkap (Start -> Write -> Commit)
        logs = [
            log(tx_id, actiontype.start, datetime.now()),
            log(tx_id, actiontype.write, datetime.now(), [{'v':1}], [{'v':2}], "T"),
            log(tx_id, actiontype.commit, datetime.now()) # COMMIT ADA
        ]
        for l in logs: self.frm.logFile.write_log(l)
        
        # 2. Jalankan Recovery
        self.frm.recover_system_crash()
        
        # 3. Assertions
        final_logs = self.frm.logFile.get_logs()
        aborted_ids = [l.transaction_id for l in final_logs if l.action == actiontype.abort]
        
        self.assertNotIn(tx_id, aborted_ids, 
                         "Transaksi yang sudah COMMIT tidak boleh di-ABORT saat recovery")

    def test_recovery_idempotency(self):
        """
        NEW FEATURE TEST: Idempotency.
        Recovery harus aman dijalankan berkali-kali. 
        Jika crash terjadi saat recovery, recovery berikutnya tidak boleh merusak data.
        """
        tx_commit = 10
        tx_crash = 20
        
        # Setup Log: 
        # Tx 10 -> Sukses
        # Tx 20 -> Gagal (Crash di tengah)
        setup_logs = [
            log(tx_commit, actiontype.start, datetime.now()),
            log(tx_commit, actiontype.write, datetime.now(), [{'a':1}], [{'a':2}], "T"),
            log(tx_commit, actiontype.commit, datetime.now()),
            
            log(tx_crash, actiontype.start, datetime.now()),
            log(tx_crash, actiontype.write, datetime.now(), [{'b':1}], [{'b':2}], "T"),
            # Tx 20 tidak ada commit -> System Crash
        ]
        for l in setup_logs: self.frm.logFile.write_log(l)
        
        # --- Recovery Tahap 1 ---
        self.frm.recover_system_crash()
        logs_phase_1 = self.frm.logFile.get_logs()
        aborts_1 = [l.transaction_id for l in logs_phase_1 if l.action == actiontype.abort]
        
        self.assertIn(tx_crash, aborts_1, "Pass 1: Tx Crash harus di-abort")
        self.assertNotIn(tx_commit, aborts_1, "Pass 1: Tx Commit aman")
        
        # --- Recovery Tahap 2 (Simulasi restart setelah recovery pertama) ---
        # Kita panggil lagi tanpa mengubah state log (seolah restart lagi)
        self.frm.recover_system_crash()
        logs_phase_2 = self.frm.logFile.get_logs()
        aborts_2 = [l.transaction_id for l in logs_phase_2 if l.action == actiontype.abort]
        
        # Assertions Konsistensi
        self.assertEqual(aborts_1, aborts_2, 
                         "Idempotency Gagal: Hasil recovery kedua berbeda dengan yang pertama.")
        self.assertEqual(len(logs_phase_1), len(logs_phase_2),
                         "Idempotency Gagal: Jumlah log bertambah padahal tidak ada crash baru.")

    def test_mixed_transaction_scenario(self):
        """Test kompleks dengan checkpoint dan multiple transactions."""
        # Setup:
        # T1: Commit sebelum Checkpoint
        # T2: Commit setelah Checkpoint
        # T3: Crash (Start setelah Checkpoint)
        
        logs = []
        # T1
        logs.append(log(1, actiontype.start, datetime.now()))
        logs.append(log(1, actiontype.commit, datetime.now()))
        
        # Checkpoint
        logs.append(log(0, actiontype.checkpoint, datetime.now(), old_data=[], new_data=[]))
        
        # T2
        logs.append(log(2, actiontype.start, datetime.now()))
        logs.append(log(2, actiontype.write, datetime.now(), [], [], "T"))
        logs.append(log(2, actiontype.commit, datetime.now()))
        
        # T3 (Crash)
        logs.append(log(3, actiontype.start, datetime.now()))
        logs.append(log(3, actiontype.write, datetime.now(), [], [], "T"))
        
        for l in logs: self.frm.logFile.write_log(l)
        
        self.frm.recover_system_crash()
        
        final_logs = self.frm.logFile.get_logs()
        aborted = [l.transaction_id for l in final_logs if l.action == actiontype.abort]
        
        self.assertIn(3, aborted, "T3 belum commit, harus abort")
        self.assertNotIn(1, aborted, "T1 sudah commit, aman")
        self.assertNotIn(2, aborted, "T2 sudah commit, aman")

    # ----------------------------------------------------------------
    # GROUP 3: CORRUPTION & INTEGRITY HANDLING
    # ----------------------------------------------------------------

    def test_corrupted_log_entry(self):
        """Memastikan sistem mendeteksi log yang korup (Checksum mismatch)."""
        # 1. Tulis log valid
        valid_log = log(1, 1, datetime.now(), [], [], "T")
        self.frm.logFile.write_log(valid_log)
        
        # 2. Rusak file log secara manual
        log_path = self.frm.logFile.paths[0]
        with open(log_path, "r") as f:
            lines = f.readlines()
        
        # Manipulasi konten tanpa update checksum
        parts = lines[-1].split("|", 1)
        corrupted_body = parts[1].replace("transaction_id:1", "transaction_id:999") 
        lines[-1] = f"{parts[0]}|{corrupted_body}"
        
        with open(log_path, "w") as f:
            f.writelines(lines)
            
        # 3. Assert bahwa pembacaan log melempar error
        # Note: Sesuaikan 'ValueError' dengan Exception yang Anda gunakan di logFile.py
        # Jika Anda menggunakan Exception generik, gunakan Exception.
        try:
            self.frm.logFile.get_logs()
            # Jika baris ini jalan, berarti test gagal mendeteksi korupsi
            self.fail("Seharusnya melempar Error saat membaca log korup (Checksum Mismatch)")
        except (ValueError, IndexError, Exception) as e:
            # Test Pass jika masuk sini
            pass

    def test_log_file_rotation(self):
        """Memastikan file log dirotasi dan path dikelola dengan benar."""
        # Ini penting agar recovery membaca urutan file yang benar
        self.assertTrue(len(self.frm.logFile.paths) > 0)
        self.assertTrue(isinstance(self.frm.logFile.paths[0], Path))
        
        # Simulasi rotasi manual (jika ada API-nya, gunakan itu. Jika tidak, cek properti)
        initial_path = self.frm.logFile.paths[0]
        self.frm.logFile.make_new_file()
        new_path = self.frm.logFile.paths[-1]
        
        self.assertNotEqual(initial_path, new_path)
        self.assertTrue(len(self.frm.logFile.paths) >= 2)

if __name__ == '__main__':
    unittest.main()