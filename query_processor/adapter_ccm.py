import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from concurrency_control_manager.src import *

class AdapterCCM:
    
    def __init__(self, algorithm: AlgorithmType):
        """
        Inisialisasi adapter dan instance CCManager internal
        berdasarkan algoritma yang dipilih.
        
        Args:
            algorithm (AlgorithmType): Algoritma yang akan digunakan (misal: LockBased).
        """
        self.ccm = CCManager(algorithm=algorithm)

    def begin_transaction(self) -> int:
        """
        Memulai transaksi baru via CCM dan mengembalikan ID transaksi.
        
        Returns:
            int: ID transaksi yang baru dibuat.
        """
        return self.ccm.begin_transaction()

    def commit_transaction(self, transaction_id: int):
        """
        Menjalankan proses commit untuk transaksi via CCM.
        Memanggil 'end_transaction' pada CCManager.
        
        Args:
            transaction_id (int): ID transaksi yang akan di-commit.
        """
        self.ccm.end_transaction(transaction_id)

    def abort_transaction(self, transaction_id: int):
        """
        Menjalankan proses abort untuk transaksi via CCM.
        Memanggil 'abort_transaction' pada CCManager.
        
        Args:
            transaction_id (int): ID transaksi yang akan di-abort.
        """
        self.ccm.abort_transaction(transaction_id)

    def validate_action(self, transaction_id: int, table_name: str, action_type: str) -> Response:
        """
        Memvalidasi apakah suatu aksi (read/write) diizinkan pada sebuah objek (tabel).
        
        Args:
            transaction_id (int): ID transaksi yang melakukan aksi.
            table_name (str): Nama tabel (objek) yang divalidasi.
            action_type (str): Tipe aksi ('read' atau 'write').
            
        Returns:
            Response: Objek Response dari CCM (berisi 'allowed' dan 'transaction_id').
        """
        if action_type == 'read':
            cc_action = ActionType.READ
        elif action_type == 'write':
            cc_action = ActionType.WRITE
        else:
            raise ValueError("Tipe aksi tidak valid untuk CCM, harus 'read' atau 'write'")

        cc_row = Row(object_id=table_name)
        
        return self.ccm.validate_object(cc_row, transaction_id, cc_action)

    def log_object(self, transaction_id: int, table_name: str):
        """
        Mencatat (log) objek/tabel yang diakses oleh transaksi ke CCM.
        
        Args:
            transaction_id (int): ID transaksi yang mengakses.
            table_name (str): Nama tabel (objek) yang di-log.
        """
        cc_row = Row(object_id=table_name)
        
        self.ccm.log_object(cc_row, transaction_id)



if __name__ == "__main__":
    
    # Buat tes doang
    test_adapter = AdapterCCM(algorithm=AlgorithmType.LockBased)
    
    # Skenario 1: Transaksi Berhasil (Commit)
    print("\n[SKENARIO 1: COMMIT]")
    try:
        tx1_id = test_adapter.begin_transaction()
        print(f"  [TEST] Transaksi {tx1_id} dimulai.")
        
        print(f"  [TEST] Validasi READ 'users' untuk {tx1_id}...")
        resp1 = test_adapter.validate_action(tx1_id, "users", "read")
        print(f"  [TEST] Hasil: {'Diizinkan' if resp1.allowed else 'Ditolak'}")
        
        if resp1.allowed:
            test_adapter.log_object(tx1_id, "users")
            print(f"  [TEST] Log READ 'users' untuk {tx1_id} berhasil.")
        
        test_adapter.commit_transaction(tx1_id)
        print(f"  [TEST] Transaksi {tx1_id} di-commit.")
        
    except Exception as e:
        print(f"  [TEST] Error Skenario 1: {e}")

    # Skenario 2: Transaksi Gagal (Abort)
    print("\n[SKENARIO 2: ABORT]")
    try:
        tx2_id = test_adapter.begin_transaction()
        print(f"  [TEST] Transaksi {tx2_id} dimulai.")

        print(f"  [TEST] Validasi WRITE 'orders' untuk {tx2_id}...")
        resp3 = test_adapter.validate_action(tx2_id, "orders", "write")
        print(f"  [TEST] Hasil: {'Diizinkan' if resp3.allowed else 'Ditolak'}")

        print(f"  [TEST] Meminta ABORT untuk transaksi {tx2_id}...")
        test_adapter.abort_transaction(tx2_id)
        print(f"  [TEST] Transaksi {tx2_id} di-abort.")

    except Exception as e:
        print(f"  [TEST] Error Skenario 2: {e}")