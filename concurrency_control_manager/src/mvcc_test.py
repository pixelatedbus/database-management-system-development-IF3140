"""
MVCC Algorithm Test Cases
Tests untuk MVTO, MV2PL, dan Snapshot Isolation dengan transaksi konkuren
"""

import sys
from typing import Dict, List, Any
from datetime import datetime

# Assuming these imports work in your project structure
# Adjust paths as needed based on your project structure
from .algorithms.mvcc import MVCCAlgorithm, MVCCVariant, IsolationPolicy
from .transaction import Transaction
from .row import Row
from .enums import ActionType


class MVCCTester:
    """Test runner untuk algoritma MVCC"""
    
    def __init__(self, variant: str, isolation_policy: str = "FIRST_COMMITTER_WIN"):
        self.mvcc = MVCCAlgorithm(variant=variant, isolation_policy=isolation_policy)
        self.variant = variant
        self.transactions: Dict[int, Transaction] = {}
        self.results = []
        self.execution_trace = [] # Menyimpan data untuk tabel simulasi
        self.step_counter = 0 # Tambahkan penghitung langkah unik per operasi
        
    def create_transaction(self, tid: int, trans_type: str = "UPDATE"):
        """Create a new transaction"""
        t = Transaction(transaction_id=tid)
        self.transactions[tid] = t
        self.mvcc.begin_transaction(t)
        if trans_type == "READ_ONLY":
            self.mvcc.set_transaction_type(tid, "READ_ONLY")
        return t
    
    def _record_trace(self, op_num: int, tid: int, op_type: str, object_id: str = None, value: Any = None, response: Any = None):
        """Mencatat langkah eksekusi dalam format yang bisa dicetak sebagai tabel.
           Setiap pemanggilan fungsi kini menghasilkan baris baru (unique op_num).
        """
        
        current_step = {'step': op_num, 'T': {}}

        # Memformat operasi dan pesan
        op_action = ""
        if op_type == 'READ':
            op_action = f"R({object_id})"
        elif op_type == 'WRITE':
            op_action = f"W({object_id})"
        elif op_type == 'COMMIT':
            op_action = "Commit"
        elif op_type == 'ABORT':
            op_action = "Abort"
        
        message = response.message if hasattr(response, 'message') else ""
        
        # Tambahkan operasi ke kolom transaksi yang bersangkutan
        current_step['T'][tid] = op_action
        
        # Tambahkan penjelasan (untuk dicetak di kolom terpisah)
        current_step['Action dan Penjelasan'] = []
            
        status = "✓" if response.allowed else "✗"
        if op_type == 'READ' and response.allowed and hasattr(response, 'value'):
             current_step['Action dan Penjelasan'].append(f"{status} T{tid}: {message} (Value: {response.value})")
        else:
             current_step['Action dan Penjelasan'].append(f"{status} T{tid}: {message}")
             
        self.execution_trace.append(current_step)

    def read(self, tid: int, object_id: str, expected_version: str = None):
        """Execute read operation"""
        t = self.transactions[tid]
        row = Row(object_id=object_id, table_name='test_table', data={'value': 0})
        # Note: self.mvcc.validate_read akan memicu error jika mvcc.py masih memanggil self.schedule
        response = self.mvcc.validate_read(t, row)
        
        self.step_counter += 1 # INCREMENT STEP COUNTER
        self._record_trace(self.step_counter, tid, 'READ', object_id=object_id, response=response)
        return response
    
    def write(self, tid: int, object_id: str, value: Any):
        """Execute write operation"""
        t = self.transactions[tid]
        row = Row(object_id=object_id, table_name='test_table', data={'value': value})
        # Note: self.mvcc.validate_write akan memicu error jika mvcc.py masih memanggil self.schedule
        response = self.mvcc.validate_write(t, row)
        
        self.step_counter += 1 # INCREMENT STEP COUNTER
        self._record_trace(self.step_counter, tid, 'WRITE', object_id=object_id, value=value, response=response)
        return response
    
    def commit(self, tid: int):
        """Commit transaction"""
        t = self.transactions[tid]
        response = self.mvcc.commit_transaction(t)
        
        self.step_counter += 1 # INCREMENT STEP COUNTER
        self._record_trace(self.step_counter, tid, 'COMMIT', response=response)
        return response
    
    def abort(self, tid: int):
        """Abort transaction"""
        t = self.transactions[tid]
        response = self.mvcc.abort_transaction(t)
        
        self.step_counter += 1 # INCREMENT STEP COUNTER
        self._record_trace(self.step_counter, tid, 'ABORT', response=response)
        return response
    
    def print_execution_table(self):
        """Mencetak trace eksekusi dalam format tabel seperti gambar"""
        if not self.transactions:
            return

        # Ambil TIDs T1-T7 untuk kolom
        tx_ids = sorted(self.transactions.keys())
        tx_headers = [f"T{i}" for i in tx_ids]
        
        # Header Tabel
        # Lebar kolom T_i adalah 10 karakter
        header = f"{'R/W':<5} " + "".join(f"{h:<10}" for h in tx_headers) + " | Action dan Penjelasan"
        
        # Tentukan panjang total header untuk garis pemisah
        total_header_length = len(header)
        
        print("\n" + "=" * total_header_length)
        print(header)
        print("-" * total_header_length)

        # Baris Data
        # Hitung padding untuk penjelasan agar sejajar
        padding_width = len("".join(f"{h:<10}" for h in tx_headers)) + 3 # 3 untuk spasi R/W
        
        for row_data in self.execution_trace:
            step = row_data['step']
            row_output = f"{step:<5} "
            
            for tid in tx_ids:
                op = row_data['T'].get(tid, "")
                row_output += f"{op:<10}"
            
            # Kolom Penjelasan
            explanation_str = " | ".join(row_data['Action dan Penjelasan'])
            
            # Cetak baris pertama dari penjelasan di baris yang sama dengan operasi
            print(row_output + " | " + explanation_str)

        print("=" * total_header_length)

    def print_data_versions(self):
        """Print final data versions"""
        print("\n" + "="*80)
        print("FINAL DATA VERSIONS")
        print("="*80)
        for object_id, versions in self.mvcc.data_versions.items():
            print(f"\n{object_id}:")
            for v in versions:
                print(f"  {v}")
    
    def print_header(self, title: str):
        """Print section header"""
        print("\n" + "="*80)
        print(title)
        print("="*80 + "\n")


def test_mvto_case1():
    """
    MVTO Test Case 1: Cascading Rollback Scenario
    """
    print("\n" + "#"*80)
    print("# MVTO TEST CASE 1: Cascading Rollback")
    print("#"*80)
    
    tester = MVCCTester("MVTO")
    tester.print_header("Initializing Transactions T1-T7 (TS: 1-7)")
    
    for i in range(1, 8):
        tester.create_transaction(i)
    
    tester.print_header("Phase 1: Eksekusi Operasi")
    
    tester.read(7, 'A')
    tester.read(3, 'B')
    tester.write(5, 'C', 50)
    tester.read(1, 'D')
    tester.write(6, 'A', 100)
    
    tester.read(2, 'A') 
    tester.write(4, 'B', 200)
    tester.read(5, 'B') 
    tester.read(7, 'C') 
    tester.write(3, 'D', 300)
    
    tester.print_header("Phase 2: Triggering Rollback (T4)")
    
    tester.read(6, 'E')
    tester.write(4, 'E', 400) # Rollback T4, Cascading T5
    
    tester.print_header("Phase 3: Re-execution after Rollback")
    
    tester.create_transaction(4) # Re-begin T4
    tester.write(4, 'B', 200)
    tester.write(4, 'E', 400)
    
    tester.create_transaction(5) # Re-begin T5
    tester.write(5, 'C', 50) 
    tester.read(5, 'B')
    
    tester.print_header("Phase 4: Final Operations and Commits")
    
    tester.write(1, 'A', 10) 
    tester.read(2, 'C') 
    tester.write(7, 'D', 700)
    tester.read(3, 'E') 
    tester.write(2, 'E', 20) 
    tester.read(6, 'B') 
    
    for i in [1, 2, 3, 4, 5, 6, 7]:
        tester.commit(i)
    
    tester.print_execution_table()
    tester.print_data_versions()


def test_mvto_case2():
    """
    MVTO Test Case 2: Multiple Rollbacks
    """
    print("\n" + "#"*80)
    print("# MVTO TEST CASE 2: Multiple Rollbacks")
    print("#"*80)
    
    tester = MVCCTester("MVTO")
    for i in range(1, 8):
        tester.create_transaction(i)
    
    tester.print_header("Phase 1: Setup for First Rollback")
    
    tester.read(5, 'P') 
    tester.read(7, 'Q') 
    tester.write(3, 'R', 30)
    tester.read(6, 'S') 
    tester.write(4, 'T', 40)
    
    tester.print_header("Phase 2: First Rollback (T2)")
    
    tester.write(2, 'P', 20) # ROLLBACK T2
    tester.create_transaction(2)
    tester.write(2, 'P', 20)
    
    tester.print_header("Phase 3: Setup for Second Rollback")
    
    tester.read(1, 'Q') 
    tester.write(5, 'Q', 50)
    tester.read(7, 'R') 
    tester.read(4, 'P')
    
    tester.print_header("Phase 4: Second Rollback (T3)")
    
    tester.write(3, 'S', 30) # ROLLBACK T3, Cascading T7
    
    tester.create_transaction(3) # Re-begin T3
    tester.write(3, 'R', 30)
    tester.write(3, 'S', 30)
    
    tester.create_transaction(7) # Re-begin T7
    tester.read(7, 'Q')
    tester.read(7, 'R')
    
    tester.print_header("Phase 5: Final Operations and Commits")
    
    tester.write(6, 'T', 60)
    tester.read(1, 'T')
    tester.write(4, 'Q', 40)
    tester.read(5, 'S')
    tester.write(1, 'R', 10)
    
    for i in range(1, 8):
        tester.commit(i)
    
    tester.print_execution_table()
    tester.print_data_versions()


def test_mv2pl_case1():
    """
    MV2PL Test Case 1: Lock Conflicts and Queue
    """
    print("\n" + "#"*80)
    print("# MV2PL TEST CASE 1: Lock Conflicts")
    print("#"*80)
    
    tester = MVCCTester("MV2PL")
    tester.create_transaction(1, "UPDATE")
    tester.create_transaction(2, "READ_ONLY")
    tester.create_transaction(3, "UPDATE")
    tester.create_transaction(4, "READ_ONLY")
    tester.create_transaction(5, "UPDATE")
    tester.create_transaction(6, "UPDATE")
    tester.create_transaction(7, "READ_ONLY")
    
    tester.print_header("Phase 1: Initial Operations")
    
    tester.read(1, 'A')
    tester.read(2, 'B')
    tester.write(3, 'C', 30)
    tester.read(4, 'A')
    tester.read(5, 'D')
    
    tester.print_header("Phase 2: Lock Conflicts")
    
    tester.write(1, 'A', 10)
    tester.read(6, 'A')   # BLOCKED
    tester.write(5, 'D', 50)
    tester.read(3, 'E')
    tester.write(6, 'B', 60)
    
    tester.print_header("Phase 3: T1 Commits (Releases Locks)")
    
    tester.commit(1)
    
    tester.print_header("Phase 4: T6 Continues")
    
    tester.read(6, 'A')
    
    tester.print_header("Phase 5: More Operations")
    
    tester.write(3, 'B', 30) # BLOCKED
    tester.read(7, 'C')
    tester.read(2, 'D')
    tester.write(5, 'E', 50) # BLOCKED
    
    tester.print_header("Phase 6: T3 Commits")
    
    tester.commit(3)
    
    tester.print_header("Phase 7: T6 Commits (Releases B)")
    
    tester.commit(6)
    
    tester.print_header("Phase 8: Final Operations and Commits")
    
    tester.read(5, 'A')
    tester.write(5, 'C', 55)
    
    tester.commit(2)
    tester.commit(4)
    tester.commit(5)
    tester.commit(7)
    
    tester.print_execution_table()
    tester.print_data_versions()


def test_mv2pl_case2():
    """
    MV2PL Test Case 2: Read-Only and Update Mix
    """
    print("\n" + "#"*80)
    print("# MV2PL TEST CASE 2: Read-Only and Update Mix")
    print("#"*80)
    
    tester = MVCCTester("MV2PL")
    tester.create_transaction(1, "UPDATE")
    tester.create_transaction(2, "UPDATE")
    tester.create_transaction(3, "READ_ONLY")
    tester.create_transaction(4, "UPDATE")
    tester.create_transaction(5, "READ_ONLY")
    tester.create_transaction(6, "UPDATE")
    tester.create_transaction(7, "READ_ONLY")
    
    tester.print_header("Phase 1: Concurrent Reads and Writes")
    
    tester.write(1, 'P', 10)
    tester.read(3, 'P')
    tester.write(2, 'Q', 20)
    tester.read(5, 'Q')
    tester.write(4, 'R', 40)
    
    tester.print_header("Phase 2: Lock Conflicts")
    
    tester.read(6, 'P')   # BLOCKED
    tester.read(7, 'R')
    tester.write(2, 'S', 25)
    tester.read(1, 'Q')   # BLOCKED
    tester.write(6, 'T', 60)
    
    tester.print_header("Phase 3: T1 Commits")
    
    tester.commit(1)
    
    tester.print_header("Phase 4: T6 Can Read P Now")
    
    tester.read(6, 'P')
    
    tester.print_header("Phase 5: T2 Commits")
    
    tester.commit(2)
    
    tester.print_header("Phase 6: More Operations")
    
    tester.read(4, 'S')
    tester.write(6, 'Q', 65)
    tester.read(3, 'T')
    tester.write(4, 'P', 45) # BLOCKED
    
    tester.print_header("Phase 7: T6 Commits")
    
    tester.commit(6)
    
    tester.print_header("Phase 8: T4 Continues")
    
    tester.write(4, 'P', 45)
    
    tester.print_header("Phase 9: Final Commits")
    
    tester.commit(3)
    tester.commit(4)
    tester.commit(5)
    tester.commit(7)
    
    tester.print_execution_table()
    tester.print_data_versions()


def test_snapshot_fcw_case1():
    """
    Snapshot Isolation (First-Committer-Win) Test Case 1
    """
    print("\n" + "#"*80)
    print("# SNAPSHOT ISOLATION (FCW) TEST CASE 1: Write-Write Conflicts")
    print("#"*80)
    
    tester = MVCCTester("SNAPSHOT_ISOLATION", "FIRST_COMMITTER_WIN")
    for i in range(1, 8):
        tester.create_transaction(i)
    
    tester.print_header("Phase 1: Reads and Writes")
    
    tester.read(1, 'A')
    tester.read(2, 'A')
    tester.read(3, 'B')
    tester.read(4, 'C')
    tester.read(5, 'D')
    
    tester.write(1, 'A', 10) 
    tester.write(2, 'A', 20) 
    tester.write(3, 'B', 30) 
    tester.write(4, 'C', 40) 
    tester.write(5, 'D', 50) 
    
    tester.print_header("Phase 2: Conflicts and Commits")
    
    tester.read(6, 'A') 
    tester.read(7, 'B') 
    tester.read(1, 'E') 
    tester.read(2, 'E') 
    
    tester.write(6, 'B', 60) 
    tester.write(7, 'C', 70) 
    tester.write(1, 'E', 15) 
    tester.write(3, 'D', 35) 
    
    tester.commit(1)     # T1 COMMITS
    tester.commit(2)     # T2 ABORTS
    tester.commit(3)     # T3 COMMITS
    tester.commit(4)     # T4 COMMITS
    tester.commit(5)     # T5 ABORTS
    tester.commit(6)     # T6 ABORTS
    tester.commit(7)     # T7 ABORTS
    
    tester.print_header("Phase 3: Re-execution of Aborted Transactions")
    
    tester.create_transaction(2) 
    tester.read(2, 'A')
    tester.write(2, 'A', 20)
    tester.read(2, 'E')
    tester.commit(2)
    
    tester.create_transaction(5)
    tester.read(5, 'D')
    tester.write(5, 'D', 50)
    tester.commit(5)
    
    tester.create_transaction(6)
    tester.read(6, 'A')
    tester.read(6, 'B')
    tester.write(6, 'B', 60)
    tester.commit(6)
    
    tester.create_transaction(7)
    tester.read(7, 'B')
    tester.read(7, 'C')
    tester.write(7, 'C', 70)
    tester.commit(7)
    
    tester.print_execution_table()
    tester.print_data_versions()


def test_snapshot_fcw_case2():
    """
    Snapshot Isolation (First-Committer-Win) Test Case 2
    """
    print("\n" + "#"*80)
    print("# SNAPSHOT ISOLATION (FCW) TEST CASE 2: Complex Conflicts")
    print("#"*80)
    
    tester = MVCCTester("SNAPSHOT_ISOLATION", "FIRST_COMMITTER_WIN")
    for i in range(1, 8):
        tester.create_transaction(i)
    
    tester.print_header("Phase 1: Initial Operations")
    
    tester.read(1, 'P') 
    tester.read(2, 'Q') 
    tester.write(1, 'P', 10) 
    tester.write(2, 'Q', 20) 
    tester.read(3, 'R') 
    
    tester.print_header("Phase 2: Overlapping Writes")
    
    tester.write(3, 'P', 30) 
    tester.write(3, 'R', 35) 
    tester.read(4, 'Q') 
    tester.write(4, 'Q', 40) 
    tester.write(4, 'S', 45) 
    
    tester.print_header("Phase 3: More Complex Patterns")
    
    tester.read(5, 'P') 
    tester.read(5, 'R') 
    tester.write(5, 'T', 50) 
    tester.read(6, 'S') 
    tester.write(6, 'S', 60) 
    tester.read(7, 'T') 
    tester.write(7, 'T', 70) 
    tester.write(7, 'R', 75) 
    
    tester.print_header("Phase 4: Commits and Aborts")
    
    tester.commit(1) 
    tester.commit(2) 
    tester.commit(3) # ABORTS
    tester.commit(5) 
    tester.commit(4) # ABORTS
    tester.commit(6)
    tester.commit(7) # ABORTS
    
    tester.print_header("Phase 5: Re-execute Aborted Transactions")
    
    tester.create_transaction(3)
    tester.read(3, 'P') 
    tester.read(3, 'R') 
    tester.write(3, 'P', 30) 
    tester.write(3, 'R', 35) 
    tester.commit(3) 
    
    tester.create_transaction(4)
    tester.read(4, 'Q') 
    tester.read(4, 'S') 
    tester.write(4, 'Q', 40) 
    tester.write(4, 'S', 45) 
    tester.commit(4) 
    
    tester.create_transaction(7)
    tester.read(7, 'T') 
    tester.read(7, 'R') 
    tester.write(7, 'T', 70) 
    tester.write(7, 'R', 75) 
    tester.commit(7) 
    
    tester.print_execution_table()
    tester.print_data_versions()


def test_snapshot_fuw_case1():
    """
    Snapshot Isolation (First-Updater-Win) Test Case 1
    """
    print("\n" + "#"*80)
    print("# SNAPSHOT ISOLATION (FUW) TEST CASE 1: Exclusive Locks")
    print("#"*80)
    
    tester = MVCCTester("SNAPSHOT_ISOLATION", "FIRST_UPDATER_WIN")
    for i in range(1, 8):
        tester.create_transaction(i)
    
    tester.print_header("Phase 1: Initial Operations")
    
    tester.read(1, 'A')
    tester.read(2, 'B')
    tester.read(3, 'C')
    tester.read(4, 'D')
    tester.read(5, 'E')
    
    tester.write(1, 'A', 10) 
    tester.write(2, 'B', 20) 
    tester.write(3, 'C', 30) 
    
    tester.print_header("Phase 2: Conflicting Writes (Abort)")
    
    tester.write(4, 'D', 40) # ABORTS
    tester.write(5, 'E', 50) # ABORTS
    
    tester.print_header("Phase 3: More Aborts")
    
    tester.read(6, 'A') 
    tester.write(6, 'A', 60) # ABORTS
    tester.read(7, 'B') 
    tester.write(7, 'C', 70) # ABORTS
    
    tester.print_header("Phase 4: Commits and Re-execution")
    
    tester.commit(1) 
    tester.commit(2) 
    tester.commit(3) 
    
    tester.create_transaction(4)
    tester.read(4, 'D')
    tester.write(4, 'D', 40)
    tester.commit(4)
    
    tester.create_transaction(5)
    tester.read(5, 'E')
    tester.write(5, 'E', 50)
    tester.commit(5)
    
    tester.create_transaction(6)
    tester.read(6, 'A')
    tester.write(6, 'A', 60)
    tester.commit(6)
    
    tester.create_transaction(7)
    tester.read(7, 'B')
    tester.read(7, 'C')
    tester.write(7, 'C', 70)
    tester.commit(7)
    
    tester.print_execution_table()
    tester.print_data_versions()


def test_snapshot_fuw_case2():
    """
    Snapshot Isolation (First-Updater-Win) Test Case 2
    """
    print("\n" + "#"*80)
    print("# SNAPSHOT ISOLATION (FUW) TEST CASE 2: Complex Lock Patterns")
    print("#"*80)
    
    tester = MVCCTester("SNAPSHOT_ISOLATION", "FIRST_UPDATER_WIN")
    for i in range(1, 8):
        tester.create_transaction(i)
    
    tester.print_header("Phase 1: Reads and First Write")
    
    tester.read(1, 'P') 
    tester.read(2, 'Q') 
    tester.read(3, 'R') 
    tester.write(1, 'P', 10) 
    
    tester.print_header("Phase 2: Cascade of Aborts")
    
    tester.write(2, 'Q', 20) # ABORTS
    tester.write(3, 'R', 30) # ABORTS
    tester.read(4, 'S') 
    tester.write(4, 'S', 40) # ABORTS
    tester.read(5, 'T') 
    tester.write(5, 'T', 50) # ABORTS
    
    tester.print_header("Phase 3: Read-Only Operations")
    
    tester.read(6, 'P') 
    tester.read(6, 'Q')
    tester.read(7, 'R')
    tester.read(7, 'S')
    
    tester.print_header("Phase 4: More Failed Writes")
    
    tester.write(6, 'Q', 60) # ABORTS
    tester.write(7, 'S', 70) # ABORTS
    
    tester.print_header("Phase 5: T1 Commits and Re-execution")
    
    tester.commit(1) 
    
    tester.create_transaction(2)
    tester.read(2, 'Q')
    tester.write(2, 'Q', 20)
    tester.commit(2)
    
    tester.create_transaction(3)
    tester.read(3, 'R')
    tester.write(3, 'R', 30)
    tester.commit(3)
    
    tester.create_transaction(4)
    tester.read(4, 'S')
    tester.write(4, 'S', 40)
    tester.commit(4)
    
    tester.create_transaction(5)
    tester.read(5, 'T')
    tester.write(5, 'T', 50)
    tester.commit(5)
    
    tester.create_transaction(6)
    tester.read(6, 'P')
    tester.read(6, 'Q')
    tester.write(6, 'Q', 60)
    tester.commit(6)
    
    tester.create_transaction(7)
    tester.read(7, 'R')
    tester.read(7, 'S')
    tester.write(7, 'S', 70)
    tester.commit(7)
    
    tester.print_execution_table()
    tester.print_data_versions()


def print_summary():
    """Print test summary"""
    print("\n" + "="*80)
    print("TEST SUMMARY")
    print("="*80)
    print("""
This test suite covers:

1. MVTO (Multiversion Timestamp Ordering):
    - Case 1: Cascading rollback scenario
    - Case 2: Multiple independent rollbacks
    - Key features tested:
      * Read timestamp updates
      * Write timestamp validation
      * Rollback and timestamp reassignment
      * Cascading rollback propagation

2. MV2PL (Multiversion Two-Phase Locking):
    - Case 1: Lock conflicts and queue management
    - Case 2: Mix of read-only and update transactions
    - Key features tested:
      * Shared and exclusive locks
      * Lock queue processing
      * Read-only transactions (no locks)
      * Timestamp assignment at commit

3. Snapshot Isolation - First-Committer-Win:
    - Case 1: Write-write conflict detection
    - Case 2: Complex overlapping write patterns
    - Key features tested:
      * Snapshot reads
      * Buffered writes
      * Commit-time conflict detection
      * Transaction abort on conflicts

4. Snapshot Isolation - First-Updater-Win:
    - Case 1: Exclusive lock behavior
    - Case 2: Complex lock patterns
    - Key features tested:
      * Exclusive lock on first write
      * Blocking all other writes
      * Read-only operations allowed
      * Sequential re-execution

All test cases involve:
- 7 concurrent transactions
- 5 data items
- Non-serializable schedules
- Proper handling of aborts and re-executions
    """)


def main():
    """Run all test cases"""
    print("="*80)
    print("MVCC ALGORITHM COMPREHENSIVE TEST SUITE")
    print("="*80)
    
    try:
        # MVTO Tests
        test_mvto_case1()
        test_mvto_case2()
        
        # MV2PL Tests
        test_mv2pl_case1()
        test_mv2pl_case2()
        
        # Snapshot Isolation Tests
        test_snapshot_fcw_case1()
        test_snapshot_fcw_case2()
        test_snapshot_fuw_case1()
        test_snapshot_fuw_case2()
        
        # Summary
        print_summary()
        
        print("\n" + "="*80)
        print("ALL TESTS COMPLETED")
        print("="*80)
        
    except Exception as e:
        print(f"\n\nERROR: Test failed with exception:")
        print(f"{type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())