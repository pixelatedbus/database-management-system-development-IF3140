"""
MVCCTester: Class utility bersama untuk testing algoritma MVCC

Menyediakan:
- Transaction management: Membuat dan mengelola transaksi untuk testing
- Execution tracking: Merekam setiap operasi untuk visualisasi
- Result visualization: Menampilkan hasil eksekusi dalam bentuk tabel dan data versions

Digunakan oleh test_mvto.py, test_mv2pl.py, test_snapshot_fcw.py, dan test_snapshot_fuw.py
"""

import sys
import io
from typing import Dict, Any

# Set stdout untuk menggunakan UTF-8 encoding agar mendukung ANSI color codes
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from ..algorithms.mvcc import MVCCAlgorithm
from ..transaction import Transaction
from ..row import Row


class MVCCTester:
    """Helper class untuk testing algoritma MVCC dengan output terformat"""
    
    # ANSI color codes untuk output berwarna
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    RESET = '\033[0m'
    BOLD = '\033[1m'
    
    def __init__(self, algorithm_name, isolation_policy="FIRST_COMMITTER_WIN"):
        """
        Initialize tester with specified algorithm
        Args:
            algorithm_name: "MVTO", "MV2PL", "Snapshot_FCW", or "Snapshot_FUW"
            isolation_policy: For Snapshot variants - "FIRST_COMMITTER_WIN" or "FIRST_UPDATER_WIN"
        """
        # Map test names to MVCC variant names
        variant_map = {
            "MVTO": "MVTO",
            "MV2PL": "MV2PL",
            "Snapshot_FCW": "SNAPSHOT_ISOLATION",
            "Snapshot_FUW": "SNAPSHOT_ISOLATION"
        }
        
        policy_map = {
            "Snapshot_FCW": "FIRST_COMMITTER_WIN",
            "Snapshot_FUW": "FIRST_UPDATER_WIN"
        }
        
        variant = variant_map.get(algorithm_name, algorithm_name)
        if algorithm_name in policy_map:
            isolation_policy = policy_map[algorithm_name]
            
        self.mvcc = MVCCAlgorithm(variant=variant, isolation_policy=isolation_policy)
        self.algorithm_name = algorithm_name
        self.variant = variant  # Store variant for conditional logic
        self.transactions: Dict[int, Transaction] = {}
        self.execution_trace = []
        self.step_counter = 0
        self.transaction_creation_count: Dict[int, int] = {}  # Track transaction re-creations
        
    def create_transaction(self, tid):
        """Create a new transaction"""
        # Track if this is a re-creation (rollback scenario)
        if tid not in self.transaction_creation_count:
            self.transaction_creation_count[tid] = 0
        self.transaction_creation_count[tid] += 1
        
        t = Transaction(transaction_id=tid)
        self.transactions[tid] = t
        self.mvcc.begin_transaction(t)
        print(f"{self.CYAN}Creating Transaction T{tid}{self.RESET}")
        return t
    
    def _record_trace(self, op_num: int, tid: int, op_type: str, object_id: str = None, value: Any = None, response: Any = None):
        """Record execution step in table format"""
        
        current_step = {'step': op_num, 'T': {}, 'is_rollback': {}, 'is_abort': {}}

        # Format operation
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
        
        # Check if this transaction has been rolled back
        is_after_rollback = False
        if tid in self.transactions:
            t = self.transactions[tid]
            if hasattr(self.mvcc, 'transaction_info') and t.transaction_id in self.mvcc.transaction_info:
                trans_info = self.mvcc.transaction_info[t.transaction_id]
                if hasattr(trans_info, 'rollback_count'):
                    is_after_rollback = trans_info.rollback_count > 0
        
        # Check if this is a re-execution after abort
        is_reexecution = self.transaction_creation_count.get(tid, 1) > 1
        
        # Check if this operation triggered an internal rollback (MVTO auto-rollback)
        is_internal_rollback = "after rollback" in message and response.allowed
        
        # Check if this operation came from queue (MV2PL)
        is_from_queue = ("from queue" in message or "acquired from queue" in message) and response.allowed
        
        # Check if this is an abort operation
        is_abort = (not response.allowed and op_type in ['WRITE', 'COMMIT']) or op_type == 'ABORT'
        
        # Check if this is a blocked operation (MV2PL)
        is_blocked = not response.allowed and "blocked waiting" in message
        
        # Add operation to transaction column
        current_step['T'][tid] = op_action
        current_step['is_rollback'][tid] = is_after_rollback or is_internal_rollback or is_from_queue or is_reexecution
        current_step['is_abort'][tid] = is_abort or is_blocked
        
        # Tambahkan explanation
        current_step['Action dan Penjelasan'] = []
            
        # Status marker: [OK] atau [FAIL]
        status = "[OK]" if response.allowed else "[FAIL]"
        explanation = ""
        if op_type == 'READ' and response.allowed and hasattr(response, 'value'):
             explanation = f"{status} T{tid}: {message} (Value: {response.value})"
        else:
             explanation = f"{status} T{tid}: {message}"
        
        # Apply color coding
        if is_abort or is_blocked:
            explanation = f"{self.RED}{explanation}{self.RESET}"
        elif is_after_rollback or is_internal_rollback or is_from_queue or is_reexecution:
            explanation = f"{self.YELLOW}{explanation}{self.RESET}"
        
        current_step['Action dan Penjelasan'].append(explanation)
             
        self.execution_trace.append(current_step)
        
    def read(self, tid, item):
        """Execute read operation"""
        t = self.transactions[tid]
        row = Row(object_id=item, table_name='test_table', data={'value': 0})
        result = self.mvcc.validate_read(t, row)
        
        self.step_counter += 1
        self._record_trace(self.step_counter, tid, 'READ', object_id=item, response=result)
        
        # Record operation for simple output
        operation = f"R{tid}({item})"
        
        # Check if this is MV2PL wait/block scenario
        if not result.allowed and "blocked waiting" in (result.message or ""):
            status = "WAIT"
            color = self.YELLOW
        else:
            status = "OK" if result.allowed else "ABORT"
            color = self.GREEN if result.allowed else self.RED
        
        message = result.message or ""
        
        print(f"{color}{operation}: {status} - {message}{self.RESET}")
        
        return result
        
    def write(self, tid, item, value):
        """Execute write operation"""
        t = self.transactions[tid]
        row = Row(object_id=item, table_name='test_table', data={'value': value})
        result = self.mvcc.validate_write(t, row, auto_reexecute=True)
        
        # Handle cascading rollback recording for MVTO
        if self.variant == 'MVTO' and hasattr(result, 'cascaded_tids') and result.cascaded_tids is not None:
            # Rollback occurred
            if not result.allowed:
                # Not first operation - only record ABORT
                self.step_counter += 1
                abort_response = type('Response', (), {'allowed': False, 'message': result.message})()
                self._record_trace(self.step_counter, tid, 'WRITE', object_id=item, value=value, response=abort_response)
                color = self.RED
                status = "ABORT"
            else:
                # First operation - auto re-executed, record ABORT then success
                abort_msg = result.abort_message if hasattr(result, 'abort_message') else result.message
                
                self.step_counter += 1
                abort_response = type('Response', (), {'allowed': False, 'message': abort_msg})()
                self._record_trace(self.step_counter, tid, 'WRITE', object_id=item, value=value, response=abort_response)
                
                # Then record the successful re-execution
                self.step_counter += 1
                self._record_trace(self.step_counter, tid, 'WRITE', object_id=item, value=value, response=result)
                color = self.YELLOW
                status = "RE-EXEC"
            
            # Handle cascading aborts
            cascaded_tids = result.cascaded_tids if result.cascaded_tids else []
            
            # Record all cascading aborts
            for cascaded_tid in cascaded_tids:
                self.step_counter += 1
                abort_response = type('Response', (), {'allowed': False, 'message': f'T{cascaded_tid} ABORTED: cascading rollback from T{tid}'})()
                self._record_trace(self.step_counter, cascaded_tid, 'ABORT', response=abort_response)
        else:
            # No rollback, just record normally
            self.step_counter += 1
            self._record_trace(self.step_counter, tid, 'WRITE', object_id=item, value=value, response=result)
            
            # Check if this is MV2PL wait/block scenario
            if not result.allowed and "blocked waiting" in (result.message or ""):
                color = self.YELLOW
                status = "WAIT"
            elif result.allowed:
                color = self.GREEN
                status = "OK"
            else:
                color = self.RED
                status = "ABORT"
        
        operation = f"W{tid}({item})"
        message = result.message or ""
        
        print(f"{color}{operation}: {status} - {message}{self.RESET}")
        
        return result
        
    def commit(self, tid):
        """Commit transaction"""
        t = self.transactions[tid]
        result = self.mvcc.commit_transaction(t)
        
        self.step_counter += 1
        self._record_trace(self.step_counter, tid, 'COMMIT', response=result)
        
        # Handle executed_ops from MV2PL commit (operations from queue)
        if self.variant == 'MV2PL' and hasattr(result, 'executed_ops') and result.executed_ops:
            for op_type, op_tid, op_message, op_value in result.executed_ops:
                self.step_counter += 1
                
                # Create response object for queue operations
                queue_response = type('Response', (), {
                    'allowed': True,
                    'message': op_message,
                    'value': op_value
                })()
                
                if op_type == 'read':
                    self._record_trace(self.step_counter, op_tid, 'READ', object_id='queue_op', response=queue_response)
                elif op_type == 'write':
                    self._record_trace(self.step_counter, op_tid, 'WRITE', object_id='queue_op', response=queue_response)
        
        operation = f"Commit T{tid}"
        status = "OK" if result.allowed else "FAIL"
        color = self.GREEN if result.allowed else self.RED
        message = result.message or ""
        
        print(f"{color}{operation}: {status} - {message}{self.RESET}")
        
        return result
        
    def print_header(self, text):
        """Print formatted section header"""
        print(f"\n{self.BOLD}{self.BLUE}{'='*70}{self.RESET}")
        print(f"{self.BOLD}{self.BLUE}{text:^70}{self.RESET}")
        print(f"{self.BOLD}{self.BLUE}{'='*70}{self.RESET}\n")
        
    def print_execution_table(self):
        """Print execution trace in table format like test_mvcc.py"""
        if not self.transactions:
            return

        # Get TIDs for columns
        tx_ids = sorted(self.transactions.keys())
        tx_headers = [f"T{i}" for i in tx_ids]
        
        # Header
        header = f"{'R/W':<5} " + "".join(f"{h:<10}" for h in tx_headers) + " | Action dan Penjelasan"
        total_header_length = len(header)
        
        print("\n" + "=" * total_header_length)
        print(header)
        print("-" * total_header_length)

        # Data rows
        for row_data in self.execution_trace:
            step = row_data['step']
            row_output = f"{step:<5} "
            
            for tid in tx_ids:
                cell_value = row_data['T'].get(tid, '')
                is_rollback = row_data.get('is_rollback', {}).get(tid, False)
                is_abort = row_data.get('is_abort', {}).get(tid, False)
                
                # Add color to table cells
                if cell_value:
                    if is_abort:
                        colored = f"{self.RED}{cell_value}{self.RESET}"
                        padding = 10 - len(cell_value)
                        row_output += colored + " " * padding
                    elif is_rollback:
                        colored = f"{self.YELLOW}{cell_value}{self.RESET}"
                        padding = 10 - len(cell_value)
                        row_output += colored + " " * padding
                    else:
                        row_output += f"{cell_value:<10}"
                else:
                    row_output += f"{cell_value:<10}"
            
            # Explanation column
            explanation_str = " | ".join(row_data['Action dan Penjelasan'])
            print(row_output + " | " + explanation_str)

        print("=" * total_header_length)
            
    def print_data_versions(self):
        """Tampilkan semua versi data item jika tersedia"""
        if hasattr(self.mvcc, 'data_versions') and self.mvcc.data_versions:
            print(f"\n{self.BOLD}{'='*80}{self.RESET}")
            print(f"{self.BOLD}DATA ITEM VERSIONS (Final State):{self.RESET}")
            print(f"{self.BOLD}{'='*80}{self.RESET}")
            
            for object_id in sorted(self.mvcc.data_versions.keys()):
                versions_list = self.mvcc.data_versions[object_id]
                print(f"\n{self.CYAN}> Object [{object_id}]:{self.RESET}")
                
                if versions_list:
                    print(f"  Total versions: {len(versions_list)}")
                    for i, version in enumerate(sorted(versions_list, key=lambda v: v.write_ts)):
                        # Determine status
                        if version.commit_ts is not None:
                            status_marker = f"{self.GREEN}[COMMITTED at TS={version.commit_ts}]{self.RESET}"
                        elif version.creator_ts is not None:
                            status_marker = f"{self.YELLOW}[CREATED by TS={version.creator_ts}]{self.RESET}"
                        elif version.write_ts > 0:
                            status_marker = f"{self.GREEN}[COMMITTED]{self.RESET}"
                        else:
                            status_marker = "[INITIAL]"
                            
                        print(f"  {self.YELLOW}Version {i+1}:{self.RESET} "
                              f"Value={self.BOLD}{version.value}{self.RESET}, "
                              f"Write-TS={version.write_ts}, "
                              f"Read-TS={version.read_ts} "
                              f"{status_marker}")
                else:
                    print(f"  {self.YELLOW}No versions{self.RESET}")
                        
            print(f"\n{self.BOLD}{'='*80}{self.RESET}")
                        
    def print_transaction_summary(self):
        """Print summary of all transactions"""
        print(f"\n{self.BOLD}TRANSACTION SUMMARY:{self.RESET}")
        print(f"{'TID':<8} {'Status':<15}")
        print("-" * 40)
        
        for tid, trans in sorted(self.transactions.items()):
            status = trans.status
            color = self.GREEN if status == "Committed" else self.YELLOW
            
            print(f"{color}T{tid:<7} {status:<15}{self.RESET}")
