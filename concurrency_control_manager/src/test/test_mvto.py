"""
MVTO (Multi-Version Timestamp Ordering) Test Cases
5 comprehensive test cases for MVTO algorithm
"""

import sys
from .mvcc_tester import MVCCTester


def test_mvto_case1():
    """
    MVTO Test Case 1: Cascading Rollback with Auto Re-execution
    Tests: First operation auto re-execute, non-first operation manual restart
    """
    print("\n" + "#"*80)
    print("# MVTO TEST CASE 1: Cascading Rollback with Auto Re-execution")
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
    
    tester.print_header("Phase 2: Triggering ABORT + Immediate Rollback & Re-execution")
    
    # T6 writes to A again - this is an intentional overwrite/update
    # (Previous W(A) at Phase 1 aborted and auto re-executed successfully)
    tester.write(6, 'A', 100)  # Overwrite the same version created by previous W(A)
    tester.read(6, 'E')        # Continue with next operation
    
    # T4 will abort - not first operation, must manually re-execute from beginning
    tester.write(4, 'E', 400)  # This aborts
    
    tester.print_header("Phase 3: Re-execution of Rolled Back Operations")
    
    # T4 re-executes all operations from beginning after abort
    tester.write(4, 'B', 200)  # First operation
    tester.write(4, 'E', 400)  # Then the operation that aborted
    
    # T5 cascading abort - must re-execute from first operation
    tester.write(5, 'C', 50)   # First operation
    tester.read(5, 'B')        # Second operation
    
    # T7 cascading abort - must re-execute from first operation
    tester.read(7, 'A')        # First operation
    tester.read(7, 'C')        # Second operation
    
    tester.print_header("Phase 4: Final Operations and Commits")
    
    # T1 will abort - must re-execute from first operation
    tester.write(1, 'A', 10)   # This aborts
    
    # T1 re-executes from first operation
    tester.read(1, 'D')        # First operation
    tester.write(1, 'A', 10)   # Then the operation that aborted
    
    tester.read(2, 'C') 
    
    # T7 will abort again
    tester.write(7, 'D', 700)  # This aborts
    
    tester.read(3, 'E')
    
    # T2 will abort - must re-execute from first operation
    tester.write(2, 'E', 20)   # This aborts
    
    # T2 re-executes from first operation
    tester.read(2, 'A')        # First operation
    tester.read(2, 'C')        # Second operation
    tester.write(2, 'E', 20)   # Then the operation that aborted
    
    tester.read(6, 'B') 
    
    for i in [1, 2, 3, 4, 5, 6, 7]:
        tester.commit(i)
    
    tester.print_execution_table()
    tester.print_data_versions()


def test_mvto_case2():
    """
    MVTO Test Case 2: Multiple Rollbacks with Immediate Re-execution
    Tests: Sequential aborts with immediate restarts
    """
    print("\n" + "#"*80)
    print("# MVTO TEST CASE 2: Multiple Rollbacks with Immediate Re-execution")
    print("#"*80)
    
    tester = MVCCTester("MVTO")
    for i in range(1, 8):
        tester.create_transaction(i)
    
    tester.print_header("Phase 1: Setup for First ABORT")
    
    tester.read(5, 'P') 
    tester.read(7, 'Q') 
    tester.write(3, 'R', 30)
    tester.read(6, 'S') 
    tester.write(4, 'T', 40)
    
    tester.print_header("Phase 2: First ABORT + Immediate Rollback & Re-execution (T2)")
    
    # T2's first operation W(P) will:
    # 1. Detect conflict → ABORT T2 (RED)
    # 2. Rollback T2 with new TS
    # 3. Auto re-execute W2(P) immediately (first operation) (YELLOW)
    tester.write(2, 'P', 20)
    
    tester.print_header("Phase 3: Setup for Second ABORT")
    
    tester.read(1, 'Q') 
    
    # T5 will abort - W(Q) is not first operation (already did R(P))
    # So immediately re-execute from first operation
    tester.write(5, 'Q', 50)  # This aborts
    
    # T5 operations after rollback - restart from first immediately
    tester.read(5, 'P')
    tester.write(5, 'Q', 50)
    
    tester.read(7, 'R') 
    tester.read(4, 'P')
    
    tester.print_header("Phase 4: Second ABORT + Cascading ABORT & Re-execution (T3)")
    
    # T3's W(S) will abort (not first operation, T3 already did W(R)):
    # 1. Detect conflict → ABORT T3 (RED)
    # 2. Cascading ABORT T7 (RED) - because T7 read T3's write
    # 3. Return abort (not first operation)
    # 4. Manually re-execute from first operation
    tester.write(3, 'S', 30)
    
    # T3 operations after rollback - restart from first
    tester.write(3, 'R', 30)
    tester.write(3, 'S', 30)
    
    # T7 operations after rollback - restart from first
    tester.read(7, 'Q')
    tester.read(7, 'R')
    
    tester.print_header("Phase 5: More Operations and T4 ABORT")
    
    tester.write(6, 'T', 60)
    tester.read(1, 'T')
    # T4 will abort - not first operation (already did W(T) and R(P))
    tester.write(4, 'Q', 40)
    
    # T4 operations after rollback - restart from first
    tester.write(4, 'T', 40)
    tester.read(4, 'P')
    tester.write(4, 'Q', 40)
    
    tester.print_header("Phase 6: Final Operations and T1 ABORT")
    
    tester.read(5, 'S')
    # T1 will abort - not first operation (already did R(Q) and R(T))
    tester.write(1, 'R', 10)
    
    # T1 operations after rollback - restart from first  
    tester.read(1, 'Q')
    tester.read(1, 'T')
    tester.write(1, 'R', 10)
    
    tester.print_header("Phase 7: Final Commits")
    
    for i in range(1, 8):
        tester.commit(i)
    
    tester.print_execution_table()
    tester.print_data_versions()


def test_mvto_case3():
    """
    MVTO Test Case 3: Read-Write Conflicts
    Tests: Read timestamp conflicts causing write aborts
    """
    print("\n" + "#"*80)
    print("# MVTO TEST CASE 3: Read-Write Conflicts")
    print("#"*80)
    
    tester = MVCCTester("MVTO")
    for i in range(1, 6):
        tester.create_transaction(i)
    
    tester.print_header("Phase 1: Reads Establish High R-TS")
    
    tester.read(5, 'X')  # T5 reads X, R-TS(X) = 5
    tester.read(4, 'Y')  # T4 reads Y, R-TS(Y) = 4
    tester.read(3, 'Z')  # T3 reads Z, R-TS(Z) = 3
    
    tester.print_header("Phase 2: Older Transactions Try to Write (Conflicts)")
    
    # T1 tries to write X but TS(1) < R-TS(5) → ABORT
    tester.write(1, 'X', 100)  # First operation - auto re-execute
    
    # T2 tries to write Y but TS(2) < R-TS(4) → ABORT
    tester.write(2, 'Y', 200)  # First operation - auto re-execute
    
    tester.print_header("Phase 3: Continue After Re-execution")
    
    tester.write(1, 'Z', 150)
    tester.read(2, 'X')
    tester.write(3, 'X', 300)
    tester.write(4, 'Z', 400)
    tester.write(5, 'Y', 500)
    
    tester.print_header("Phase 4: Commits")
    
    for i in range(1, 6):
        tester.commit(i)
    
    tester.print_execution_table()
    tester.print_data_versions()


def test_mvto_case4():
    """
    MVTO Test Case 4: Complex Cascading Chain
    Tests: Multi-level cascading rollback (A→B→C→D)
    """
    print("\n" + "#"*80)
    print("# MVTO TEST CASE 4: Complex Cascading Chain")
    print("#"*80)
    
    tester = MVCCTester("MVTO")
    for i in range(1, 7):
        tester.create_transaction(i)
    
    tester.print_header("Phase 1: Build Dependency Chain")
    
    # T1 writes A
    tester.write(1, 'A', 10)
    
    # T2 reads A (depends on T1)
    tester.read(2, 'A')
    tester.write(2, 'B', 20)
    
    # T3 reads B (depends on T2)
    tester.read(3, 'B')
    tester.write(3, 'C', 30)
    
    # T4 reads C (depends on T3)
    tester.read(4, 'C')
    tester.write(4, 'D', 40)
    
    # T5 reads D (depends on T4)
    tester.read(5, 'D')
    
    tester.print_header("Phase 2: Trigger Chain Reaction")
    
    # T6 reads A with high timestamp
    tester.read(6, 'A')
    
    # T1 tries to update A again - this will cascade abort T2→T3→T4→T5
    tester.write(1, 'A', 15)  # ABORT + cascades
    
    tester.print_header("Phase 3: Re-execute All Affected Transactions")
    
    # T1 re-executes
    tester.write(1, 'A', 10)
    tester.write(1, 'A', 15)
    
    # T2 re-executes
    tester.read(2, 'A')
    tester.write(2, 'B', 20)
    
    # T3 re-executes
    tester.read(3, 'B')
    tester.write(3, 'C', 30)
    
    # T4 re-executes
    tester.read(4, 'C')
    tester.write(4, 'D', 40)
    
    # T5 re-executes
    tester.read(5, 'D')
    
    tester.print_header("Phase 4: Commits")
    
    for i in range(1, 7):
        tester.commit(i)
    
    tester.print_execution_table()
    tester.print_data_versions()


def test_mvto_case5():
    """
    MVTO Test Case 5: Interleaved Read-Write Pattern
    Tests: Complex interleaving with multiple version reads
    """
    print("\n" + "#"*80)
    print("# MVTO TEST CASE 5: Interleaved Read-Write Pattern")
    print("#"*80)
    
    tester = MVCCTester("MVTO")
    for i in range(1, 8):
        tester.create_transaction(i)
    
    tester.print_header("Phase 1: Initial Writes Create Versions")
    
    tester.write(1, 'A', 10)
    tester.write(2, 'B', 20)
    tester.write(3, 'C', 30)
    
    tester.print_header("Phase 2: Interleaved Reads and Writes")
    
    tester.read(4, 'A')   # Reads A version from T1
    tester.write(1, 'A', 11)  # T1 updates A
    
    tester.read(5, 'B')   # Reads B version from T2
    tester.write(2, 'B', 21)  # T2 updates B
    
    tester.read(6, 'C')   # Reads C version from T3
    tester.write(3, 'C', 31)  # T3 updates C
    
    tester.print_header("Phase 3: Cross References")
    
    tester.read(7, 'A')   # T7 reads A (latest version)
    tester.read(7, 'B')   # T7 reads B (latest version)
    tester.read(7, 'C')   # T7 reads C (latest version)
    
    tester.write(4, 'D', 40)
    tester.write(5, 'E', 50)
    tester.write(6, 'F', 60)
    
    tester.print_header("Phase 4: Late Writers Conflict")
    
    # Older transaction tries to write after newer reads
    tester.write(1, 'D', 14)  # May conflict with T4's write
    tester.write(2, 'E', 24)  # May conflict with T5's write
    
    tester.print_header("Phase 5: Commits")
    
    for i in range(1, 8):
        tester.commit(i)
    
    tester.print_execution_table()
    tester.print_data_versions()


def main():
    """Run all MVTO test cases"""
    print("="*80)
    print("MVTO ALGORITHM TEST SUITE")
    print("="*80)
    
    try:
        test_mvto_case1()
        test_mvto_case2()
        test_mvto_case3()
        test_mvto_case4()
        test_mvto_case5()
        
        print("\n" + "="*80)
        print("ALL MVTO TESTS COMPLETED")
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
