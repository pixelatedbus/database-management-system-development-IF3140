"""
MV2PL (Multi-Version Two-Phase Locking) Test Cases
5 comprehensive test cases for MV2PL with Wound-Wait deadlock prevention
"""

import sys
from .mvcc_tester import MVCCTester


def test_mv2pl_case1():
    """
    MV2PL Test Case 1: Lock Conflicts with Queue Management
    Tests: Read/write lock conflicts, queue ordering
    """
    print("\n" + "#"*80)
    print("# MV2PL TEST CASE 1: Lock Conflicts with Queue Management")
    print("#"*80)
    
    tester = MVCCTester("MV2PL")
    
    tester.print_header("Initializing Transactions T1-T5 (TID: 1-5)")
    
    for i in range(1, 6):
        tester.create_transaction(i)
    
    tester.print_header("Phase 1: Concurrent Reads (Shared Locks)")
    
    # Multiple transactions read the same item - all get shared locks
    tester.read(1, 'X')
    tester.read(2, 'X')
    tester.read(3, 'X')
    
    tester.print_header("Phase 2: Write Attempt (Exclusive Lock Conflict)")
    
    # T4 tries to write X - must wait for T1, T2, T3 to release shared locks
    tester.write(4, 'X', 100)
    
    tester.print_header("Phase 3: Release Locks by Commit")
    
    # As T1, T2, T3 commit, T4 should get the exclusive lock
    tester.commit(1)
    tester.commit(2)
    tester.commit(3)
    
    tester.print_header("Phase 4: Write Lock Granted")
    
    # T4's write should now succeed
    tester.read(5, 'Y')
    tester.write(5, 'X', 200)  # T5 must wait for T4
    
    tester.print_header("Phase 5: Final Commits")
    
    tester.commit(4)
    tester.commit(5)
    
    tester.print_execution_table()
    tester.print_data_versions()


def test_mv2pl_case2():
    """
    MV2PL Test Case 2: Heavy Lock Contention
    Tests: Multiple transactions contending for same resources
    """
    print("\n" + "#"*80)
    print("# MV2PL TEST CASE 2: Heavy Lock Contention")
    print("#"*80)
    
    tester = MVCCTester("MV2PL")
    
    for i in range(1, 8):
        tester.create_transaction(i)
    
    tester.print_header("Phase 1: Establishing Initial Locks")
    
    tester.read(1, 'A')
    tester.write(2, 'B', 20)
    tester.read(3, 'C')
    
    tester.print_header("Phase 2: Contention on Item A")
    
    tester.read(4, 'A')   # Shared lock - OK (T1 also has shared)
    tester.read(5, 'A')   # Shared lock - OK
    tester.write(6, 'A', 60)  # Exclusive lock - must WAIT for T1, T4, T5
    
    tester.print_header("Phase 3: Contention on Item B")
    
    tester.read(7, 'B')   # Shared lock - must WAIT for T2's exclusive lock
    tester.write(1, 'B', 10)  # Exclusive lock - must WAIT
    
    tester.print_header("Phase 4: Releasing Some Locks")
    
    tester.commit(2)  # Releases exclusive lock on B
    tester.commit(3)
    
    tester.print_header("Phase 5: Cascade of Lock Grants")
    
    tester.read(4, 'D')
    tester.commit(4)  # Releases shared lock on A
    tester.commit(5)  # Releases shared lock on A
    
    tester.print_header("Phase 6: Final Operations")
    
    tester.commit(1)  # Releases locks on A
    # Now T6 can get exclusive lock on A
    
    tester.commit(6)
    tester.commit(7)
    
    tester.print_execution_table()
    tester.print_data_versions()


def test_mv2pl_case3():
    """
    MV2PL Test Case 3: Wound-Wait Deadlock Prevention
    Tests: TID-based priority, wound younger transactions, older waits
    """
    print("\n" + "#"*80)
    print("# MV2PL TEST CASE 3: Wound-Wait Deadlock Prevention")
    print("#"*80)
    
    tester = MVCCTester("MV2PL")
    
    tester.print_header("Initializing Transactions with Different Priorities (TID 1-6)")
    
    for i in range(1, 7):
        tester.create_transaction(i)
    
    tester.print_header("Phase 1: Younger Transactions Acquire Locks First")
    
    # T5 (younger, low priority) gets exclusive lock on A
    tester.write(5, 'A', 50)
    
    # T6 (youngest, lowest priority) gets exclusive lock on B
    tester.write(6, 'B', 60)
    
    # T4 gets exclusive lock on C
    tester.write(4, 'C', 40)
    
    tester.print_header("Phase 2: Demonstration 1 - Older WOUNDS Younger")
    
    # T1 (oldest, highest priority) tries to write A
    # T5 holds lock, but T1 (TID=1) < T5 (TID=5) → T1 WOUNDS T5
    # T5 is aborted, T1 gets the lock
    tester.write(1, 'A', 10)
    
    tester.print_header("Phase 3: T5 Re-executes After Being Wounded")
    
    # T5 must re-execute its operations
    tester.write(5, 'A', 50)
    
    tester.print_header("Phase 4: Demonstration 2 - Another Wound Scenario")
    
    # T2 (older) tries to write B held by T6 (younger)
    # T2 (TID=2) < T6 (TID=6) → T2 WOUNDS T6
    tester.write(2, 'B', 20)
    
    tester.print_header("Phase 5: T6 Re-executes After Being Wounded")
    
    # T6 must re-execute
    tester.write(6, 'B', 60)
    
    tester.print_header("Phase 6: Demonstration 3 - Younger WAITS for Older")
    
    # T3 gets lock on D first
    tester.write(3, 'D', 30)
    
    # T5 (younger) tries to write D held by T3 (older)
    # T5 (TID=5) > T3 (TID=3) → T5 WAITS (no wound)
    tester.write(5, 'D', 55)
    
    tester.print_header("Phase 7: Complete Remaining Operations")
    
    tester.read(1, 'C')
    tester.read(2, 'A')
    tester.write(4, 'E', 40)
    
    tester.print_header("Phase 8: Commits Release Locks")
    
    for i in [1, 2, 3, 4, 5, 6]:
        tester.commit(i)
    
    tester.print_execution_table()
    tester.print_data_versions()


def test_mv2pl_case4():
    """
    MV2PL Test Case 4: Read-Only and Update Mix
    Tests: Mixed workload with read-only transactions
    """
    print("\n" + "#"*80)
    print("# MV2PL TEST CASE 4: Read-Only and Update Mix")
    print("#"*80)
    
    tester = MVCCTester("MV2PL")
    
    for i in range(1, 6):
        tester.create_transaction(i)
    
    tester.print_header("Phase 1: Initial Writes")
    
    tester.write(1, 'P', 10)
    tester.write(2, 'Q', 20)
    tester.write(3, 'R', 30)
    
    tester.print_header("Phase 2: Commit Writers")
    
    tester.commit(1)
    tester.commit(2)
    tester.commit(3)
    
    tester.print_header("Phase 3: Read-Only Transactions (No Lock Conflicts)")
    
    # Read-only transactions can read committed versions
    tester.read(4, 'P')
    tester.read(4, 'Q')
    tester.read(4, 'R')
    
    tester.read(5, 'P')
    tester.read(5, 'Q')
    
    tester.print_header("Phase 4: Updater Arrives")
    
    # New writer needs exclusive locks
    for i in range(1, 4):
        tester.create_transaction(i + 5)  # T6, T7, T8
    
    tester.write(6, 'P', 60)  # Must wait if T4, T5 still reading
    
    tester.print_header("Phase 5: Readers Commit, Writer Proceeds")
    
    tester.commit(4)
    tester.commit(5)
    
    # Now T6 can proceed
    tester.write(7, 'Q', 70)
    tester.write(8, 'R', 80)
    
    tester.print_header("Phase 6: Final Commits")
    
    for i in [6, 7, 8]:
        tester.commit(i)
    
    tester.print_execution_table()
    tester.print_data_versions()


def test_mv2pl_case5():
    """
    MV2PL Test Case 5: Complex Wound-Wait Scenarios
    Tests: Multiple wound situations with priority inversions
    """
    print("\n" + "#"*80)
    print("# MV2PL TEST CASE 5: Complex Wound-Wait Scenarios")
    print("#"*80)
    
    tester = MVCCTester("MV2PL")
    
    for i in range(1, 9):
        tester.create_transaction(i)
    
    tester.print_header("Phase 1: Younger Transactions Build Lock Chain")
    
    # Chain: T8 → X, T7 → Y, T6 → Z
    tester.write(8, 'X', 80)
    tester.write(7, 'Y', 70)
    tester.write(6, 'Z', 60)
    
    tester.print_header("Phase 2: Older Transactions WOUND Chain")
    
    # T1 wounds T8 for X
    tester.write(1, 'X', 10)
    
    # T2 wounds T7 for Y
    tester.write(2, 'Y', 20)
    
    # T3 wounds T6 for Z
    tester.write(3, 'Z', 30)
    
    tester.print_header("Phase 3: Wounded Transactions Re-execute")
    
    # Re-execute in order
    tester.write(6, 'Z', 60)  # T6 re-executes
    tester.write(7, 'Y', 70)  # T7 re-executes
    tester.write(8, 'X', 80)  # T8 re-executes
    
    tester.print_header("Phase 4: Middle Priority Transactions")
    
    # T4 (medium priority) waits for T3 (older)
    tester.write(4, 'Z', 40)  # Must wait for T3
    
    # T5 wounds T6 (younger)
    tester.write(5, 'Z', 50)  # T5 > T3, but T5 < T6
    
    tester.print_header("Phase 5: Cascading Re-executions")
    
    tester.write(6, 'Z', 60)  # T6 re-executes again
    
    tester.print_header("Phase 6: Final Commits")
    
    for i in range(1, 9):
        tester.commit(i)
    
    tester.print_execution_table()
    tester.print_data_versions()


def main():
    """Run all MV2PL test cases"""
    print("="*80)
    print("MV2PL ALGORITHM TEST SUITE")
    print("="*80)
    
    try:
        test_mv2pl_case1()
        test_mv2pl_case2()
        test_mv2pl_case3()
        test_mv2pl_case4()
        test_mv2pl_case5()
        
        print("\n" + "="*80)
        print("ALL MV2PL TESTS COMPLETED")
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
