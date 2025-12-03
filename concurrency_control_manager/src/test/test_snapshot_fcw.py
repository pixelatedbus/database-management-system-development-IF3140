"""
Snapshot Isolation with First-Committer-Wins Test Cases
5 comprehensive test cases for SI-FCW algorithm
"""

import sys
from .mvcc_tester import MVCCTester


def test_snapshot_fcw_case1():
    """
    SI-FCW Test Case 1: Basic First-Committer-Wins
    Tests: Write-write conflict detection, first committer wins
    """
    print("\n" + "#"*80)
    print("# SNAPSHOT ISOLATION FCW TEST CASE 1: Basic First-Committer-Wins")
    print("#"*80)
    
    tester = MVCCTester("Snapshot_FCW")
    
    tester.print_header("Initializing Transactions T1-T5")
    
    for i in range(1, 6):
        tester.create_transaction(i)
    
    tester.print_header("Phase 1: Concurrent Reads (Snapshot Isolation)")
    
    # All read from their snapshot - no conflicts
    tester.read(1, 'X')
    tester.read(2, 'X')
    tester.read(3, 'Y')
    tester.read(4, 'Y')
    
    tester.print_header("Phase 2: Concurrent Writes (Buffered)")
    
    # All writes are buffered, no immediate conflicts
    tester.write(1, 'X', 10)
    tester.write(2, 'X', 20)  # Same item as T1
    tester.write(3, 'Y', 30)
    tester.write(4, 'Y', 40)  # Same item as T3
    
    tester.print_header("Phase 3: Commit Phase (First-Committer-Wins)")
    
    # T1 commits first - wins for X
    tester.commit(1)
    
    # T2 commits second - loses (T1 already committed X)
    result_t2 = tester.commit(2)
    
    # T3 commits first for Y - wins
    tester.commit(3)
    
    # T4 commits second for Y - loses
    result_t4 = tester.commit(4)
    
    tester.print_header("Phase 4: Restart Aborted Transactions")
    
    # T2 restarts after abort
    if not result_t2.allowed:
        tester.create_transaction(2)  # Re-create with new snapshot
        tester.read(2, 'X')  # Re-read from new snapshot (sees T1's write)
        tester.write(2, 'X', 25)  # Try to write again
        tester.commit(2)
    
    # T4 restarts after abort
    if not result_t4.allowed:
        tester.create_transaction(4)  # Re-create with new snapshot
        tester.read(4, 'Y')  # Re-read from new snapshot (sees T3's write)
        tester.write(4, 'Y', 45)  # Try to write again
        tester.commit(4)
    
    # T5 no conflicts
    tester.write(5, 'Z', 50)
    tester.commit(5)
    
    tester.print_execution_table()
    tester.print_data_versions()


def test_snapshot_fcw_case2():
    """
    SI-FCW Test Case 2: Multiple Item Conflicts
    Tests: Transactions updating multiple items with conflicts
    """
    print("\n" + "#"*80)
    print("# SNAPSHOT ISOLATION FCW TEST CASE 2: Multiple Item Conflicts")
    print("#"*80)
    
    tester = MVCCTester("Snapshot_FCW")
    
    for i in range(1, 6):
        tester.create_transaction(i)
    
    tester.print_header("Phase 1: Read Phase")
    
    tester.read(1, 'A')
    tester.read(1, 'B')
    tester.read(2, 'A')
    tester.read(2, 'C')
    tester.read(3, 'B')
    tester.read(3, 'C')
    
    tester.print_header("Phase 2: Write Phase (Multiple Items)")
    
    # T1 writes A and B
    tester.write(1, 'A', 10)
    tester.write(1, 'B', 11)
    
    # T2 writes A and C (conflicts with T1 on A)
    tester.write(2, 'A', 20)
    tester.write(2, 'C', 21)
    
    # T3 writes B and C (conflicts with T1 on B, T2 on C)
    tester.write(3, 'B', 30)
    tester.write(3, 'C', 31)
    
    tester.print_header("Phase 3: Commit Race")
    
    # T1 commits first - wins for A, B
    tester.commit(1)
    
    # T2 commits - loses on A (T1 won), but C is still available
    result_t2 = tester.commit(2)
    
    # T3 commits - loses on B (T1 won) and C (depends on T2)
    result_t3 = tester.commit(3)
    
    tester.print_header("Phase 4: Restart Aborted Transactions")
    
    # T2 restarts after abort
    if not result_t2.allowed:
        tester.create_transaction(2)
        tester.read(2, 'A')  # Re-read with new snapshot
        tester.read(2, 'C')
        tester.write(2, 'A', 25)  # Conflict again with T1's committed value
        tester.write(2, 'C', 25)  # But C might be available
        tester.commit(2)
    
    # T3 restarts after abort
    if not result_t3.allowed:
        tester.create_transaction(3)
        tester.read(3, 'B')  # Re-read with new snapshot
        tester.read(3, 'C')
        tester.write(3, 'B', 35)  # Conflict with T1
        tester.write(3, 'C', 35)  # Conflict with whoever won C
        tester.commit(3)
    
    # Clean transactions
    tester.write(4, 'D', 40)
    tester.write(5, 'E', 50)
    tester.commit(4)
    tester.commit(5)
    
    tester.print_execution_table()
    tester.print_data_versions()


def test_snapshot_fcw_case3():
    """
    SI-FCW Test Case 3: Long-Running Transactions
    Tests: Stale snapshots, delayed commits
    """
    print("\n" + "#"*80)
    print("# SNAPSHOT ISOLATION FCW TEST CASE 3: Long-Running Transactions")
    print("#"*80)
    
    tester = MVCCTester("Snapshot_FCW")
    
    for i in range(1, 7):
        tester.create_transaction(i)
    
    tester.print_header("Phase 1: T1 Starts Early, Reads Snapshot")
    
    # T1 starts and reads - gets snapshot at TS=1
    tester.read(1, 'P')
    tester.read(1, 'Q')
    
    tester.print_header("Phase 2: Other Transactions Commit Changes")
    
    # T2 updates P and commits
    tester.write(2, 'P', 20)
    tester.commit(2)
    
    # T3 updates Q and commits
    tester.write(3, 'Q', 30)
    tester.commit(3)
    
    # T4 updates R and commits
    tester.write(4, 'R', 40)
    tester.commit(4)
    
    tester.print_header("Phase 3: T1 Continues with Stale Snapshot")
    
    # T1 still sees old snapshot - reads don't see T2, T3, T4 changes
    tester.read(1, 'R')
    
    # T1 tries to write - will conflict at commit
    tester.write(1, 'P', 10)  # Conflicts with T2
    
    tester.print_header("Phase 4: T5 and T6 Race")
    
    tester.write(5, 'S', 50)
    tester.write(6, 'S', 60)
    
    tester.commit(5)  # Wins
    tester.commit(6)  # Loses
    
    tester.print_header("Phase 5: T1 Commits (Conflict Detection)")
    
    # T1 commits - should detect conflict with T2 on P
    result_t1 = tester.commit(1)
    
    tester.print_header("Phase 6: Restart Aborted Transactions")
    
    # Restart T1 if aborted
    if not result_t1.allowed:
        tester.create_transaction(1)
        tester.read(1, 'P')  # Re-read with new snapshot
        tester.read(1, 'Q')
        tester.read(1, 'R')
        tester.write(1, 'P', 10)
        tester.commit(1)
    
    tester.print_execution_table()
    tester.print_data_versions()


def test_snapshot_fcw_case4():
    """
    SI-FCW Test Case 4: Buffered Writes Validation
    Tests: Write buffer, commit validation
    """
    print("\n" + "#"*80)
    print("# SNAPSHOT ISOLATION FCW TEST CASE 4: Buffered Writes Validation")
    print("#"*80)
    
    tester = MVCCTester("Snapshot_FCW")
    
    for i in range(1, 5):
        tester.create_transaction(i)
    
    tester.print_header("Phase 1: Initial Setup")
    
    # T1 writes and commits - establishes baseline
    tester.write(1, 'X', 10)
    tester.write(1, 'Y', 11)
    tester.commit(1)
    
    tester.print_header("Phase 2: Concurrent Transactions Start")
    
    # T2 and T3 read same snapshot
    tester.read(2, 'X')
    tester.read(3, 'X')
    tester.read(2, 'Y')
    tester.read(3, 'Y')
    
    tester.print_header("Phase 3: Buffer Multiple Writes")
    
    # T2 buffers writes
    tester.write(2, 'X', 20)
    tester.write(2, 'Z', 21)
    
    # T3 buffers writes (conflict on X)
    tester.write(3, 'X', 30)
    tester.write(3, 'W', 31)
    
    # T4 independent
    tester.write(4, 'V', 40)
    
    tester.print_header("Phase 4: Commit and Validate")
    
    # T2 commits first - all writes succeed
    result_t2 = tester.commit(2)
    
    # T3 commits - conflict on X
    result_t3 = tester.commit(3)
    
    # T4 commits - no conflict
    tester.commit(4)
    
    tester.print_header("Phase 5: Restart Aborted Transactions")
    
    # Restart T3 if aborted
    if not result_t3.allowed:
        tester.create_transaction(3)
        tester.read(3, 'X')  # Re-read with new snapshot
        tester.read(3, 'Y')
        tester.write(3, 'X', 30)
        tester.write(3, 'W', 31)
        tester.commit(3)
    
    tester.print_execution_table()
    tester.print_data_versions()


def test_snapshot_fcw_case5():
    """
    SI-FCW Test Case 5: High Contention Scenario
    Tests: Many transactions competing for same items
    """
    print("\n" + "#"*80)
    print("# SNAPSHOT ISOLATION FCW TEST CASE 5: High Contention Scenario")
    print("#"*80)
    
    tester = MVCCTester("Snapshot_FCW")
    
    # Create 8 transactions for high contention
    for i in range(1, 9):
        tester.create_transaction(i)
    
    tester.print_header("Phase 1: All Read Same Items")
    
    for i in range(1, 9):
        tester.read(i, 'HOT')
    
    tester.print_header("Phase 2: All Try to Write Same Item")
    
    # All 8 transactions try to update HOT item
    for i in range(1, 9):
        tester.write(i, 'HOT', i * 10)
    
    tester.print_header("Phase 3: Commit Race (Only First Wins)")
    
    # Commits in order - only T1 should succeed
    results = {}
    for i in range(1, 9):
        results[i] = tester.commit(i)
    
    tester.print_header("Phase 4: Restart Aborted Transactions (Serial)")
    
    # Restart T2-T8 one by one
    for i in range(2, 9):
        if not results[i].allowed:
            tester.create_transaction(i)
            tester.read(i, 'HOT')  # Re-read with new snapshot
            tester.write(i, 'HOT', i * 10)
            tester.commit(i)
    
    tester.print_header("Phase 5: Additional Operations")
    
    # Create new transactions for other items
    for i in range(9, 13):
        tester.create_transaction(i)
    
    tester.write(9, 'A', 90)
    tester.write(10, 'B', 100)
    tester.write(11, 'C', 110)
    tester.write(12, 'D', 120)
    
    for i in range(9, 13):
        tester.commit(i)
    
    tester.print_execution_table()
    tester.print_data_versions()


def main():
    """Run all Snapshot Isolation FCW test cases"""
    print("="*80)
    print("SNAPSHOT ISOLATION FIRST-COMMITTER-WINS TEST SUITE")
    print("="*80)
    
    try:
        test_snapshot_fcw_case1()
        test_snapshot_fcw_case2()
        test_snapshot_fcw_case3()
        test_snapshot_fcw_case4()
        test_snapshot_fcw_case5()
        
        print("\n" + "="*80)
        print("ALL SNAPSHOT FCW TESTS COMPLETED")
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
