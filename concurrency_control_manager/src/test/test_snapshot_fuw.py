"""
Snapshot Isolation with First-Updater-Wins Test Cases
5 comprehensive test cases for SI-FUW algorithm
"""

import sys
from .mvcc_tester import MVCCTester


def test_snapshot_fuw_case1():
    """
    SI-FUW Test Case 1: Basic First-Updater-Wins
    Tests: Immediate conflict detection on writes, exclusive locks
    """
    print("\n" + "#"*80)
    print("# SNAPSHOT ISOLATION FUW TEST CASE 1: Basic First-Updater-Wins")
    print("#"*80)
    
    tester = MVCCTester("Snapshot_FUW")
    
    tester.print_header("Initializing Transactions T1-T5")
    
    for i in range(1, 6):
        tester.create_transaction(i)
    
    tester.print_header("Phase 1: Concurrent Reads (No Conflicts)")
    
    # Reads from snapshot - no locks needed
    tester.read(1, 'X')
    tester.read(2, 'X')
    tester.read(3, 'Y')
    tester.read(4, 'Y')
    
    tester.print_header("Phase 2: First Writes (Acquire Exclusive Locks)")
    
    # T1 writes X - gets exclusive lock immediately
    tester.write(1, 'X', 10)
    
    # T2 tries to write X - BLOCKED (T1 has exclusive lock)
    tester.write(2, 'X', 20)
    
    # T3 writes Y - gets exclusive lock
    tester.write(3, 'Y', 30)
    
    # T4 tries to write Y - BLOCKED (T3 has exclusive lock)
    tester.write(4, 'Y', 40)
    
    tester.print_header("Phase 3: Commits Release Locks")
    
    # T1 commits - releases exclusive lock on X
    tester.commit(1)
    
    # T3 commits - releases exclusive lock on Y
    tester.commit(3)
    
    tester.print_header("Phase 4: Restart Aborted Transactions")
    
    # T2 was aborted - restart with new snapshot
    tester.create_transaction(2)
    tester.read(2, 'X')  # Re-read from new snapshot (sees T1's commit)
    tester.write(2, 'X', 25)  # Write with new value
    tester.commit(2)
    
    # T4 was aborted - restart with new snapshot
    tester.create_transaction(4)
    tester.read(4, 'Y')  # Re-read from new snapshot (sees T3's commit)
    tester.write(4, 'Y', 45)  # Write with new value
    tester.commit(4)
    
    tester.print_header("Phase 5: Independent Transaction")
    
    # T5 independent
    tester.write(5, 'Z', 50)
    tester.commit(5)
    
    tester.print_execution_table()
    tester.print_data_versions()


def test_snapshot_fuw_case2():
    """
    SI-FUW Test Case 2: Lock Wait Chains
    Tests: Multiple transactions waiting for same lock
    """
    print("\n" + "#"*80)
    print("# SNAPSHOT ISOLATION FUW TEST CASE 2: Lock Wait Chains")
    print("#"*80)
    
    tester = MVCCTester("Snapshot_FUW")
    
    for i in range(1, 7):
        tester.create_transaction(i)
    
    tester.print_header("Phase 1: T1 Acquires Lock")
    
    # T1 gets exclusive lock on A
    tester.write(1, 'A', 10)
    
    tester.print_header("Phase 2: Multiple Transactions Wait")
    
    # T2, T3, T4 all try to write A - all must wait
    tester.write(2, 'A', 20)
    tester.write(3, 'A', 30)
    tester.write(4, 'A', 40)
    
    tester.print_header("Phase 3: T1 Commits, Others Aborted")
    
    # When T1 commits, T2-T4 detect conflict and abort
    tester.commit(1)
    
    # These all aborted due to exclusive lock conflict
    tester.commit(2)
    tester.commit(3)
    tester.commit(4)
    
    tester.print_header("Phase 4: Restart Aborted Transactions")
    
    # T2 restarts with new snapshot
    tester.create_transaction(2)
    tester.read(2, 'A')  # Sees T1's committed value
    tester.write(2, 'A', 25)
    tester.commit(2)
    
    # T3 restarts with new snapshot
    tester.create_transaction(3)
    tester.read(3, 'A')  # Sees T2's committed value
    tester.write(3, 'A', 35)
    tester.commit(3)
    
    tester.print_header("Phase 5: Independent Transactions")
    
    tester.write(5, 'B', 50)
    tester.write(6, 'C', 60)
    tester.commit(5)
    tester.commit(6)
    
    tester.print_execution_table()
    tester.print_data_versions()


def test_snapshot_fuw_case3():
    """
    SI-FUW Test Case 3: Mixed Read-Write with Exclusive Locks
    Tests: Read-only vs update transactions
    """
    print("\n" + "#"*80)
    print("# SNAPSHOT ISOLATION FUW TEST CASE 3: Mixed Read-Write")
    print("#"*80)
    
    tester = MVCCTester("Snapshot_FUW")
    
    for i in range(1, 6):
        tester.create_transaction(i)
    
    tester.print_header("Phase 1: Initial Data")
    
    tester.write(1, 'P', 10)
    tester.write(1, 'Q', 11)
    tester.commit(1)
    
    tester.print_header("Phase 2: Read-Only Transactions")
    
    # T2 and T3 are read-only - use snapshots
    tester.read(2, 'P')
    tester.read(2, 'Q')
    tester.read(3, 'P')
    tester.read(3, 'Q')
    
    tester.print_header("Phase 3: Update Transaction Gets Lock")
    
    # T4 updates - gets exclusive lock
    tester.write(4, 'P', 40)
    
    tester.print_header("Phase 4: Another Updater Conflicts")
    
    # T5 tries to update same item - blocked
    tester.write(5, 'P', 50)
    
    tester.print_header("Phase 5: Commits")
    
    # Read-only transactions commit without issues
    tester.commit(2)
    tester.commit(3)
    
    # T4 commits - T5 will abort
    tester.commit(4)
    result_t5 = tester.commit(5)  # Aborts
    
    tester.print_header("Phase 6: Restart Aborted Transaction")
    
    # Restart T5 if aborted
    if not result_t5.allowed:
        tester.create_transaction(5)
        tester.read(5, 'P')  # Re-read with new snapshot
        tester.write(5, 'P', 50)
        tester.commit(5)
    
    tester.print_execution_table()
    tester.print_data_versions()


def test_snapshot_fuw_case4():
    """
    SI-FUW Test Case 4: Interleaved Updates
    Tests: Alternating lock acquisition and release
    """
    print("\n" + "#"*80)
    print("# SNAPSHOT ISOLATION FUW TEST CASE 4: Interleaved Updates")
    print("#"*80)
    
    tester = MVCCTester("Snapshot_FUW")
    
    for i in range(1, 7):
        tester.create_transaction(i)
    
    tester.print_header("Phase 1: Alternating Locks")
    
    # T1 locks X
    tester.write(1, 'X', 10)
    
    # T2 locks Y (different item - no conflict)
    tester.write(2, 'Y', 20)
    
    # T3 locks Z
    tester.write(3, 'Z', 30)
    
    tester.print_header("Phase 2: Conflicts Arise")
    
    # T4 tries X (conflicts with T1)
    tester.write(4, 'X', 40)
    
    # T5 tries Y (conflicts with T2)
    tester.write(5, 'Y', 50)
    
    # T6 tries Z (conflicts with T3)
    tester.write(6, 'Z', 60)
    
    tester.print_header("Phase 3: Release Locks in Order")
    
    tester.commit(1)  # T4 can now detect conflict
    tester.commit(2)  # T5 can now detect conflict
    tester.commit(3)  # T6 can now detect conflict
    
    tester.print_header("Phase 4: Conflicted Transactions Abort")
    
    result_t4 = tester.commit(4)  # Aborts
    result_t5 = tester.commit(5)  # Aborts
    result_t6 = tester.commit(6)  # Aborts
    
    tester.print_header("Phase 5: Restart Aborted Transactions")
    
    # Restart T4, T5, T6 one by one
    if not result_t4.allowed:
        tester.create_transaction(4)
        tester.read(4, 'X')  # Re-read with new snapshot
        tester.write(4, 'X', 40)
        tester.commit(4)
    
    if not result_t5.allowed:
        tester.create_transaction(5)
        tester.read(5, 'Y')  # Re-read with new snapshot
        tester.write(5, 'Y', 50)
        tester.commit(5)
    
    if not result_t6.allowed:
        tester.create_transaction(6)
        tester.read(6, 'Z')  # Re-read with new snapshot
        tester.write(6, 'Z', 60)
        tester.commit(6)
    
    tester.print_execution_table()
    tester.print_data_versions()


def test_snapshot_fuw_case5():
    """
    SI-FUW Test Case 5: High Contention with Immediate Aborts
    Tests: Many transactions competing, immediate conflict detection
    """
    print("\n" + "#"*80)
    print("# SNAPSHOT ISOLATION FUW TEST CASE 5: High Contention")
    print("#"*80)
    
    tester = MVCCTester("Snapshot_FUW")
    
    # Create 10 transactions for stress test
    for i in range(1, 11):
        tester.create_transaction(i)
    
    tester.print_header("Phase 1: First Transaction Gets Lock")
    
    # T1 gets exclusive lock on HOT
    tester.write(1, 'HOT', 10)
    
    tester.print_header("Phase 2: Avalanche of Conflicts")
    
    # T2-T10 all try to write HOT - all blocked
    for i in range(2, 11):
        tester.write(i, 'HOT', i * 10)
    
    tester.print_header("Phase 3: T1 Commits, Cascade of Aborts")
    
    # When T1 commits, T2-T10 all abort due to conflict
    tester.commit(1)
    
    results = {}
    for i in range(2, 11):
        results[i] = tester.commit(i)  # All should abort
    
    tester.print_header("Phase 4: Restart Aborted Transactions (Serial)")
    
    # Restart T2-T10 one by one
    for i in range(2, 11):
        if not results[i].allowed:
            tester.create_transaction(i)
            tester.read(i, 'HOT')  # Re-read with new snapshot
            tester.write(i, 'HOT', i * 10)
            tester.commit(i)
    
    tester.print_header("Phase 5: New Round on Different Items")
    
    # Create new transactions
    for i in range(11, 16):
        tester.create_transaction(i)
    
    # Each updates different item - no conflicts
    tester.write(11, 'A', 110)
    tester.write(12, 'B', 120)
    tester.write(13, 'C', 130)
    tester.write(14, 'D', 140)
    tester.write(15, 'E', 150)
    
    for i in range(11, 16):
        tester.commit(i)
    
    tester.print_execution_table()
    tester.print_data_versions()


def main():
    """Run all Snapshot Isolation FUW test cases"""
    print("="*80)
    print("SNAPSHOT ISOLATION FIRST-UPDATER-WINS TEST SUITE")
    print("="*80)
    
    try:
        test_snapshot_fuw_case1()
        test_snapshot_fuw_case2()
        test_snapshot_fuw_case3()
        test_snapshot_fuw_case4()
        test_snapshot_fuw_case5()
        
        print("\n" + "="*80)
        print("ALL SNAPSHOT FUW TESTS COMPLETED")
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
