import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
from datetime import datetime
from ..cc_manager import CCManager
from ..enums import AlgorithmType, ActionType, TransactionStatus
from ..row import Row


def print_section(title):
    """Print a formatted section header"""
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


def print_response(response, action_desc):
    """Print response in formatted way"""
    status = "[OK] ALLOWED" if response.allowed else "[X] DENIED"
    print(f"  {action_desc}: {status}")
    print(f"    Message: {response.message}")


def demo_basic_operations():
    """Demonstrate basic read and write operations"""
    print_section("Demo 1: Basic Read and Write Operations")

    # Initialize CCManager with Timestamp-Based algorithm
    ccm = CCManager(AlgorithmType.TimestampBased)
    print("[OK] Concurrency Control Manager initialized with Timestamp-Based Algorithm")

    # Create test data
    obj1 = Row("account_1", "accounts", {"id": 1, "balance": 1000})

    # Begin transaction
    t1_id = ccm.begin_transaction()
    print(f"\n[OK] Transaction {t1_id} started")

    # Log and validate read
    ccm.log_object(obj1, t1_id)
    response = ccm.validate_object(obj1, t1_id, ActionType.READ)
    print_response(response, f"T{t1_id} reads account_1")

    # Log and validate write
    response = ccm.validate_object(obj1, t1_id, ActionType.WRITE)
    print_response(response, f"T{t1_id} writes account_1")

    # End transaction
    ccm.end_transaction(t1_id)
    print(f"\n[OK] Transaction {t1_id} committed")


def demo_serializable_schedule():
    """Demonstrate a serializable schedule"""
    print_section("Demo 2: Serializable Schedule (T1 -> T2)")

    ccm = CCManager(AlgorithmType.TimestampBased)

    obj1 = Row("account_1", "accounts", {"id": 1, "balance": 1000})

    # Transaction 1
    t1_id = ccm.begin_transaction()
    print(f"\n[OK] Transaction {t1_id} started")
    time.sleep(0.01)  # Small delay to ensure different timestamps

    # T1 reads
    response = ccm.validate_object(obj1, t1_id, ActionType.READ)
    print_response(response, f"T{t1_id} reads account_1")

    # T1 writes
    response = ccm.validate_object(obj1, t1_id, ActionType.WRITE)
    print_response(response, f"T{t1_id} writes account_1")

    # Transaction 2 (newer timestamp)
    t2_id = ccm.begin_transaction()
    print(f"\n[OK] Transaction {t2_id} started (newer timestamp)")
    time.sleep(0.01)

    # T2 reads (should succeed - newer than T1's write)
    response = ccm.validate_object(obj1, t2_id, ActionType.READ)
    print_response(response, f"T{t2_id} reads account_1")

    # T2 writes (should succeed - newer than T1)
    response = ccm.validate_object(obj1, t2_id, ActionType.WRITE)
    print_response(response, f"T{t2_id} writes account_1")

    # Commit transactions
    ccm.end_transaction(t1_id)
    ccm.end_transaction(t2_id)
    print(f"\n[OK] Both transactions committed successfully")


def demo_conflict_detection():
    """Demonstrate conflict detection and transaction abort"""
    print_section("Demo 3: Conflict Detection - Older Transaction Tries to Write")

    ccm = CCManager(AlgorithmType.TimestampBased)

    obj1 = Row("account_1", "accounts", {"id": 1, "balance": 1000})

    # Transaction 1 (older)
    t1_id = ccm.begin_transaction()
    print(f"\n[OK] Transaction {t1_id} started")
    time.sleep(0.01)

    # T1 reads
    response = ccm.validate_object(obj1, t1_id, ActionType.READ)
    print_response(response, f"T{t1_id} reads account_1")

    # Transaction 2 (newer)
    t2_id = ccm.begin_transaction()
    print(f"\n[OK] Transaction {t2_id} started (newer timestamp)")
    time.sleep(0.01)

    # T2 writes (should succeed)
    response = ccm.validate_object(obj1, t2_id, ActionType.WRITE)
    print_response(response, f"T{t2_id} writes account_1")

    # T1 tries to write (should fail - older than T2's write)
    response = ccm.validate_object(obj1, t1_id, ActionType.WRITE)
    print_response(response, f"T{t1_id} tries to write account_1")

    if not response.allowed:
        print(f"\n[X] Transaction {t1_id} must be aborted due to conflict")
        ccm.abort_transaction(t1_id)
        status = ccm.get_transaction_status(t1_id)
        print(f"  Transaction {t1_id} status: {status.name}")

    # T2 can proceed
    ccm.end_transaction(t2_id)
    print(f"\n[OK] Transaction {t2_id} committed successfully")


def demo_multiple_objects():
    """Demonstrate operations on multiple objects"""
    print_section("Demo 4: Multiple Objects - Bank Transfer")

    ccm = CCManager(AlgorithmType.TimestampBased)

    account1 = Row("account_1", "accounts", {"id": 1, "balance": 1000})
    account2 = Row("account_2", "accounts", {"id": 2, "balance": 500})

    # Transaction 1: Transfer from account1 to account2
    t1_id = ccm.begin_transaction()
    print(f"\n[OK] Transaction {t1_id} started (Transfer $100 from account_1 to account_2)")
    time.sleep(0.01)

    # Read account1
    response = ccm.validate_object(account1, t1_id, ActionType.READ)
    print_response(response, f"T{t1_id} reads account_1 (balance: $1000)")

    # Write account1
    response = ccm.validate_object(account1, t1_id, ActionType.WRITE)
    print_response(response, f"T{t1_id} writes account_1 (new balance: $900)")

    # Read account2
    response = ccm.validate_object(account2, t1_id, ActionType.READ)
    print_response(response, f"T{t1_id} reads account_2 (balance: $500)")

    # Write account2
    response = ccm.validate_object(account2, t1_id, ActionType.WRITE)
    print_response(response, f"T{t1_id} writes account_2 (new balance: $600)")

    # Commit
    ccm.end_transaction(t1_id)
    print(f"\n[OK] Transaction {t1_id} committed - Transfer successful")


def demo_concurrent_transactions():
    """Demonstrate concurrent transactions with interleaved operations"""
    print_section("Demo 5: Concurrent Transactions - Interleaved Operations")

    ccm = CCManager(AlgorithmType.TimestampBased)

    account1 = Row("account_1", "accounts", {"id": 1, "balance": 1000})
    account2 = Row("account_2", "accounts", {"id": 2, "balance": 500})

    # Start both transactions
    t1_id = ccm.begin_transaction()
    print(f"\n[OK] Transaction {t1_id} started")
    time.sleep(0.01)

    t2_id = ccm.begin_transaction()
    print(f"[OK] Transaction {t2_id} started")
    time.sleep(0.01)

    # T1 reads account1
    response = ccm.validate_object(account1, t1_id, ActionType.READ)
    print_response(response, f"T{t1_id} reads account_1")

    # T2 reads account2
    response = ccm.validate_object(account2, t2_id, ActionType.READ)
    print_response(response, f"T{t2_id} reads account_2")

    # T1 writes account1
    response = ccm.validate_object(account1, t1_id, ActionType.WRITE)
    print_response(response, f"T{t1_id} writes account_1")

    # T2 writes account2
    response = ccm.validate_object(account2, t2_id, ActionType.WRITE)
    print_response(response, f"T{t2_id} writes account_2")

    # T2 tries to read account1 (should succeed - T2 is newer)
    response = ccm.validate_object(account1, t2_id, ActionType.READ)
    print_response(response, f"T{t2_id} reads account_1")

    # Commit both
    ccm.end_transaction(t1_id)
    ccm.end_transaction(t2_id)
    print(f"\n[OK] Both transactions committed successfully")


def demo_algorithm_switching():
    """Demonstrate switching between algorithms"""
    print_section("Demo 6: Algorithm Switching")

    ccm = CCManager(AlgorithmType.TimestampBased)
    print("[OK] Initialized with Timestamp-Based Algorithm")

    obj1 = Row("account_1", "accounts", {"id": 1, "balance": 1000})

    # Perform operation with Timestamp-Based
    t1_id = ccm.begin_transaction()
    response = ccm.validate_object(obj1, t1_id, ActionType.READ)
    print_response(response, "Operation with Timestamp-Based")
    ccm.end_transaction(t1_id)

    # Try to switch algorithm (should fail if there are active transactions)
    print("\n[OK] Switching algorithm (no active transactions)...")
    try:
        ccm.set_algorithm(AlgorithmType.LockBased)
        print("[OK] Successfully switched to Lock-Based Algorithm")
    except RuntimeError as e:
        print(f"[X] Failed to switch: {e}")


def main():
    """Main function to run all demos"""
    print("\n" + "=" * 70)
    print("  CONCURRENCY CONTROL MANAGER - TIMESTAMP-BASED ALGORITHM DEMO")
    print("=" * 70)

    try:
        demo_basic_operations()
        demo_serializable_schedule()
        demo_conflict_detection()
        demo_multiple_objects()
        demo_concurrent_transactions()
        demo_algorithm_switching()

        print("\n" + "=" * 70)
        print("  ALL DEMOS COMPLETED SUCCESSFULLY")
        print("=" * 70 + "\n")

    except Exception as e:
        print(f"\n[X] Error occurred: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
