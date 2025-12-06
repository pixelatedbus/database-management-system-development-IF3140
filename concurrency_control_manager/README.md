# Concurrency Control Manager

## Overview
This is the **Concurrency Control Manager** component for the Database Management System (DBMS) project. The CCM provides multiple concurrency control algorithms to ensure transaction isolation and data consistency in a multi-user database environment.

## Algorithms Implemented
The CCM supports four different concurrency control algorithms:
1. **Lock-Based (Wait-Die)** - Pessimistic locking with deadlock prevention
2. **Timestamp-Based** - Timestamp ordering protocol
3. **Validation-Based (Optimistic)** - Optimistic concurrency control with validation phase
4. **MVCC** - Multi-Version Concurrency Control with three variants (MVTO, MV2PL, Snapshot Isolation)

## Algorithm Details

### 1. Lock-Based (Wait-Die)
Uses **Wait-Die** protocol to prevent deadlocks:
- **Older transactions wait** for younger ones
- **Younger transactions abort** when conflicting with older ones
- Supports shared (READ) and exclusive (WRITE) locks
- Lock upgrade from READ to WRITE when needed

### 2. Timestamp-Based
Implements timestamp ordering protocol:
- Each transaction gets a timestamp at start
- Objects track read and write timestamps
- Operations validated against timestamp ordering rules
- Aborts transactions that violate serialization order

### 3. Validation-Based (Optimistic)
Optimistic concurrency control approach:
- Transactions execute without restrictions
- Validation performed at commit time
- Checks for conflicts with concurrent transactions
- Aborts if validation fails

### 4. MVCC (Multi-Version Concurrency Control)
Maintains multiple versions of data:
- **MVTO** - Multi-Version Timestamp Ordering
- **MV2PL** - Multi-Version Two-Phase Locking
- **Snapshot Isolation** - First-Committer-Win or First-Updater-Win policies
- Readers don't block writers, writers don't block readers

## Project Structure

```
concurrency_control_manager/
├── cc_log.txt                       # Log file
├── README.md                        # This file
├── docs/
│   └── class_diagram.puml           # PlantUML class diagram
└── src/
    ├── __init__.py
    ├── cc_manager.py                # Main CCManager class
    ├── action.py                    # Action class
    ├── transaction.py               # Transaction class
    ├── row.py                       # Row (database object) class
    ├── response.py                  # Response classes
    ├── enums.py                     # Enumerations
    ├── log_handler.py               # Transaction log handler
    ├── algorithms/
    │   ├── __init__.py
    │   ├── base.py                  # Abstract base class
    │   ├── lock_based.py            # Lock-Based (Wait-Die)
    │   ├── timestamp_based.py       # Timestamp-Based
    │   ├── validation_based.py      # Validation-Based (Optimistic)
    │   └── mvcc.py                  # MVCC (3 variants)
    └── test/
        ├── unit_test.py             # Comprehensive unit tests
        ├── driver.py                # Demo program
        └── [other test files]       # Algorithm-specific tests
```

## Installation & Setup

No external dependencies required. Uses only standard Python libraries.

**Python Version:** Python 3.7+

## Running Tests

### Run Unit Tests
```bash
cd concurrency_control_manager/src
python test/unit_test.py
```

Tests cover:
- Basic CCManager functionality
- All four algorithms (Lock-Based, Timestamp-Based, Validation-Based, MVCC)
- Transaction lifecycle management
- Conflict detection and resolution

### Run Integration Tests
```bash
python test_main.py
```

Tests all algorithms with concurrent transaction scenarios and logs results to a timestamped file.

## Usage Example

```python
from concurrency_control_manager.src.cc_manager import CCManager
from concurrency_control_manager.src.enums import AlgorithmType, ActionType
from concurrency_control_manager.src.row import Row

# Initialize CCManager with desired algorithm
ccm = CCManager(AlgorithmType.LockBased, log_file="cc_log.txt")

# Create a database object
account = Row("account_1", "accounts", {"balance": 1000})

# Begin transaction
t1_id = ccm.begin_transaction()

# Validate operations
response = ccm.validate_object(account, t1_id, ActionType.READ)
if response.allowed:
    # Perform read operation
    pass

response = ccm.validate_object(account, t1_id, ActionType.WRITE)
if response.allowed:
    # Perform write operation
    pass
else:
    ccm.abort_transaction(t1_id)
    
# Commit transaction
ccm.end_transaction(t1_id)

# Switch algorithm (when no active transactions)
ccm.set_algorithm(AlgorithmType.TimestampBased)
```

## API Reference

### CCManager

**`__init__(algorithm: AlgorithmType, log_file: str = "cc_log.txt")`**
- Initialize CCManager with specified algorithm

**`begin_transaction() -> int`**
- Start a new transaction and return its ID

**`validate_object(obj: Row, transaction_id: int, action: ActionType) -> Response`**
- Validate if an operation is allowed by the concurrency control algorithm
- Returns Response with `allowed` (bool), `message` (str), and optional `value`

**`end_transaction(transaction_id: int) -> None`**
- Commit the transaction

**`abort_transaction(transaction_id: int) -> None`**
- Abort the transaction and release resources

**`set_algorithm(algorithm: AlgorithmType) -> None`**
- Switch to a different concurrency control algorithm
- Only allowed when no active transactions exist

**`get_transaction_status(transaction_id: int) -> Optional[TransactionStatus]`**
- Get current status of a transaction

**`get_active_transactions() -> Dict[int, Transaction]`**
- Get all currently active transactions

**`clear_completed_transactions() -> None`**
- Remove terminated transactions from memory

## Key Features

- **Four algorithms** supporting different concurrency control strategies
- **Dynamic algorithm switching** at runtime
- **Transaction lifecycle management** (Active → Committed/Aborted → Terminated)
- **Conflict detection and resolution** specific to each algorithm
- **Comprehensive logging** for debugging and audit trails
- **Deadlock prevention** (Lock-Based Wait-Die)
- **Multi-version support** (MVCC with 3 variants)
- **Type-safe** with full type hints

## Algorithm Comparison

| Feature | Lock-Based | Timestamp | Validation | MVCC |
|---------|-----------|-----------|------------|------|
| **Blocking** | Yes (older waits) | No | No | Depends on variant |
| **Deadlock** | Prevented (Wait-Die) | Free | Free | Free |
| **Overhead** | Lock management | Timestamp checks | Validation phase | Version storage |
| **Best For** | High contention | Read-heavy | Low contention | Read-heavy, analytical |
| **Abort Rate** | Low | Medium | Medium | Low |

## Integration with DBMS

The CCM is integrated with other DBMS components:
- **Query Processor** - Validates operations before execution
- **Storage Manager** - Coordinates data access
- **Failure Recovery Manager** - Separate logging for recovery
- **Query Optimizer** - Independent optimization layer

## References

- Database System Concepts (Silberschatz, Korth, Sudarshan)
- Transaction Processing (Gray & Reuter)
- IF3140 Course Materials - Institut Teknologi Bandung

---

## Contributor
* 13523074 - Ahsan Malik Al Farisi
* 13523056 - Salman Hanif
* 13622076 - Ziyan Agil Nur Ramadhan
* 13522004 - Eduardus Alvito Kristiadi
* 13523026 - Bertha Soliany Frandy

**Course**: IF3140 - Sistem Basis Data  
**Institution**: Institut Teknologi Bandung  
**Year**: 2024/2025
