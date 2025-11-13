# Concurrency Control Manager - Timestamp-Based Algorithm

## Overview
This is the **Concurrency Control Manager** component for the mini Database Management System (mDBMS) project. This implementation focuses on the **Timestamp-Based Concurrency Control Algorithm** along with supporting data structures including **Queue**, **PriorityQueue**, and **Schedule**.

## Team Member
- **Component**: Concurrency Control Manager
- **Algorithms Implemented**: Timestamp-Based Concurrency Control
- **Supporting Classes**: Queue, PriorityQueue, PriorityItem, Schedule

## Components Implemented

### 1. Queue (FIFO Queue)
A generic First-In-First-Out (FIFO) queue implementation.

**Features:**
- `enqueue(item)` - Add item to the back of queue
- `dequeue()` - Remove and return item from front
- `is_empty()` - Check if queue is empty
- `size()` - Get number of items in queue
- `peek()` - View front item without removing
- `clear()` - Clear all items

**Time Complexity:**
- Enqueue: O(1)
- Dequeue: O(n) due to list.pop(0)
- Peek: O(1)

### 2. PriorityItem
A wrapper class for items in the priority queue that includes priority value and comparison operators.

**Features:**
- Supports comparison operators (`<`, `<=`, `>`, `>=`, `==`)
- Lower priority value = higher priority
- Generic type support

### 3. PriorityQueue (Min-Heap Implementation)
A priority queue implemented using a min-heap data structure.

**Features:**
- `enqueue(item, priority)` - Add item with priority
- `dequeue()` - Remove and return highest priority item
- `is_empty()` - Check if queue is empty
- `size()` - Get number of items
- `peek()` - View highest priority item without removing
- `clear()` - Clear all items

**Time Complexity:**
- Enqueue: O(log n)
- Dequeue: O(log n)
- Peek: O(1)

**Heap Properties:**
- Min-heap: parent priority ≤ children priorities
- Array-based representation
- Parent at index i, children at 2i+1 and 2i+2

### 4. Schedule
Manages the scheduling of transaction actions with three internal queues:

**Queues:**
1. **input_queue**: Newly arrived actions (FIFO Queue)
2. **ready_list**: Actions ready to execute (List)
3. **blocked_queue**: Blocked actions with retry priority (PriorityQueue)

**Features:**
- `enqueue(action)` - Add new action to input queue
- `get_next_action()` - Get next action respecting priority order
- `mark_ready(action)` - Mark action as ready
- `mark_blocked(action)` - Block action with priority calculation
- `remove_transaction_actions(tid)` - Remove all actions for a transaction
- `retry_blocked_actions()` - Retry blocked actions

**Priority Order:**
1. Ready list (highest)
2. Input queue (medium)
3. Blocked queue (lowest, but prioritized internally)

**Blocked Action Priority:**
- Priority = (retry_count × 10) - wait_time
- Lower value = higher priority
- Favors: fewer retries and longer wait times

### 5. TimestampBased Algorithm
Implementation of the Timestamp-Based Concurrency Control algorithm.

**Principle:**
- Each transaction gets a timestamp at start
- Each object tracks read_timestamp (R-TS) and write_timestamp (W-TS)
- Decisions based on timestamp comparisons

**Rules:**

**For READ operation by transaction T on object X:**
- ✓ **Allow** if: TS(T) ≥ W-TS(X)
  - Update R-TS(X) = max(R-TS(X), TS(T))
- ✗ **Deny** if: TS(T) < W-TS(X)
  - T is trying to read obsolete data

**For WRITE operation by transaction T on object X:**
- ✓ **Allow** if: TS(T) ≥ R-TS(X) AND TS(T) ≥ W-TS(X)
  - Update W-TS(X) = TS(T)
- ✗ **Deny** if: TS(T) < R-TS(X) OR TS(T) < W-TS(X)
  - T is trying to overwrite data read/written by newer transaction

**Advantages:**
- Deadlock-free (no waiting)
- Serializable schedules
- Good for read-heavy workloads

**Disadvantages:**
- May cause transaction aborts
- Cascading aborts possible
- Higher abort rate than locking

## Project Structure

```
concurrency_control_manager/
├── __init__.py
├── cc_manager.py                    # Main CCManager class
├── schedule.py                       # Queue, PriorityQueue, Schedule
├── action.py                         # Action class
├── transaction.py                    # Transaction class
├── row.py                            # Row (database object) class
├── response.py                       # Response protocol
├── enums.py                          # Enumerations
├── log_handler.py                    # Log handler
├── algorithms/
│   ├── __init__.py
│   ├── base.py                      # Abstract base class
│   ├── timestamp_based.py           # Timestamp-Based implementation
│   ├── lock_based.py                # Lock-Based (other group)
│   ├── mvcc.py                      # MVCC (other group)
│   └── validation_based.py          # Validation-Based (other group)
├── test_schedule.py                 # Unit tests for Queue/Schedule
├── test_timestamp_based.py          # Unit tests for algorithm
├── driver.py                        # Demo program
└── README.md                        # This file
```

## Installation & Setup

No external dependencies required. Only standard Python libraries are used:
- `datetime` - For timestamps
- `typing` - For type hints
- `abc` - For abstract base classes
- `enum` - For enumerations

**Python Version:** Python 3.7+

## Running Tests

### Run Schedule Tests (Queue, PriorityQueue, Schedule)
```bash
cd /path/to/database-management-system-development-IF3140
python -m unittest concurrency_control_manager.test_schedule -v
```

**Expected Result:** 18 tests passed

### Run Timestamp-Based Algorithm Tests
```bash
cd /path/to/database-management-system-development-IF3140
python -m unittest concurrency_control_manager.test_timestamp_based -v
```

**Expected Result:** 18 tests passed

### Run All Tests
```bash
cd /path/to/database-management-system-development-IF3140
python -m unittest discover concurrency_control_manager -p "test_*.py" -v
```

**Expected Result:** 36 tests passed

## Running Demo Program

The driver program demonstrates 6 different scenarios:

```bash
cd /path/to/database-management-system-development-IF3140
python -m concurrency_control_manager.driver
```

**Demos:**
1. **Basic Operations** - Simple read and write
2. **Serializable Schedule** - Valid T1 → T2 execution
3. **Conflict Detection** - Detecting and aborting conflicting transactions
4. **Multiple Objects** - Bank transfer scenario
5. **Concurrent Transactions** - Interleaved operations
6. **Algorithm Switching** - Changing concurrency control algorithm

## Usage Example

```python
from concurrency_control_manager.cc_manager import CCManager
from concurrency_control_manager.enums import AlgorithmType, ActionType
from concurrency_control_manager.row import Row

# Initialize with Timestamp-Based algorithm
ccm = CCManager(AlgorithmType.TimestampBased)

# Create a database object (row)
account = Row("account_1", "accounts", {"id": 1, "balance": 1000})

# Begin transaction
t1_id = ccm.begin_transaction()

# Log object access
ccm.log_object(account, t1_id)

# Validate read operation
response = ccm.validate_object(account, t1_id, ActionType.READ)
if response.allowed:
    print("Read allowed!")
    # Perform actual read operation
else:
    print(f"Read denied: {response.message}")
    ccm.abort_transaction(t1_id)

# Validate write operation
response = ccm.validate_object(account, t1_id, ActionType.WRITE)
if response.allowed:
    print("Write allowed!")
    # Perform actual write operation
else:
    print(f"Write denied: {response.message}")
    ccm.abort_transaction(t1_id)

# End transaction (commit)
ccm.end_transaction(t1_id)
```

## API Reference

### CCManager

Main interface for concurrency control.

#### Methods:

**`begin_transaction() -> int`**
- Starts a new transaction
- Returns: transaction ID

**`log_object(obj: Row, transaction_id: int) -> None`**
- Logs access to an object
- Parameters:
  - `obj`: Row object being accessed
  - `transaction_id`: ID of transaction

**`validate_object(obj: Row, transaction_id: int, action: ActionType) -> Response`**
- Validates if action is allowed
- Parameters:
  - `obj`: Row object
  - `transaction_id`: Transaction ID
  - `action`: ActionType.READ or ActionType.WRITE
- Returns: Response with `allowed` (bool) and `message` (str)

**`end_transaction(transaction_id: int) -> None`**
- Commits transaction
- Parameters:
  - `transaction_id`: Transaction ID

**`abort_transaction(transaction_id: int) -> None`**
- Aborts transaction
- Parameters:
  - `transaction_id`: Transaction ID

**`set_algorithm(algorithm: AlgorithmType) -> None`**
- Changes concurrency control algorithm
- Can only switch when no active transactions
- Parameters:
  - `algorithm`: AlgorithmType enum value

## Testing Results

All tests pass successfully:

### Schedule Tests (18 tests)
- ✓ Queue operations (enqueue, dequeue, peek, empty, size)
- ✓ PriorityItem comparisons
- ✓ PriorityQueue operations with priority ordering
- ✓ Schedule action management (ready, blocked, removal)

### Timestamp-Based Algorithm Tests (18 tests)
- ✓ ObjectTimestamp operations
- ✓ Read/Write validation rules
- ✓ Conflict detection
- ✓ Serializable schedule validation
- ✓ Multiple objects handling
- ✓ Transaction commit/abort

### Demo Program
- ✓ All 6 demos executed successfully
- ✓ Correct conflict detection and transaction abort
- ✓ Proper timestamp ordering enforcement

## Implementation Details

### Timestamp Assignment
- Timestamps assigned using `datetime.now()` at transaction start
- Ensures unique ordering of transactions
- More recent transaction = higher timestamp value

### Conflict Resolution
- **Thomas Write Rule NOT implemented** (strict timestamp ordering)
- When conflict detected: transaction must abort
- No waiting mechanism (deadlock-free)

### Abort Handling
- Aborted transactions removed from schedule
- Timestamps of committed operations remain
- No cascading rollback needed (due to strict ordering)

## Performance Considerations

### Time Complexity:
- `begin_transaction()`: O(1)
- `validate_object()`: O(1) average
- `end_transaction()`: O(1)
- Schedule operations: O(log n) for priority queue

### Space Complexity:
- O(N) for N transactions
- O(M) for M unique objects accessed
- O(K) for K actions in schedule

### Scalability:
- Efficient for read-heavy workloads
- May have high abort rate with many writes
- No lock management overhead
- Suitable for distributed systems (with distributed timestamps)

## Known Limitations

1. **Abort Rate**: Higher than lock-based for write-heavy workloads
2. **Starvation**: Older transactions more likely to abort
3. **Timestamp Precision**: Limited by `datetime.now()` precision
4. **No Thomas Write Rule**: Strict ordering, more aborts

## Future Enhancements

Possible improvements:
1. **Thomas Write Rule**: Reduce aborts for certain write patterns
2. **Timestamp Intervals**: Reduce conflicts
3. **Multiversion Timestamp Ordering**: Combine with MVCC
4. **Distributed Timestamps**: For distributed DBMS
5. **Adaptive Algorithm Switching**: Based on workload characteristics

## References

1. Database System Concepts (Silberschatz, Korth, Sudarshan)
2. Transaction Processing (Gray & Reuter)
3. IF3140 - Sistem Basis Data Course Materials

## Contact

For questions or issues regarding this implementation, please contact the development team.

---

**Course**: IF3140 - Sistem Basis Data
**Institution**: Institut Teknologi Bandung
**Year**: 2025
