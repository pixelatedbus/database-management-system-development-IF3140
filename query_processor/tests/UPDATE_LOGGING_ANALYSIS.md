# UPDATE Logging Analysis

## Current Behavior: Logs Written PER OPERATION ⚠️

### Summary
**UPDATE operations are currently logged to FRM (Failure Recovery Manager) immediately when the UPDATE statement is executed, NOT when COMMIT happens.**

This means:
- ✅ Each UPDATE writes to WAL (Write-Ahead Log) immediately
- ✅ Provides durability if system crashes during transaction
- ⚠️ BUT: Creates logs for operations that might be ROLLED BACK
- ⚠️ Creates multiple log entries for chained updates that get collapsed at COMMIT

---

## Current Implementation Flow

### UPDATE Execution Path

```python
# query_execution.py - execute_update() method

# STEP 1: Read matching rows from storage
matching_rows = self.storage_manager.read_block(...)

# STEP 2: Apply updates and evaluate expressions
rows_to_update = [...]  # Modified rows

# STEP 3: LOG TO FRM IMMEDIATELY (PER OPERATION) ⚠️
if self.frm_adapter and transaction_id:
    self.frm_adapter.log_write(
        transaction_id=transaction_id,
        query=f"UPDATE {table_name}",
        table_name=table_name,
        old_data=matching_rows,    # Original rows for rollback
        new_data=rows_to_update    # Modified rows
    )

# STEP 4: Buffer the update (doesn't write to storage yet)
self.transaction_buffer.buffer_update(
    transaction_id=transaction_id,
    table_name=table_name,
    old_data=old_row,
    new_data=new_row,
    conditions=row_conditions
)
```

### COMMIT Execution Path

```python
# query_processor.py - COMMIT handling

# STEP 1: Group and collapse buffered operations
buffered_ops = self.transaction_buffer.get_buffered_operations(t_id)
# Collapse: old1→new1, new1→new2 becomes old1→new2

# STEP 2: Write to storage (batch update)
self.adapter_storage.batch_update_data(...)

# STEP 3: Log COMMIT to FRM
self.adapter_frm.log_commit(t_id)  # Flushes WAL to disk

# STEP 4: Clear buffer
self.transaction_buffer.clear_transaction(t_id)
```

---

## Comparison: INSERT, UPDATE, DELETE

| Operation | When Logged to FRM | When Written to Storage |
|-----------|-------------------|-------------------------|
| **INSERT** | Per operation ⚠️ | On COMMIT ✅ |
| **UPDATE** | Per operation ⚠️ | On COMMIT ✅ |
| **DELETE** | Per operation ⚠️ | On COMMIT ✅ |
| **COMMIT** | On COMMIT ✅ | N/A |
| **ROLLBACK** | On ROLLBACK ✅ | Nothing written |

**All DML operations follow the same pattern**: Log immediately, write to storage on COMMIT.

---

## Example Scenario

### Transaction with Chained Updates

```sql
BEGIN TRANSACTION;
UPDATE users SET score = 150 WHERE id = 1;  -- Logged: old={score:100}, new={score:150}
UPDATE users SET score = 200 WHERE id = 1;  -- Logged: old={score:150}, new={score:200}
UPDATE users SET status = 'premium' WHERE id = 1;  -- Logged: old={status:'active'}, new={status:'premium'}
COMMIT;  -- Logged: COMMIT
```

### FRM Log Entries

```
[Transaction 1] START
[Transaction 1] WRITE: UPDATE users - old={id:1,score:100,status:'active'} new={id:1,score:150,status:'active'}
[Transaction 1] WRITE: UPDATE users - old={id:1,score:150,status:'active'} new={id:1,score:200,status:'active'}
[Transaction 1] WRITE: UPDATE users - old={id:1,score:200,status:'active'} new={id:1,score:200,status:'premium'}
[Transaction 1] COMMIT
```

### Storage Write (On COMMIT)

Only **1 batch write** occurs:
```
old_data: {id:1, score:100, status:'active'}
new_data: {id:1, score:200, status:'premium'}
```

---

## Issues with Current Approach

### 1. **Redundant Log Entries** 📝
- 3 UPDATE operations logged
- Only 1 actual storage write performed
- Log contains intermediate states that never hit storage

### 2. **Rollback Complexity** ↩️
```sql
BEGIN TRANSACTION;
UPDATE users SET score = 200 WHERE id = 1;  -- Logged
UPDATE users SET score = 300 WHERE id = 1;  -- Logged
ROLLBACK;  -- Both operations logged but never committed
```
- FRM has 2 UPDATE logs + 1 ROLLBACK log
- Recovery needs to identify all operations belong to aborted transaction

### 3. **Log Size Growth** 📈
For chained updates:
- N operations → N log entries
- But only 1 storage write
- Log size grows faster than actual data changes

---

## Alternative Approach: Log on COMMIT

### Proposed Change

Move logging from per-operation to COMMIT time:

```python
# query_execution.py - execute_update()
# REMOVE log_write() call here

# query_processor.py - COMMIT handling
if query_type == "COMMIT":
    # After collapsing operations...
    for (table_name, op_type), ops in ops_by_table_type.items():
        if op_type == "UPDATE":
            # Collapse updates
            state_map = {...}
            old_data_list = [...]
            new_data_list = [...]
            
            # Log collapsed updates to FRM
            self.adapter_frm.log_write(
                transaction_id=t_id,
                query=f"UPDATE {table_name}",
                table_name=table_name,
                old_data=old_data_list,
                new_data=new_data_list
            )
            
            # Write to storage
            self.adapter_storage.batch_update_data(...)
```

### Benefits ✅

1. **Cleaner Logs**: One log entry per unique row updated
2. **No Rollback Clutter**: Aborted transactions leave no UPDATE logs
3. **Matches Storage**: Log reflects actual storage state changes
4. **Smaller Logs**: N operations → M unique rows (M ≤ N)

### Tradeoffs ⚠️

1. **Lost Intermediate States**: Can't see operation sequence
2. **Crash Recovery**: Must reconstruct from collapsed state
3. **Debugging Harder**: Can't trace individual UPDATE steps

---

## Recommendation

### Current System is CORRECT but VERBOSE

The current approach (log per operation) is:
- ✅ **Safe**: All operations logged before COMMIT
- ✅ **Durable**: Survives crashes during transaction
- ✅ **Recoverable**: Can undo individual operations
- ⚠️ **Verbose**: More log entries than storage writes

### When to Change

**Keep current approach if:**
- Detailed audit trail needed
- Fine-grained rollback required
- Debugging transaction internals important

**Switch to COMMIT-time logging if:**
- Log size is a concern
- Only final state matters
- Batch operations are common

---

## Current Status

**Implementation**: Logs written **PER OPERATION** ✅  
**Storage writes**: Batched on **COMMIT** ✅  
**Consistency**: Maintained by transaction buffer ✅  
**Issue**: Log verbosity for chained updates ⚠️

The system is working correctly - the verbosity is a design choice, not a bug.
