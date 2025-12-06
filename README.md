# PostgreZQL - Mini Database Management System

**IF3140 - Database Management System Development**  
Institut Teknologi Bandung

## Overview

PostgreZQL adalah implementasi mini DBMS yang mencakup:
- **Query Processor**: Transaction management dan query execution
- **Query Optimizer**: Cost-based optimization
- **Storage Manager**: B-tree & Hash indexing
- **Concurrency Control Manager**: Lock-based protocol dengan deadlock detection
- **Failure Recovery Manager**: Write-Ahead Logging (WAL)  



## How to Run

### 1. Start Database Server

Jalankan server di terminal pertama:

```bash
python socket-main-db.py
```

Output:
```
======================================================================
  DATABASE SERVER - POSTGREZQL
  IF3140 - STEI ITB
======================================================================

Starting server on 127.0.0.1:5433...
Server listening on 127.0.0.1:5433
Waiting for connections...
```

### 2. Connect with Client

Buka terminal baru dan jalankan client:

```bash
python socket-main-client.py
```

Output:
```
======================================================================
  DATABASE CLIENT - POSTGREZQL
  IF3140 - STEI ITB
======================================================================

Connecting to 127.0.0.1:5433...
Connected to database server!

Type your SQL queries (end with ';' to execute) or 'quit' to exit.

dbms>
```

### 3. Execute SQL Commands

```sql
-- Create table
dbms> CREATE TABLE accounts (
    account_id INTEGER PRIMARY KEY,
    account_name VARCHAR(50),
    balance INTEGER
);

-- Insert data
dbms> INSERT INTO accounts VALUES (1, 'Alice', 1000);
dbms> INSERT INTO accounts VALUES (2, 'Bob', 500);

-- Query data
dbms> SELECT * FROM accounts;

-- Update with expression
dbms> UPDATE accounts SET balance = balance + 100 WHERE account_id = 1;

-- Use transactions
dbms> BEGIN TRANSACTION;
dbms*> UPDATE accounts SET balance = balance - 200 WHERE account_id = 1;
dbms*> UPDATE accounts SET balance = balance + 200 WHERE account_id = 2;
dbms*> COMMIT;

-- Delete data
dbms> DELETE FROM accounts WHERE account_id = 2;
```

### Multiple Clients

Untuk concurrent testing, buka multiple terminal dan jalankan `python socket-main-client.py` di masing-masing.
