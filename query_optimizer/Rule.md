## Overview

Dokumen ini Hanya untuk penjelasan sementara bagaimana query_check menganggap hasil parser nanti, kalau ada yang mau didiskusikan bilang aja di grup atau pc

Note: LIMIT bisa di-handle sebagai parameter terpisah atau sebagai modifier pada PROJECT.
Tapi belum pasti, perlu diskusi, secara implementasi lebih gampang modifier, tapi bisa ribet buat equivalency di milestone berikutnya.

---

## 1. Tokenizer Rules

### 1.1 Token Types

Tokenizer harus mengidentifikasi token-token berikut:

#### Keywords

```
SELECT, FROM, WHERE, JOIN, ON, NATURAL
UPDATE, SET, INSERT, INTO, DELETE
ORDER BY, LIMIT
BEGIN TRANSACTION, COMMIT
AND, OR, NOT
```

#### Operators

**Comparison Operators:**

```
=, <>, >, >=, <, <=
```

**Arithmetic Operators:**

```
+, -, *, /
```

#### Identifiers

- Nama tabel: `users`, `profiles`, `orders`
- Nama kolom: `id`, `name`, `email`, `users.id`

**Note:** Alias (AS) belum diimplementasikan untuk saat ini.

#### Literals

- String: `'test'`, `"test"`
- Number: `123`, `45.67`
- Boolean: `TRUE`, `FALSE`
- NULL: `NULL`

#### Delimiters

```
,  (comma)
;  (semicolon)
(  (open parenthesis)
)  (close parenthesis)
```

### 1.2 Tokenization Example

**Input SQL:**

```sql
SELECT id, name FROM users WHERE id > 10 ORDER BY name LIMIT 5;
```

**Tokens:**

```
[SELECT] [id] [,] [name] [FROM] [users] [WHERE] [id] [>] [10] [ORDER BY] [name] [LIMIT] [5] [;]
```

---

## 2. Parser Rules

### 2.1 SELECT Query Structure

**SQL Pattern:**

```sql
SELECT columns FROM table [WHERE condition] [ORDER BY columns] [LIMIT n];
```

**Query Tree Structure:**

```
PROJECT (columns)
└── [SORT (ORDER BY columns)]
    └── [FILTER (WHERE condition)]
        └── RELATION (table)
```

**Parsing Steps:**

1. Identifikasi keyword `SELECT` → buat node `PROJECT` dengan value = columns
2. Identifikasi keyword `FROM` → buat node `RELATION` dengan value = table name
3. Jika ada `WHERE` → buat node `FILTER` dengan value = condition, insert antara PROJECT dan RELATION
4. Jika ada `ORDER BY` → buat node `SORT` dengan value = order columns, insert antara PROJECT dan FILTER/RELATION
5. Jika ada `LIMIT` → buat node `LIMIT` sebagai leaf dengan value = limit number

**Example:**

```sql
SELECT id, name FROM users WHERE id = 1;
```

→

```
PROJECT("id, name")
└── FILTER("id = 1")
    └── RELATION("users")
```

---

### 2.2 JOIN Query Structure

**SQL Pattern:**

```sql
SELECT columns FROM table1 JOIN table2 ON condition;
SELECT columns FROM table1 NATURAL JOIN table2;
```

**Query Tree Structure:**

```
PROJECT (columns)
└── JOIN ("ON condition" | "NATURAL")
    ├── RELATION (table1)
    └── RELATION (table2)
```

**Parsing Steps:**

1. Identifikasi `SELECT` → node `PROJECT`
2. Identifikasi `FROM table1` → node `RELATION(table1)`
3. Identifikasi `JOIN table2` → node `JOIN` dengan 2 children
4. Identifikasi join type:
   - Jika ada `ON` → value = `"ON condition"`
   - Jika ada `NATURAL` → value = `"NATURAL"`
5. Attach kedua RELATION sebagai children dari JOIN

**Example:**

```sql
SELECT * FROM users JOIN profiles ON users.id = profiles.user_id;
```

→

```
PROJECT("*")
└── JOIN("ON users.id = profiles.user_id")
    ├── RELATION("users")
    └── RELATION("profiles")
```

---

### 2.3 UPDATE Query Structure

**SQL Pattern:**

```sql
UPDATE table SET column1 = value1, column2 = value2 [WHERE condition];
```

**Query Tree Structure:**

```
UPDATE ("SET expressions")
└── [FILTER (WHERE condition)]
    └── RELATION (table)
```

**Parsing Steps:**

1. Identifikasi `UPDATE table` → node `UPDATE`
2. Identifikasi `SET` clause → ambil semua set expressions sebagai value dari UPDATE
3. Identifikasi table name → buat node `RELATION(table)`
4. Jika ada `WHERE` → buat node `FILTER` antara UPDATE dan RELATION
5. Connect: UPDATE → [FILTER] → RELATION

**Example:**

```sql
UPDATE users SET name = 'test', email = 'test@example.com' WHERE id = 1;
```

→

```
UPDATE("name = 'test', email = 'test@example.com'")
└── FILTER("id = 1")
    └── RELATION("users")
```

---

### 2.4 INSERT Query Structure

**SQL Pattern:**

```sql
INSERT INTO table (columns) VALUES (values);
```

**Query Tree Structure:**

```
INSERT ("column1 = value1, column2 = value2, ...")
└── RELATION (table)
```

**Parsing Steps:**

1. Identifikasi `INSERT INTO table` → node `INSERT`
2. Parse columns dan values dari `(columns) VALUES (values)`
3. Format sebagai SET-like expression untuk value: `"column1 = value1, column2 = value2"`
4. Buat node `RELATION(table)` sebagai child
5. Connect: INSERT → RELATION

**Constraints:**

- Hanya menerima **1 record** (single VALUES clause)
- **Tidak menerima subquery** (tidak ada SELECT dalam INSERT)
- INSERT harus punya exactly 1 child (RELATION)

**Example:**

```sql
INSERT INTO users (name, email) VALUES ('John', 'john@example.com');
```

→

```
INSERT("name = 'John', email = 'john@example.com'")
└── RELATION("users")
```

---

### 2.5 DELETE Query Structure

**SQL Pattern:**

```sql
DELETE FROM table [WHERE condition];
```

**Query Tree Structure:**

```
DELETE
└── [FILTER (WHERE condition)]
    └── RELATION (table)
```

**Parsing Steps:**

1. Identifikasi `DELETE FROM table` → node `DELETE`
2. Buat node `RELATION(table)`
3. Jika ada `WHERE` → buat node `FILTER`
4. Connect: DELETE → [FILTER] → RELATION

**Example:**

```sql
DELETE FROM users WHERE id = 1;
```

→

```
DELETE
└── FILTER("id = 1")
    └── RELATION("users")
```

---

### 2.6 Transaction Structure

**SQL Pattern:**

```sql
BEGIN TRANSACTION;
UPDATE users SET name = 'test';
DELETE FROM orders WHERE id = 1;
COMMIT;
```

**Query Tree Structure:**

```
BEGIN_TRANSACTION
├── UPDATE
│   └── RELATION
├── DELETE
│   └── RELATION
└── COMMIT
```

**Parsing Steps:**

1. Identifikasi `BEGIN TRANSACTION` → node `BEGIN_TRANSACTION`
2. Parse semua statement berikutnya sebagai children
3. Stop saat menemukan `COMMIT` → tambahkan node `COMMIT` sebagai last child

---

## 3. Operator Precedence & Associativity

### 3.1 Query Operator Order (Top to Bottom)

```
1. PROJECT (SELECT)       - paling atas
2. SORT (ORDER BY)        - optional
3. FILTER (WHERE)         - optional
4. JOIN / RELATION        - data source
5. LIMIT                  - masih belum pasti
```

---

## 4. Special Cases & Edge Cases

### 4.1 Multiple JOINs

**SQL:**

```sql
SELECT * FROM users JOIN profiles JOIN orders;
```

**Structure:**

```
PROJECT("*")
└── JOIN
    ├── JOIN
    │   ├── RELATION("users")
    │   └── RELATION("profiles")
    └── RELATION("orders")
```

### 4.2 Nested SELECT

**Note:** Subquery dan nested SELECT (termasuk IN clause dengan subquery) **belum didukung** untuk saat ini.

---

## 5. Validation Rules During Parsing

### 5.1 Required Checks

- [ ] **SELECT** harus punya FROM (atau subquery)
- [ ] **JOIN** harus punya ON/NATURAL
- [ ] **UPDATE** harus punya SET clause
- [ ] **INSERT** harus punya VALUES atau SELECT
- [ ] **Table names** harus valid (check dari `get_statistic()`)
- [ ] **Column names** harus valid untuk table yang digunakan

### 5.2 Structural Checks

- [ ] **UNARY operators** (PROJECT, FILTER, SORT) punya exactly 1 child
- [ ] **BINARY operators** (JOIN) punya exactly 2 children
- [ ] **LEAF nodes** (RELATION, LIMIT) tidak punya children
- [ ] **SPECIAL operators** sesuai aturan masing-masing:
  - UPDATE: exactly 1 child (RELATION)
  - INSERT: exactly 1 child (RELATION)
  - DELETE: exactly 1 child (RELATION)
  - BEGIN_TRANSACTION: 0 atau lebih children
  - COMMIT: 0 atau lebih children

---

## 6. Example: Complete Flow

### Input SQL:

```sql
SELECT users.id, users.name, profiles.bio
FROM users
JOIN profiles ON users.id = profiles.user_id
WHERE users.id > 10
ORDER BY users.name
LIMIT 5;
```

### Tokenization:

```
[SELECT] [users.id] [,] [users.name] [,] [profiles.bio]
[FROM] [users]
[JOIN] [profiles] [ON] [users.id] [=] [profiles.user_id]
[WHERE] [users.id] [>] [10]
[ORDER BY] [users.name]
[LIMIT] [5]
[;]
```

### Parsing Steps:

1. Parse `SELECT` → `PROJECT("users.id, users.name, profiles.bio")`
2. Parse `ORDER BY` → `SORT("users.name")`
3. Parse `WHERE` → `FILTER("users.id > 10")`
4. Parse `FROM users JOIN profiles ON` → `JOIN("ON users.id = profiles.user_id")`
5. Parse tables → `RELATION("users")`, `RELATION("profiles")`
6. Parse `LIMIT` → `LIMIT("5")` (masih belum fix)

### Final Query Tree:

```
PROJECT("users.id, users.name, profiles.bio")
└── SORT("users.name")
    └── FILTER("users.id > 10")
        └── JOIN("ON users.id = profiles.user_id")
            ├── RELATION("users")
            └── RELATION("profiles")
```
---

## Summary

**Tokenizer:** Pecah SQL string menjadi tokens (keywords, identifiers, operators, literals)

**Parser:** Bangun Query Tree dengan aturan:

- SELECT → PROJECT di root
- FROM → RELATION di leaf
- WHERE → FILTER di tengah
- JOIN → JOIN dengan 2 RELATION children
- ORDER BY → SORT
- UPDATE/INSERT/DELETE → node special dengan aturan masing-masing

**Validation:** Check structure menggunakan `check_query()`