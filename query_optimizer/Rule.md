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
IN, EXISTS
```

**Note:**

- `IN` digunakan untuk kondisi membership (contoh: `WHERE id IN (1, 2, 3)`)
- `EXISTS` digunakan untuk subquery check (contoh: `WHERE EXISTS (SELECT ...)`)

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
,   (comma)
;   (semicolon)
(   (open parenthesis)
)   (close parenthesis)
```

**Note:**

- `()` digunakan untuk array literals dalam query tree (contoh: `(1, 2, 3)` untuk IN clause)
- Format array konsisten antara SQL dan query tree, keduanya menggunakan `()`

### 1.2 Tokenization Examples

#### Example 1: Simple SELECT with WHERE

**Input SQL:**

```sql
SELECT id, name FROM users WHERE id > 10 ORDER BY name LIMIT 5;
```

**Tokens:**

```
[SELECT] [id] [,] [name] [FROM] [users] [WHERE] [id] [>] [10] [ORDER BY] [name] [LIMIT] [5] [;]
```

#### Example 2: SELECT with IN clause

**Input SQL:**

```sql
SELECT name FROM users WHERE id IN (1, 2, 3);
```

**Tokens:**

```
**Tokens:**

```

[SELECT] [name] [FROM] [users] [WHERE] [id] [IN] [(] [1] [,] [2] [,] [3] [)] [;]

```

**Query Tree Representation:**

```

FILTER("IN id")
├── RELATION("users")
└── ARRAY("(1, 2, 3)")

```

**Note:** Dalam query tree, `(1, 2, 3)` direpresentasikan sebagai ARRAY node dengan value `(1, 2, 3)`
```

**Query Tree Representation:**

```
[SELECT] [name] [FROM] [users] [WHERE] [id] [IN] [[] [1] [,] [2] [,] [3] []]
```

#### Example 3: SELECT with EXISTS

**Input SQL:**

```sql
SELECT * FROM users WHERE EXISTS (SELECT 1 FROM profiles WHERE profiles.user_id = users.id);
```

**Tokens:**

```
[SELECT] [*] [FROM] [users] [WHERE] [EXISTS] [(] [SELECT] [1] [FROM] [profiles]
[WHERE] [profiles.user_id] [=] [users.id] [)] [;]
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
└── FILTER("WHERE id = 1")
    └── RELATION("users")
```

---

### 2.2 FILTER Structure (Special Operator)

**FILTER adalah SPECIAL_OPERATOR dengan aturan fleksibel:**

#### Pattern 1: Single Child (WHERE condition)

**SQL Pattern:**

```sql
SELECT * FROM users WHERE id > 10;
SELECT * FROM (users JOIN profiles) WHERE users.id > 10;
```

**Query Tree Structure:**

```
FILTER ("WHERE condition")
└── <any_operator>   # Bisa RELATION, JOIN, SORT, PROJECT, dll
```

**Karakteristik:**

- 1 child: continuation tree (bisa operator apapun)
- Value: `"WHERE condition"`
- Filter langsung di-apply pada hasil child

**Examples:**

```
# Filter on RELATION
FILTER("WHERE id > 10")
└── RELATION("users")

# Filter on JOIN result
FILTER("WHERE users.id > 10")
└── JOIN("ON users.id = profiles.user_id")
    ├── RELATION("users")
    └── RELATION("profiles")

# Filter on SORT result
FILTER("WHERE id > 10")
└── SORT("name")
    └── RELATION("users")

# Filter on PROJECT result (subquery-like)
FILTER("WHERE id > 10")
└── PROJECT("id, name")
    └── RELATION("users")
```

#### Pattern 2: Two Children (IN clause or subquery)

**SQL Pattern:**

```sql
SELECT * FROM users WHERE id IN (1, 2, 3);
SELECT * FROM users WHERE EXISTS (SELECT 1 FROM profiles WHERE profiles.user_id = users.id);
```

**Query Tree Structure:**

```
FILTER ("IN column" | "EXIST")
├── <first_child>    # Continuation tree (operator apapun)
└── <second_child>   # Value: ARRAY, RELATION, atau PROJECT (subquery)
```

**Karakteristik:**

- 2 children:
  - **Anak pertama:** continuation tree (hasil yang akan di-filter)
  - **Anak kedua:** value (ARRAY untuk IN list, RELATION/PROJECT untuk subquery)
- Value: `"IN column"` atau `"EXIST"`

**Examples:**

```
# IN with ARRAY
FILTER("IN id")
├── RELATION("users")
└── ARRAY("(1, 2, 3)")

# EXIST with subquery (RELATION)
FILTER("EXIST")
├── RELATION("users")
└── RELATION("profiles")   # Represents subquery

# EXIST with subquery (PROJECT)
FILTER("EXIST")
├── RELATION("users")
└── PROJECT("1")           # Subquery: SELECT 1 FROM ...
    └── FILTER("WHERE profiles.user_id = users.id")
        └── RELATION("profiles")
```

#### Validation Rules:

- **Minimum 1 child, maksimum 2 children**
- **1 child:** Bisa operator apapun (RELATION, JOIN, SORT, PROJECT, dll)
- **2 children:**
  - Anak pertama: continuation tree (operator apapun)
  - Anak kedua: harus ARRAY, RELATION, atau PROJECT
- **Value format:**
  - Single word: `"EXIST"` (untuk EXISTS check)
  - Multiple words: `"WHERE condition"` atau `"IN column"`

---

### 2.4 JOIN Query Structure

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

### 2.5 UPDATE Query Structure

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
└── FILTER("WHERE id = 1")
    └── RELATION("users")
```

---

### 2.6 INSERT Query Structure

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

### 2.7 DELETE Query Structure

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
└── FILTER("WHERE id = 1")
    └── RELATION("users")
```

---

### 2.8 Transaction Structure

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
3. FILTER (WHERE)         - optional, bisa di berbagai level
4. JOIN / RELATION        - data source
5. LIMIT                  - masih belum pasti
```

**Catatan Penting tentang FILTER:**

- FILTER bisa ditempatkan di **berbagai level** dalam query tree
- FILTER bisa applied pada hasil operator apapun (RELATION, JOIN, SORT, PROJECT)
- Query optimizer akan menentukan posisi optimal FILTER (selection pushdown)

**Example:**

```sql
SELECT * FROM users JOIN profiles ON users.id = profiles.user_id WHERE users.id > 10;
```

Bisa direpresentasikan sebagai:

```
PROJECT("*")
└── FILTER("WHERE users.id > 10")      # Filter setelah JOIN
    └── JOIN("ON users.id = profiles.user_id")
        ├── RELATION("users")
        └── RELATION("profiles")
```

Atau optimizer bisa push down filter menjadi:

```
PROJECT("*")
└── JOIN("ON users.id = profiles.user_id")
    ├── FILTER("WHERE users.id > 10")   # Filter di-push down ke users
    │   └── RELATION("users")
    └── RELATION("profiles")
```

---

## 4. Operator Categories

### 4.1 Operator Classification

**UNARY_OPERATORS (1 child):**

- `PROJECT` - projection
- `SORT` - ordering

**BINARY_OPERATORS (2 children):**

- `JOIN` - join operations

**LEAF_NODES (0 children):**

- `RELATION` - table reference
- `ARRAY` - array literal
- `LIMIT` - limit value
- `COMMIT` - transaction commit

**SPECIAL_OPERATORS (custom rules):**

- `FILTER` - 1-2 children with special rules
- `UPDATE` - exactly 1 child (relation)
- `INSERT` - exactly 1 child (relation)
- `DELETE` - exactly 1 child (relation)
- `BEGIN_TRANSACTION` - 0+ children

---

## 5. Special Cases & Edge Cases

### 5.1 Multiple JOINs

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

### 5.2 Nested Filters

**SQL:**

```sql
SELECT * FROM users WHERE id > 10 AND id IN (1, 2, 3);
```

**Structure (Multiple Filters):**

```
PROJECT("*")
└── FILTER("WHERE id > 10")
    └── FILTER("IN id")
        ├── RELATION("users")
        └── ARRAY("(1, 2, 3)")
```

### 5.3 Filter on Complex Operations

**SQL:**

```sql
SELECT * FROM (SELECT id, name FROM users ORDER BY name) WHERE id > 10;
```

**Structure:**

```
FILTER("WHERE id > 10")
└── PROJECT("id, name")
    └── SORT("name")
        └── RELATION("users")
```

---

## 6. Validation Rules During Parsing

### 6.1 Required Checks

- [ ] **SELECT** harus punya FROM (atau subquery)
- [ ] **JOIN** harus punya ON/NATURAL
- [ ] **UPDATE** harus punya SET clause
- [ ] **INSERT** harus punya VALUES atau SELECT
- [ ] **Table names** harus valid (check dari `get_statistic()`)
- [ ] **Column names** harus valid untuk table yang digunakan

### 6.2 Structural Checks

- [ ] **UNARY operators** (PROJECT, SORT) punya exactly 1 child
- [ ] **BINARY operators** (JOIN) punya exactly 2 children
- [ ] **LEAF nodes** (RELATION, ARRAY, LIMIT, COMMIT) tidak punya children
- [ ] **SPECIAL operators** sesuai aturan masing-masing:
  - **FILTER:** 1-2 children
    - 1 child: continuation tree (bisa operator apapun)
    - 2 children: first = continuation tree, second = value (ARRAY/RELATION/PROJECT)
  - **UPDATE:** exactly 1 child (RELATION atau FILTER → RELATION)
  - **INSERT:** exactly 1 child (RELATION)
  - **DELETE:** exactly 1 child (RELATION atau FILTER → RELATION)
  - **BEGIN_TRANSACTION:** 0+ children
  - **COMMIT:** 0 children

---

## 7. Example: Complete Flow

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
3. Parse `WHERE` → `FILTER("WHERE users.id > 10")`
4. Parse `FROM users JOIN profiles ON` → `JOIN("ON users.id = profiles.user_id")`
5. Parse tables → `RELATION("users")`, `RELATION("profiles")`
6. Parse `LIMIT` → `LIMIT("5")` (masih belum fix)

### Final Query Tree:

```
PROJECT("users.id, users.name, profiles.bio")
└── SORT("users.name")
    └── FILTER("WHERE users.id > 10")
        └── JOIN("ON users.id = profiles.user_id")
            ├── RELATION("users")
            └── RELATION("profiles")
```

---

## 8. Summary

**Tokenizer:** Pecah SQL string menjadi tokens (keywords, identifiers, operators, literals)

**Parser:** Bangun Query Tree dengan aturan:

- SELECT → PROJECT di root
- FROM → RELATION di leaf
- WHERE → FILTER (fleksibel, bisa di berbagai level)
- JOIN → JOIN dengan 2 RELATION children
- ORDER BY → SORT
- UPDATE/INSERT/DELETE → node special dengan aturan masing-masing

**FILTER Special Rules:**

- **1 child:** Continuation tree (bisa RELATION, JOIN, SORT, PROJECT, atau operator lainnya)
- **2 children:** First child = continuation tree, second child = value (ARRAY/RELATION/PROJECT)

**Validation:** Check structure menggunakan `check_query()`
