# Overview

Ini adalah rencana lanjutan untuk implementasi filter, Perkiraan dikerjakan di **Milestone 2**, ada concern kalau processor masih perlu melakukan "parsing" dari query tree filter jika ada banyak kondisi (and, or, not dll). Idenya adalah dengan membuat Query Tree filter lebih modular dan jelas strukturnya, Tokenizer tidak akan berubah seharusnya, Parser akan merubah cara menaruh filter. Selain itu perubahan ini juga seharusnya bisa mempermudah equivalence rule terutama yang berhubungan dengan kondisi agar lebih fleksibel dirubah

**Key Design Decisions:**

- **Source di awal:** Logical operators (AND/OR/NOT) menaruh source tree di child pertama untuk natural reading order
- **No duplication:** Menghindari duplikasi source tree dengan menempatkan source hanya sekali
- **Nested operators:** Nested logical operators tidak perlu source sendiri (inherit dari parent)
- **Special Keyword:** Perlukah dibuat special keyword seperti PARENT untuk filter paling awal ? sepertinya tidak perlu jadi untuk sekarang tidak dibuat.
- jika memang diperlukan source sendiri, source langsung akan overwrite inherit parent. Meskipun aku masih ga tau kondisi apa yang bisa gini, tapi feasible.

Kalau ada saran / koreksi chat aja di grup atau pc

## Problem

**Current Issue:** FILTER menggunakan string untuk kondisi kompleks (contoh: `"WHERE (age > 25 AND name = 'John') OR status = 'active'"`), yang menyulitkan query optimizer untuk:

1. Melakukan selection pushdown
2. Reorder conditions untuk performa
3. Split/merge conditions (equivalency rules)
4. Menentukan precedence dengan jelas (Sebenarnya lebih ke processor)

**Proposed Solution:** Implementasi nested FILTER nodes dengan logical operators (AND/OR/NOT) sebagai tree structure, dengan **source tree di awal** untuk mengurangi overhead.

---

## Goals

1. Support complex conditions dengan precedence yang jelas
2. Memudahkan equivalency rules di milestone berikutnya
3. Minimal overhead untuk processor
4. Backward compatible dengan FILTER sederhana

---

## Implementation

### Current Implementation

**Structure:**

```
FILTER("WHERE (age > 25 AND name = 'John') OR status = 'active'")
└── RELATION("users")
```

**Problems:**

- Kondisi dalam bentuk string
- Parser harus parse ulang saat optimization
- Sulit untuk split/reorder
- Precedence tidak eksplisit

### Future Implementation

**Structure (Source di Awal):**

```
FILTER("OR")
├── RELATION("users")                # SOURCE - Child 0
├── FILTER("AND")                    # Nested logical (no source)
│   ├── FILTER("WHERE age > 25")
│   └── FILTER("WHERE name = 'John'")
└── FILTER("WHERE status = 'active'")
```

**Benefits:**

- Kondisi atomic sudah separated
- Tree structure mudah untuk manipulasi
- Precedence jelas dari hierarchy
- Tidak ada duplikasi source tree
- Source di awal (natural order: FROM ... WHERE)
- Easy untuk parallelization

---

## Design Details

### 1. FILTER Node Types

**A. Atomic FILTER**

```python
FILTER("WHERE column op value")
└── <source_tree>  # 1 child
```

Examples:

- `FILTER("WHERE id = 1")`
- `FILTER("WHERE age > 25")`
- `FILTER("WHERE name LIKE '%John%'")` (future)

**B. IN FILTER**

```python
FILTER("IN column")
├── <source_tree>      # Child 0
└── ARRAY("(value1, value2, ...)")  # Child 1
```

**C. EXISTS FILTER**

```python
FILTER("EXIST")
└── <subquery_tree>  # 1 child
```

**D. Logical AND FILTER**

```python
FILTER("AND")
├── <source_tree>      # Child 0 - SOURCE (if present)
├── FILTER(...)        # Child 1 - Condition 1
├── FILTER(...)        # Child 2 - Condition 2
└── FILTER(...)        # Child N - Condition N-1

# OR nested (no source, inherit from parent)
FILTER("AND")
├── FILTER(...)        # All children are conditions
├── FILTER(...)
└── FILTER(...)
```

**E. Logical OR FILTER**

```python
FILTER("OR")
├── <source_tree>      # Child 0 - SOURCE (if present)
├── FILTER(...)        # Child 1 - Condition 1
├── FILTER(...)        # Child 2 - Condition 2
└── FILTER(...)        # Child N - Condition N-1

# OR nested (no source, inherit from parent)
FILTER("OR")
├── FILTER(...)        # All children are conditions
├── FILTER(...)
└── FILTER(...)
```

**F. Logical NOT FILTER**

```python
FILTER("NOT")
└── FILTER(...)        # Exactly 1 child - condition to negate
```

### 2. Validation Rules

#### Rule 1: Atomic FILTER

- **Value:** `"WHERE condition"`, `"IN column"`, `"EXIST"`
- **Children:**
  - 1 child: source tree (any operator)
  - 2 children: source tree + value (ARRAY/RELATION/PROJECT)

#### Rule 2: Logical FILTER (AND/OR)

- **Value:** `"AND"` atau `"OR"` (single word)
- **Children (dengan source):**
  - Minimum 3 children (1 source + 2+ conditions)
  - Child 1 MUST NOT be FILTER (it's the source)
  - Children 2..N MUST all be FILTER nodes
- **Children (nested, no source):**
  - Minimum 2 children (2+ conditions)
  - All children MUST be FILTER nodes
  - Inherits source from parent logical FILTER

**Detection:** If child[0].type != "FILTER", it's a source. Otherwise, it's nested (no source).

#### Rule 3: Logical NOT FILTER

- **Value:** `"NOT"` (single word)
- **Children:**
  - Exactly 1 child
  - Child MUST be FILTER node
- **Semantics:** Negates the result of child condition

#### Rule 4: Precedence

- Parentheses `()` (highest)
- NOT
- AND
- OR (lowest)
- `NOT A AND B OR C` = `((NOT A) AND B) OR C`
- Parser handles precedence automatically

## Parser Guidelines

**Operator Precedence:**

1. Parentheses `()`
2. NOT (highest precedence)
3. AND
4. OR (lowest precedence)

**Parsing Algorithm for** `WHERE (age > 25 AND name = 'John') OR status = 'active'`

```
Step 1: Split by OR → ["(age > 25 AND name = 'John')", "status = 'active'"]
Step 2: Split first part by AND → ["age > 25", "name = 'John'"]
Step 3: Create atomic FILTERs (no children yet)
Step 4: Build nested AND node with atomic FILTERs as children
Step 5: Build OR node with source + AND node + atomic FILTER
```

**Tree Structure Examples:**

```
# Simple AND (source di awal)
FILTER("AND")
├── RELATION("users")               # SOURCE - Child 0
├── FILTER("WHERE age > 25")        # Condition 1
└── FILTER("WHERE name = 'John'")   # Condition 2

# Simple OR (source di awal)
FILTER("OR")
├── RELATION("users")               # SOURCE - Child 0
├── FILTER("WHERE status = 'active'")    # Condition 1
└── FILTER("WHERE status = 'pending'")   # Condition 2

# Complex: (age > 25 AND name = 'John') OR status = 'active'
FILTER("OR")
├── RELATION("users")               # SOURCE - Child 0
├── FILTER("AND")                   # Nested (no source)
│   ├── FILTER("WHERE age > 25")
│   └── FILTER("WHERE name = 'John'")
└── FILTER("WHERE status = 'active'")

# With NOT
FILTER("AND")
├── RELATION("users")               # SOURCE
├── FILTER("WHERE age > 25")
└── FILTER("NOT")
    └── FILTER("WHERE status = 'inactive'")

# JOIN source dengan conditions dari multiple tables
FILTER("AND")
├── JOIN("users.dept_id = dept.id") # SOURCE menghasilkan users + dept
│   ├── RELATION("users")
│   └── RELATION("dept")
├── FILTER("WHERE users.age > 25")      # From users table
└── FILTER("WHERE dept.budget > 100000") # From dept table

# Complete query with PROJECT
PROJECT("id, name")
└── FILTER("AND")
    ├── SORT("age DESC")            # SOURCE (sorted relation)
    │   └── RELATION("users")
    ├── FILTER("WHERE age > 25")
    └── FILTER("WHERE salary > 50000")

# Complex nested with NOT
FILTER("AND")
├── RELATION("users")
├── FILTER("WHERE age > 25")
└── FILTER("NOT")
    └── FILTER("OR")
        ├── FILTER("WHERE status = 'inactive'")
        └── FILTER("WHERE deleted = true")
```

---