# Overview

Ini adalah rencana lanjutan untuk implementasi filter, Perkiraan dikerjakan di **Milestone 2**, ada concern kalau processor masih perlu melakukan "parsing" dari query tree filter jika ada banyak kondisi (and, or, not dll). Idenya adalah dengan membuat Query Tree filter lebih modular dan jelas strukturnya, Tokenizer tidak akan berubah seharusnya, Parser akan merubah cara menaruh filter. Selain itu perubahan ini juga seharusnya bisa mempermudah equivalence rule terutama yang berhubungan dengan kondisi agar lebih fleksibel dirubah

**Key Design Decisions:**

- **Separate Operators:** Logical operators (AND/OR/NOT) menggunakan type OPERATOR dan OPERATOR_S, bukan FILTER
- **OPERATOR:** Untuk nested logical operators tanpa explicit source (inherit dari parent)
- **OPERATOR_S:** Untuk logical operators dengan explicit source tree di child pertama
- **FILTER:** Hanya untuk conditional expressions (WHERE/IN/EXIST)
- **Source di awal (OPERATOR_S):** Source tree ditempatkan di child[0] untuk natural reading order
- **No duplication:** Menghindari duplikasi source tree dengan menempatkan source hanya sekali
- **Nested operators:** Nested logical operators tidak perlu source sendiri (inherit dari parent)
- **Special Keyword:** Perlukah dibuat special keyword seperti PARENT untuk filter paling awal ? sepertinya tidak perlu jadi untuk sekarang tidak dibuat.
- jika memang diperlukan source sendiri, source langsung akan overwrite inherit parent. Meskipun aku masih ga tau kondisi apa yang bisa gini, tapi feasible.

Kalau ada saran / koreksi chat aja di grup atau pc

## Problem

**Original Issue:** FILTER menggunakan string untuk kondisi kompleks (contoh: `"WHERE (age > 25 AND name = 'John') OR status = 'active'"`), yang menyulitkan query optimizer untuk:

1. Melakukan selection pushdown
2. Reorder conditions untuk performa
3. Split/merge conditions (equivalency rules)
4. Menentukan precedence dengan jelas (Sebenarnya lebih ke processor)

**Solution Implemented:** Pemisahan logical operators menjadi OPERATOR/OPERATOR_S dan FILTER untuk conditional expressions, dengan tree structure yang jelas.

---

## Goals

1. Support complex conditions dengan precedence yang jelas
2. Memudahkan equivalency rules di milestone berikutnya
3. Minimal overhead untuk processor
4. Backward compatible dengan FILTER sederhana

---

## Implementation

### Implementation Sebelum Milestone 1

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

### Implementasi sekarang

**Structure (OPERATOR_S with Source):**

```
OPERATOR_S("OR")
├── RELATION("users")                # SOURCE - Child 0
├── OPERATOR("AND")                  # Nested logical (no source)
│   ├── FILTER("WHERE age > 25")
│   └── FILTER("WHERE name = 'John'")
└── FILTER("WHERE status = 'active'")
```

**Benefits:**

- Clean separation: OPERATOR/OPERATOR_S untuk logic, FILTER untuk conditions
- Kondisi atomic sudah separated
- Tree structure mudah untuk manipulasi
- Precedence jelas dari hierarchy
- Tidak ada duplikasi source tree
- Source di awal (natural order: FROM ... WHERE)
- Easy untuk parallelization

---

## Design Details

### 1. Node Types

**A. Atomic FILTER (Conditional Expression)**

```python
FILTER("WHERE column op value")
└── <source_tree>  # 1 child
```

Contoh:

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

**D. OPERATOR_S (Logical AND/OR with Source)**

```python
OPERATOR_S("AND" | "OR")
├── <source_tree>              # Child 0 - SOURCE (eksplisit)
├── FILTER(...) | OPERATOR(...) | OPERATOR_S(...)  # Child 1 - Kondisi 1
├── FILTER(...) | OPERATOR(...) | OPERATOR_S(...)  # Child 2 - Kondisi 2
└── FILTER(...) | OPERATOR(...) | OPERATOR_S(...)  # Child N - Kondisi N-1
```

**Key Rules:**

- Minimum 3 children (1 source + 2+ kondisi)
- Child[0] HARUS operator yang menghasilkan source (RELATION, JOIN, SORT, PROJECT, OPERATOR_S, atau FILTER dengan children)
- Child[0] TIDAK BOLEH OPERATOR (tidak menghasilkan data) atau FILTER leaf (tidak ada data)
- Children[1..N] bisa FILTER, OPERATOR, atau OPERATOR_S

**E. OPERATOR (Logical AND/OR/NOT without Explicit Source)**

```python
# AND/OR - Nested logic (inherits source from parent)
OPERATOR("AND" | "OR")
├── FILTER(...) | OPERATOR(...) | OPERATOR_S(...)  # Child 0 - Kondisi 1
├── FILTER(...) | OPERATOR(...) | OPERATOR_S(...)  # Child 1 - Kondisi 2
└── FILTER(...) | OPERATOR(...) | OPERATOR_S(...)  # Child N - Kondisi N-1

# NOT - Operator negasi
OPERATOR("NOT")
└── FILTER(...) | OPERATOR(...) | OPERATOR_S(...)  # Tepat 1 child
```

**Key Rules:**

- AND/OR: Minimum 2 children, semua harus FILTER/OPERATOR/OPERATOR_S
- NOT: Tepat 1 child, harus FILTER/OPERATOR/OPERATOR_S
- OPERATOR inherit data source dari parent (biasanya OPERATOR_S)
- Semua children harus berupa kondisi (FILTER/OPERATOR/OPERATOR_S)

### 2. Validation Rules

#### Rule 1: FILTER (Hanya Conditional Expressions)

- **Type:** `"FILTER"`
- **Value:** `"WHERE condition"`, `"IN column"`, `"EXIST"` (satu atau beberapa kata)
- **Pembatasan:** Value TIDAK BOLEH hanya `"AND"`, `"OR"`, atau `"NOT"` (gunakan OPERATOR/OPERATOR_S)
- **Children:**
  - 1 child: source tree (operator apapun)
  - 2 children: source tree + value (ARRAY/RELATION/PROJECT untuk IN/EXIST)
  - 0 children: Invalid (FILTER harus punya source)

#### Rule 2: OPERATOR_S (Logical AND/OR dengan Source Eksplisit)

- **Type:** `"OPERATOR_S"`
- **Value:** `"AND"` atau `"OR"` (hanya satu kata)
- **Children:**
  - Minimum 3 children (1 source + 2+ conditions)
  - **Child[0] (Source):** MUST be source-producing operator:
    -  Allowed: RELATION, JOIN, SORT, PROJECT, OPERATOR_S, FILTER(≥1 children)
    -  Rejected: OPERATOR (no data), FILTER(0 children / leaf)
  - **Children[1..N] (Conditions):** MUST be FILTER, OPERATOR, or OPERATOR_S
- **Semantics:** Applies logical operation on conditions using explicit source

#### Rule 3: OPERATOR (Logical AND/OR/NOT tanpa Source)

- **Type:** `"OPERATOR"`
- **Value:** `"AND"`, `"OR"`, atau `"NOT"` (hanya satu kata)
- **Children:**
  - **AND/OR:** Minimum 2 children, semua harus FILTER/OPERATOR/OPERATOR_S
  - **NOT:** Tepat 1 child, harus FILTER/OPERATOR/OPERATOR_S
- **Semantics:** Nested logical operations that inherit source from parent OPERATOR_S

#### Rule 4: Precedence

- Parentheses `()` (tertinggi)
- NOT
- AND
- OR (terendah)
- `NOT A AND B OR C` = `((NOT A) AND B) OR C`
- Parser handles precedence automatically with OPERATOR nesting

## Parser Guidelines

**Operator Precedence:**

1. Parentheses `()`
2. NOT (highest precedence)
3. AND
4. OR (lowest precedence)

**Algoritma Parsing untuk** `WHERE (age > 25 AND name = 'John') OR status = 'active'`

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
OPERATOR_S("AND")
├── RELATION("users")               # SOURCE - Child 0
├── FILTER("WHERE age > 25")        # Kondisi 1
└── FILTER("WHERE name = 'John'")   # Kondisi 2

# Simple OR (source di awal)
OPERATOR_S("OR")
├── RELATION("users")               # SOURCE - Child 0
├── FILTER("WHERE status = 'active'")    # Kondisi 1
└── FILTER("WHERE status = 'pending'")   # Kondisi 2

# Complex: (age > 25 AND name = 'John') OR status = 'active'
OPERATOR_S("OR")
├── RELATION("users")               # SOURCE - Child 0 (eksplisit)
├── OPERATOR("AND")                 # Nested (no source, inherits from parent)
│   ├── FILTER("WHERE age > 25")
│   └── FILTER("WHERE name = 'John'")
└── FILTER("WHERE status = 'active'")

# With NOT
OPERATOR_S("AND")
├── RELATION("users")               # SOURCE
├── FILTER("WHERE age > 25")
└── OPERATOR("NOT")
    └── FILTER("WHERE status = 'inactive'")

# JOIN source dengan kondisi dari multiple tables
OPERATOR_S("AND")
├── JOIN("users.dept_id = dept.id") # SOURCE menghasilkan users + dept
│   ├── RELATION("users")
│   └── RELATION("dept")
├── FILTER("WHERE users.age > 25")      # Dari tabel users
└── FILTER("WHERE dept.budget > 100000") # Dari tabel dept

# Query lengkap dengan PROJECT
PROJECT("id, name")
└── OPERATOR_S("AND")
    ├── SORT("age DESC")            # SOURCE (relation terurut)
    │   └── RELATION("users")
    ├── FILTER("WHERE age > 25")
    └── FILTER("WHERE salary > 50000")

# Complex nested with NOT
OPERATOR_S("AND")
├── RELATION("users")
├── FILTER("WHERE age > 25")
└── OPERATOR("NOT")
    └── OPERATOR("OR")              # Nested OR inherits source
        ├── FILTER("WHERE status = 'inactive'")
        └── FILTER("WHERE deleted = true")

# Nested OPERATOR_S as source (valid)
OPERATOR_S("OR")
├── OPERATOR_S("AND")               # SOURCE - menghasilkan data terfilter
│   ├── RELATION("users")
│   ├── FILTER("WHERE age > 18")
│   └── FILTER("WHERE verified = true")
├── FILTER("WHERE status = 'admin'")
└── FILTER("WHERE role = 'moderator'")

# OPERATOR sebagai child (valid - kondisi, bukan source)
OPERATOR_S("AND")
├── RELATION("users")               # SOURCE
├── OPERATOR("OR")                  # Kondisi (inherit source)
│   ├── FILTER("WHERE status = 'active'")
│   └── FILTER("WHERE status = 'pending'")
└── FILTER("WHERE age > 25")

# FILTER sebagai source (valid - karena bukan leaf)
OPERATOR_S("AND")
├── FILTER("WHERE verified = true")
│   └── RELATION("users")
├── FILTER("WHERE status = 'admin'")
└── FILTER("WHERE age > 25")
```

**NOTE** untuk filter sebagai source kalau filternya bertingkat kek gini:
```
# FILTER sebagai source (valid - karena bukan leaf)
OPERATOR_S("AND")
├── FILTER("WHERE verified = true")
│   └── FILTER("WHERE role = 'moderator'")
│       └── RELATION("users")
├── FILTER("WHERE status = 'admin'")
└── FILTER("WHERE age > 25")
```
kalau RELATION di sana ga ada bakal keliatan bener untuk implementasi **sekarang** sebelum milestone-2. Nanti bakal kutambahin

---
