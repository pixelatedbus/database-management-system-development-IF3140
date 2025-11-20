# Grammar Plan: Detailed Parser

---

## Overview

Dokumen ini berisi rencana untuk merombak parser query optimizer agar memiliki struktur grammar yang lebih formal dan detail, mirip dengan parser compiler. Tujuan utama adalah membuat representasi query tree dengan unit atomik yang jelas (identifier, literal, operator) sehingga memudahkan optimasi dan manipulasi tree.

**Key Changes:**

- Struktur atomik yang jelas untuk setiap komponen (tidak lagi string-based)
- Expression tree yang terpisah untuk kondisi dan aritmatika
- Type-safe nodes untuk identifier, literal, dan operator
- Lebih mudah untuk implementasi optimization rules

---

## Grammar Specification (Formal BNF)

### Level 1: Lexical Elements

```bnf
# Basic Characters
<digit>          ::= '0' | '1' | '2' | '3' | '4' | '5' | '6' | '7' | '8' | '9'
<letter>         ::= 'a'..'z' | 'A'..'Z'
<underscore>     ::= '_'
<whitespace>     ::= ' ' | '\t' | '\n' | '\r'

# Identifiers
<identifier>     ::= (<letter> | <underscore>) (<letter> | <digit> | <underscore>)*
<qualified_id>   ::= <identifier> ('.' <identifier>)?

# Literals
<integer>        ::= <digit>+
<decimal>        ::= <digit>+ '.' <digit>+
<number>         ::= <integer> | <decimal>
<string>         ::= "'" <char>* "'" | '"' <char>* '"'
<boolean>        ::= 'TRUE' | 'FALSE' | 'true' | 'false'
<null>           ::= 'NULL' | 'null'

<literal>        ::= <number> | <string> | <boolean> | <null>

# Operators
<comp_op>        ::= '=' | '<>' | '!=' | '>' | '>=' | '<' | '<=' | 'LIKE' | 'ILIKE'
<arith_op>       ::= '+' | '-' | '*' | '/' | '%'
<logical_op>     ::= 'AND' | 'OR' | 'NOT'

# Keywords
<keyword>        ::= 'SELECT' | 'FROM' | 'WHERE' | 'JOIN' | 'ON' | 'NATURAL' | 'INNER'
                   | 'ORDER' | 'BY' | 'ASC' | 'DESC' | 'LIMIT'
                   | 'UPDATE' | 'SET' | 'INSERT' | 'INTO' | 'VALUES' | 'DELETE'
                   | 'BEGIN' | 'TRANSACTION' | 'COMMIT'
                   | 'CREATE' | 'TABLE' | 'DROP' | 'CASCADE' | 'RESTRICT'
                   | 'IN' | 'EXISTS' | 'BETWEEN' | 'AS' | 'NOT'
```

### Level 2: Expressions

```bnf
# Column References
<column_ref>     ::= <qualified_id>
                   | <identifier>

# Value Expressions
<primary_expr>   ::= <literal>
                   | <column_ref>
                   | '(' <value_expr> ')'

<value_expr>     ::= <primary_expr>
                   | <arith_expr>
                   | <function_call>

<arith_expr>     ::= <value_expr> <arith_op> <value_expr>

<function_call>  ::= <identifier> '(' [<value_expr> (',' <value_expr>)*] ')'

# Conditions
<atom_condition> ::= <comparison>
                   | <in_expr>
                   | <exists_expr>
                   | <between_expr>
                   | <is_null_expr>

<comparison>     ::= <value_expr> <comp_op> <value_expr>

<in_expr>        ::= <column_ref> ['NOT'] 'IN' '(' <value_list> ')'
                   | <column_ref> ['NOT'] 'IN' '(' <select_stmt> ')'

<exists_expr>    ::= ['NOT'] 'EXISTS' '(' <select_stmt> ')'

<between_expr>   ::= <value_expr> ['NOT'] 'BETWEEN' <value_expr> 'AND' <value_expr>

<is_null_expr>   ::= <column_ref> 'IS' ['NOT'] 'NULL'

<value_list>     ::= <literal> (',' <literal>)*

# Logical Conditions
<condition>      ::= <or_condition>

<or_condition>   ::= <and_condition> ('OR' <and_condition>)*

<and_condition>  ::= <not_condition> ('AND' <not_condition>)*

<not_condition>  ::= ['NOT'] <primary_condition>

<primary_condition> ::= <atom_condition>
                      | '(' <condition> ')'
```

### Level 3: Queries

```bnf
# SELECT Statement
<select_stmt>    ::= 'SELECT' <select_list>
                     'FROM' <table_expr>
                     [<where_clause>]
                     [<order_by_clause>]
                     [<limit_clause>]

<select_list>    ::= '*'
                   | <select_item> (',' <select_item>)*

<select_item>    ::= <value_expr>

# FROM Clause
<table_expr>     ::= <table_ref>
                   | <table_expr> <join_clause>

<table_ref>      ::= <identifier> [<alias>]

<alias>          ::= ['AS'] <identifier>

<join_clause>    ::= [<join_type>] 'JOIN' <table_ref> [<join_condition>]

<join_type>      ::= 'INNER' | 'NATURAL'

<join_condition> ::= 'ON' <condition>

# WHERE Clause
<where_clause>   ::= 'WHERE' <condition>

# ORDER BY Clause
<order_by_clause> ::= 'ORDER' 'BY' <order_item>  # Only 1 attribute

<order_item>     ::= <column_ref> [<order_dir>]

<order_dir>      ::= 'ASC' | 'DESC'

# LIMIT Clause
<limit_clause>   ::= 'LIMIT' <number>

# UPDATE Statement
<update_stmt>    ::= 'UPDATE' <table_ref>
                     'SET' <set_list>
                     [<where_clause>]  # Only 1 condition in WHERE

<set_list>       ::= <assignment> (',' <assignment>)*

<assignment>     ::= <column_ref> '=' <value_expr>

# INSERT Statement
<insert_stmt>    ::= 'INSERT' 'INTO' <table_ref>
                     '(' <column_list> ')'
                     'VALUES' '(' <value_list> ')'

<column_list>    ::= <identifier> (',' <identifier>)*

# DELETE Statement
<delete_stmt>    ::= 'DELETE' 'FROM' <table_ref>
                     [<where_clause>]  # Only 1 condition in WHERE

# Transaction Statement
<transaction_stmt> ::= 'BEGIN' 'TRANSACTION' <stmt>* 'COMMIT'

<stmt>           ::= <select_stmt> | <update_stmt> | <insert_stmt> | <delete_stmt>

# CREATE TABLE Statement
<create_table>   ::= 'CREATE' 'TABLE' <identifier> '(' <column_def> (',' <column_def>)* ')'

<column_def>     ::= <identifier> <data_type> [<column_constraint>]

<data_type>      ::= 'INTEGER' | 'FLOAT' | 'CHAR' '(' <number> ')' | 'VARCHAR' '(' <number> ')'

<column_constraint> ::= 'PRIMARY' 'KEY'
                      | 'FOREIGN' 'KEY' 'REFERENCES' <identifier> '(' <identifier> ')'

# DROP TABLE Statement
<drop_table>     ::= 'DROP' 'TABLE' <identifier> [<drop_behavior>]

<drop_behavior>  ::= 'CASCADE' | 'RESTRICT'
```

---

## Query Tree Node Types (Detailed)

### Design Philosophy

**Main Query Structure Nodes:**

- `PROJECT` - SELECT clause dengan column references
- `FILTER` - WHERE clause container dengan explicit source
- `OPERATOR` - Logical operators (AND/OR/NOT)
- `SORT` - ORDER BY dengan order items
- `RELATION` - Table reference
- `JOIN` - JOIN operations

**Atomic Detail Nodes:**

- `IDENTIFIER` - Nama dasar (atomic leaf)
- `LITERAL_*` - Nilai literal (number, string, boolean, null)
- `COLUMN_NAME` - Wrapper untuk nama kolom (berisi IDENTIFIER)
- `TABLE_NAME` - Wrapper untuk nama table/alias (berisi IDENTIFIER)
- `COLUMN_REF` - Referensi kolom (dapat simple atau qualified)
  - Simple: 1 child (COLUMN_NAME)
  - Qualified: 2 children (COLUMN_NAME, TABLE_NAME)
  - Child 0 = COLUMN_NAME (wajib)
  - Child 1 = TABLE_NAME (opsional)
- `COMPARISON` - Operasi perbandingan
- `ARITH_EXPR` - Ekspresi aritmatika
- `IN_EXPR`, `EXISTS_EXPR`, `BETWEEN_EXPR`, `IS_NULL_EXPR`, `LIKE_EXPR` - Ekspresi kondisi
- Negated versions: `NOT_IN_EXPR`, `NOT_EXISTS_EXPR`, `NOT_BETWEEN_EXPR`, `IS_NOT_NULL_EXPR`

**Node Terminology:**

- `FILTER` = WHERE clause container + explicit source (2 children: source + condition_tree)
- `OPERATOR` = Logical operators (AND/OR/NOT) untuk combining conditions

---

### Node Structure Overview

**Atomic Nodes**

```
IDENTIFIER("users")          # Simple identifier (leaf), value = "users"
LITERAL_NUMBER(25)           # Numeric literal (leaf), value = "25"
LITERAL_STRING("active")     # String literal (leaf), value = "active"
LITERAL_BOOLEAN(True)        # Boolean literal (leaf), value = "True"
LITERAL_NULL()               # NULL literal (leaf), no value
COLUMN_NAME                  # Wrapper for column name
└── IDENTIFIER("age")
TABLE_NAME                   # Wrapper for table/alias name
└── IDENTIFIER("users")
```

**COLUMN_REF Node**

```
# Simple column reference (tanpa table/alias)
COLUMN_REF
└── COLUMN_NAME            # Child 0: column name (wajib)
    └── IDENTIFIER("age")

# Qualified column reference (dengan table/alias)
COLUMN_REF
├── COLUMN_NAME            # Child 0: column name (wajib)
│   └── IDENTIFIER("age")
└── TABLE_NAME             # Child 1: table/alias name (opsional)
    └── IDENTIFIER("users")

# Dengan alias
COLUMN_REF
├── COLUMN_NAME            # Child 0: column name (wajib)
│   └── IDENTIFIER("name")
└── TABLE_NAME             # Child 1: table/alias name (opsional)
    └── IDENTIFIER("u")
```

**PROJECT Node**

```
# Select specific columns
PROJECT  # no value
├── COLUMN_REF        # Selected columns (1+)
├── COLUMN_REF
└── [source_tree]     # Last child is always the source

# Select all
PROJECT("*")  # value = "*"
└── [source_tree]
```

**FILTER Node**

```
FILTER
├── [source_tree]     # Child 0: RELATION, JOIN, etc
└── [condition_tree]  # Child 1: COMPARISON, OPERATOR, IN_EXPR, etc

# Example: Simple WHERE
FILTER
├── RELATION("users")
└── COMPARISON(">")
    ├── COLUMN_REF("age")
    └── LITERAL_NUMBER(25)

# Example: WHERE with AND
FILTER
├── RELATION("users")
└── OPERATOR("AND")
    ├── COMPARISON(">")
    │   ├── COLUMN_REF("age")
    │   └── LITERAL_NUMBER(25)
    └── COMPARISON("=")
        ├── COLUMN_REF("status")
        └── LITERAL_STRING("active")
```

**OPERATOR Node**

```
OPERATOR("AND" | "OR" | "NOT")  # value = operator string
├── [comparison_tree] # 2+ for AND/OR, 1 for NOT
├── [comparison_tree] # Can be COMPARISON, IN_EXPR, OPERATOR (nested), etc
└── [comparison_tree]

# Can be used as:
# - Child of FILTER (top-level WHERE logic)
# - Nested inside another OPERATOR (complex logic)

# Example: age > 25 AND city = 'Jakarta'
OPERATOR("AND")
├── COMPARISON(">")
│   ├── COLUMN_REF
│   │   └── COLUMN_NAME
│   │       └── IDENTIFIER("age")
│   └── LITERAL_NUMBER(25)
└── COMPARISON("=")
    ├── COLUMN_REF
    │   └── COLUMN_NAME
    │       └── IDENTIFIER("city")
    └── LITERAL_STRING("Jakarta")
```

**SORT Node**

```
SORT
├── ORDER_ITEM("DESC")  # value = direction ("ASC" or "DESC")
│   └── COLUMN_REF
│       └── COLUMN_NAME
│           └── IDENTIFIER("age")
└── [source_tree]
```

**RELATION Node**

```
# Without alias
RELATION("users")  # value = table name

# With alias (ALIAS sebagai parent)
ALIAS("u")         # value = alias name
└── RELATION("users")  # What is being aliased
```

**COMPARISON Node**

```
COMPARISON("=" | "<>" | ">" | ">=" | "<" | "<=")  # value = operator
├── [left_expr]   # COLUMN_REF, ARITH_EXPR, LITERAL_*, etc
└── [right_expr]  # COLUMN_REF, ARITH_EXPR, LITERAL_*, etc
```

**JOIN Node**

```
JOIN("INNER" | "NATURAL")  # value = join type
├── RELATION("users")
├── RELATION("profiles")
└── COMPARISON("=")  # Optional for INNER JOIN, omitted for NATURAL
    ├── COLUMN_REF
    │   ├── COLUMN_NAME
    │   │   └── IDENTIFIER("id")
    │   └── TABLE_NAME
    │       └── IDENTIFIER("users")
    └── COLUMN_REF
        ├── COLUMN_NAME
        │   └── IDENTIFIER("user_id")
        └── TABLE_NAME
            └── IDENTIFIER("profiles")
```

**DML Nodes**

```
UPDATE_QUERY
├── RELATION
├── ASSIGNMENT+
└── FILTER?

INSERT_QUERY
├── RELATION
├── COLUMN_LIST
└── VALUES_CLAUSE

DELETE_QUERY
├── RELATION
└── FILTER?
```

---

## Example Transformations

### Example 1: Simple WHERE Clause

**SQL:**

```sql
SELECT name FROM users WHERE age > 25
```

**New Detailed Output:**

```
PROJECT
├── COLUMN_REF
│   └── COLUMN_NAME
│       └── IDENTIFIER("name")
└── FILTER
    ├── RELATION("users")
    └── COMPARISON(">")
        ├── COLUMN_REF
        │   └── COLUMN_NAME
        │       └── IDENTIFIER("age")
        └── LITERAL_NUMBER(25)
```

**Key Points:**

- ✅ FILTER untuk WHERE clause dengan explicit source
- ✅ Direct COMPARISON untuk simple condition
- ✅ COLUMN*REF dan LITERAL*\* untuk atomic values

---

### Example 2: WHERE with AND

**SQL:**

```sql
SELECT * FROM users WHERE age > 25 AND status = 'active'
```

**Query Tree:**

```
PROJECT("*")  # value = "*" untuk select all, atau null untuk select columns
└── FILTER
    ├── RELATION("users")
    └── OPERATOR("AND")
        ├── COMPARISON(">")
        │   ├── COLUMN_REF
        │   │   └── COLUMN_NAME
        │   │       └── IDENTIFIER("age")
        │   └── LITERAL_NUMBER(25)
        └── COMPARISON("=")
            ├── COLUMN_REF
            │   └── COLUMN_NAME
            │       └── IDENTIFIER("status")
            └── LITERAL_STRING("active")
```

**Key Points:**

- ✅ FILTER dengan 2 children: source + condition_tree
- ✅ OPERATOR untuk logical operations (AND/OR/NOT)
- ✅ Direct COMPARISON sebagai children dari OPERATOR

---

### Example 3: IN Clauseondition Expressions

**SQL:**

```sql
SELECT * FROM products
WHERE category IN ('Electronics', 'Books')
  AND price BETWEEN 100 AND 500
  AND description IS NOT NULL
  AND name LIKE '%Phone%'
```

**Query Tree:**

```
PROJECT("*")
└── FILTER
    ├── RELATION("products")
    └── OPERATOR("AND")
        ├── IN_EXPR
        │   ├── COLUMN_REF
        │   │   └── COLUMN_NAME
        │   │       └── IDENTIFIER("category")
        │   └── LIST
        │       ├── LITERAL_STRING("Electronics")
        │       └── LITERAL_STRING("Books")
        ├── BETWEEN_EXPR(negated=False)
        │   ├── COLUMN_REF
        │   │   └── COLUMN_NAME
        │   │       └── IDENTIFIER("price")
        │   ├── LITERAL_NUMBER(100)
        │   └── LITERAL_NUMBER(500)
        ├── IS_NULL_EXPR(negated=True)
        │   └── COLUMN_REF
        │       └── COLUMN_NAME
        │           └── IDENTIFIER("description")
        └── LIKE_EXPR
            ├── COLUMN_REF
            │   └── COLUMN_NAME
            │       └── IDENTIFIER("name")
            └── LITERAL_STRING("%Phone%")
```

**Key Points:**

- ✅ OPERATOR children dapat berupa berbagai expression types: COMPARISON, IN_EXPR, BETWEEN_EXPR, IS_NULL_EXPR, LIKE_EXPR
- ✅ Negation menggunakan node type terpisah: NOT_IN_EXPR, IS_NOT_NULL_EXPR, NOT_BETWEEN_EXPR, dll

---

### Example 4: Arithmetic Expression

**SQL:**

```sql
SELECT salary * 1.1 FROM employees WHERE salary + bonus > 50000
```

**New Detailed Output:**

```
PROJECT
├── ARITH_EXPR("*")
│   ├── COLUMN_REF
│   │   └── COLUMN_NAME
│   │       └── IDENTIFIER("salary")
│   └── LITERAL_NUMBER(1.1)
└── FILTER
    ├── RELATION("employees")
    └── COMPARISON(">")
        ├── ARITH_EXPR("+")
        │   ├── COLUMN_REF
        │   │   └── COLUMN_NAME
        │   │       └── IDENTIFIER("salary")
        │   └── COLUMN_REF
        │       └── COLUMN_NAME
        │           └── IDENTIFIER("bonus")
        └── LITERAL_NUMBER(50000)
```

**Key Points:**

- ✅ Arithmetic expressions sebagai tree nodes
- ✅ FILTER untuk WHERE dengan arithmetic comparison

---

### Example 5: Complex Nested Logic

**SQL:**

```sql
SELECT * FROM users WHERE (age > 25 AND city = 'Jakarta') OR status = 'admin'
```

**New Detailed Output:**

```
PROJECT("*")
└── FILTER  ← FILTER untuk container dengan source
    ├── RELATION("users")
    └── OPERATOR("OR")  ← Top-level OR
        ├── OPERATOR("AND")  ← Nested AND
        │   ├── COMPARISON(">")
        │   │   ├── COLUMN_REF
        │   │   │   └── COLUMN_NAME
        │   │   │       └── IDENTIFIER("age")
        │   │   └── LITERAL_NUMBER(25)
        │   └── COMPARISON("=")
        │       ├── COLUMN_REF
        │       │   └── COLUMN_NAME
        │       │       └── IDENTIFIER("city")
        │       └── LITERAL_STRING("Jakarta")
        └── COMPARISON("=")
            ├── COLUMN_REF
            │   └── COLUMN_NAME
            │       └── IDENTIFIER("status")
            └── LITERAL_STRING("admin")
```

**Key Points:**

- ✅ FILTER sebagai container WHERE dengan source
- ✅ OPERATOR dapat nested untuk complex logic
- ✅ Precedence jelas dari tree structure

---

### Example 6: JOIN with WHERE

**SQL:**

```sql
SELECT users.name, profiles.bio
FROM users
JOIN profiles ON users.id = profiles.user_id
WHERE users.age > 25
```

**New Detailed Output:**

```
PROJECT
├── COLUMN_REF
│   ├── COLUMN_NAME
│   │   └── IDENTIFIER("name")     # Child 0: column
│   └── TABLE_NAME
│       └── IDENTIFIER("users")    # Child 1: table
├── COLUMN_REF
│   ├── COLUMN_NAME
│   │   └── IDENTIFIER("bio")      # Child 0: column
│   └── TABLE_NAME
│       └── IDENTIFIER("profiles") # Child 1: table
└── FILTER
    ├── JOIN("INNER")
    │   ├── RELATION("users")
    │   ├── RELATION("profiles")
    │   └── COMPARISON("=")
    │       ├── COLUMN_REF
    │       │   ├── COLUMN_NAME
    │       │   │   └── IDENTIFIER("id")
    │       │   └── TABLE_NAME
    │       │       └── IDENTIFIER("users")
    │       └── COLUMN_REF
    │           ├── COLUMN_NAME
    │           │   └── IDENTIFIER("user_id")
    │           └── TABLE_NAME
    │               └── IDENTIFIER("profiles")
    └── COMPARISON(">")
        ├── COLUMN_REF
        │   ├── COLUMN_NAME
        │   │   └── IDENTIFIER("age")
        │   └── TABLE_NAME
        │       └── IDENTIFIER("users")
        └── LITERAL_NUMBER(25)
```

**Key Points:**

- ✅ Qualified columns (table.column) clearly represented
- ✅ Join condition sebagai detailed tree
- ✅ FILTER untuk WHERE clause

---

### Example 7: BETWEEN and IS NULL

**SQL:**

```sql
SELECT * FROM products
WHERE price BETWEEN 100 AND 500
  AND description IS NOT NULL
```

**New Detailed Output:**

```
PROJECT("*")
└── FILTER
    ├── RELATION("products")
    └── OPERATOR("AND")
        ├── BETWEEN_EXPR  # Use NOT_BETWEEN_EXPR for NOT BETWEEN
        │   ├── COLUMN_REF
        │   │   └── COLUMN_NAME
        │   │       └── IDENTIFIER("price")
        │   ├── LITERAL_NUMBER(100)
        │   └── LITERAL_NUMBER(500)
        └── IS_NOT_NULL_EXPR  # Separate node type
            └── COLUMN_REF
                └── COLUMN_NAME
                    └── IDENTIFIER("description")
```

**Key Points:**

- ✅ `FILTER` sebagai container (no value)
- ✅ `OPERATOR("AND")` untuk combine multiple conditions
- ✅ Specialized expression nodes (BETWEEN_EXPR, IS_NULL_EXPR)

---

### Example 8: UPDATE Statement

**SQL:**

```sql
UPDATE employees
SET salary = salary * 1.1, bonus = 1000
WHERE department = 'IT'
```

**New Detailed Output:**

```
UPDATE
├── ASSIGNMENT
│   ├── COLUMN_REF
│   │   └── COLUMN_NAME
│   │       └── IDENTIFIER("salary")
│   └── ARITH_EXPR("*")
│       ├── COLUMN_REF
│       │   └── COLUMN_NAME
│       │       └── IDENTIFIER("salary")
│       └── LITERAL_NUMBER(1.1)
├── ASSIGNMENT
│   ├── COLUMN_REF
│   │   └── COLUMN_NAME
│   │       └── IDENTIFIER("bonus")
│   └── LITERAL_NUMBER(1000)
└── FILTER  ← FILTER untuk WHERE dengan source
    ├── RELATION("employees")
    └── COMPARISON("=")
        ├── COLUMN_REF
        │   └── COLUMN_NAME
        │       └── IDENTIFIER("department")
        └── LITERAL_STRING("IT")
```

**Key Points:**

- ✅ `ASSIGNMENT` nodes untuk SET clause
- ✅ `FILTER` untuk WHERE clause dengan source
- ✅ Arithmetic expressions dalam SET

---

## Complete Query Tree Examples

### Example A: Simple SELECT with JOIN

**SQL:**

```sql
SELECT u.name, p.city
FROM users AS u
INNER JOIN profiles AS p ON u.id = p.user_id
WHERE u.age > 18
ORDER BY u.name ASC
LIMIT 10
```

**Complete Query Tree:**

```
PROJECT
├── COLUMN_REF
│   ├── COLUMN_NAME
│   │   └── IDENTIFIER("name")    # Child 0: column name
│   └── TABLE_NAME
│       └── IDENTIFIER("u")        # Child 1: table alias
├── COLUMN_REF
│   ├── COLUMN_NAME
│   │   └── IDENTIFIER("city")     # Child 0: column name
│   └── TABLE_NAME
│       └── IDENTIFIER("p")        # Child 1: table alias
└── LIMIT(10)
    └── SORT
        ├── ORDER_ITEM("ASC")  # value = direction
        │   └── COLUMN_REF
        │       ├── COLUMN_NAME
        │       │   └── IDENTIFIER("name")
        │       └── TABLE_NAME
        │           └── IDENTIFIER("u")
        └── FILTER
            ├── JOIN("INNER")  # value = join type
            │   ├── ALIAS("u")
            │   │   └── RELATION("users")
            │   ├── ALIAS("p")
            │   │   └── RELATION("profiles")
            │   └── COMPARISON("=")
            │       ├── COLUMN_REF
            │       │   ├── COLUMN_NAME
            │       │   │   └── IDENTIFIER("id")
            │       │   └── TABLE_NAME
            │       │       └── IDENTIFIER("u")
            │       └── COLUMN_REF
            │           ├── COLUMN_NAME
            │           │   └── IDENTIFIER("user_id")
            │           └── TABLE_NAME
            │               └── IDENTIFIER("p")
            └── COMPARISON(">")
                ├── COLUMN_REF
                │   ├── COLUMN_NAME
                │   │   └── IDENTIFIER("age")
                │   └── TABLE_NAME
                │       └── IDENTIFIER("u")
                └── LITERAL_NUMBER(18)
```

### Example B: Complex WHERE with Multiple Conditions

**SQL:**

```sql
SELECT *
FROM products
WHERE (category IN ('Electronics', 'Books') AND price < 1000)
   OR (stock > 0 AND discount IS NOT NULL)
```

**Complete Query Tree:**

```
PROJECT("*")
└── FILTER
    ├── RELATION("products")
    └── OPERATOR("OR")
        ├── OPERATOR("AND")
        │   ├── IN_EXPR  # Use NOT_IN_EXPR for negated version
        │   │   ├── COLUMN_REF
        │   │   │   └── COLUMN_NAME
        │   │   │       └── IDENTIFIER("category")
        │   │   └── LIST
        │   │       ├── LITERAL_STRING("Electronics")
        │   │       └── LITERAL_STRING("Books")
        │   └── COMPARISON("<")
        │       ├── COLUMN_REF
        │       │   └── COLUMN_NAME
        │       │       └── IDENTIFIER("price")
        │       └── LITERAL_NUMBER(1000)
        └── OPERATOR("AND")
            ├── COMPARISON(">")
            │   ├── COLUMN_REF
            │   │   └── COLUMN_NAME
            │   │       └── IDENTIFIER("stock")
            │   └── LITERAL_NUMBER(0)
            └── IS_NOT_NULL_EXPR  # Separate node type for IS NOT NULL
                └── COLUMN_REF
                    └── COLUMN_NAME
                        └── IDENTIFIER("discount")
```

### Example C: INSERT Statement

**SQL:**

```sql
INSERT INTO employees (name, salary, department)
VALUES ('John Doe', 75000, 'Engineering')
```

**Complete Query Tree:**

```
INSERT_QUERY
├── RELATION("employees")
├── COLUMN_LIST
│   ├── IDENTIFIER("name")
│   ├── IDENTIFIER("salary")
│   └── IDENTIFIER("department")
└── VALUES_CLAUSE
    ├── LITERAL_STRING("John Doe")
    ├── LITERAL_NUMBER(75000)
    └── LITERAL_STRING("Engineering")
```

### Example D: Transaction with Multiple Operations

**SQL:**

```sql
BEGIN TRANSACTION;
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;
COMMIT;
```

**Complete Query Tree:**

```
BEGIN_TRANSACTION
├── UPDATE_QUERY
│   ├── RELATION("accounts")
│   ├── ASSIGNMENT
│   │   ├── COLUMN_REF
│   │   │   └── COLUMN_NAME
│   │   │       └── IDENTIFIER("balance")
│   │   └── ARITH_EXPR("-")  # value = operator
│   │       ├── COLUMN_REF
│   │       │   └── COLUMN_NAME
│   │       │       └── IDENTIFIER("balance")
│   │       └── LITERAL_NUMBER(100)
│   └── FILTER
│       ├── RELATION("accounts")
│       └── COMPARISON("=")
│           ├── COLUMN_REF
│           │   └── COLUMN_NAME
│           │       └── IDENTIFIER("id")
│           └── LITERAL_NUMBER(1)
├── UPDATE_QUERY
│   ├── RELATION("accounts")
│   ├── ASSIGNMENT
│   │   ├── COLUMN_REF
│   │   │   └── COLUMN_NAME
│   │   │       └── IDENTIFIER("balance")
│   │   └── ARITH_EXPR("+")
│   │       ├── COLUMN_REF
│   │       │   └── COLUMN_NAME
│   │       │       └── IDENTIFIER("balance")
│   │       └── LITERAL_NUMBER(100)
│   └── FILTER
│       ├── RELATION("accounts")
│       └── COMPARISON("=")
│           ├── COLUMN_REF
│           │   └── COLUMN_NAME
│           │       └── IDENTIFIER("id")
│           └── LITERAL_NUMBER(2)
└── COMMIT
```

### Example E: CREATE TABLE with Constraints

**SQL:**

```sql
CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    user_id INTEGER FOREIGN KEY REFERENCES users(id),
    total FLOAT,
    status VARCHAR(50)
)
```

**Complete Query Tree:**

```
CREATE_TABLE
├── IDENTIFIER("orders")
└── COLUMN_DEF_LIST
    ├── COLUMN_DEF
    │   ├── IDENTIFIER("id")
    │   ├── DATA_TYPE("INTEGER")  # value = type name
    │   └── PRIMARY_KEY
    ├── COLUMN_DEF
    │   ├── IDENTIFIER("user_id")
    │   ├── DATA_TYPE("INTEGER")
    │   └── FOREIGN_KEY
    │       ├── REFERENCES("users")  # value = table name
    │       └── IDENTIFIER("id")
    ├── COLUMN_DEF
    │   ├── IDENTIFIER("total")
    │   └── DATA_TYPE("FLOAT")
    └── COLUMN_DEF
        ├── IDENTIFIER("status")
        └── DATA_TYPE("VARCHAR(50)")  # value = type with size
```

---

**Node Categories:**

1. **Atomic Nodes**: IDENTIFIER, LITERAL\_\*
2. **Expression Nodes**: COLUMN_REF, COMPARISON, ARITH_EXPR, IN_EXPR, EXISTS_EXPR, BETWEEN_EXPR, IS_NULL_EXPR, LIKE_EXPR
3. **Logical Nodes**: OPERATOR
4. **Structure Nodes**: PROJECT, FILTER, SORT, RELATION, JOIN
5. **DML Nodes**: UPDATE, INSERT, DELETE, ASSIGNMENT
6. **DDL Nodes**: CREATE_TABLE, DROP_TABLE, COLUMN_DEF
7. **Transaction Nodes**: BEGIN_TRANSACTION, COMMIT

**Supported Features:**

- ✅ SELECT with \* or specific columns
- ✅ FROM with cartesian product (multiple tables)
- ✅ JOIN ON and NATURAL JOIN (INNER only)
- ✅ WHERE with comparisons (=, <>, >, >=, <, <=)
- ✅ WHERE with IN (list or subquery) and EXISTS (subquery)
- ✅ WHERE with BETWEEN and IS NULL/NOT NULL
- ✅ ORDER BY (1 attribute, ASC/DESC)
- ✅ LIMIT (bonus)
- ✅ UPDATE SET WHERE (1 condition)
- ✅ DELETE WHERE (1 condition)
- ✅ INSERT INTO VALUES (1 record)
- ✅ CREATE TABLE with PK/FK
- ✅ DROP TABLE with CASCADE/RESTRICT
- ✅ BEGIN TRANSACTION and COMMIT
- ✅ AS (alias for tables)

**Not Implemented (excluded from grammar):**

- ❌ DISTINCT
- ❌ GROUP BY / HAVING
- ❌ Aggregate functions (SUM, COUNT, AVG, etc.)
- ❌ LEFT/RIGHT/FULL OUTER JOIN
- ❌ UNION / INTERSECT / EXCEPT
- ❌ CASE WHEN
- ❌ Window functions
- ❌ Multiple attributes in ORDER BY

**Implementation Details:**

- Formal BNF grammar: See Level 1-3 sections above
- Node definitions: See Category 1-5 sections above
- Examples: See Example 1-8 transformations above
