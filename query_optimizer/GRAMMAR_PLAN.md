# Grammar Plan: Detailed Parser

---

## Overview

Dokumen ini berisi rencana untuk merombak parser query optimizer agar memiliki struktur grammar yang lebih formal dan detail, mirip dengan parser compiler. Tujuan utama adalah membuat representasi query tree dengan unit atomik yang jelas (identifier, literal, operator) sehingga memudahkan optimasi dan manipulasi tree.

**Key Changes:**

- ✨ Struktur atomik yang jelas untuk setiap komponen (tidak lagi string-based)
- ✨ Expression tree yang terpisah untuk kondisi dan aritmatika
- ✨ Type-safe nodes untuk identifier, literal, dan operator
- ✨ Lebih mudah untuk implementasi optimization rules

---

## Motivasi

### Masalah Saat Ini

**1. String-based Conditions**

```python
# Current approach
FILTER("WHERE age > 25")
FILTER("WHERE status = 'active'")
```

**Masalah:**

- Parser harus parse ulang string saat optimization
- Sulit extract informasi (column names, operators, values)
- Tidak type-safe
- Sulit untuk cost estimation yang akurat

**2. Operator Tidak Eksplisit**

```python
# Operator hanya string dalam value
FILTER("WHERE age > 25")  # ">" tidak punya representasi node
```

**Masalah:**

- Tidak bisa query "berikan semua comparison dengan operator '>'"
- Tidak bisa dengan mudah cek selectivity berdasarkan operator
- Sulit implement index-aware optimization

**3. Equivalency Rule Sulit Dibuat**

```python
# Column name pada project perlu di parsing lagi
"users, admin, table1"
```

### Solusi yang Diusulkan

**Representasi Atomik & Terstruktur:**

```python
FILTER
└── COMPARISON(">")
    ├── COLUMN_REF
    │   ├── table: "users"
    │   └── column: "age"
    └── LITERAL_NUMBER(25)
```

**Keuntungan:**

- ✅ Setiap komponen punya node sendiri
- ✅ Type-safe dan mudah divalidasi
- ✅ Extract informasi tanpa string parsing
- ✅ Optimization rules lebih powerful

---

## 📐 Grammar Specification (Formal BNF)

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
<keyword>        ::= 'SELECT' | 'FROM' | 'WHERE' | 'JOIN' | 'ON' | 'NATURAL'
                   | 'ORDER' | 'BY' | 'ASC' | 'DESC' | 'LIMIT'
                   | 'UPDATE' | 'SET' | 'INSERT' | 'INTO' | 'VALUES' | 'DELETE'
                   | 'BEGIN' | 'TRANSACTION' | 'COMMIT'
                   | 'IN' | 'EXISTS' | 'AS'
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
<select_stmt>    ::= 'SELECT' [<distinct>] <select_list>
                     'FROM' <table_expr>
                     [<where_clause>]
                     [<order_by_clause>]
                     [<limit_clause>]

<distinct>       ::= 'DISTINCT'

<select_list>    ::= '*'
                   | <select_item> (',' <select_item>)*

<select_item>    ::= <value_expr> [<alias>]

<alias>          ::= ['AS'] <identifier>

# FROM Clause
<table_expr>     ::= <table_ref>
                   | <table_expr> <join_clause>

<table_ref>      ::= <identifier> [<alias>]

<join_clause>    ::= [<join_type>] 'JOIN' <table_ref> [<join_condition>]

<join_type>      ::= 'INNER' | 'LEFT' | 'RIGHT' | 'FULL' | 'NATURAL'

<join_condition> ::= 'ON' <condition>

# WHERE Clause
<where_clause>   ::= 'WHERE' <condition>

# ORDER BY Clause
<order_by_clause> ::= 'ORDER' 'BY' <order_item> (',' <order_item>)*

<order_item>     ::= <column_ref> [<order_dir>]

<order_dir>      ::= 'ASC' | 'DESC'

# LIMIT Clause
<limit_clause>   ::= 'LIMIT' <number>

# UPDATE Statement
<update_stmt>    ::= 'UPDATE' <table_ref>
                     'SET' <set_list>
                     [<where_clause>]

<set_list>       ::= <assignment> (',' <assignment>)*

<assignment>     ::= <column_ref> '=' <value_expr>

# INSERT Statement
<insert_stmt>    ::= 'INSERT' 'INTO' <table_ref>
                     '(' <column_list> ')'
                     'VALUES' '(' <value_list> ')'

<column_list>    ::= <identifier> (',' <identifier>)*

# DELETE Statement
<delete_stmt>    ::= 'DELETE' 'FROM' <table_ref>
                     [<where_clause>]

# Transaction Statement
<transaction_stmt> ::= 'BEGIN' 'TRANSACTION' <stmt>* 'COMMIT'

<stmt>           ::= <select_stmt> | <update_stmt> | <insert_stmt> | <delete_stmt>
```

---

## 🌳 Query Tree Node Types (Detailed)

### Design Philosophy: Hybrid Approach

**Keep existing high-level nodes:**

- `PROJECT` - untuk SELECT clause (tetap)
- `FILTER` - untuk WHERE conditions (tetap)
- `SORT` - untuk ORDER BY (tetap)
- `RELATION` - untuk table reference (tetap)
- `JOIN` - untuk JOIN operations (tetap)

**Add atomic detail nodes as children:**

- Detail atomik (IDENTIFIER, LITERAL\_\*, COMPARISON, dll) menjadi children dari node utama
- Backward compatible - existing code tetap jalan
- Gradual migration - bisa adopt detail nodes secara bertahap

---

### Category 1: Atomic Leaf Nodes

**Identifier Nodes**

```python
class IdentifierNode(QueryTree):
    """Simple identifier: table name, column name"""
    type: str = "IDENTIFIER"
    name: str

    # Example: IDENTIFIER(name="users")

class QualifiedIdentifierNode(QueryTree):
    """Qualified identifier: table.column"""
    type: str = "QUALIFIED_ID"
    table: str
    column: str

    # Example: QUALIFIED_ID(table="users", column="age")
```

**Literal Nodes**

```python
class LiteralNumberNode(QueryTree):
    """Numeric literal"""
    type: str = "LITERAL_NUMBER"
    value: float

    # Example: LITERAL_NUMBER(value=42.5)

class LiteralStringNode(QueryTree):
    """String literal"""
    type: str = "LITERAL_STRING"
    value: str

    # Example: LITERAL_STRING(value="John")

class LiteralBooleanNode(QueryTree):
    """Boolean literal"""
    type: str = "LITERAL_BOOLEAN"
    value: bool

    # Example: LITERAL_BOOLEAN(value=True)

class LiteralNullNode(QueryTree):
    """NULL literal"""
    type: str = "LITERAL_NULL"

    # Example: LITERAL_NULL()
```

### Category 2: Expression Nodes

**Column Reference**

```python
class ColumnRefNode(QueryTree):
    """Reference to a column"""
    type: str = "COLUMN_REF"
    # Children: 1 child (IDENTIFIER or QUALIFIED_ID)

    # Example:
    # COLUMN_REF
    # └── IDENTIFIER("age")
    #
    # Or:
    # COLUMN_REF
    # └── QUALIFIED_ID(table="users", column="age")
```

**Arithmetic Expression**

```python
class ArithExprNode(QueryTree):
    """Arithmetic operation"""
    type: str = "ARITH_EXPR"
    operator: str  # "+", "-", "*", "/", "%"
    # Children: 2 (left, right)

    # Example: salary * 1.1
    # ARITH_EXPR(operator="*")
    # ├── COLUMN_REF
    # │   └── IDENTIFIER("salary")
    # └── LITERAL_NUMBER(1.1)
```

**Comparison Expression**

```python
class ComparisonNode(QueryTree):
    """Comparison operation (atomic condition)"""
    type: str = "COMPARISON"
    operator: str  # "=", "<>", ">", ">=", "<", "<=", "LIKE"
    # Children: 2 (left, right)

    # Example: age > 25
    # COMPARISON(operator=">")
    # ├── COLUMN_REF
    # │   └── IDENTIFIER("age")
    # └── LITERAL_NUMBER(25)
```

**IN Expression**

```python
class InExprNode(QueryTree):
    """IN clause expression"""
    type: str = "IN_EXPR"
    negated: bool = False  # True for NOT IN
    # Children: 2 (column_ref, value_list or subquery)

    # Example: id IN (1, 2, 3)
    # IN_EXPR(negated=False)
    # ├── COLUMN_REF
    # │   └── IDENTIFIER("id")
    # └── VALUE_LIST
    #     ├── LITERAL_NUMBER(1)
    #     ├── LITERAL_NUMBER(2)
    #     └── LITERAL_NUMBER(3)
```

**EXISTS Expression**

```python
class ExistsExprNode(QueryTree):
    """EXISTS clause expression"""
    type: str = "EXISTS_EXPR"
    negated: bool = False  # True for NOT EXISTS
    # Children: 1 (subquery)

    # Example: EXISTS (SELECT ...)
    # EXISTS_EXPR(negated=False)
    # └── SELECT_QUERY
    #     └── ...
```

**BETWEEN Expression**

```python
class BetweenExprNode(QueryTree):
    """BETWEEN clause expression"""
    type: str = "BETWEEN_EXPR"
    negated: bool = False  # True for NOT BETWEEN
    # Children: 3 (value, lower_bound, upper_bound)

    # Example: age BETWEEN 20 AND 30
    # BETWEEN_EXPR(negated=False)
    # ├── COLUMN_REF
    # │   └── IDENTIFIER("age")
    # ├── LITERAL_NUMBER(20)
    # └── LITERAL_NUMBER(30)
```

**IS NULL Expression**

```python
class IsNullExprNode(QueryTree):
    """IS NULL / IS NOT NULL expression"""
    type: str = "IS_NULL_EXPR"
    negated: bool = False  # True for IS NOT NULL
    # Children: 1 (column_ref)

    # Example: email IS NOT NULL
    # IS_NULL_EXPR(negated=True)
    # └── COLUMN_REF
    #     └── IDENTIFIER("email")
```

### Category 3: Logical Nodes

**Binary Logical Operators**

```python
class LogicalAndNode(QueryTree):
    """Logical AND operation"""
    type: str = "LOGICAL_AND"
    # Children: 2+ (left, right, ...)

    # Example: age > 25 AND status = 'active'
    # LOGICAL_AND
    # ├── COMPARISON(">")
    # │   ├── COLUMN_REF("age")
    # │   └── LITERAL_NUMBER(25)
    # └── COMPARISON("=")
    #     ├── COLUMN_REF("status")
    #     └── LITERAL_STRING("active")

class LogicalOrNode(QueryTree):
    """Logical OR operation"""
    type: str = "LOGICAL_OR"
    # Children: 2+ (left, right, ...)
```

**Unary Logical Operator**

```python
class LogicalNotNode(QueryTree):
    """Logical NOT operation"""
    type: str = "LOGICAL_NOT"
    # Children: 1 (condition)

    # Example: NOT (age > 25)
    # LOGICAL_NOT
    # └── COMPARISON(">")
    #     ├── COLUMN_REF("age")
    #     └── LITERAL_NUMBER(25)
```

### Category 4: Main Query Structure Nodes (EXISTING - KEEP AS IS)

**PROJECT Node (for SELECT clause)**

```python
class ProjectNode(QueryTree):
    """PROJECT operator - represents SELECT clause"""
    type: str = "PROJECT"

    # BACKWARD COMPATIBLE:
    # Old way: value = "id, name" (string)
    # New way: children = [COLUMN_REF, COLUMN_REF, ...] (detailed)

    # Example (Old - still supported):
    # PROJECT("id, name")
    # └── FILTER(...)

    # Example (New - with detail):
    # PROJECT
    # ├── COLUMN_REF
    # │   └── IDENTIFIER("id")
    # ├── COLUMN_REF
    # │   └── IDENTIFIER("name")
    # └── [source_tree]  # Last child is always the source
```

**FILTER Node (for WHERE clause)**

```python
class FilterNode(QueryTree):
    """FILTER operator - represents WHERE conditions"""
    type: str = "FILTER"

    # BACKWARD COMPATIBLE:
    # Old way: value = "WHERE age > 25" (string)
    # New way: first child = COMPARISON/LOGICAL_AND/etc (detailed)

    # Example (Old - still supported):
    # FILTER("WHERE age > 25")
    # └── RELATION("users")

    # Example (New - with detail):
    # FILTER
    # ├── COMPARISON(">")
    # │   ├── COLUMN_REF
    # │   │   └── IDENTIFIER("age")
    # │   └── LITERAL_NUMBER(25)
    # └── RELATION("users")  # Last child is always the source
```

**SORT Node (for ORDER BY clause)**

```python
class SortNode(QueryTree):
    """SORT operator - represents ORDER BY clause"""
    type: str = "SORT"

    # BACKWARD COMPATIBLE:
    # Old way: value = "age, name DESC" (string)
    # New way: children = [ORDER_ITEM, ORDER_ITEM, ...] (detailed)

    # Example (Old - still supported):
    # SORT("age, name DESC")
    # └── RELATION("users")

    # Example (New - with detail):
    # SORT
    # ├── ORDER_ITEM(direction="ASC")
    # │   └── COLUMN_REF
    # │       └── IDENTIFIER("age")
    # ├── ORDER_ITEM(direction="DESC")
    # │   └── COLUMN_REF
    # │       └── IDENTIFIER("name")
    # └── RELATION("users")  # Last child is always the source
```

**RELATION Node (table reference)**

```python
class RelationNode(QueryTree):
    """RELATION operator - table reference"""
    type: str = "RELATION"

    # BACKWARD COMPATIBLE:
    # Old way: value = "users" (string)
    # New way: value = "users" or child = IDENTIFIER("users")

    # Example (Old - still supported):
    # RELATION("users")

    # Example (New - with detail, optional):
    # RELATION
    # └── IDENTIFIER("users")

    # Note: For RELATION, string value is already atomic enough
    # Detailed child is optional for consistency
```

**JOIN Node (join operations)**

```python
class JoinNode(QueryTree):
    """JOIN operator - join operations"""
    type: str = "JOIN"

    # BACKWARD COMPATIBLE:
    # Old way: value = "ON users.id = profiles.user_id" (string)
    # New way: third child = COMPARISON (detailed condition)

    # Example (Old - still supported):
    # JOIN("ON users.id = profiles.user_id")
    # ├── RELATION("users")
    # └── RELATION("profiles")

    # Example (New - with detail):
    # JOIN("INNER")  # or "LEFT", "RIGHT", "NATURAL"
    # ├── RELATION("users")
    # ├── RELATION("profiles")
    # └── COMPARISON("=")  # Join condition as detailed tree
    #     ├── COLUMN_REF
    #     │   └── QUALIFIED_ID(table="users", column="id")
    #     └── COLUMN_REF
    #         └── QUALIFIED_ID(table="profiles", column="user_id")
```

**Helper Nodes for Detailed Structure**

```python
class OrderItemNode(QueryTree):
    """Single ORDER BY item (used inside SORT)"""
    type: str = "ORDER_ITEM"
    direction: str = "ASC"  # "ASC" or "DESC"
    # Children: 1 (column_ref)

    # Example:
    # ORDER_ITEM(direction="DESC")
    # └── COLUMN_REF
    #     └── IDENTIFIER("name")
```

### Category 5: DML Nodes

**Update**

```python
class UpdateQueryNode(QueryTree):
    """UPDATE statement"""
    type: str = "UPDATE_QUERY"
    # Children: [table_ref, set_clause, where_clause?]

class SetClauseNode(QueryTree):
    """SET clause for UPDATE"""
    type: str = "SET_CLAUSE"
    # Children: 1+ (assignments)

class AssignmentNode(QueryTree):
    """Single assignment: column = value"""
    type: str = "ASSIGNMENT"
    # Children: 2 (column_ref, value_expr)
```

**Insert**

```python
class InsertQueryNode(QueryTree):
    """INSERT statement"""
    type: str = "INSERT_QUERY"
    # Children: [table_ref, column_list, values_clause]
```

**Delete**

```python
class DeleteQueryNode(QueryTree):
    """DELETE statement"""
    type: str = "DELETE_QUERY"
    # Children: [table_ref, where_clause?]
```

---

## 📊 Example Transformations

### Example 1: Simple WHERE Clause

**SQL:**

```sql
SELECT name FROM users WHERE age > 25
```

**Current Parser Output (TETAP DIDUKUNG):**

```
PROJECT("name")
└── FILTER("WHERE age > 25")
    └── RELATION("users")
```

**New Detailed Output (HYBRID - Keep main structure):**

```
PROJECT
├── COLUMN_REF
│   └── IDENTIFIER("name")
└── FILTER
    ├── COMPARISON(operator=">")
    │   ├── COLUMN_REF
    │   │   └── IDENTIFIER("age")
    │   └── LITERAL_NUMBER(25)
    └── RELATION("users")
```

**Key Changes:**

- ✅ `PROJECT`, `FILTER`, `RELATION` nodes tetap ada
- ✅ Detail atomik (COLUMN_REF, COMPARISON, dll) menjadi children
- ✅ Struktur utama tidak berubah drastis
- ✅ Backward compatible dengan existing code

---

### Example 2: Complex AND Conditions

**SQL:**

```sql
SELECT * FROM users WHERE age > 25 AND status = 'active' AND city = 'Jakarta'
```

**Current Parser Output (TETAP DIDUKUNG):**

```
PROJECT("*")
└── OPERATOR_S("AND")
    ├── RELATION("users")
    ├── FILTER("WHERE age > 25")
    ├── FILTER("WHERE status = 'active'")
    └── FILTER("WHERE city = 'Jakarta'")
```

**New Detailed Output (Option A - Keep OPERATOR_S, add detail):**

```
PROJECT("*")  # Keep as is
└── OPERATOR_S("AND")  # Keep structure
    ├── RELATION("users")  # Keep as is
    ├── FILTER  # FILTER node tetap, tapi children-nya detailed
    │   └── COMPARISON(">")
    │       ├── COLUMN_REF
    │       │   └── IDENTIFIER("age")
    │       └── LITERAL_NUMBER(25)
    ├── FILTER
    │   └── COMPARISON("=")
    │       ├── COLUMN_REF
    │       │   └── IDENTIFIER("status")
    │       └── LITERAL_STRING("active")
    └── FILTER
        └── COMPARISON("=")
            ├── COLUMN_REF
            │   └── IDENTIFIER("city")
            └── LITERAL_STRING("Jakarta")
```

**New Detailed Output (Option B - Use LOGICAL_AND under FILTER):**

```
PROJECT("*")
└── FILTER  # Single FILTER node
    ├── LOGICAL_AND  # Logical operator as first child
    │   ├── COMPARISON(">")
    │   │   ├── COLUMN_REF
    │   │   │   └── IDENTIFIER("age")
    │   │   └── LITERAL_NUMBER(25)
    │   ├── COMPARISON("=")
    │   │   ├── COLUMN_REF
    │   │   │   └── IDENTIFIER("status")
    │   │   └── LITERAL_STRING("active")
    │   └── COMPARISON("=")
    │       ├── COLUMN_REF
    │       │   └── IDENTIFIER("city")
    │       └── LITERAL_STRING("Jakarta")
    └── RELATION("users")  # Source as last child
```

**Recommended:** Option B - More consistent with single FILTER node containing detailed logical structure

---

### Example 3: IN Clause

**SQL:**

```sql
SELECT name FROM users WHERE id IN (1, 2, 3)
```

**Current Parser Output (TETAP DIDUKUNG):**

```
PROJECT("name")
└── FILTER("IN id")
    ├── RELATION("users")
    └── ARRAY("(1, 2, 3)")
```

**New Detailed Output (Keep main structure):**

```
PROJECT
├── COLUMN_REF
│   └── IDENTIFIER("name")
└── FILTER
    ├── IN_EXPR(negated=False)
    │   ├── COLUMN_REF
    │   │   └── IDENTIFIER("id")
    │   └── VALUE_LIST
    │       ├── LITERAL_NUMBER(1)
    │       ├── LITERAL_NUMBER(2)
    │       └── LITERAL_NUMBER(3)
    └── RELATION("users")
```

**Key Points:**

- ✅ `PROJECT`, `FILTER` tetap sebagai node utama
- ✅ `ARRAY` node diganti dengan `VALUE_LIST` yang lebih deskriptif
- ✅ Detail IN expression menjadi child dari FILTER

---

### Example 4: Arithmetic Expression

**SQL:**

```sql
SELECT salary * 1.1 FROM employees WHERE salary + bonus > 50000
```

**New Detailed Output (Keep PROJECT, FILTER structure):**

```
PROJECT
├── ARITH_EXPR(operator="*")
│   ├── COLUMN_REF
│   │   └── IDENTIFIER("salary")
│   └── LITERAL_NUMBER(1.1)
└── FILTER
    ├── COMPARISON(operator=">")
    │   ├── ARITH_EXPR(operator="+")
    │   │   ├── COLUMN_REF
    │   │   │   └── IDENTIFIER("salary")
    │   │   └── COLUMN_REF
    │   │       └── IDENTIFIER("bonus")
    │   └── LITERAL_NUMBER(50000)
    └── RELATION("employees")
```

**Benefits:**

- ✅ `PROJECT` node tetap untuk SELECT clause
- ✅ `FILTER` node tetap untuk WHERE clause
- ✅ Arithmetic expressions represented as tree (not string)
- ✅ Easy to analyze and optimize arithmetic operations

---

### Example 5: Complex Nested Logic with OR

**SQL:**

```sql
SELECT * FROM users WHERE (age > 25 AND city = 'Jakarta') OR status = 'admin'
```

**New Detailed Output (Keep PROJECT, FILTER structure):**

```
PROJECT("*")  # Can keep string for SELECT *
└── FILTER
    ├── LOGICAL_OR
    │   ├── LOGICAL_AND
    │   │   ├── COMPARISON(">")
    │   │   │   ├── COLUMN_REF
    │   │   │   │   └── IDENTIFIER("age")
    │   │   │   └── LITERAL_NUMBER(25)
    │   │   └── COMPARISON("=")
    │   │       ├── COLUMN_REF
    │   │       │   └── IDENTIFIER("city")
    │   │       └── LITERAL_STRING("Jakarta")
    │   └── COMPARISON("=")
    │       ├── COLUMN_REF
    │       │   └── IDENTIFIER("status")
    │       └── LITERAL_STRING("admin")
    └── RELATION("users")
```

**Benefits:**

- ✅ `PROJECT`, `FILTER`, `RELATION` tetap sebagai backbone
- ✅ Logical operators (AND/OR) represented as tree nodes
- ✅ Precedence jelas dari tree structure
- ✅ Easy to split/reorder conditions for optimization

---

### Example 6: JOIN with Qualified Columns

**SQL:**

```sql
SELECT users.name, profiles.bio
FROM users
JOIN profiles ON users.id = profiles.user_id
WHERE users.age > 25
```

**New Detailed Output (Keep PROJECT, FILTER, JOIN structure):**

```
PROJECT
├── COLUMN_REF
│   └── QUALIFIED_ID(table="users", column="name")
├── COLUMN_REF
│   └── QUALIFIED_ID(table="profiles", column="bio")
└── FILTER
    ├── COMPARISON(">")
    │   ├── COLUMN_REF
    │   │   └── QUALIFIED_ID(table="users", column="age")
    │   └── LITERAL_NUMBER(25)
    └── JOIN("INNER")  # JOIN node tetap ada
        ├── RELATION("users")
        ├── RELATION("profiles")
        └── COMPARISON("=")  # Join condition as detailed tree
            ├── COLUMN_REF
            │   └── QUALIFIED_ID(table="users", column="id")
            └── COLUMN_REF
                └── QUALIFIED_ID(table="profiles", column="user_id")
```

**Benefits:**

- ✅ `PROJECT`, `FILTER`, `JOIN`, `RELATION` tetap sebagai main nodes
- ✅ Qualified columns (table.column) clearly represented
- ✅ Join condition sebagai detailed tree (bukan string)
- ✅ Easy to analyze which columns come from which table

---

### Example 7: BETWEEN and IS NULL

**SQL:**

```sql
SELECT * FROM products
WHERE price BETWEEN 100 AND 500
  AND description IS NOT NULL
```

**New Detailed Output (Keep PROJECT, FILTER structure):**

```
PROJECT("*")
└── FILTER
    ├── LOGICAL_AND
    │   ├── BETWEEN_EXPR(negated=False)
    │   │   ├── COLUMN_REF
    │   │   │   └── IDENTIFIER("price")
    │   │   ├── LITERAL_NUMBER(100)
    │   │   └── LITERAL_NUMBER(500)
    │   └── IS_NULL_EXPR(negated=True)
    │       └── COLUMN_REF
    │           └── IDENTIFIER("description")
    └── RELATION("products")
```

**Benefits:**

- ✅ `PROJECT`, `FILTER` structure maintained
- ✅ BETWEEN and IS NULL as specialized expression nodes
- ✅ Clear representation of complex conditions

---

### Example 8: UPDATE with Expression

**SQL:**

```sql
UPDATE employees
SET salary = salary * 1.1, bonus = 1000
WHERE department = 'IT'
```

**Current Parser Output (TETAP DIDUKUNG):**

```
UPDATE("salary = salary * 1.1, bonus = 1000")
└── FILTER("WHERE department = 'IT'")
    └── RELATION("employees")
```

**New Detailed Output (Keep UPDATE, FILTER structure):**

```
UPDATE  # UPDATE node tetap
├── ASSIGNMENT
│   ├── COLUMN_REF
│   │   └── IDENTIFIER("salary")
│   └── ARITH_EXPR(operator="*")
│   │   ├── COLUMN_REF
│   │   │   └── IDENTIFIER("salary")
│   │   └── LITERAL_NUMBER(1.1)
├── ASSIGNMENT
│   ├── COLUMN_REF
│   │   └── IDENTIFIER("bonus")
│   └── LITERAL_NUMBER(1000)
└── FILTER  # FILTER node tetap untuk WHERE
    ├── COMPARISON("=")
    │   ├── COLUMN_REF
    │   │   └── IDENTIFIER("department")
    │   └── LITERAL_STRING("IT")
    └── RELATION("employees")
```

**Benefits:**

- ✅ `UPDATE`, `FILTER`, `RELATION` structure maintained
- ✅ Assignments as detailed nodes (not string)
- ✅ Arithmetic expressions in SET clause clearly represented

---

## 💡 Advantages of Detailed Grammar

### 1. Precise Tree Manipulation

**Extract Column Dependencies:**

```python
def get_columns_used(node: QueryTree) -> set[str]:
    """Extract all column references from any node"""
    columns = set()
    for col_ref in node.find_nodes_by_type("COLUMN_REF"):
        if col_ref.children[0].type == "QUALIFIED_ID":
            columns.add(f"{col_ref.children[0].table}.{col_ref.children[0].column}")
        else:
            columns.add(col_ref.children[0].name)
    return columns

# Usage
where_clause = query.find_node("WHERE_CLAUSE")
columns = get_columns_used(where_clause)
print(f"Columns used in WHERE: {columns}")
# Output: {'age', 'status', 'city'}
```

**Check Filter Pushdown Eligibility:**

```python
def can_push_down_to_table(condition: QueryTree, table_name: str) -> bool:
    """Check if condition only uses columns from specific table"""
    column_refs = condition.find_nodes_by_type("COLUMN_REF")

    for col_ref in column_refs:
        identifier = col_ref.children[0]
        if identifier.type == "QUALIFIED_ID":
            if identifier.table != table_name:
                return False
        # Unqualified columns might be ambiguous - need context

    return True

# Usage
join_node = query.find_node("JOIN")
where_condition = query.find_node("WHERE_CLAUSE").children[0]

if can_push_down_to_table(where_condition, "users"):
    # Push condition down to users table
    pass
```

### 2. Accurate Cost Estimation

**Selectivity Based on Operator:**

```python
def estimate_selectivity(comparison: ComparisonNode) -> float:
    """Estimate selectivity based on comparison operator"""
    operator = comparison.operator

    # Check if column is indexed
    left = comparison.children[0]
    if left.type == "COLUMN_REF":
        column_name = left.children[0].name  # or table.column
        is_indexed = check_if_indexed(column_name)

        if operator == "=":
            return 0.1 if is_indexed else 0.2
        elif operator in [">", "<", ">=", "<="]:
            return 0.3 if is_indexed else 0.5
        elif operator in ["<>", "!="]:
            return 0.9
        elif operator == "LIKE":
            # Check if starts with wildcard
            right = comparison.children[1]
            if right.type == "LITERAL_STRING":
                if right.value.startswith("%"):
                    return 0.7  # Poor selectivity
                else:
                    return 0.3  # Better selectivity

    return 0.5  # Default

# Usage
comparison = where_clause.find_node("COMPARISON")
selectivity = estimate_selectivity(comparison)
estimated_rows = total_rows * selectivity
```

**Cost of Arithmetic Expression:**

```python
def estimate_arith_cost(arith_expr: ArithExprNode) -> int:
    """Estimate CPU cost of arithmetic expression"""
    base_cost = 1

    # Nested expressions cost more
    for child in arith_expr.children:
        if child.type == "ARITH_EXPR":
            base_cost += estimate_arith_cost(child)
        elif child.type == "COLUMN_REF":
            base_cost += 1  # Column lookup
        # Literals are free

    return base_cost
```

### 3. Advanced Optimization Rules

**Rule: Predicate Pushdown Across Join**

```python
def pushdown_filters_through_join(query: SelectQueryNode) -> SelectQueryNode:
    """Push WHERE conditions down to appropriate table before JOIN"""
    where_clause = query.find_node("WHERE_CLAUSE")
    if not where_clause:
        return query

    from_clause = query.find_node("FROM_CLAUSE")
    join_node = from_clause.find_node("JOIN")
    if not join_node:
        return query

    condition = where_clause.children[0]

    # Split AND conditions
    if condition.type == "LOGICAL_AND":
        conditions = condition.children
    else:
        conditions = [condition]

    # Categorize conditions by table
    left_table_name = join_node.children[0].table_name
    right_table_name = join_node.children[1].table_name

    left_conditions = []
    right_conditions = []
    join_conditions = []

    for cond in conditions:
        columns = get_columns_used(cond)

        # Check which table(s) are used
        uses_left = any(col.startswith(left_table_name) for col in columns)
        uses_right = any(col.startswith(right_table_name) for col in columns)

        if uses_left and not uses_right:
            left_conditions.append(cond)
        elif uses_right and not uses_left:
            right_conditions.append(cond)
        else:
            join_conditions.append(cond)

    # Build new query tree with pushed down filters
    # ... (implementation details)

    return optimized_query
```

**Rule: Index-Aware Filter Reordering**

```python
def reorder_filters_by_cost(logical_and: LogicalAndNode) -> LogicalAndNode:
    """Reorder AND conditions by estimated cost (cheapest first)"""
    conditions = logical_and.children

    # Calculate cost for each condition
    condition_costs = []
    for cond in conditions:
        cost = estimate_condition_cost(cond)
        condition_costs.append((cost, cond))

    # Sort by cost (ascending)
    condition_costs.sort(key=lambda x: x[0])

    # Rebuild LOGICAL_AND with sorted conditions
    new_and = LogicalAndNode()
    for _, cond in condition_costs:
        new_and.add_child(cond)

    return new_and

def estimate_condition_cost(condition: QueryTree) -> int:
    """Estimate execution cost of a condition"""
    if condition.type == "COMPARISON":
        left = condition.children[0]
        if left.type == "COLUMN_REF":
            column = left.children[0].name
            if is_indexed(column):
                return 10  # Index lookup is cheap
            else:
                return 100  # Full scan is expensive
        elif left.type == "ARITH_EXPR":
            return 50  # Arithmetic computation

    elif condition.type == "IN_EXPR":
        value_list = condition.children[1]
        list_size = len(value_list.children)
        return 20 * list_size  # Cost proportional to list size

    elif condition.type == "EXISTS_EXPR":
        return 1000  # Subquery is expensive

    return 50  # Default
```

### 4. Type Safety & Validation

**Compile-Time Checks:**

```python
def validate_comparison_types(comparison: ComparisonNode) -> bool:
    """Validate that comparison operands are compatible"""
    left = comparison.children[0]
    right = comparison.children[1]

    left_type = infer_expression_type(left)
    right_type = infer_expression_type(right)

    # Type compatibility rules
    if comparison.operator in ["=", "<>", ">", ">=", "<", "<="]:
        return are_types_compatible(left_type, right_type)
    elif comparison.operator == "LIKE":
        return left_type == "STRING" and right_type == "STRING"

    return False

def infer_expression_type(expr: QueryTree) -> str:
    """Infer the data type of an expression"""
    if expr.type == "LITERAL_NUMBER":
        return "NUMBER"
    elif expr.type == "LITERAL_STRING":
        return "STRING"
    elif expr.type == "LITERAL_BOOLEAN":
        return "BOOLEAN"
    elif expr.type == "COLUMN_REF":
        # Look up column type from schema
        column_name = expr.children[0].name
        return get_column_type_from_schema(column_name)
    elif expr.type == "ARITH_EXPR":
        return "NUMBER"  # Arithmetic results are numeric

    return "UNKNOWN"
```

### 5. Better Error Messages

**With Current Parser:**

```
ParserError: Expected condition expression at line 1, column 45
```

**With Detailed Parser:**

```
TypeError at WHERE clause, line 1, column 45:
  Cannot compare STRING column 'name' with NUMBER literal '25'

  SELECT * FROM users WHERE name > 25
                                  ^^

  Suggestion: Did you mean name = '25' (as string)?
```

---

## 🔄 Migration Strategy

### Phase 1: Foundation (Week 1-2)

**Goals:**

- [ ] Implement atomic node classes (IDENTIFIER, LITERAL\_\*, etc.)
- [ ] Update QueryTree to support new node types
- [ ] Write unit tests for each node type

**Deliverables:**

- `query_tree_nodes.py` - All new node classes
- `test_nodes.py` - Unit tests
- Documentation for node types

### Phase 2: Expression Parser (Week 3-4)

**Goals:**

- [ ] Implement expression parsing (COLUMN_REF, COMPARISON, ARITH_EXPR)
- [ ] Update parser to build expression trees
- [ ] Handle IN, EXISTS, BETWEEN, IS NULL expressions

**Deliverables:**

- Updated `parser.py` with expression methods
- `test_expressions.py` - Expression parsing tests
- Example queries with detailed output

### Phase 3: Logical Operators (Week 5)

**Goals:**

- [ ] Implement LOGICAL_AND, LOGICAL_OR, LOGICAL_NOT nodes
- [ ] Parse complex WHERE conditions
- [ ] Handle operator precedence correctly

**Deliverables:**

- Logical operator parsing
- `test_logical_ops.py` - Tests for complex conditions
- Precedence validation

### Phase 4: Query Structure (Week 6-7)

**Goals:**

- [ ] Implement SELECT_QUERY, FROM_CLAUSE, WHERE_CLAUSE nodes
- [ ] Parse complete SELECT statements
- [ ] Handle JOIN, ORDER BY, LIMIT

**Deliverables:**

- Complete SELECT parsing
- `test_select.py` - Integration tests
- Example transformations

### Phase 5: DML Statements (Week 8)

**Goals:**

- [ ] Implement UPDATE, INSERT, DELETE nodes
- [ ] Parse DML statements with detailed structure
- [ ] Validate assignments and constraints

**Deliverables:**

- DML parsing
- `test_dml.py` - DML tests
- Complete parser functionality

### Phase 6: Optimization Integration (Week 9-10)

**Goals:**

- [ ] Update optimization rules for new structure
- [ ] Implement column-aware filter reordering
- [ ] Add index-aware optimization
- [ ] Update cost estimation

**Deliverables:**

- Updated `rules_registry.py`
- New optimization rules leveraging detailed structure
- Performance benchmarks

### Phase 7: Testing & Documentation (Week 11-12)

**Goals:**

- [ ] Comprehensive integration tests
- [ ] Performance testing and optimization
- [ ] Complete documentation
- [ ] Migration guide for existing code

**Deliverables:**

- `test_integration_detailed.py` - Full integration tests
- Performance report
- Updated README.md
- Migration guide

---

## 🎯 Implementation Decisions

### Decision 1: Binary vs N-ary Logical Trees

**Binary Tree (Recommended):**

```
LOGICAL_AND
├── LOGICAL_AND
│   ├── condition1
│   └── condition2
└── condition3
```

**Pros:**

- Standard compiler approach
- Easy to implement recursive algorithms
- Clear precedence and associativity

**Cons:**

- Deeper trees for many conditions
- More memory overhead

**N-ary Tree (Alternative):**

```
LOGICAL_AND
├── condition1
├── condition2
└── condition3
```

**Pros:**

- Flatter tree structure
- Less memory for many conditions
- Easy to reorder children

**Cons:**

- Non-standard for compilers
- Precedence less explicit

**Decision:** Use **Binary Tree** for correctness and standard approach, but allow optimization rules to flatten when appropriate.

### Decision 2: WHERE_CLAUSE Position

**Option A: WHERE_CLAUSE as separate clause node**

```
SELECT_QUERY
├── SELECT_LIST
├── FROM_CLAUSE
└── WHERE_CLAUSE
    └── condition
```

**Option B: Conditions directly under FROM_CLAUSE**

```
SELECT_QUERY
├── SELECT_LIST
└── FROM_CLAUSE
    ├── table_expr
    └── conditions
```

**Decision:** Use **Option A** - separate WHERE_CLAUSE node for clarity and standard SQL structure.

### Decision 3: Backward Compatibility

**Approach:** Hybrid system - Keep main structure, add detail

**Core Principle:**

- ✅ **NEVER remove** existing node types: `PROJECT`, `FILTER`, `SORT`, `RELATION`, `JOIN`, `UPDATE`, `INSERT`, `DELETE`
- ✅ **ADD** detail nodes as children of existing nodes
- ✅ **SUPPORT both** old string-based values and new detailed children
- ✅ **GRADUAL adoption** - components can migrate independently

**Migration Path:**

```python
# Phase 1: Both formats supported
# Old format (still works)
filter_node = QueryTree("FILTER", "WHERE age > 25")
filter_node.add_child(relation)

# New format (with detail)
filter_node = QueryTree("FILTER", "")  # Empty value
comparison = QueryTree("COMPARISON", ">")
comparison.add_child(column_ref)
comparison.add_child(literal)
filter_node.add_child(comparison)
filter_node.add_child(relation)

# Phase 2: Helper functions
def create_detailed_filter(comparison_tree, source_tree):
    """Create FILTER node with detailed structure"""
    filter_node = QueryTree("FILTER", "")
    filter_node.add_child(comparison_tree)
    filter_node.add_child(source_tree)
    return filter_node

# Phase 3: Detection function
def has_detailed_children(node: QueryTree) -> bool:
    """Check if node uses detailed structure"""
    if node.type == "FILTER":
        # Old: has string value like "WHERE age > 25"
        # New: has COMPARISON/LOGICAL_AND/etc as first child
        if node.val and node.val.startswith("WHERE"):
            return False  # Old format
        if node.children and node.children[0].type in ["COMPARISON", "LOGICAL_AND", "LOGICAL_OR", "IN_EXPR"]:
            return True  # New format
    return False

# Phase 4: Converter functions
def convert_filter_to_detailed(filter_node: QueryTree) -> QueryTree:
    """Convert old FILTER to detailed format"""
    if has_detailed_children(filter_node):
        return filter_node  # Already detailed

    # Parse string value "WHERE age > 25" into detailed tree
    condition_str = filter_node.val.replace("WHERE ", "")
    comparison = parse_condition_string(condition_str)

    new_filter = QueryTree("FILTER", "")
    new_filter.add_child(comparison)
    for child in filter_node.children:
        new_filter.add_child(child)

    return new_filter

def get_filter_condition(filter_node: QueryTree):
    """Get condition from FILTER (works with both formats)"""
    if has_detailed_children(filter_node):
        return filter_node.children[0]  # First child is condition
    else:
        # Parse from string value
        return filter_node.val.replace("WHERE ", "")
```

**Compatibility Matrix:**

| Component | Old Format                          | New Format                      | Both Supported? |
| --------- | ----------------------------------- | ------------------------------- | --------------- |
| PROJECT   | `PROJECT("id, name")`               | `PROJECT` + COLUMN_REF children | ✅ Yes          |
| FILTER    | `FILTER("WHERE age > 25")`          | `FILTER` + COMPARISON child     | ✅ Yes          |
| SORT      | `SORT("age DESC")`                  | `SORT` + ORDER_ITEM children    | ✅ Yes          |
| RELATION  | `RELATION("users")`                 | `RELATION("users")`             | ✅ Same         |
| JOIN      | `JOIN("ON users.id = profiles.id")` | `JOIN` + COMPARISON child       | ✅ Yes          |
| UPDATE    | `UPDATE("salary = 1000")`           | `UPDATE` + ASSIGNMENT children  | ✅ Yes          |

**Migration Timeline:**

1. **Week 1-4:** Implement detail nodes, keep old format working
2. **Week 5-8:** Add helper functions for creating detailed structures
3. **Week 9-12:** Gradually convert parser to produce detailed output
4. **Week 13+:** Convert optimization rules to use detailed structures
5. **Future:** Deprecate string-based values (but keep node types!)

---

## 📈 Performance Considerations

### Memory Usage

**Estimation:**

- Old approach: ~10 nodes per query
- New approach: ~30-50 nodes per query (3-5x increase)

**Mitigation:**

- Lazy node creation (only create when needed)
- Node pooling for frequently used types
- Compact representation for simple cases

### Parsing Speed

**Expected Impact:**

- 2-3x slower parsing initially
- Optimization through caching and memoization

**Mitigation:**

- Parse result caching
- Incremental parsing for large queries
- Optimized tokenizer

### Optimization Speed

**Expected Impact:**

- Faster optimization due to direct node access
- No re-parsing needed

**Benefits:**

- Tree traversal is O(n) instead of string parsing O(n²)
- Direct attribute access vs regex matching

---

## ✅ Validation Rules

### Node Structure Validation

```python
def validate_comparison_node(node: ComparisonNode) -> list[str]:
    """Validate COMPARISON node structure"""
    errors = []

    # Must have exactly 2 children
    if len(node.children) != 2:
        errors.append(f"COMPARISON must have 2 children, got {len(node.children)}")

    # Operator must be valid
    valid_ops = ["=", "<>", "!=", ">", ">=", "<", "<=", "LIKE", "ILIKE"]
    if node.operator not in valid_ops:
        errors.append(f"Invalid comparison operator: {node.operator}")

    # Children must be value expressions
    for i, child in enumerate(node.children):
        if child.type not in ["COLUMN_REF", "LITERAL_NUMBER", "LITERAL_STRING",
                               "ARITH_EXPR", "LITERAL_BOOLEAN", "LITERAL_NULL"]:
            errors.append(f"Child {i} must be a value expression, got {child.type}")

    return errors

def validate_logical_and_node(node: LogicalAndNode) -> list[str]:
    """Validate LOGICAL_AND node structure"""
    errors = []

    # Must have at least 2 children
    if len(node.children) < 2:
        errors.append(f"LOGICAL_AND must have at least 2 children, got {len(node.children)}")

    # All children must be conditions
    valid_condition_types = ["COMPARISON", "IN_EXPR", "EXISTS_EXPR", "BETWEEN_EXPR",
                              "IS_NULL_EXPR", "LOGICAL_AND", "LOGICAL_OR", "LOGICAL_NOT"]
    for i, child in enumerate(node.children):
        if child.type not in valid_condition_types:
            errors.append(f"Child {i} must be a condition, got {child.type}")

    return errors

def validate_query_tree(tree: QueryTree) -> list[str]:
    """Validate entire query tree"""
    errors = []

    # Recursively validate each node
    for node in tree.traverse():
        node_errors = validate_node(node)
        errors.extend(node_errors)

    return errors
```

### Semantic Validation

```python
def validate_column_references(tree: QueryTree, schema: dict) -> list[str]:
    """Validate that all column references exist in schema"""
    errors = []

    for col_ref in tree.find_nodes_by_type("COLUMN_REF"):
        identifier = col_ref.children[0]

        if identifier.type == "QUALIFIED_ID":
            table = identifier.table
            column = identifier.column

            if table not in schema:
                errors.append(f"Unknown table: {table}")
            elif column not in schema[table]:
                errors.append(f"Unknown column: {table}.{column}")

        elif identifier.type == "IDENTIFIER":
            # Unqualified column - check if exists in any table
            column = identifier.name
            found = False
            for table in schema.values():
                if column in table:
                    found = True
                    break

            if not found:
                errors.append(f"Unknown column: {column}")

    return errors
```

---

## 🧪 Testing Strategy

### Unit Tests

```python
# test_atomic_nodes.py
def test_identifier_node():
    node = IdentifierNode(name="users")
    assert node.type == "IDENTIFIER"
    assert node.name == "users"
    assert len(node.children) == 0

def test_literal_number():
    node = LiteralNumberNode(value=42.5)
    assert node.type == "LITERAL_NUMBER"
    assert node.value == 42.5

def test_comparison_node():
    # age > 25
    comp = ComparisonNode(operator=">")
    comp.add_child(ColumnRefNode().add_child(IdentifierNode("age")))
    comp.add_child(LiteralNumberNode(25))

    assert comp.operator == ">"
    assert len(comp.children) == 2
```

### Integration Tests

```python
# test_parser_integration.py
def test_parse_simple_select():
    sql = "SELECT name FROM users WHERE age > 25"
    parser = DetailedParser(Tokenizer(sql))
    tree = parser.parse()

    assert tree.type == "SELECT_QUERY"
    assert tree.find_node("WHERE_CLAUSE") is not None

    comparison = tree.find_node("COMPARISON")
    assert comparison.operator == ">"

def test_parse_complex_where():
    sql = "SELECT * FROM users WHERE (age > 25 AND status = 'active') OR admin = true"
    parser = DetailedParser(Tokenizer(sql))
    tree = parser.parse()

    where_clause = tree.find_node("WHERE_CLAUSE")
    condition = where_clause.children[0]

    assert condition.type == "LOGICAL_OR"
    assert condition.children[0].type == "LOGICAL_AND"
```

### Performance Tests

```python
# test_performance.py
def test_parsing_speed():
    sql = "SELECT * FROM users WHERE " + " AND ".join(
        [f"col{i} > {i}" for i in range(100)]
    )

    start = time.time()
    parser = DetailedParser(Tokenizer(sql))
    tree = parser.parse()
    parse_time = time.time() - start

    assert parse_time < 1.0  # Should parse in under 1 second

def test_optimization_speed():
    # Test that detailed tree enables faster optimization
    # ...
```

---

## 📚 References

### Academic Papers

- "Extensibility and Control in Query Compilation" - Tahboub, et al. (SIGMOD 2018)
- "Efficiently Compiling Efficient Query Plans for Modern Hardware" - Neumann (VLDB 2011)
- "The Volcano Optimizer Generator" - Graefe & McKenna (1993)

### Books

- "Database System Concepts" - Silberschatz, Korth, Sudarshan (Chapter on Query Processing)
- "Compilers: Principles, Techniques, and Tools" - Aho, Lam, Sethi, Ullman (Dragon Book)

### Open Source Projects

- **PostgreSQL Parser** - Reference for SQL grammar
- **Apache Calcite** - Query optimizer framework
- **SQLGlot** - Python SQL parser and transpiler

---

## 📝 Notes & Open Questions

### Questions to Resolve

1. **Should we support SQL functions (SUM, COUNT, etc.) in Phase 1?**
   - Leaning towards: No, add in Phase 6 as extension
2. **How to handle ambiguous column references?**
   - Need schema context during parsing?
   - Or resolve during validation phase?
3. **Should LIMIT be a separate clause or modifier on SELECT_QUERY?**

   - Leaning towards: Separate LIMIT_CLAUSE node for consistency

4. **How to represent aggregate functions (SUM, AVG, COUNT)?**

   - New node type: AGGREGATE_EXPR?
   - Need GROUP BY support?

5. **Alias support - when to resolve?**
   - Parse time or optimization time?
   - Store in node or separate symbol table?

### Future Extensions

- [ ] Subquery support (IN, EXISTS with SELECT)
- [ ] Common Table Expressions (WITH clause)
- [ ] Window functions (OVER, PARTITION BY)
- [ ] Aggregate functions (SUM, COUNT, AVG, GROUP BY, HAVING)
- [ ] UNION, INTERSECT, EXCEPT
- [ ] Correlated subqueries
- [ ] Lateral joins

---

## 👥 Team Assignments (Proposal)

### Parser Core (2 people)

- Implement atomic nodes
- Write expression parser
- Handle operator precedence

### Query Structure (2 people)

- Implement SELECT/FROM/WHERE clauses
- Handle JOIN logic
- Parse DML statements

### Optimization (1 person)

- Update existing rules
- Implement new column-aware rules
- Cost estimation

### Testing & Documentation (1 person)

- Write comprehensive tests
- Create examples
- Documentation

---

## 🎉 Success Criteria

### Must Have (MVP)

- ✅ All atomic nodes implemented and tested
- ✅ Parse SELECT with WHERE (comparison operators)
- ✅ Parse AND/OR/NOT logic
- ✅ Basic JOIN support
- ✅ Validation for node structure
- ✅ At least 2 optimization rules updated

### Should Have

- ✅ Parse IN, EXISTS, BETWEEN, IS NULL
- ✅ Arithmetic expressions
- ✅ UPDATE/INSERT/DELETE statements
- ✅ Column dependency tracking
- ✅ Filter pushdown optimization
- ✅ Performance tests

### Nice to Have

- ✅ Subquery support
- ✅ ORDER BY, LIMIT
- ✅ Alias support
- ✅ Schema validation
- ✅ Type checking
- ✅ Migration tool from old to new format

---

**End of Document**

---

## Appendix A: Node Type Quick Reference

| Node Type       | Category   | Children | Purpose          |
| --------------- | ---------- | -------- | ---------------- |
| IDENTIFIER      | Atomic     | 0        | Simple name      |
| QUALIFIED_ID    | Atomic     | 0        | table.column     |
| LITERAL_NUMBER  | Atomic     | 0        | Numeric value    |
| LITERAL_STRING  | Atomic     | 0        | String value     |
| LITERAL_BOOLEAN | Atomic     | 0        | Boolean value    |
| LITERAL_NULL    | Atomic     | 0        | NULL value       |
| COLUMN_REF      | Expression | 1        | Column reference |
| ARITH_EXPR      | Expression | 2        | Arithmetic op    |
| COMPARISON      | Expression | 2        | Comparison op    |
| IN_EXPR         | Expression | 2        | IN clause        |
| EXISTS_EXPR     | Expression | 1        | EXISTS clause    |
| BETWEEN_EXPR    | Expression | 3        | BETWEEN clause   |
| IS_NULL_EXPR    | Expression | 1        | IS NULL clause   |
| LOGICAL_AND     | Logical    | 2+       | AND operation    |
| LOGICAL_OR      | Logical    | 2+       | OR operation     |
| LOGICAL_NOT     | Logical    | 1        | NOT operation    |
| SELECT_QUERY    | Query      | 2-5      | SELECT statement |
| FROM_CLAUSE     | Query      | 1        | FROM clause      |
| WHERE_CLAUSE    | Query      | 1        | WHERE clause     |
| ORDER_BY_CLAUSE | Query      | 1+       | ORDER BY clause  |
| RELATION        | Query      | 0        | Table reference  |
| JOIN            | Query      | 3        | JOIN operation   |

## Appendix B: Comparison with Other Parsers

### vs PostgreSQL Parser

- **PostgreSQL:** C-based, yacc/bison generated
- **Our approach:** Pure Python, hand-written recursive descent
- **Similarity:** Node-based AST representation
- **Difference:** We focus on optimization, they focus on execution

### vs Apache Calcite

- **Calcite:** Java-based, JavaCC generated
- **Our approach:** Python, hand-written
- **Similarity:** Relational algebra tree
- **Difference:** They support multiple SQL dialects, we focus on optimization rules

### vs SQLGlot

- **SQLGlot:** Python, transpiler focus
- **Our approach:** Python, optimizer focus
- **Similarity:** Python implementation
- **Difference:** They focus on SQL-to-SQL translation, we focus on query optimization

---

**Document Version:** 1.0  
**Last Updated:** November 20, 2025  
**Status:** Draft for Review
