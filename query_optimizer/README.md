# Query Optimizer

---

## 1. Overview

Query Optimizer adalah modul yang bertanggung jawab untuk mengoptimalkan query SQL dengan menggunakan **Genetic Algorithm (GA)**. Modul ini mengimplementasikan kombinasi rules optimasi yang menghasilkan query dengan cost eksekusi terendah.

### Fitur Utama

- **SQL Parser**: Parse SQL → Query Tree
- **Deterministic Rules**: Rule 3 (projection elimination), Rule 7 (filter pushdown), Rule 8 (projection over joins) — selalu dijalankan sebelum GA
- **Non-deterministic Rules (GA-driven)**: Rule 1 (filter cascading/reordering), Rule 2 (filter reordering), Rule 4 (push selection into joins), Rule 5 (join commutativity), Rule 6 (join associativity)
- **Genetic Algorithm Optimizer**: GA meng-explore parameter space terpisah per-operation (`filter_params`, `join_params`, `join_child_params`, `join_associativity_params`, `join_method_params`)
- **Join Method Shuffling**: GA dapat memilih metode eksekusi JOIN per node (`nested_loop` atau `hash`)
- **Query Tree Manipulation**: Transformasi struktur dengan preservasi node ID
- **Cost Estimation**: Model cost berbasis statistik (block/tupel, indeks, selectivity)

**Notes:**

- `filter_params` dan `join_params` sering diperlakukan sebagai _coupled_ dalam crossover untuk menjaga konsistensi
- `join_child_params`, `join_associativity_params`, dan `join_method_params` diperlakukan sebagai operasi independen

### Komponen Utama

```
query_optimizer/
├── tokenizer.py              # SQL tokenization
├── parser.py                 # SQL parsing ke Query Tree
├── query_tree.py             # Query Tree data structure
├── query_check.py            # Query validation
├── optimization_engine.py    # Main optimization engine
├── genetic_optimizer.py      # Genetic Algorithm with unified params
├── rule_params_manager.py    # Unified parameter management
├── cost.py                   # Cost estimation model
├── demo.py                   # Demo program
├── rule/                     # Optimization rules
│   ├── rule_1_2.py           # Filter cascading & reordering (non-deterministic)
│   ├── rule_3.py             # Projection elimination (deterministic)
│   ├── rule_4.py             # Push selection into joins (non-deterministic)
│   ├── rule_5.py             # Join commutativity (non-deterministic)
│   ├── rule_6.py             # Join associativity (non-deterministic)
│   ├── rule_7.py             # Filter pushdown over join (deterministic)
│   └── rule_8.py             # Projection over join (deterministic)
└── tests/                    # Unit tests
    ├── test_tokenizer.py
    ├── test_parser.py
    ├── test_check.py
    ├── test_rule_1_2.py
    ├── test_rule_4.py
    ├── test_rule_5.py
    ├── test_rule_6.py
    ├── test_rule_7.py
    ├── test_rule_8.py
    ├── test_rule_deterministik.py
    ├── test_genetic.py
    └── integration.py
```

---

## 2. Cara Pemanggilan

### 2.1 Installation & Setup

```bash
# Clone repository
git clone <repository-url>
cd database-management-system-development-IF3140

# Pastikan berada di root directory project
# Tidak perlu instalasi dependencies khusus (pure Python)
```

### 2.2 Running Demo

Demo program supports per-rule scenarios and broader demos. Use `python -m query_optimizer.demo N` (or `N.S` for specific scenario):

**Demo Numbers:**

- `1` — Rule 1 (filter cascading scenarios)
- `2` — Rule 2 (filter reordering scenarios)
- `3` — Rule 3 (projection elimination)
- `4` — Rule 7 (filter pushdown over joins)
- `5` — Rule 8 (projection over joins)
- `6` — Rule 6 (join associativity): reassociation, semantic validation, natural joins
- `7` — Rule 4 (push selection into joins)
- `8` — Rule 5 (join commutativity) and GA exploration
- `9` — Parsing & utility demos
- `10` — All demos runner
- `11` — GA internals demo (step-by-step)
- `12` — Full/combined runs

**Examples:**

```powershell
python -m query_optimizer.demo 6        # Rule 6 associativity scenarios
python -m query_optimizer.demo 11       # GA internals (init, crossover, mutation)
python -m query_optimizer.demo 10       # Run broad demo sequence
```

**Notes:**

- Some demos use mocked metadata (demo wrappers patch metadata where needed)
- Use `python -m query_optimizer.demo` with no args to see help and scenario indices

### 2.3 Programmatic Usage

#### Basic Query Parsing

```python
from query_optimizer.optimization_engine import OptimizationEngine

# Initialize engine
engine = OptimizationEngine()

# Parse SQL query
sql = "SELECT id, name FROM users WHERE age > 25 AND status = 'active'"
query = engine.parse_query(sql)

# Access query tree
print(query.query_tree.tree())

# Calculate cost
cost = engine.get_cost(query)
print(f"Cost: {cost}")
```

#### Optimization with Genetic Algorithm

```python
from query_optimizer.optimization_engine import OptimizationEngine

engine = OptimizationEngine()
query = engine.parse_query("SELECT * FROM users WHERE age > 25 AND status = 'active'")

# Optimize dengan GA
# Note: Deterministic rules (Rule 3, 7, 8) otomatis dijalankan SEBELUM GA
optimized = engine.optimize_query(
    query,
    use_genetic=True,
    population_size=50,
    generations=100
)

print("Original cost:", engine.get_cost(query))
print("Optimized cost:", engine.get_cost(optimized))
```

### 2.4 Running Tests

```bash
# Run all tests
python -m unittest discover query_optimizer/tests

# Run specific test file
python -m unittest query_optimizer.tests.test_tokenizer
python -m unittest query_optimizer.tests.test_parser
python -m unittest query_optimizer.tests.test_rule_1
python -m unittest query_optimizer.tests.test_rule_7
python -m unittest query_optimizer.tests.test_rule_deterministik

# Run with verbose output
python -m unittest query_optimizer.tests.test_rule_7 -v
python -m unittest query_optimizer.tests.test_rule_deterministik -v
```

---

## 3. Deskripsi Implementasi Detail

### 3.1 Query Tree Structure

Query Tree adalah representasi internal dari SQL query dengan atomic node structure. Setiap node merepresentasikan operator atau operand dalam query.

#### Node Design Philosophy

**Main Query Structure Nodes:**

- `PROJECT` - SELECT clause dengan column references
- `FILTER` - WHERE clause container dengan explicit source (2 children: source + condition)
- `OPERATOR` - Logical operators (AND/OR/NOT) untuk combining conditions
- `SORT` - ORDER BY (single attribute with direction)
- `RELATION` - Table reference
- `JOIN` - JOIN operations (INNER/NATURAL)
- `LIMIT` - LIMIT clause

**Atomic Detail Nodes:**

- `IDENTIFIER` - Nama dasar (atomic leaf)
- `LITERAL_NUMBER`, `LITERAL_STRING`, `LITERAL_BOOLEAN`, `LITERAL_NULL` - Nilai literal
- `COLUMN_NAME` - Wrapper untuk nama kolom (berisi IDENTIFIER)
- `TABLE_NAME` - Wrapper untuk nama table/alias (berisi IDENTIFIER)
- `COLUMN_REF` - Referensi kolom (simple: 1 child, qualified: 2 children)

**Condition Expression Nodes:**

- `COMPARISON` - Operasi perbandingan (=, <>, >, >=, <, <=)
- `IN_EXPR`, `NOT_IN_EXPR` - IN / NOT IN expressions
- `EXISTS_EXPR`, `NOT_EXISTS_EXPR` - EXISTS / NOT EXISTS
- `BETWEEN_EXPR`, `NOT_BETWEEN_EXPR` - BETWEEN / NOT BETWEEN
- `IS_NULL_EXPR`, `IS_NOT_NULL_EXPR` - IS NULL / IS NOT NULL
- `ARITH_EXPR` - Ekspresi aritmatika (+, -, \*, /, %)

**DML Nodes:**

- `UPDATE_QUERY`, `INSERT_QUERY`, `DELETE_QUERY` - DML operations
- `ASSIGNMENT` - SET clause assignment

**Transaction Nodes:**

- `BEGIN_TRANSACTION`, `COMMIT` - Transaction control

#### Key Node Structures

**COLUMN_REF Node (Column References)**

```
# Simple column reference
COLUMN_REF
└── COLUMN_NAME            # Child 0: wajib
    └── IDENTIFIER("age")

# Qualified column reference (table.column)
COLUMN_REF
├── COLUMN_NAME            # Child 0: column name
│   └── IDENTIFIER("age")
└── TABLE_NAME             # Child 1: table/alias
    └── IDENTIFIER("users")
```

**FILTER Node (WHERE Clause Container)**

FILTER selalu memiliki 2 children: source + condition tree

```
FILTER
├── RELATION("users")      # Child 0: source
└── COMPARISON(">")         # Child 1: condition tree
    ├── COLUMN_REF("age")
    └── LITERAL_NUMBER(25)
```

**OPERATOR Node (Logical Operations)**

OPERATOR untuk combining conditions (AND/OR/NOT):

```
OPERATOR("AND")
├── COMPARISON(">")
│   ├── COLUMN_REF("age")
│   └── LITERAL_NUMBER(25)
└── COMPARISON("=")
    ├── COLUMN_REF("status")
    └── LITERAL_STRING("active")
```

**PROJECT Node (SELECT Clause)**

```
# Select specific columns
PROJECT
├── COLUMN_REF             # Selected columns
├── COLUMN_REF
└── [source_tree]          # Last child = source

# Select all
PROJECT("*")               # value = "*"
└── [source_tree]
```

Lihat **[Parse_Query.md](doc/Parse_Query.md)** untuk detail lengkap semua node types.

#### Query Tree Example

SQL:

```sql
SELECT id, name FROM users WHERE age > 25 AND status = 'active' ORDER BY name
```

Query Tree:

```
PROJECT("id, name")
└── SORT("name")
    └── FILTER("WHERE age > 25")
        └── FILTER("WHERE status = 'active'")
            └── RELATION("users")
```

Atau dengan OPERATOR_S structure:

```
PROJECT("id, name")
└── SORT("name")
    └── OPERATOR_S("AND")
        ├── RELATION("users")
        ├── FILTER("WHERE age > 25")
        └── FILTER("WHERE status = 'active'")
```

### 3.2 Tokenization & Parsing

#### Grammar Overview

Parser menggunakan formal BNF grammar untuk parsing SQL. Lihat **[Parse_Query.md](doc/Parse_Query.md)** untuk grammar lengkap.

**Lexical Elements:**

- Identifiers: `<letter>(<letter>|<digit>|'_')*`
- Literals: numbers, strings, boolean, null
- Operators: `=`, `<>`, `>`, `>=`, `<`, `<=`, `+`, `-`, `*`, `/`, `%`
- Keywords: `SELECT`, `FROM`, `WHERE`, `JOIN`, `ORDER BY`, dll

**Expression Hierarchy:**

1. Value expressions: `COLUMN_REF`, `LITERAL_*`, `ARITH_EXPR`
2. Atomic conditions: `COMPARISON`, `IN_EXPR`, `BETWEEN_EXPR`, `IS_NULL_EXPR`
3. Logical conditions: `OPERATOR` (AND/OR/NOT)

#### Tokenizer

Tokenizer melakukan lexical analysis:

```python
from query_optimizer.tokenizer import Tokenizer

sql = "SELECT id, name FROM users WHERE age > 25"
tokenizer = Tokenizer(sql)
```

#### Parser

Parser menghasilkan detailed query tree:

```python
from query_optimizer.parser import Parser
from query_optimizer.tokenizer import Tokenizer

sql = "SELECT * FROM users WHERE age > 25"
tokenizer = Tokenizer(sql)
parser = Parser(tokenizer)

query_tree = parser.parse()
print(query_tree.tree())
```

**Query Tree Structure:**

```
PROJECT("*")
└── FILTER
    ├── RELATION("users")
    └── COMPARISON(">")
        ├── COLUMN_REF
        │   └── COLUMN_NAME
        │       └── IDENTIFIER("age")
        └── LITERAL_NUMBER(25)
```

Lihat **[Parse_Query.md](doc/Parse_Query.md)** untuk detail lengkap grammar dan node types.

### 3.3 Optimization Pipeline

Optimasi query dilakukan dalam dua tahap:

**Phase 1 — Deterministic preprocessing** (applied once):

- Rule 3: Projection elimination
- Rule 7: Filter pushdown over join
- Rule 8: Projection pushdown over join

**Phase 2 — GA parameter exploration** (non-deterministic rules):

- Rule 1 & 2: Filter cascading/reordering (`filter_params`)
- Rule 4: Push selection into joins (`join_params`)
- Rule 5: Join commutativity (`join_child_params`)
- Rule 6: Join associativity (`join_associativity_params`)
- Join method selection (`join_method_params`)

**GA transformation order in `_apply_transformations()`:**

1. Apply `filter_params` and `join_params` (Rule 1 & 4)
2. Apply `join_associativity_params` (Rule 6)
3. Apply `join_child_params` (Rule 5)
4. Apply `join_method_params` (set `node.method` per JOIN)

### 3.4 Genetic Algorithm Implementation

GA explores parameter space for non-deterministic rules.

**Individual (Kromosom):**

```python
class Individual:
    operation_params: dict[str, dict[int, Any]]
    # e.g. {
    #   'filter_params': { <filter_node_id>: params },
    #   'join_params': { <filter_node_id>: True|False },
    #   'join_child_params': { <join_node_id>: [left_id, right_id] },
    #   'join_associativity_params': { <pattern_root_id>: 'left'|'right'|'none' },
    #   'join_method_params': { <join_node_id>: 'nested_loop'|'hash' }
    # }
    query: ParsedQuery  # cached transformed query
    fitness: float
```

**Key GA operations:**

- **Initialization**: `RuleParamsManager.analyze_*` + `generate_*` per operation type
- **Crossover**: coupled ops (`filter_params` + `join_params`) inherited as group; independent ops (`join_method_params`, `join_child_params`, `join_associativity_params`) combined per-node
- **Mutation**: delegates to `RuleParamsManager.mutate_<op>` for valid mutations
- **Evaluation**: lazy application of parameters to base query (deterministic rules first), then cost via `OptimizationEngine.get_cost()`

**Notes:**

- Adding new operation requires registration in `rule_params_manager.py` (analyze/generate/copy/mutate/validate) and application in `_apply_transformations()`
- `join_method_params` applied at end: each JOIN node gets `node.method = params[join_id]`

### 3.5 Optimization Rules Summary

**Deterministic rules** (Rule 3, 7, 8): applied once before GA — projection elimination, filter pushdown, projection pushdown.

**Non-deterministic rules** (GA-driven):

- **Rule 1 & 2** (`filter_params`): format `list[int | list[int]]` — urutan dan grouping conditions. Contoh: `[2, [0,1]]` = apply condition 2 first, then grouped AND of 0 and 1
- **Rule 4** (`join_params`): `{filter_node_id: bool}` — True = merge FILTER into JOIN (INNER JOIN), False = keep separate
- **Rule 5** (`join_child_params`): `{join_node_id: [left_id, right_id]}` — reorders join children (commutativity)
- **Rule 6** (`join_associativity_params`): `{pattern_root_id: 'left'|'right'|'none'}` — controls reassociation for (A ⋈ B) ⋈ C patterns; semantic checks validate theta-join attribute references
- **Join methods** (`join_method_params`): `{join_node_id: 'nested_loop'|'hash'}` — selects execution algorithm per JOIN; GA explores independently

GA workflow: selection → crossover → mutation → elitism, with parameter types now explicit and modular.

### 3.6 Query Validation

Query validation memastikan query tree structure valid dan semantically correct.

**Validation Rules:**

**Atomic Nodes (0 children):**

- **IDENTIFIER**: Value = identifier name (required)
- **LITERAL_NUMBER**: Value = numeric value (required)
- **LITERAL_STRING**: Value = string value (required)
- **LITERAL_BOOLEAN**: Value = boolean value (required)
- **LITERAL_NULL**: No value required

**Wrapper Nodes (1 child):**

- **COLUMN_NAME**: Child must be IDENTIFIER
- **TABLE_NAME**: Child must be IDENTIFIER

**Column Reference:**

- **COLUMN_REF**: 1-2 children. Child[0] = COLUMN_NAME (required), Child[1] = TABLE_NAME (optional for qualified reference)

**Source Nodes:**

- **RELATION**: 0 children. Value = table name (must exist in database)
- **ALIAS**: 1 child. Value = alias name (required)
- **PROJECT**: ≥1 children. Last child = source. If value = "\*", must have exactly 1 child (source only)
- **FILTER**: 2 children. Child[0] = source, Child[1] = condition tree
- **JOIN**: 2-3 children. Value = "INNER" or "NATURAL". NATURAL = 2 children, INNER = 3 children (2 relations + condition)
- **SORT**: 2 children. Child[0] = COLUMN_REF, Child[1] = source. Value = "ASC" or "DESC" (optional)
- **LIMIT**: 1 child (source). Value = limit number

**Condition Nodes:**

- **OPERATOR**: ≥1 children. Value = "AND"/"OR"/"NOT" (required). AND/OR = ≥2 children, NOT = 1 child
- **COMPARISON**: 2 children (left, right expressions). Value = operator ("=", "<>", "!=", ">", ">=", "<", "<=")
- **IN_EXPR**: 2 children (column_ref, LIST or subquery)
- **NOT_IN_EXPR**: 2 children (column_ref, LIST or subquery)
- **EXISTS_EXPR**: 1 child (subquery)
- **NOT_EXISTS_EXPR**: 1 child (subquery)
- **BETWEEN_EXPR**: 3 children (value, lower, upper)
- **NOT_BETWEEN_EXPR**: 3 children (value, lower, upper)
- **IS_NULL_EXPR**: 1 child (column_ref)
- **IS_NOT_NULL_EXPR**: 1 child (column_ref)

**Value Expression Nodes:**

- **ARITH_EXPR**: 2 children (left, right). Value = operator ("+", "-", "\*", "/", "%")

**Other Nodes:**

- **LIST**: 0+ children (list items)

**DML Nodes:**

- **UPDATE_QUERY**: ≥2 children (relation, assignments, optional filter)
- **INSERT_QUERY**: 3 children (relation, column_list, values_clause)
- **DELETE_QUERY**: 1-2 children (relation, optional filter)
- **ASSIGNMENT**: 2 children (column_ref, value_expr)

**Transaction Nodes:**

- **BEGIN_TRANSACTION**: 0+ children (statements)
- **COMMIT**: 0 children

**Important Notes:**

- Atomic nodes (IDENTIFIER, LITERAL\_\*) are leaf nodes with no children
- Wrapper nodes (COLUMN_NAME, TABLE_NAME) wrap IDENTIFIER
- COLUMN_REF represents column references (simple or qualified with table/alias)
- FILTER has exactly 2 children: source + condition tree
- Logical operators use OPERATOR node with AND/OR/NOT values
- Comparison and condition expressions are separate node types (COMPARISON, IN_EXPR, etc.)
- Validation is performed recursively on the entire tree structure

### 3.7 Estimasi Cost

Estimasi cost diimplementasikan di `CostCalculator` class (`cost.py`). Model cost menggabungkan **I/O cost** dan **CPU cost** menggunakan statistik storage.

#### Struktur Cost

```python
@dataclass
class CostResult:
    io_cost: float              # Cost I/O (block access)
    cpu_cost: float = 0.0       # Cost CPU (processing)
    estimated_cardinality: int  # Jumlah tuple output
    estimated_blocks: int       # Jumlah block output
    
    @property
    def total_cost(self) -> float:
        return self.io_cost + self.cpu_cost
```

#### Parameter Konfigurasi

- I/O: `sequential_io_cost` (1.0), `random_io_cost` (1.5), `write_cost` (2.0)
- CPU: `cpu_per_tuple` (0.01), `cpu_per_comparison` (0.001), `cpu_per_hash` (0.005), `cpu_per_sort_compare` (0.002)
- Memory: `memory_blocks` (100), `sort_memory_blocks` (10)

#### Formula Cost per Operator

**Table Scan:** I/O = `b_r × 1.0`, CPU = `n_r × 0.01`

**Index Scan:**
- Hash: I/O = `1.5 + data_blocks`
- B-Tree (equality): I/O = `(height + 1) × 1.5 + data_blocks`
- B-Tree (range): I/O = `(height + 1) × 1.5 + leaf_scan + data_blocks × 1.5`

**Filter:** I/O = `source_io` (pipelined), CPU = `n_tuples × conditions × 0.001`
- Cascade filter: CPU lebih rendah karena early filtering

**Join (Block Nested Loop):** I/O = `b_outer + b_inner + (b_outer × b_inner)`

**Join (Hash):** I/O in-memory = `build_io + build_blocks × 2.0 + probe_io`

**Join (Index Nested Loop):** I/O = `outer_io + n_outer × index_cost + data_blocks`

**Sort:** I/O = `source_io + 2 × b_r × (passes + 1)`, CPU = `n × log₂(n) × 0.002`

#### Estimasi Selectivity

- Equality (`=`): `1 / V(a,r)`
- Range (`<`, `>`, `<=`, `>=`): 0.33
- AND: perkalian selectivity
- OR: `sel₁ + sel₂ - (sel₁ × sel₂)`
- BETWEEN: 0.25, LIKE: 0.05, IN: 0.3, EXISTS: 0.5

#### Penggunaan

```python
engine = OptimizationEngine()
query = engine.parse_query(sql)

# Get total cost
cost = engine.get_cost(query)

# Get detail
result = engine.get_cost_result(query)
print(f"I/O: {result.io_cost}, CPU: {result.cpu_cost}")
print(f"Total: {result.total_cost}")
```

Lihat `query_optimizer/cost.py` untuk detail implementasi.

---

## Appendix

### A. References

- **[Parse_Query.md](doc/Parse_Query.md)**: Detail lengkap tentang tokenization dan parsing rules
- Genetic Algorithms: Holland, J. H. (1992). Adaptation in Natural and Artificial Systems
- Query Optimization: Graefe, G. (1993). Query Evaluation Techniques for Large Databases

---

**Contributors:**

- FORTRAN - IF3140 - Institut Teknologi Bandung
