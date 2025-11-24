# Query Optimizer

---

## 1. Overview

Query Optimizer adalah modul yang bertanggung jawab untuk mengoptimalkan query SQL dengan menggunakan **Genetic Algorithm (GA)**. Modul ini mengimplementasikan kombinasi rules optimasi yang menghasilkan query dengan cost eksekusi terendah.

### Fitur Utama

- **SQL Parser**: Mengubah SQL string menjadi Query Tree representation
- **Deterministic Rules**: Rule 3 (projection elimination), Rule 7 (filter pushdown), Rule 8 (projection over joins)
- **Genetic Algorithm Optimizer**: Optimasi query menggunakan unified filter params
- **Query Tree Manipulation**: Transformasi dan manipulasi struktur query tree
- **Cost Estimation**: Estimasi cost eksekusi query
- **Unified Filter Params**: Format `list[int | list[int]]` menggabungkan reordering dan cascading

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
├── rule_1.py                 # Filter cascading (non-deterministic)
├── rule_2.py                 # Filter reordering (non-deterministic)
├── rule_3.py                 # Projection elimination (deterministic)
├── rule_7.py                 # Filter pushdown over join (deterministic)
├── rule_8.py                 # Projection over join (deterministic)
├── demo.py                   # Demo program
└── tests/                    # Unit tests
    ├── test_tokenizer.py
    ├── test_parser.py
    ├── test_check.py
    ├── test_rule_1.py
    ├── test_rule_2.py
    ├── test_rule_7.py
    ├── test_rule_8.py
    ├── test_rule_deterministik.py
    ├── test_genetic.py
    ├── test_integration_rules.py
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

Demo program menyediakan 9 mode:

**Demo 1: Basic Parsing**

```bash
python -m query_optimizer.demo 1
```

Output:
- Parse multiple SQL queries
- Display query tree structure

**Demo 2: Basic Optimization Comparison**

```bash
python -m query_optimizer.demo 2
```

Output:
- Compare optimization with/without GA
- Show cost difference

**Demo 3: Rule 3 - Projection Elimination (Deterministic)**

```bash
python -m query_optimizer.demo 3
```

Output:
- Demonstrates nested projection elimination
- Shows before/after tree structure
- Explains Rule 3 is applied ONCE before GA
- PROJECT count reduction

**Demo 4: Rule 7 - Filter Pushdown over Join (Deterministic)**

```bash
python -m query_optimizer.demo 4
```

Output:
- Demonstrates FILTER → JOIN pattern detection
- Shows filter pushdown to left/right/both sides
- Explains benefit: reduced data before join
- FILTER count increase (closer to source)

**Demo 5: Rule 8 - Projection over Join (Deterministic)**

```bash
python -m query_optimizer.demo 5
```

Output:
- Demonstrates PROJECT → JOIN pattern detection
- Shows projection pushdown to join children
- Explains benefit: reduced tuple width before join
- PROJECT count increase (child projections added)
- Each relation only projects needed columns

**Demo 6: Rule 1 - Filter Cascading (Non-deterministic)**

```bash
python -m query_optimizer.demo 6
```

Output:
- Filter cascading transformation
- Mixed cascade orders (unified format)
- Uncascade back to AND structure
- Random order generation

**Demo 7: Genetic Algorithm with Unified Params**

```bash
python -m query_optimizer.demo 7
```

Output:
- Full genetic optimization (Rule 3 → Rule 7 → Rule 8 → GA)
- Original query tree & cost
- Optimized query tree & cost
- Improvement statistics
- Best solution (unified filter_params: reorder + cascade)
- Evolution progress per generation

**Demo 8: Rule 2 - Filter Reordering (Non-deterministic)**

```bash
python -m query_optimizer.demo 8
```

Output:
- AND condition reordering
- Multiple reorder strategies
- Preserves tree structure

**Demo 9: Run All Demos**

```bash
python -m query_optimizer.demo 9
```

Output:
- Execute all demos sequentially (Rule 3 → Rule 7 → Rule 8 → Rule 1 → Rule 2 → GA)
- Comprehensive showcase of all features

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

#### Custom Fitness Function

```python
from query_optimizer.optimization_engine import OptimizationEngine, ParsedQuery

def custom_fitness(query: ParsedQuery) -> float:
    """
    Custom fitness function.
    Return: float (lower is better)
    """
    # Implement your own cost calculation
    # Example: count number of filters
    filter_count = len(query.query_tree.find_nodes_by_type("FILTER"))
    return float(filter_count * 50)

engine = OptimizationEngine()
query = engine.parse_query("SELECT * FROM users WHERE age > 25 AND status = 'active'")

# Use custom fitness function
# Deterministic rules (3, 7, 8) tetap dijalankan di awal, kemudian GA dengan custom fitness
optimized = engine.optimize_query(
    query,
    population_size=30,
    generations=50,
    fitness_func=custom_fitness  # Use custom function
)

print(f"Optimized with custom fitness: {custom_fitness(optimized)}")
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

Optimasi query dilakukan dalam 2 tahap:

#### Phase 1: Deterministic Rules (Always Applied)

Rules yang selalu menghasilkan improvement dan diterapkan sekali:

1. **Rule 3: Projection Elimination** - Eliminasi nested projection
2. **Rule 7: Filter Pushdown over Join** - Push filter mendekati data source
3. **Rule 8: Projection over Join** - Push projection ke join children

```
Original Query
      ↓
   Rule 3 (eliminasi projection)
      ↓
   Rule 7 (pushdown filter)
      ↓
   Rule 8 (pushdown projection)
      ↓
Phase 2: Genetic Algorithm
```

#### Phase 2: Genetic Algorithm (Parameter Space Exploration)

Rules dengan parameter space yang dioptimasi oleh GA:

- **Rule 1: Filter Cascading** - Order dan grouping dari filter conditions
- **Rule 2: Filter Reordering** - Permutasi AND conditions

### 3.4 Genetic Algorithm Implementation

Genetic Algorithm adalah metode optimasi yang terinspirasi dari evolusi biologis untuk mengeksplorasi parameter space dari Rule 1 dan Rule 2.

#### Core Concepts

**Individual (Kromosom)**

Setiap individual merepresentasikan kombinasi parameter untuk Rule 1 dan Rule 2:

```python
class Individual:
    operation_params: dict[str, dict[int, Any]]  # Unified parameters
    query: ParsedQuery                            # Query hasil (after Rule 3,7,8 + GA params)
    fitness: float                                # Cost (semakin rendah semakin baik)
```

Contoh (Unified Format):

```python
Individual(
    operation_params={
        'filter_params': {
            42: [2, [0, 1]]  # Unified: reorder to [2,0,1] + cascade: 2 single, [0,1] grouped
        }
    },
    fitness=220.0
)

Penjelasan unified format:
- [2, [0, 1]] = order [cond2, cond0, cond1] dengan cond2 single filter, [cond0,cond1] grouped
- int = condition cascade sebagai single filter
- list[int] = conditions stay grouped dalam AND operator
```

**Population**

Kumpulan individu (kromosom) yang merepresentasikan solusi berbeda:

```
Population (size=50):
├── Individual 1: filter_params={42: [0,1,2]}, fitness=250
├── Individual 2: filter_params={42: [2,[0,1]]}, fitness=230
├── Individual 3: filter_params={42: [1,0,2]}, fitness=240
└── ... (47 more individuals)
```

#### Genetic Algorithm Workflow

```
┌─────────────────────────────────────────────────────────────┐
│ 0. PREPROCESSING (Outside GA)                               │
│    • Apply Rule 3 (projection elimination)                  │
│    • Apply Rule 7 (filter pushdown)                         │
│    • Apply Rule 8 (projection over join)                    │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 1. INITIALIZATION                                           │
│    • Analyze query for AND operators                        │
│    • Generate random population:                            │
│      - Random unified filter_params (reorder + cascade)     │
│      - Each individual has unique params combination        │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. EVALUATION                                               │
│    • Calculate fitness for each individual                  │
│    • Sort population by fitness (best first)                │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. SELECTION (Tournament)                                   │
│    • Select 3 random individuals                            │
│    • Choose best as parent                                  │
│    • Repeat for parent 2                                    │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 4. CROSSOVER (Uniform Crossover)                            │
│    • Combine unified filter_params from parents             │
│    • Each param type inherited from random parent           │
│    • Create 2 offspring                                     │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 5. MUTATION                                                 │
│    • Mutate unified params (swap/group/ungroup)             │
│    • Combines permutation and grouping mutations            │
│    • Probability: mutation_rate                             │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 6. ELITISM                                                  │
│    • Keep N best individuals from previous generation       │
│    • Fill rest with offspring                               │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
                 ┌───────┴───────┐
                 │ More gens?    │
                 └───┬───────┬───┘
                 Yes │       │ No
                     │       │
                     ▼       ▼
                 [Loop]  [Return Best]
```

### 3.5 Optimization Rules Detail

Optimization rules mengimplementasikan equivalency rules untuk query transformation.

#### Rule 3: Projection Elimination (Deterministic)

**Equivalency:**  
`PROJECT_1(PROJECT_2(Source))` ≡ `PROJECT_1(Source)`

**Timing:** Rule 3 dijalankan **SEKALI di awal** proses optimasi (sebelum genetic algorithm), tidak diikutkan dalam iterasi GA.

**Purpose:** Mengeliminasi nested projection yang redundant. Outer projection mengambil alih inner projection.

**Transformation:**
```
Before:
PROJECT(name, age)
└── PROJECT(*)
    └── RELATION(users)

After:
PROJECT(name, age)
└── RELATION(users)
```

**Implementation:**
- Executed in `optimize_query()` before genetic algorithm
- Not included in GA parameter space
- Deterministic (no variations)

#### Rule 7: Filter Pushdown over Join (Deterministic)

**Equivalency:**  
`FILTER(JOIN(R, S), cond)` → `JOIN(FILTER(R, cond_R), FILTER(S, cond_S))`

**Timing:** Applied **ONCE** after Rule 3, before Rule 8.

**Purpose:** Push filter conditions closer to data sources to reduce join size.

**Transformation:**
```
Before:
FILTER
├── JOIN
│   ├── RELATION(users)
│   └── RELATION(profiles)
└── AND
    ├── users.age > 18
    └── profiles.verified = true

After:
JOIN
├── FILTER
│   ├── RELATION(users)
│   └── users.age > 18
└── FILTER
    ├── RELATION(profiles)
    └── profiles.verified = true
```

**Benefits:**
- Reduces data volume before expensive join operation
- Each table filtered independently with relevant conditions
- Can push to left, right, or both sides depending on condition references

**Implementation:**
- Pattern detection: finds FILTER → JOIN structures
- Condition analysis: determines which filters reference which tables
- Push strategy: left/right/both/none based on table references
- Executed deterministically before GA

#### Rule 8: Projection over Join (Deterministic)

**Equivalency:**  
`PROJECT(cols, JOIN(R, S))` → `PROJECT(cols', JOIN(PROJECT(R_cols), PROJECT(S_cols)))`

**Timing:** Applied **ONCE** after Rule 7, before genetic algorithm.

**Purpose:** Push projections to join children to reduce tuple size before join.

**Transformation:**
```
Before:
PROJECT(name, age)
└── JOIN
    ├── RELATION(users)
    └── RELATION(profiles)

After:
PROJECT(name, age)
└── JOIN
    ├── PROJECT(id, name, age)  # Only needed columns
    │   └── RELATION(users)
    └── PROJECT(user_id)        # Only join key
        └── RELATION(profiles)
```

**Benefits:**
- Reduces tuple width before join
- Less data to transfer and compare during join
- Only required columns are projected from each relation

**Implementation:**
- Analyzes projected columns and join conditions
- Determines minimal column set for each relation
- Creates child projections under join
- Executed deterministically before GA

#### Rule 1 & 2: Unified Filter Params (Non-deterministic, Optimized by GA)

**Equivalency:**  
`σ(c1 ∧ c2 ∧ ... ∧ cn)(R)` ≡ `σ(cπ(1))(σ(cπ(2))(...(σ(cπ(n))(R))...))`

**Format:** `list[int | list[int]]`

- int: condition cascade sebagai single filter
- list[int]: conditions stay grouped dalam AND

**Transformasi:**  
OPERATOR(AND) dengan multiple conditions → Reordered + Cascaded filters

**Before:**

```
FILTER
├── RELATION("users")
└── OPERATOR("AND")
    ├── COMPARISON(">") [age > 25, idx=0]
    ├── COMPARISON("=") [status = 'active', idx=1]
    └── COMPARISON("=") [city = 'Jakarta', idx=2]
```

**After (dengan unified params [1, [0, 2]]):**

```
FILTER [status = 'active']  # idx=1 single (most selective)
└── FILTER
    ├── RELATION("users")
    └── OPERATOR("AND")      # [0,2] grouped
        ├── COMPARISON(">") [age > 25]
        └── COMPARISON("=") [city = 'Jakarta']
```

Penjelasan:

- Order: [1, [0, 2]] = reorder to [status, age, city]
- Cascade: 1 single, [0,2] grouped
- Result: Most selective filter first, others stay in AND

#### Unified Format Examples

**Example 1: All singles (full cascade)**

```python
params = [2, 1, 0]  # Reorder + all single filters
```

Result: `FILTER(c2) → FILTER(c1) → FILTER(c0) → RELATION`

**Example 2: Mixed (some grouped)**

```python
params = [2, [0, 1]]  # c2 single, [c0, c1] grouped
```

Result: `FILTER(c2) → FILTER(AND(c0, c1)) → RELATION`

**Example 3: All grouped (no cascade)**

```python
params = [[2, 1, 0]]  # All stay in AND (just reordered)
```

Result: `FILTER(AND(c2, c1, c0)) → RELATION`

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

### 3.7 Cost Estimation

Cost estimation menghitung estimasi biaya eksekusi query.

**TO DO**

### 3.8 Extending the Optimizer

#### Adding New Parameter Type

1. Create parameter functions in `rule_params_manager.py`:

```python
def analyze_for_my_params(query: ParsedQuery) -> dict:
    """Analyze query to find nodes for new param type."""
    # Find applicable nodes
    return {node_id: metadata}

def generate_my_params(metadata):
    """Generate random parameters."""
    # Return params structure
    return params

def mutate_my_params(params):
    """Mutate parameters."""
    # Return mutated params
    return mutated_params
```

2. Register in `RuleParamsManager`:

```python
manager.register_rule('my_params', {
    'analyze': analyze_for_my_params,
    'generate': generate_my_params,
    'mutate': mutate_my_params,
    'copy': lambda p: copy.deepcopy(p),
    'validate': lambda p, m: True
})
```

3. Update `_apply_transformations` in `genetic_optimizer.py`:

```python
if 'my_params' in self.operation_params:
    current_query = apply_my_transformation(
        current_query,
        self.operation_params['my_params']
    )
```

#### Custom Fitness Function

Create custom fitness function for specific optimization goals:

```python
from query_optimizer.optimization_engine import OptimizationEngine, ParsedQuery

def fitness_minimize_joins(query: ParsedQuery) -> float:
    """Prioritize minimizing number of joins."""
    joins = len(query.query_tree.find_nodes_by_type("JOIN"))
    filters = len(query.query_tree.find_nodes_by_type("FILTER"))

    # Heavy penalty for joins
    return (joins * 100) + (filters * 10)

engine = OptimizationEngine()
optimized = engine.optimize_query(query, fitness_func=fitness_minimize_joins)
```

```python
def fitness_selectivity(query: ParsedQuery) -> float:
    """Consider filter selectivity."""
    cost = 0
    filters = query.query_tree.find_nodes_by_type("FILTER")

    for f in filters:
        # Estimate selectivity based on condition
        if "=" in f.val:
            cost += 5  # Equality is selective
        elif ">" in f.val or "<" in f.val:
            cost += 20  # Range is less selective
        else:
            cost += 30  # Other conditions

    return float(cost)

engine = OptimizationEngine()
optimized = engine.optimize_query(query, fitness_func=fitness_selectivity)
```

---

## Appendix

### A. References

- **[Rule.md](Rule.md)**: Detail lengkap tentang tokenization, parsing rules, dan operator precedence
- **[Filter.md](Filter.md)**: Detail lengkap tentang FILTER structure dan logical operators
- Genetic Algorithms: Holland, J. H. (1992). Adaptation in Natural and Artificial Systems
- Query Optimization: Graefe, G. (1993). Query Evaluation Techniques for Large Databases

---

**Contributors:**

- FORTRAN - IF3140 - Institut Teknologi Bandung
