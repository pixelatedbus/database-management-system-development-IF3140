# Query Optimizer

---

## 1. Overview

Query Optimizer adalah modul yang bertanggung jawab untuk mengoptimalkan query SQL dengan menggunakan **Genetic Algorithm (GA)**. Modul ini mengimplementasikan kombinasi rules optimasi yang menghasilkan query dengan cost eksekusi terendah.

### Fitur Utama

- **SQL Parser**: Mengubah SQL string menjadi Query Tree representation
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
├── rule_1.py                 # Filter cascading transformation
├── rules_registry.py         # Optimization rules registry
├── demo.py                   # Demo program
└── tests/                    # Unit tests
    ├── test_tokenizer.py
    ├── test_parser.py
    ├── test_rule_1.py
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

Demo program menyediakan 2 mode:

**Demo 1: Basic Parsing**

```bash
python -m query_optimizer.demo 1
```

Output:

- Menampilkan SQL query original
- Query Tree structure
- Estimated cost

**Demo 2-4: Genetic Algorithm Optimization**

```bash
python -m query_optimizer.demo 2  # Rule 1 only
python -m query_optimizer.demo 3  # Basic GA
python -m query_optimizer.demo 4  # GA with unified filter params
```

Output:

- Original query tree & cost
- Optimized query tree & cost
- Improvement statistics
- Best solution (unified filter_params: reorder + cascade)
- Evolution progress per generation

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

#### Genetic Algorithm Optimization

```python
from query_optimizer.optimization_engine import OptimizationEngine, ParsedQuery
from query_optimizer.query_tree import QueryTree

# Initialize engine
engine = OptimizationEngine()

# Build query tree (manual atau dari parser)
relation = QueryTree("RELATION", "users")
filter1 = QueryTree("FILTER", "WHERE age > 25")
filter2 = QueryTree("FILTER", "WHERE status = 'active'")
filter3 = QueryTree("FILTER", "WHERE city = 'Jakarta'")

# Create OPERATOR_S structure dengan explicit source
operator_and = QueryTree("OPERATOR_S", "AND")
operator_and.add_child(relation)  # Child 0: source
operator_and.add_child(filter1)   # Child 1+: kondisi
operator_and.add_child(filter2)
operator_and.add_child(filter3)

project = QueryTree("PROJECT", "*")
project.add_child(operator_and)

query = ParsedQuery(project, "SELECT * FROM users WHERE ...")

# Optimize using Genetic Algorithm (integrated in OptimizationEngine)
optimized_query = engine.optimize_query(
    query,
    use_genetic=True,        # Enable GA optimization
    population_size=50,      # Ukuran populasi
    generations=100,         # Jumlah generasi
    mutation_rate=0.1,       # Probabilitas mutasi (0.0-1.0)
    crossover_rate=0.8,      # Probabilitas crossover (0.0-1.0)
    elitism=2               # Jumlah individu terbaik yang dipertahankan
)

# Calculate costs
original_cost = engine.get_cost(query)
optimized_cost = engine.get_cost(optimized_query)

print(f"Original Cost: {original_cost}")
print(f"Optimized Cost: {optimized_cost}")
print(f"Improvement: {original_cost - optimized_cost}")
```

**Alternative: Quick Optimization dengan Default Settings**

```python
# Simpler usage with default GA parameters
engine = OptimizationEngine()
query = engine.parse_query("SELECT * FROM users WHERE age > 25 AND status = 'active'")

# Optimize with default settings (population_size=50, generations=100)
optimized = engine.optimize_query(query)

print(f"Cost: {engine.get_cost(query)} → {engine.get_cost(optimized)}")
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
optimized = engine.optimize_query(
    query,
    population_size=30,
    generations=50,
    fitness_func=custom_fitness  # Use custom function
)

print(f"Optimized with custom fitness: {custom_fitness(optimized)}")
```

#### Using Optimization Rules

```python
from query_optimizer.rules_registry import get_all_rules, apply_random_rule

# Get all available rules
rules = get_all_rules()
for name, func in rules:
    print(f"Rule: {name}")

# Apply specific rule
from query_optimizer.seleksi_konjungtif import seleksi_konjungtif
optimized_query = seleksi_konjungtif(query)

# Apply random rule
transformed_query, rule_name = apply_random_rule(query)
print(f"Applied rule: {rule_name}")
```

### 2.4 Running Tests

```bash
# Run all tests
python -m unittest discover query_optimizer/tests

# Run specific test file
python -m unittest query_optimizer.tests.test_tokenizer
python -m unittest query_optimizer.tests.test_parser
python -m unittest query_optimizer.tests.test_rule_1

# Run with verbose output
python -m unittest query_optimizer.tests.test_rule_1 -v
```

---

## 3. Deskripsi Implementasi Detail

### 3.1 Query Tree Structure

Query Tree adalah representasi internal dari SQL query dalam bentuk tree structure. Setiap node merepresentasikan operator atau operand dalam query.

#### Node Types & Categories

**UNARY OPERATORS (1 child):**

- `PROJECT` - Selection projection (SELECT clause)
- `SORT` - Ordering (ORDER BY clause)

**BINARY OPERATORS (2 children):**

- `JOIN` - Join operations

**LEAF NODES (0 children):**

- `RELATION` - Table reference
- `ARRAY` - Array literal (untuk IN clause)
- `LIMIT` - Limit value

**LOGICAL_OPERATORS (custom rules):**

- `OPERATOR_S` - Logical AND/OR dengan explicit source (≥3 children)
- `OPERATOR` - Nested AND/OR/NOT tanpa explicit source (AND/OR: ≥2 children, NOT: 1 child)

**SPECIAL OPERATORS (custom rules):**

- `FILTER` - Conditional expressions (WHERE/IN/EXIST) (1-2 children)
- `UPDATE` - Update operation (1 child)
- `INSERT` - Insert operation (1 child)
- `DELETE` - Delete operation (1 child)
- `BEGIN_TRANSACTION` - Transaction start (0+ children)

#### Node Details

**FILTER Node (Conditional Expressions)**

FILTER adalah node untuk conditional expressions (WHERE/IN/EXIST):

**Pattern 1: Single Child (WHERE condition)**

```
FILTER("WHERE condition")
└── <any_operator>   # Bisa RELATION, JOIN, SORT, dll
```

**Pattern 2: Two Children (IN/EXISTS)**

```
FILTER("IN column")
├── <source_tree>    # Source data
└── ARRAY("(1,2,3)") # Value list
```

**OPERATOR_S Node (Logical Operators dengan Source)**

OPERATOR_S untuk logical operations (AND/OR) dengan explicit source:

```
OPERATOR_S("AND")
├── <source_tree>           # Child 0: source (eksplisit)
├── FILTER("WHERE cond1")   # Child 1: kondisi 1
├── FILTER("WHERE cond2")   # Child 2: kondisi 2
└── FILTER("WHERE cond3")   # Child N: kondisi N
```

**OPERATOR Node (Nested Logical Operators)**

OPERATOR untuk nested logic (AND/OR/NOT) yang mewarisi source dari parent:

```
OPERATOR("NOT")
└── FILTER("WHERE condition")  # Negasi kondisi
```

Lihat **[Filter.md](Filter.md)** untuk detail lengkap tentang OPERATOR, OPERATOR_S, dan FILTER structure.

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

#### Tokenizer

Tokenizer melakukan lexical analysis, memecah SQL string menjadi tokens.

**Token Types:**

- Keywords: `SELECT`, `FROM`, `WHERE`, `JOIN`, `AND`, `OR`, dll
- Operators: `=`, `>`, `<`, `>=`, `<=`, `<>`, `+`, `-`, `*`, `/`
- Identifiers: table names, column names
- Literals: strings, numbers, booleans
- Delimiters: `,`, `;`, `(`, `)`

**Example:**

```python
from query_optimizer.tokenizer import Tokenizer

sql = "SELECT id, name FROM users WHERE age > 25"
tokenizer = Tokenizer(sql)

# Tokenizer akan menghasilkan:
# [SELECT] [id] [,] [name] [FROM] [users] [WHERE] [age] [>] [25]
```

#### Parser

Parser melakukan syntax analysis dan membangun Query Tree.

**Parsing Steps:**

1. Parse SELECT → Create PROJECT node
2. Parse FROM → Create RELATION node
3. Parse WHERE → Create FILTER node(s)
4. Parse JOIN → Create JOIN node
5. Parse ORDER BY → Create SORT node
6. Validate structure

**Example:**

```python
from query_optimizer.parser import Parser
from query_optimizer.tokenizer import Tokenizer

sql = "SELECT * FROM users WHERE age > 25"
tokenizer = Tokenizer(sql)
parser = Parser(tokenizer)

query_tree = parser.parse()
print(query_tree.tree())
```

Lihat **[Rule.md](Rule.md)** dan **[Filter.md](Filter.md)** untuk detail lengkap.

### 3.3 Genetic Algorithm Implementation

Genetic Algorithm adalah metode optimasi yang terinspirasi dari evolusi biologis.

Rule yang sudah ada:

- Seleksi Konjungtif

Rule yang belum ada:

- Seleksi komutatif
- Proyeksi Terakhir
- Gabungkan Seleksi dan Join
- Join komutatif
- Join asosiatif
- Pushdown Seleksi
- Pushdown Proyeksi

#### Core Concepts

**Individual (Kromosom)**

```python
class Individual:
    operation_params: dict[str, dict[int, Any]]  # Unified parameters
    query: ParsedQuery                            # Query hasil
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

### 3.4 Optimization Rules

Optimization rules mengimplementasikan equivalency rules untuk query transformation.

#### Unified Filter Params (Reordering + Cascading)

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

### 3.5 Query Validation

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
- **PROJECT**: ≥1 children. Last child = source. If value = "*", must have exactly 1 child (source only)
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
- **ARITH_EXPR**: 2 children (left, right). Value = operator ("+", "-", "*", "/", "%")

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

- Atomic nodes (IDENTIFIER, LITERAL_*) are leaf nodes with no children
- Wrapper nodes (COLUMN_NAME, TABLE_NAME) wrap IDENTIFIER
- COLUMN_REF represents column references (simple or qualified with table/alias)
- FILTER has exactly 2 children: source + condition tree
- Logical operators use OPERATOR node with AND/OR/NOT values
- Comparison and condition expressions are separate node types (COMPARISON, IN_EXPR, etc.)
- Validation is performed recursively on the entire tree structure

### 3.6 Cost Estimation

Cost estimation menghitung estimasi biaya eksekusi query.

**TO DO**

### 3.7 Extending the Optimizer

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
