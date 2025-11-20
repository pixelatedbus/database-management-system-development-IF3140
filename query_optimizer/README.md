# Query Optimizer

---

## 1. Overview

Query Optimizer adalah modul yang bertanggung jawab untuk mengoptimalkan query SQL dengan menggunakan **Genetic Algorithm (GA)**. Modul ini mengimplementasikan kombinasi rules optimasi yang menghasilkan query dengan cost eksekusi terendah.

### Fitur Utama

- **SQL Parser**: Mengubah SQL string menjadi Query Tree representation
- **Genetic Algorithm Optimizer**: Optimasi query menggunakan rule equivalency
- **Query Tree Manipulation**: Transformasi dan manipulasi struktur query tree
- **Cost Estimation**: Estimasi cost eksekusi query
- **Optimization Rules**: Implementasi equivalency rules untuk query transformation

### Komponen Utama

```
query_optimizer/
├── tokenizer.py              # SQL tokenization
├── parser.py                 # SQL parsing ke Query Tree
├── query_tree.py             # Query Tree data structure
├── query_check.py            # Query validation
├── optimization_engine.py    # Main optimization engine
├── genetic_optimizer.py      # Genetic Algorithm implementation
├── seleksi_konjungtif.py     # Rule 1: Conjunctive selection
├── rules_registry.py         # Optimization rules registry
├── demo.py                   # Demo program
└── tests/                    # Unit tests
    ├── test_tokenizer.py
    ├── test_parser.py
    ├── test_rule_1.py
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

**Demo 2: Genetic Algorithm Optimization**

```bash
python -m query_optimizer.demo 2
```

Output:

- Original query tree & cost
- Optimized query tree & cost
- Improvement statistics
- Best solution (filter orders & applied rules)
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
    filter_orders: dict[int, list[int]]  # Urutan filter
    applied_rules: list[str]             # Rules yang diterapkan
    query: ParsedQuery                    # Query hasil
    fitness: float                        # Cost (semakin rendah semakin baik)
```

Contoh:

```python
Individual(
    filter_orders={0: [2, 0, 1]},  # Terapkan filter 2, 0, 1 secara berurutan
    applied_rules=["seleksi_konjungtif"],
    fitness=220.0
)
```

**Population**

Kumpulan individu (kromosom) yang merepresentasikan solusi berbeda:

```
Population (size=50):
├── Individual 1: orders=[0,1,2], rules=["seleksi_konjungtif"], fitness=250
├── Individual 2: orders=[2,1,0], rules=[], fitness=230
├── Individual 3: orders=[1,0,2], rules=["seleksi_konjungtif"], fitness=240
└── ... (47 more individuals)
```

#### Genetic Algorithm Workflow

```
┌─────────────────────────────────────────────────────────────┐
│ 1. INITIALIZATION                                           │
│    • Analyze query for AND filters                          │
│    • Generate random population:                            │
│      - Random filter orders                                 │
│      - Random optimization rules                            │
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
│ 4. CROSSOVER (Order Crossover)                              │
│    • Combine filter orders from parents                     │
│    • Mix optimization rules                                 │
│    • Create 2 offspring                                     │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 5. MUTATION                                                 │
│    • Swap 2 positions in filter order                       │
│    • Add/remove/replace rules                               │
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

#### Rule 1: Seleksi Konjungtif (Conjunctive Selection)

**Equivalency:**  
`σ(c1 ∧ c2 ∧ ... ∧ cn)(R)` ≡ `σ(c1)(σ(c2)(...(σ(cn)(R))...))`

**Transformasi:**  
OPERATOR_S(AND) dengan multiple conditions → Cascaded filters

**Before:**

```
OPERATOR_S("AND")
├── RELATION("users")
├── FILTER("WHERE age > 25")
├── FILTER("WHERE status = 'active'")
└── FILTER("WHERE city = 'Jakarta'")
```

**After:**

```
FILTER("WHERE city = 'Jakarta'")
└── FILTER("WHERE status = 'active'")
    └── FILTER("WHERE age > 25")
        └── RELATION("users")
```

#### Rule 2: Seleksi Komutatif (Commutative Selection)

**Equivalency:**  
`σ(c1)(σ(c2)(R))` ≡ `σ(c2)(σ(c1)(R))`

**Transformasi:**  
Swap urutan filter berurutan

**Before:**

```
FILTER("WHERE age > 25")
└── FILTER("WHERE status = 'active'")
    └── RELATION("users")
```

**After:**

```
FILTER("WHERE status = 'active'")
└── FILTER("WHERE age > 25")
    └── RELATION("users")
```

### 3.5 Query Validation

Query validation memastikan query tree structure valid dan semantically correct.

```python
from query_optimizer.query_check import check_query

def check_query(query_tree: QueryTree) -> bool:
    """
    Validate query tree structure.

    Checks:
    - Unary operators have 1 child
    - Binary operators have 2 children
    - Leaf nodes have 0 children
    - FILTER has 1-2 children with correct types
    - Required clauses present (e.g., SELECT has FROM)

    Raises:
        ValueError: If validation fails
    """
```

**Validation Rules:**

- **PROJECT**: Must have exactly 1 child
- **SORT**: Must have exactly 1 child
- **JOIN**: Must have exactly 2 children (both RELATION or tree)
- **FILTER** (Conditional Expressions):
  - **1 child**: Filter operator dengan source tree (misal: `WHERE ...` → RELATION)
  - **2 children**:
    - Child pertama: source tree (RELATION, JOIN, atau operator lain)
    - Child kedua: value (ARRAY untuk IN, RELATION/PROJECT untuk subquery)
  - **Value**: "WHERE ...", "IN ...", "EXIST" (TIDAK BOLEH "AND", "OR", "NOT")

- **OPERATOR_S** (Logical AND/OR dengan Source):
  - **≥3 children**: Child[0] = source, Children[1..N] = kondisi
  - **Child[0]**: Harus operator yang menghasilkan data (RELATION, JOIN, SORT, PROJECT, OPERATOR_S, FILTER dengan children)
  - **Child[0] TIDAK BOLEH**: OPERATOR (tidak ada data) atau FILTER leaf (tidak ada data)
  - **Children[1..N]**: Harus FILTER, OPERATOR, atau OPERATOR_S
  - **Value**: "AND" atau "OR" saja

  **Contoh dengan SORT sebagai source:**

  ```
  OPERATOR_S("AND")
  ├── SORT("name")              # Child 0: source (SORT operator)
  │   └── RELATION("users")
  ├── FILTER("WHERE age > 25")  # Child 1: kondisi
  └── FILTER("WHERE active")    # Child 2: kondisi
  ```

  SQL: `SELECT * FROM users WHERE age > 25 AND active ORDER BY name`

- **OPERATOR** (Nested Logical Operators):
  - **AND/OR**: ≥2 children, semua harus FILTER/OPERATOR/OPERATOR_S
  - **NOT**: Tepat 1 child, harus FILTER/OPERATOR/OPERATOR_S
  - **Tanpa source**: Mewarisi source dari parent OPERATOR_S
  - **Value**: "AND", "OR", atau "NOT"
  
  **Contoh nested:**
  ```
  OPERATOR_S("AND")
  ├── RELATION("users")
  ├── FILTER("WHERE age > 25")
  └── OPERATOR("OR")            # Nested, mewarisi source
      ├── FILTER("WHERE city = 'Jakarta'")
      └── FILTER("WHERE city = 'Bandung'")
  ```

- **RELATION, ARRAY, LIMIT**: Must have 0 children
- **UPDATE, INSERT, DELETE**: Must have exactly 1 child (RELATION atau FILTER/OPERATOR_S → RELATION)

**Important Notes:**

- Logical operators (AND/OR/NOT) TIDAK menggunakan FILTER, tapi menggunakan OPERATOR atau OPERATOR_S
- OPERATOR_S digunakan ketika logical operator memiliki explicit source
- OPERATOR digunakan untuk nested logic yang mewarisi source dari parent
- Equivalency rules (seperti seleksi konjungtif) menghasilkan query tree yang tetap valid setelah transformasi
- Validasi dilakukan secara rekursif pada seluruh tree structure

### 3.6 Cost Estimation

Cost estimation menghitung estimasi biaya eksekusi query.

**TO DO**

### 3.7 Extending the Optimizer

#### Adding New Optimization Rule

1. Create rule function in `rules_registry.py`:

```python
def rule_my_new_rule(query: ParsedQuery) -> ParsedQuery:
    """
    Description of the rule.
    Equivalency: σ(...) ≡ σ(...)
    """
    # Clone tree to avoid modifying original
    cloned = clone_tree(query.query_tree)

    # Apply transformation
    transformed = my_transformation(cloned)

    return ParsedQuery(transformed, query.query)
```

2. Register in `ALL_RULES`:

```python
ALL_RULES = [
    ("seleksi_konjungtif", rule_seleksi_konjungtif),
    ("seleksi_komutatif", rule_seleksi_komutatif),
    ("my_new_rule", rule_my_new_rule),  # Add here
]
```

3. Use in Genetic Algorithm:

```python
# GA will automatically use all registered rules
engine = OptimizationEngine()
optimized = engine.optimize_query(query)
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
