# Detail Implementasi Query Optimizer dengan Genetic Algorithm

## Overview

Query Optimizer ini menggunakan **Genetic Algorithm (GA)** untuk mencari struktur query optimal dengan menerapkan transformasi berbasis rules. Sistem ini mengimplementasikan **unified filter_params** yang menggabungkan:

1. **Reordering**: Mengubah urutan kondisi dalam OPERATOR(AND)
2. **Cascading**: Transformasi OPERATOR(AND) menjadi cascaded FILTERs

**Format Unified**: `list[int | list[int]]` - Single int untuk cascade individual, list untuk grouping dalam AND

---

## Arsitektur Sistem

```
┌─────────────────────────────────────────────────────────────┐
│                    Genetic Optimizer                        │
│  ┌───────────────────────────────────────────────────────┐  │
│  │              Population (Individuals)                 │  │
│  │  ┌──────────────────────────────────────────────┐    │  │
│  │  │  Individual (Kromosom)                       │    │  │
│  │  │  - operation_params: {                       │    │  │
│  │  │      'filter_params': {node_id: order},     │    │  │
│  │  │      'join_params': {...}                    │    │  │
│  │  │    }                                         │    │  │
│  │  │  - fitness: float                            │    │  │
│  │  └──────────────────────────────────────────────┘    │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│              Rule Params Manager                            │
│  - Generate unified filter_params (reorder + cascade)      │
│  - Mutate params (permutation + grouping)                  │
│  - Copy params                                              │
│  - Validate params                                          │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼
              ┌──────────────────────┐
              │  Unified Transform   │
              │  (Reorder + Cascade) │
              └──────────────────────┘
```

---

## Workflow Genetic Algorithm

### 1. Inisialisasi

```
Input: Base Query (ParsedQuery)
Output: Population (List[Individual])

1. Analisis query untuk menemukan OPERATOR(AND) nodes
   - Menggunakan rule_params_manager.analyze_query()
   - Hasil: Dict[rule_name, Dict[node_id, metadata]]

2. Untuk setiap individu dalam populasi:
   a. Generate random params untuk setiap rule
   b. Create Individual dengan rule_params
   c. Tambahkan ke population

Example Analysis Result:
{
  'filter_params': {42: 3}  # Node 42 memiliki 3 kondisi
}

Example Generated Params (Unified Format):
{
  'filter_params': {
    42: [2, [0, 1]]  # Unified: reorder to [2,0,1] + cascade with 2 single, [0,1] grouped
  }
}

Penjelasan Format:
- Order: [2, [0, 1]] berarti kondisi index 2 duluan, kemudian [0,1]
- Cascading: int = single filter, list = grouped dalam AND
- Reordering: urutan elemen dalam list
```

### 2. Evaluasi Fitness

```
Untuk setiap Individual:
1. Apply transformations (Rule 2 → Rule 1 → Other rules)
2. Calculate fitness (cost function)
3. Store fitness value

Lower fitness = Better query structure
```

### 3. Selection (Tournament Selection)

```
Input: Population, tournament_size=3
Output: Selected Individual

1. Random sample tournament_size individuals
2. Return individual dengan fitness terendah

Keuntungan:
- Simple dan efisien
- Mempertahankan diversity
- Pressure seleksi dapat dikontrol via tournament_size
```

### 4. Crossover (Uniform Crossover)

```
Input: Parent1, Parent2
Output: Child1, Child2

1. Untuk setiap rule dalam (parent1.rules ∪ parent2.rules):
   if random() < 0.5:
     child1[rule] = deep_copy(parent1[rule])
     child2[rule] = deep_copy(parent2[rule])
   else:
     child1[rule] = deep_copy(parent2[rule])
     child2[rule] = deep_copy(parent1[rule])

2. Create new Individuals dengan lazy_eval=True
3. Return (child1, child2)

Example (Unified Format):
Parent1: {filter_params: {42: [0, 1, 2]}}  # All singles, original order
Parent2: {filter_params: {42: [2, [0, 1]]}}  # Reordered with grouping

Possible Child1: {filter_params: {42: [2, [0, 1]]}}  # From Parent2
Possible Child2: {filter_params: {42: [0, 1, 2]}}  # From Parent1
```

### 5. Mutation

```
Input: Individual, mutation_rate
Output: Mutated Individual

1. Shallow copy rule_params
2. If mutated_params not empty:
   a. Random pilih rule_name
   b. Random pilih node_id
   c. Deep copy params untuk rule tersebut
   d. Mutate params menggunakan rule-specific mutation

3. Create new Individual dengan lazy_eval=True

Mutation Rate: 0.1 - 0.2 (typical)
```

### 6. Evolution Loop

```
For generation in range(generations):
  1. Evaluate fitness untuk semua individuals

  2. Sort population by fitness (ascending)

  3. Track best individual

  4. Create next generation:
     a. Elitism: Keep top N individuals
     b. While population not full:
        - Select parent1, parent2 via tournament
        - If random() < crossover_rate:
            child1, child2 = crossover(parent1, parent2)
          Else:
            child1, _ = crossover(parent1, parent1)
            child2, _ = crossover(parent2, parent2)
        - If random() < mutation_rate:
            mutate(child1)
            mutate(child2)
        - Add to next_population

  5. population = next_population

  6. Record statistics

Return: Best Individual found
```

---

## Unified Filter Params (Reorder + Cascade)

### Konsep

**Format unified** menggabungkan reordering dan cascading dalam satu parameter `list[int | list[int]]`. Transformasi `FILTER dengan OPERATOR(AND)` menjadi **cascaded FILTERs** dengan urutan optimal.

```
Input:
FILTER
├── RELATION(users)
└── OPERATOR(AND)
    ├── condition_0 (age > 25)
    ├── condition_1 (status = 'active')
    └── condition_2 (city = 'Jakarta')

Output (all single):
FILTER
├── FILTER
│   ├── FILTER
│   │   ├── RELATION(users)
│   │   └── condition_0
│   └── condition_1
└── condition_2

Output (mixed [2, [0,1]]):
FILTER
├── FILTER
│   ├── RELATION(users)
│   └── OPERATOR(AND)
│       ├── condition_0
│       └── condition_1
└── condition_2
```

### Workflow Unified Filter Params

```
1. Analyze Query
   Input: ParsedQuery
   Output: Dict[node_id, num_conditions]

   Process:
   - Traverse query tree
   - Find all FILTER nodes dengan OPERATOR(AND) child
   - Count conditions dalam setiap AND operator
   - Return mapping: {operator_id: num_conditions}

2. Generate Random Params (Unified)
   Input: num_conditions
   Output: List[int | List[int]]

   Process:
   - Create permutation [0, 1, ..., n-1]
   - Random shuffle (untuk reordering)
   - Random group beberapa conditions (untuk cascading)

   Example untuk 3 conditions:
   - [0, 1, 2]           # All single, original order
   - [2, [0, 1]]         # Reorder: 2 first, then [0,1] grouped
   - [[0, 1, 2]]         # All grouped (no cascade)
   - [1, [0, 2]]         # Reorder: 1 first (single), [0,2] grouped

3. Apply Transformation (Unified)
   Input: ParsedQuery, operator_orders
   Output: Transformed ParsedQuery

   Process:
   a. Flatten order untuk reordering
      flat_order = [2, 0, 1] dari [2, [0, 1]]
   b. Reorder conditions menggunakan flat_order
      reordered_query = reorder_and_conditions(query, flat_order)
   c. Apply cascade menggunakan original structure [2, [0, 1]]
      cascaded_query = cascade_filters(reordered_query, operator_orders)

   cascade_filters(query, operator_orders):
     For each AND operator in operator_orders:
       - Get unified order untuk operator ini
       - Build cascaded structure:
         * Single items (int) → individual FILTERs
         * List items (list[int]) → FILTER dengan AND(grouped conditions)
       - Replace original FILTER node

4. Mutation (Unified)
   Strategies combine permutation and grouping:
   - swap: Swap 2 positions (affects reordering)
   - group: Combine 2 single conditions (affects cascading)
   - ungroup: Split group menjadi singles (affects cascading)
   - regroup: Split dan recombine groups

   Example mutations:
   [0, 1, 2] --swap(0,2)--> [2, 1, 0]  # Reordering mutation
   [0, 1, 2] --group(1,2)--> [0, [1, 2]]  # Cascading mutation
   [2, [0, 1]] --ungroup--> [2, 0, 1]  # Remove grouping
   [[0, 1], 2] --regroup--> [[0, 2], 1]  # Change grouping
```

### Parameter Format (Unified)

```python
# Unified order format: list[int | list[int]]
params = [2, [0, 1], 3]

Meaning:
- Position matters: order [2, [0,1], 3] berarti kondisi 2 duluan
- Index 2 → single FILTER (cascade)
- [0, 1] → grouped dalam OPERATOR(AND) (no cascade)
- Index 3 → single FILTER (cascade)

Reordering: Urutan elemen menentukan urutan eksekusi
Cascading: int vs list menentukan struktur (single vs grouped)

Constraints:
- Semua indices 0..n-1 harus ada (no duplicates)
- Bisa single (int) atau grouped (list[int])
- Order matters untuk selectivity dan structure
```

---

## Unified Format Details

### Reordering Component

Mengubah **urutan kondisi** dalam OPERATOR(AND) berdasarkan prinsip komutatif: `A AND B = B AND A`

Dalam unified format, reordering ditentukan oleh **urutan elemen dalam list**.

```
Input:
OPERATOR(AND)
├── condition_0 (selectivity=0.5)
├── condition_1 (selectivity=0.1)  <- most selective
└── condition_2 (selectivity=0.8)

Output (optimal: [1, 0, 2]):
OPERATOR(AND)
├── condition_1 (selectivity=0.1)  <- most selective first
├── condition_0 (selectivity=0.5)
└── condition_2 (selectivity=0.8)
```

### Cascading Component

Dalam unified format, cascading ditentukan oleh **type elemen: int vs list[int]**.

### Combined Workflow

```
1. Analyze Query (sama untuk keduanya)
   Input: ParsedQuery
   Output: Dict[node_id, num_conditions]

   Process:
   - Traverse query tree
   - Find all OPERATOR(AND) nodes
   - Count children untuk setiap operator
   - Return mapping: {operator_id: num_conditions}

2. Generate Random Params (Unified)
   Input: num_conditions
   Output: List[int | List[int]]

   Process:
   - Create permutation [0, 1, ..., n-1]
   - Random shuffle (determines reordering)
   - Random group (determines cascading)

   Example untuk 3 conditions:
   - [0, 1, 2]      # Original order, all singles
   - [2, 0, 1]      # Reordered, all singles
   - [1, [2, 0]]    # Reordered with grouping
   - [[2, 1, 0]]    # All grouped (reordered inside group)

3. Apply Transformation (Unified)
   Input: ParsedQuery, operator_orders (unified format)
   Output: Transformed ParsedQuery

   Process:
   _apply_transformations(query, unified_orders):
     a. Flatten order: [2, [0, 1]] → [2, 0, 1]
     b. Reorder conditions:
        reorder_and_conditions(query, {node_id: [2, 0, 1]})
     c. Apply cascade structure:
        cascade_filters(query, {node_id: [2, [0, 1]]})

   Benefit:
   - Single parameter controls both operations
   - Natural representation: order + structure
   - Easier mutation: change order or grouping

4. Validation
   Constraints untuk unified format:
   - All indices 0..n-1 must be present
   - No duplicates
   - Valid structure: int atau list[int]
   - Flattened version must be valid permutation

   Example validation:
   [2, [0, 1]] → flatten → [2, 0, 1] → valid ✓
   [2, [0, 0]] → flatten → [2, 0, 0] → invalid (duplicate) ✗
   [2, 3]      → flatten → [2, 3] → invalid (missing 0, 1) ✗
```

---

## Unified Transformation Pipeline

### Execution Flow

Unified format menerapkan transformasi dalam satu langkah terintegrasi:

```
Original Query
     │
     ▼
┌────────────────────────────────────┐
│  Parse Unified Params              │
│  Input: [2, [0, 1]]                │
│  Extract:                          │
│  - Flat order: [2, 0, 1]           │
│  - Structure: 2→single, [0,1]→AND  │
└────────────────────────────────────┘
     │
     ▼
┌────────────────────────────────────┐
│  Apply Reordering                  │
│  Use flat order untuk reorder      │
│  conditions: [2, 0, 1]             │
└────────────────────────────────────┘
     │
     ▼
┌────────────────────────────────────┐
│  Apply Cascading                   │
│  Use structure: [2, [0, 1]]        │
│  Build cascaded filters            │
└────────────────────────────────────┘
     │
     ▼
Optimized Query
```

### Workflow `_apply_transformations` (Unified)

```python
def _apply_transformations(base_query):
    # 1. Clone tree untuk isolasi
    cloned_tree = clone_tree(base_query.query_tree)
    current_query = ParsedQuery(cloned_tree, base_query.query)

    # 2. Apply unified filter_params transformation
    if 'filter_params' in operation_params and operation_params['filter_params']:
        # 2a. Uncascade existing filters ke AND
        query_and = uncascade_filters(current_query)

        unified_orders = operation_params['filter_params']

        # 2b. Flatten untuk reordering
        flat_orders = {}
        for node_id, order in unified_orders.items():
            flat_orders[node_id] = flatten_order(order)  # [2, [0,1]] → [2,0,1]

        # 2c. Apply reordering
        query_reordered = reorder_and_conditions(
            query_and,
            operator_orders=flat_orders
        )

        # 2d. Apply cascading dengan original structure
        current_query = cascade_filters(
            query_reordered,
            operator_orders=unified_orders  # Uses [2, [0,1]] structure
        )

    # 3. Apply join_params if present
    if 'join_params' in operation_params and operation_params['join_params']:
        current_query = apply_join_transformations(
            current_query,
            operation_params['join_params']
        )

    return current_query
```

### Example Lengkap

```
Input Query:
SELECT * FROM users
WHERE age > 25 AND status = 'active' AND city = 'Jakarta'

Query Tree:
FILTER
├── RELATION(users)
└── OPERATOR(AND)
    ├── COMPARISON(>) [age > 25, selectivity=0.3]
    ├── COMPARISON(=) [status = 'active', selectivity=0.1]
    └── COMPARISON(=) [city = 'Jakarta', selectivity=0.5]

Individual Params (Unified):
{
  'filter_params': {
    42: [1, [0, 2]]  # Unified: reorder [status, age, city] + cascade: 1 single, [0,2] grouped
  }
}

Interpretasi:
- Order: [1, [0, 2]] berarti kondisi index 1 duluan, kemudian [0, 2]
- Original: [0=age, 1=status, 2=city]
- Reordered: [1=status, 0=age, 2=city] (status paling selektif)
- Cascade: 1 single (status), [0,2] grouped (age AND city)

Step 1 - Flatten & Reorder:
FILTER
├── RELATION(users)
└── OPERATOR(AND)
    ├── COMPARISON(=) [status = 'active', selectivity=0.1]  ← Most selective
    ├── COMPARISON(>) [age > 25, selectivity=0.3]
    └── COMPARISON(=) [city = 'Jakarta', selectivity=0.5]

Step 2 - Apply Cascade (structure [1, [0,2]]):
FILTER [status = 'active']  ← Paling selektif, filter pertama
├── FILTER
│   ├── RELATION(users)
│   └── OPERATOR(AND)  ← Age dan city masih grouped
│       ├── COMPARISON(>) [age > 25]
│       └── COMPARISON(=) [city = 'Jakarta']
└── COMPARISON(=) [status = 'active']

Result:
- Most selective condition filtered first
- Remaining conditions grouped (bisa dioptimasi lebih lanjut)
```

---

## Rule Params Manager

### Responsibility

Centralized management untuk parameter semua rules:

```python
class RuleParamsManager:
    def __init__(self):
        self.rules = {
            'filter_params': {
                'analyze': analyze_and_operators,
                'generate': generate_filter_params,  # Unified generation
                'mutate': mutate_filter_params,      # Combined mutation
                'copy': copy_filter_params,
                'validate': validate_filter_params
            },
            'join_params': {
                'analyze': analyze_joins,
                'generate': generate_join_params,
                'mutate': mutate_join_params,
                'copy': copy_join_params,
                'validate': validate_join_params
            }
        }
```

### Methods

#### 1. `analyze_query(query, rule_name)`

```
Purpose: Analyze query untuk rule tertentu
Input: ParsedQuery, rule_name
Output: Dict[node_id, metadata]

Example:
analyze_query(query, 'filter_params')
→ {42: 3, 57: 2}  # Node 42 has 3 conditions, Node 57 has 2
```

#### 2. `generate_random_params(rule_name, metadata)`

```
Purpose: Generate random params untuk node
Input: rule_name, metadata (e.g., num_conditions)
Output: Rule-specific params

Example:
generate_random_params('filter_params', 3)
→ [2, [0, 1]]  # Unified: reordered + grouped
→ [1, 0, 2]    # Unified: reordered, all singles
→ [[0, 2], 1]  # Unified: mixed structure
```

#### 3. `mutate_params(rule_name, params)`

```
Purpose: Mutate params menggunakan rule-specific strategy
Input: rule_name, current_params
Output: Mutated params

Example:
mutate_params('filter_params', [0, 1, 2])
→ [0, [1, 2]]  # Grouped 1 and 2 (cascade mutation)
→ [2, 1, 0]    # Reordered (permutation mutation)
→ [1, [0, 2]]  # Combined: reorder + group
```

#### 4. `copy_params(rule_name, params)`

```
Purpose: Deep copy params
Input: rule_name, params
Output: Copied params

Ensures mutations don't affect original
```

#### 5. `validate_params(rule_name, params, metadata)`

```
Purpose: Validate params format
Input: rule_name, params, metadata
Output: bool

Checks:
- All indices present
- No duplicates
- Valid structure
```

---

## Fitness Function

### Default Implementation

```python
def _default_fitness(query: ParsedQuery) -> float:
    """Lower is better"""
    engine = OptimizationEngine()
    cost = engine.get_cost(query)
    return float(cost)
```

### Custom Fitness Examples

```python
# Example 1: Node count (prefer cascaded structure)
def node_count_fitness(query: ParsedQuery) -> float:
    def count_nodes(node):
        if node is None: return 0
        return 1 + sum(count_nodes(child) for child in node.childs)

    count = count_nodes(query.query_tree)

    # Count FILTERs (reward cascading)
    def count_filters(node):
        if node is None: return 0
        count = 1 if node.type == "FILTER" else 0
        return count + sum(count_filters(child) for child in node.childs)

    filters = count_filters(query.query_tree)

    # Lower is better: fewer nodes, more filters
    return float(count - filters * 2)

# Example 2: Estimated selectivity
def selectivity_fitness(query: ParsedQuery) -> float:
    # Calculate estimated rows after each filter
    # Reward queries that filter most rows early
    estimated_cost = calculate_selectivity_cost(query)
    return float(estimated_cost)
```

---

## Performance Optimizations

### 1. Lazy Evaluation

```python
class Individual:
    def __init__(self, rule_params, base_query, lazy_eval=False):
        self._query_cache = None
        if not lazy_eval:
            self._query_cache = self._apply_transformations(base_query)

    @property
    def query(self):
        if self._query_cache is None:
            self._query_cache = self._apply_transformations(self.base_query)
        return self._query_cache
```

**Benefit**: Query hanya di-transform saat dibutuhkan (fitness evaluation)

### 2. Shallow Copy untuk Mutation

```python
# Fast shallow copy
mutated_params = {k: v.copy() for k, v in individual.rule_params.items()}

# Deep copy hanya yang dimutate
if node_params:
    mutated_params[rule_name] = copy.deepcopy(mutated_params[rule_name])
    mutated_params[rule_name][node_id] = manager.mutate_params(...)
```

**Benefit**: Reduce memory allocation dan copy overhead

### 3. Elitism

```python
# Keep best N individuals
next_population.extend(population[:self.elitism])
```

**Benefit**: Guarantee best solution tidak hilang antar generasi

---

## Hyperparameters

### Recommended Values

```python
GeneticOptimizer(
    population_size=50,      # Larger = more diversity, slower
    generations=100,         # More generations = better convergence
    mutation_rate=0.1,       # 0.05-0.2 typical
    crossover_rate=0.8,      # 0.7-0.9 typical
    elitism=2                # 1-5% of population
)
```

### Tuning Guidelines

| Parameter           | Low Value                 | High Value              |
| ------------------- | ------------------------- | ----------------------- |
| **population_size** | Fast, less diversity      | Slow, more exploration  |
| **generations**     | Quick results, suboptimal | Better results, slower  |
| **mutation_rate**   | Exploitation focus        | Exploration focus       |
| **crossover_rate**  | More mutation effect      | More parent inheritance |
| **elitism**         | More diversity            | Better convergence      |

---

## Testing Strategy

### 1. Unit Tests

- `test_rule_1.py`: Tests untuk cascading transformation
- `test_rule_params_manager.py`: Tests untuk unified param generation
- Test coverage: analyze, generate, mutate, apply transformation

### 2. Integration Tests

- `test_integration_rules.py`: Tests untuk low-level transformations
- Test reorder + cascade interaction
- Test dengan multiple AND operators
- Test edge cases

### 3. Genetic Algorithm Tests

- `test_genetic.py`: 9 tests untuk unified format
- Test population initialization dengan filter_params
- Test crossover preserves unified structure
- Test mutation changes unified params correctly
- Test full optimization run dengan unified format

---

## Extensibility

### Menambah Parameter Type Baru

```python
# 1. Tambah transformation baru (e.g., join optimization)
def analyze_joins(query: ParsedQuery) -> dict:
    # Find join nodes
    pass

def generate_join_params(metadata):
    # Generate random join orders
    pass

def mutate_join_params(params):
    # Mutate join parameters
    pass

def apply_join_transformations(query: ParsedQuery, params):
    # Apply join reordering
    pass

# 2. Register di rule_params_manager.py
manager.register_rule('join_params', {
    'analyze': analyze_joins,
    'generate': generate_join_params,
    'mutate': mutate_join_params,
    'copy': lambda p: copy.deepcopy(p),
    'validate': lambda p, m: True
})

# 3. Update _apply_transformations di genetic_optimizer.py
if 'join_params' in self.operation_params:
    current_query = apply_join_transformations(
        current_query,
        self.operation_params['join_params']
    )
```

---

## Conclusion

System ini mengimplementasikan query optimization menggunakan:

1. **Genetic Algorithm** untuk search space exploration
2. **Unified Filter Params** menggabungkan reordering dan cascading
3. **Rule Params Manager** untuk centralized parameter management
4. **Extensible Architecture** untuk parameter types baru (join_params, etc.)

Keuntungan unified format:

- **Simplicity**: Single parameter untuk reorder + cascade
- **Flexibility**: Order dan structure dikontrol bersamaan
- **Extensibility**: Mudah menambah parameter types baru
- **Performance**: Lazy evaluation dan optimization strategies
- **Maintainability**: Clean separation of concerns
- **Natural Representation**: list[int | list[int]] intuitif untuk reorder+group
