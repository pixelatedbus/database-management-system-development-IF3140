# Detail Implementasi Query Optimizer dengan Genetic Algorithm

## Overview

Query Optimizer ini menggunakan **Genetic Algorithm (GA)** untuk mencari struktur query optimal dengan menerapkan transformasi berbasis rules. Sistem ini mengimplementasikan dua rules utama:

1. **Rule 1 - Seleksi Konjunktif**: Transformasi OPERATOR(AND) menjadi cascaded FILTERs
2. **Rule 2 - Seleksi Komutatif**: Reordering kondisi dalam OPERATOR(AND)

---

## Arsitektur Sistem

```
┌─────────────────────────────────────────────────────────────┐
│                    Genetic Optimizer                        │
│  ┌───────────────────────────────────────────────────────┐  │
│  │              Population (Individuals)                 │  │
│  │  ┌──────────────────────────────────────────────┐    │  │
│  │  │  Individual (Kromosom)                       │    │  │
│  │  │  - rule_params: {rule_name: {node_id: ...}} │    │  │
│  │  │  - fitness: float                            │    │  │
│  │  └──────────────────────────────────────────────┘    │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│              Rule Params Manager                            │
│  - Mengelola parameter untuk semua rules                   │
│  - Generate random params                                   │
│  - Mutate params                                            │
│  - Copy params                                              │
└─────────────────────────────────────────────────────────────┘
                          │
            ┌─────────────┴─────────────┐
            ▼                           ▼
    ┌──────────────┐           ┌──────────────┐
    │   Rule 1     │           │   Rule 2     │
    │ (Konjunktif) │           │ (Komutatif)  │
    └──────────────┘           └──────────────┘
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
  'rule_1': {42: 3},  # Node 42 memiliki 3 kondisi
  'rule_2': {42: 3}   # Node 42 memiliki 3 kondisi
}

Example Generated Params:
{
  'rule_1': {42: [2, [0, 1]]},  # Cascade: cond2 single, [cond0, cond1] grouped
  'rule_2': {42: [1, 2, 0]}     # Reorder: [cond1, cond2, cond0]
}
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

Example:
Parent1: {rule_1: {42: [0,1,2]}, rule_2: {42: [1,0,2]}}
Parent2: {rule_1: {42: [2,[0,1]]}, rule_2: {42: [2,1,0]}}

Possible Child1: {rule_1: {42: [2,[0,1]]}, rule_2: {42: [1,0,2]}}
Possible Child2: {rule_1: {42: [0,1,2]}, rule_2: {42: [2,1,0]}}
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

## Rule 1: Seleksi Konjunktif (Cascading)

### Konsep

Transformasi `FILTER dengan OPERATOR(AND)` menjadi **cascaded FILTERs** untuk memanfaatkan selectivity ordering.

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

### Workflow Rule 1

```
1. Analyze Query
   Input: ParsedQuery
   Output: Dict[node_id, num_conditions]

   Process:
   - Traverse query tree
   - Find all FILTER nodes dengan OPERATOR(AND) child
   - Count conditions dalam setiap AND operator
   - Return mapping: {operator_id: num_conditions}

2. Generate Random Params
   Input: num_conditions
   Output: List[int | List[int]]

   Process:
   - Create permutation [0, 1, ..., n-1]
   - Random shuffle
   - Random group beberapa conditions

   Example untuk 3 conditions:
   - [0, 1, 2]           # All single
   - [2, [0, 1]]         # cond2 single, [0,1] grouped
   - [[0, 1, 2]]         # All grouped (no cascade)
   - [1, [0, 2]]         # cond1 single, [0,2] grouped

3. Apply Transformation
   Input: ParsedQuery, operator_orders
   Output: Transformed ParsedQuery

   Process:
   a. Uncascade existing FILTERs ke AND structure
   b. Apply cascade_filters dengan operator_orders

   cascade_filters(query, operator_orders):
     For each AND operator in operator_orders:
       - Get order untuk operator ini
       - Build cascaded structure:
         * Single items → individual FILTERs
         * List items → FILTER dengan AND(grouped conditions)
       - Replace original FILTER node

4. Mutation
   Strategies:
   - group: Combine 2 single conditions menjadi group
   - ungroup: Split group menjadi singles
   - regroup: Split one group dan buat groups baru

   Example mutations:
   [0, 1, 2] --group--> [0, [1, 2]]
   [2, [0, 1]] --ungroup--> [2, 0, 1]
   [[0, 1], 2] --regroup--> [[0, 2], 1]
```

### Parameter Format Rule 1

```python
# Mixed order (int | List[int])
params = [2, [0, 1], 3]

Meaning:
- Index 2 → single FILTER
- [0, 1] → grouped dalam OPERATOR(AND)
- Index 3 → single FILTER

Constraints:
- Semua indices 0..n-1 harus ada (no duplicates)
- Bisa single atau grouped
- Order matters untuk selectivity
```

---

## Rule 2: Seleksi Komutatif (Reordering)

### Konsep

Mengubah **urutan kondisi** dalam OPERATOR(AND) berdasarkan prinsip komutatif: `A AND B = B AND A`

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

### Workflow Rule 2

```
1. Analyze Query
   Input: ParsedQuery
   Output: Dict[node_id, num_conditions]

   Process:
   - Traverse query tree
   - Find all OPERATOR(AND) nodes
   - Count children untuk setiap operator
   - Return mapping: {operator_id: num_conditions}

2. Generate Random Params
   Input: num_conditions
   Output: List[int] (permutation)

   Process:
   - Create list [0, 1, ..., n-1]
   - Random shuffle

   Example untuk 3 conditions:
   - [0, 1, 2]  # Original order
   - [2, 0, 1]  # Rotated
   - [1, 2, 0]  # Shuffled
   - [2, 1, 0]  # Reversed

3. Apply Transformation
   Input: ParsedQuery, operator_orders
   Output: Transformed ParsedQuery

   Process:
   reorder_and_conditions(query, operator_orders):
     For each AND operator:
       - Find operator by ID (dengan fallback)
       - Get reorder sequence
       - Clone original children
       - Reorder: new_children[i] = clone(original[order[i]])
       - Replace operator children

   Fallback Mechanism:
   - Jika node ID tidak match (karena clone):
     Use next available order dari orders_list
   - Ini handle cloned trees dengan new IDs

4. Mutation
   Strategies:
   - swap: Tukar 2 positions random
   - reverse_subseq: Reverse subsequence [i:j]
   - rotate: Rotate semua elements by k

   Example mutations:
   [0, 1, 2, 3] --swap(0,2)--> [2, 1, 0, 3]
   [0, 1, 2, 3] --reverse(1,3)--> [0, 2, 1, 3]
   [0, 1, 2, 3] --rotate(1)--> [1, 2, 3, 0]
```

### Parameter Format Rule 2

```python
# Simple permutation (List[int])
params = [1, 2, 0]

Meaning:
- Position 0 → condition_1
- Position 1 → condition_2
- Position 2 → condition_0

Constraints:
- Must be valid permutation
- All indices 0..n-1 present exactly once
- Length = num_conditions
```

---

## Integrasi Rule 1 dan Rule 2

### Execution Order

Rule 2 diterapkan **SEBELUM** Rule 1 untuk hasil optimal:

```
Original Query
     │
     ▼
┌────────────────┐
│  Rule 2        │  Reorder conditions berdasarkan selectivity
│  (Komutatif)   │  Input: FILTER + OPERATOR(AND)
│                │  Output: FILTER + OPERATOR(AND) dengan order baru
└────────────────┘
     │
     ▼
┌────────────────┐
│  Rule 1        │  Cascade FILTERs berdasarkan order yang sudah optimal
│  (Konjunktif)  │  Input: FILTER + OPERATOR(AND)
│                │  Output: Cascaded FILTERs
└────────────────┘
     │
     ▼
Optimized Query
```

### Workflow `_apply_transformations`

```python
def _apply_transformations(base_query):
    # 1. Clone tree untuk isolasi
    cloned_tree = clone_tree(base_query.query_tree)
    current_query = ParsedQuery(cloned_tree, base_query.query)

    # 2. Apply Rule 1 dengan integrated reordering
    if 'rule_1' in rule_params:
        # 2a. Uncascade existing filters ke AND
        query_and = uncascade_filters(current_query)

        if rule_params['rule_1']:
            # 2b. Check if Rule 2 params available
            reorder_params = rule_params.get('rule_2', {})

            # 2c. Apply Rule 2 FIRST if available
            if reorder_params:
                query_and = reorder_and_conditions(
                    query_and,
                    operator_orders=reorder_params
                )

            # 2d. Apply Rule 1 cascade
            current_query = cascade_filters(
                query_and,
                operator_orders=rule_params['rule_1']
            )
        else:
            current_query = query_and

    # 3. Apply Rule 2 standalone if Rule 1 not present
    elif 'rule_2' in rule_params and rule_params['rule_2']:
        current_query = reorder_and_conditions(
            current_query,
            operator_orders=rule_params['rule_2']
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

Individual Params:
{
  'rule_2': {42: [1, 0, 2]},    # Reorder: [status, age, city]
  'rule_1': {42: [0, [1, 2]]}   # Cascade: status single, [age, city] grouped
}

Step 1 - Apply Rule 2 (Reorder):
FILTER
├── RELATION(users)
└── OPERATOR(AND)
    ├── COMPARISON(=) [status = 'active', selectivity=0.1]  ← Most selective
    ├── COMPARISON(>) [age > 25, selectivity=0.3]
    └── COMPARISON(=) [city = 'Jakarta', selectivity=0.5]

Step 2 - Apply Rule 1 (Cascade dengan order [0, [1,2]]):
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
            'rule_1': {
                'analyze': analyze_and_operators,
                'generate': generate_random_rule_1_params,
                'mutate': mutate_rule_1_params,
                'copy': copy_rule_1_params,
                'validate': validate_rule_1_params
            },
            'rule_2': {
                'analyze': analyze_and_operators_for_reorder,
                'generate': generate_random_rule_2_params,
                'mutate': mutate_rule_2_params,
                'copy': copy_rule_2_params,
                'validate': validate_rule_2_params
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
analyze_query(query, 'rule_1')
→ {42: 3, 57: 2}  # Node 42 has 3 conditions, Node 57 has 2
```

#### 2. `generate_random_params(rule_name, metadata)`

```
Purpose: Generate random params untuk node
Input: rule_name, metadata (e.g., num_conditions)
Output: Rule-specific params

Example:
generate_random_params('rule_1', 3)
→ [2, [0, 1]]  # Random mixed order

generate_random_params('rule_2', 3)
→ [1, 0, 2]  # Random permutation
```

#### 3. `mutate_params(rule_name, params)`

```
Purpose: Mutate params menggunakan rule-specific strategy
Input: rule_name, current_params
Output: Mutated params

Example:
mutate_params('rule_1', [0, 1, 2])
→ [0, [1, 2]]  # Grouped 1 and 2

mutate_params('rule_2', [0, 1, 2])
→ [2, 1, 0]  # Swapped positions
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

- `test_rule_1.py`: 24 tests untuk Rule 1
- `test_rule_2.py`: 17 tests untuk Rule 2
- Test coverage: analyze, generate, mutate, apply transformation

### 2. Integration Tests

- `test_integration_rules.py`: 15 tests
- Test Rule 1 + Rule 2 interaction
- Test dengan multiple AND operators
- Test edge cases

### 3. Genetic Algorithm Tests

- `test_genetic_rule2.py`: 9 tests
- Test population initialization
- Test selection, crossover, mutation
- Test full optimization run

---

## Extensibility

### Menambah Rule Baru

```python
# 1. Buat file rule_3.py
def analyze_for_rule_3(query: ParsedQuery) -> dict:
    # Find nodes that can be transformed
    pass

def generate_random_rule_3_params(metadata):
    # Generate random parameters
    pass

def mutate_rule_3_params(params):
    # Mutate parameters
    pass

def apply_rule_3(query: ParsedQuery, params):
    # Apply transformation
    pass

# 2. Register di rule_params_manager.py
manager.register_rule('rule_3', {
    'analyze': analyze_for_rule_3,
    'generate': generate_random_rule_3_params,
    'mutate': mutate_rule_3_params,
    'copy': lambda p: copy.deepcopy(p),
    'validate': lambda p, m: True
})

# 3. Update _apply_transformations di genetic_optimizer.py
if 'rule_3' in self.rule_params:
    current_query = apply_rule_3(current_query, self.rule_params['rule_3'])
```

---

## Conclusion

System ini mengimplementasikan query optimization menggunakan:

1. **Genetic Algorithm** untuk search space exploration
2. **Rule 1 (Seleksi Konjungtif)** untuk cascading filters
3. **Rule 2 (Seleksi Komutatif)** untuk reordering conditions
4. **Rule Params Manager** untuk centralized parameter management

Kombinasi ini memungkinkan pencarian struktur query optimal dengan:

- Flexibility: Rules dapat dikombinasikan berbeda-beda
- Extensibility: Mudah menambah rules baru
- Performance: Lazy evaluation dan optimization strategies
- Maintainability: Clean separation of concerns
