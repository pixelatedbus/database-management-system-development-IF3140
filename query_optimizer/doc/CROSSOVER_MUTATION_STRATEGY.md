# Crossover & Mutation Strategy untuk Multi-Rule GA

## Struktur Data Individual

```python
Individual:
    rule_params: {
        'rule_1': {
            42: [2, [0,1]],      # Node 42: mixed order
            57: [1, 0]           # Node 57: full cascade
        },
        'rule_2': {
            123: {'level': 2},   # Node 123: pushdown level
            456: {'level': 1}    # Node 456: pushdown level
        },
        'rule_3': { ... },
        ...
    }
    applied_rules: ['rule_1', 'rule_2']  # Rules to apply
    fitness: 145.3
```

## Crossover Strategy

### Crossover untuk Rule Params

**Strategy: Per-Node Mixing**

```
Parent 1                    Parent 2
├─ rule_1:                 ├─ rule_1:
│  ├─ 42: [2, [0,1]]       │  ├─ 42: [[0,1,2]]
│  └─ 57: [1, 0]           │  └─ 57: [0, 1]
├─ rule_2:                 ├─ rule_2:
│  └─ 123: {level: 2}      │  └─ 123: {level: 3}
└─ applied_rules: [r1,r2]  └─ applied_rules: [r1,r3]

                ↓ CROSSOVER ↓

Child 1                     Child 2
├─ rule_1:                 ├─ rule_1:
│  ├─ 42: [[0,1,2]]  (P2)  │  ├─ 42: [2, [0,1]]  (P1)
│  └─ 57: [1, 0]     (P1)  │  └─ 57: [0, 1]      (P2)
├─ rule_2:                 ├─ rule_2:
│  └─ 123: {level: 2} (P1) │  └─ 123: {level: 3} (P2)
└─ applied_rules: [r1,r3]  └─ applied_rules: [r1,r2]
```

**Implementation:**

```python
def _crossover(parent1, parent2, base_query):
    child1_params = {}
    child2_params = {}
    
    # Untuk setiap rule
    for rule_name in all_rules:
        child1_params[rule_name] = {}
        child2_params[rule_name] = {}
        
        # Untuk setiap node
        for node_id in all_nodes:
            p1_params = parent1.rule_params[rule_name][node_id]
            p2_params = parent2.rule_params[rule_name][node_id]
            
            # Random strategy
            strategy = random.choice(['p1', 'p2', 'swap'])
            
            if strategy == 'p1':
                child1_params[rule_name][node_id] = copy(p1_params)
                child2_params[rule_name][node_id] = copy(p2_params)
            elif strategy == 'p2':
                child1_params[rule_name][node_id] = copy(p2_params)
                child2_params[rule_name][node_id] = copy(p1_params)
            else:  # swap
                child1_params[rule_name][node_id] = copy(p2_params)
                child2_params[rule_name][node_id] = copy(p1_params)
    
    # Mix applied_rules
    all_rules_set = set(parent1.applied_rules + parent2.applied_rules)
    child1_rules = random.sample(all_rules_set, k=random.randint(1, 3))
    child2_rules = random.sample(all_rules_set, k=random.randint(1, 3))
    
    return Child1(child1_params, child1_rules), Child2(child2_params, child2_rules)
```

### Visualisasi Crossover

```
BEFORE:
┌─────────────────┐    ┌─────────────────┐
│    Parent 1     │    │    Parent 2     │
├─────────────────┤    ├─────────────────┤
│ R1: Node42: A   │    │ R1: Node42: B   │
│     Node57: C   │    │     Node57: D   │
│ R2: Node123: E  │    │ R2: Node123: F  │
└─────────────────┘    └─────────────────┘

CROSSOVER (Random per node):
Node42 → strategy='p2'  → Child1 gets B, Child2 gets A
Node57 → strategy='p1'  → Child1 gets C, Child2 gets D
Node123 → strategy='swap' → Child1 gets F, Child2 gets E

AFTER:
┌─────────────────┐    ┌─────────────────┐
│     Child 1     │    │     Child 2     │
├─────────────────┤    ├─────────────────┤
│ R1: Node42: B   │    │ R1: Node42: A   │
│     Node57: C   │    │     Node57: D   │
│ R2: Node123: F  │    │ R2: Node123: E  │
└─────────────────┘    └─────────────────┘
```

## Mutation Strategy

### Mutation Types

**1. Params Mutation (70% probability)**

Mutate params dari salah satu rule menggunakan rule-specific mutation function.

```
BEFORE:
Individual:
  rule_1: {42: [2, [0,1]], 57: [1, 0]}
  rule_2: {123: {level: 2}}

↓ MUTATE rule_1, node 42 ↓

AFTER:
Individual:
  rule_1: {42: [[0,1], 2], 57: [1, 0]}  ← Changed!
  rule_2: {123: {level: 2}}
```

**2. Rules Mutation (30% probability)**

Modify applied_rules list.

```
BEFORE:
applied_rules: ['rule_1', 'rule_2']

↓ MUTATE (add rule_3) ↓

AFTER:
applied_rules: ['rule_1', 'rule_2', 'rule_3']
```

### Rule-Specific Mutation Examples

#### Rule 1 (Seleksi Konjungtif)

```python
# Input: [2, [0,1]]

Mutation Type: 'swap'
Result: [[0,1], 2]

Mutation Type: 'group'
Input: [2, 0, 1]
Result: [2, [0,1]]

Mutation Type: 'ungroup'
Input: [2, [0,1]]
Result: [2, 0, 1]

Mutation Type: 'regroup'
Input: [[0,1,2]]
Result: [[0,1], [2]]
```

#### Rule 2 (Pushdown Projection)

```python
# Input: {level: 2, column_order: [1,0,2], eliminate: True}

Mutation Type: 'level'
Result: {level: 3, column_order: [1,0,2], eliminate: True}

Mutation Type: 'order'
Result: {level: 2, column_order: [0,1,2], eliminate: True}

Mutation Type: 'eliminate'
Result: {level: 2, column_order: [1,0,2], eliminate: False}
```

### Visualisasi Mutation

```
┌─────────────────────────────┐
│       Individual            │
├─────────────────────────────┤
│ rule_1:                     │
│   Node42: [2, [0,1]]        │ ← Selected for mutation
│   Node57: [1, 0]            │
│ rule_2:                     │
│   Node123: {level: 2}       │
└─────────────────────────────┘
              ↓
    Call rule_1 mutation
              ↓
┌─────────────────────────────┐
│   Mutated Individual        │
├─────────────────────────────┤
│ rule_1:                     │
│   Node42: [[0,1], 2]        │ ← Mutated!
│   Node57: [1, 0]            │
│ rule_2:                     │
│   Node123: {level: 2}       │
└─────────────────────────────┘
```

## Algoritma Lengkap

### Crossover Algorithm

```
1. Initialize child1_params = {}, child2_params = {}

2. FOR each rule_name in registered_rules:
   a. FOR each node_id in rule_params[rule_name]:
      i.   Get p1_params from parent1
      ii.  Get p2_params from parent2
      iii. Random strategy ∈ {'p1', 'p2', 'swap'}
      iv.  Assign params to children based on strategy
   
3. Mix applied_rules:
   a. Combine parent1.applied_rules + parent2.applied_rules
   b. Random sample for child1
   c. Random sample for child2

4. Create Individual(child1_params, child1_rules)
   Create Individual(child2_params, child2_rules)

5. RETURN child1, child2
```

### Mutation Algorithm

```
1. Deep copy all params from individual

2. Random mutation_type ∈ {'params', 'rules'}

3. IF mutation_type == 'params':
   a. Random select rule_name
   b. Random select node_id
   c. Call manager.mutate_params(rule_name, params)
   d. Replace params with mutated version

4. ELIF mutation_type == 'rules':
   a. Random rule_mutation ∈ {'add', 'remove', 'replace'}
   b. Modify applied_rules list accordingly

5. Create Individual(mutated_params, mutated_rules)

6. RETURN mutated_individual
```

## Example: Full GA Iteration

```
Generation 0:
Population: [Ind1, Ind2, Ind3, ..., Ind50]

Selection (Tournament):
Winners: [Ind5, Ind12, Ind23, Ind8]

Crossover:
P1=Ind5, P2=Ind12 → Child1, Child2
P1=Ind23, P2=Ind8 → Child3, Child4

Mutation (10% rate):
Child1 → mutate params → Child1'
Child3 → mutate rules → Child3'

Next Generation:
Population: [Ind1, Ind2 (elite), Child1', Child2, Child3', Child4, ...]

Fitness Evaluation:
Each individual applies its rule_params to query
Calculate cost
Sort by fitness

Repeat for N generations...

RESULT: Best individual with optimal rule_params
```

## Key Advantages

✅ **Modular Mutation**: Setiap rule define mutation sendiri
✅ **Flexible Crossover**: Mix-and-match params per node
✅ **Rule-Agnostic GA**: GA tidak perlu tahu detail setiap rule
✅ **Easy Extension**: Tambah rule baru tanpa ubah crossover/mutation logic
✅ **Domain Knowledge**: Rule-specific mutation preserve validity

## Implementation Notes

1. **Manager-Based**: All operations melalui RuleParamsManager
2. **Type Safety**: Manager handle type checking per rule
3. **Validation**: Optional validation function per rule
4. **Extensibility**: New rules just register themselves
5. **Performance**: Lazy evaluation, only compute when needed
