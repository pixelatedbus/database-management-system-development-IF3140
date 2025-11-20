"""
Seleksi Konjungtif dengan Kontrol Penuh

Transformasi berdasarkan GRAMMAR_PLAN.md:
- Input: FILTER dengan OPERATOR(AND) sebagai condition tree
- Output: Cascaded FILTER nodes dengan mixed single/grouped conditions

Struktur GRAMMAR_PLAN.md:
FILTER
├── <source_tree>
└── OPERATOR("AND")
    ├── COMPARISON/condition
    ├── COMPARISON/condition
    └── COMPARISON/condition

Contoh transformasi dengan order = [2, [0,1]]:
FILTER (condition2 single)
├── FILTER (condition0 AND condition1)
│   ├── <source_tree>
│   └── OPERATOR("AND")
│       ├── condition0
│       └── condition1
└── condition2

Kontrol:
- operator_orders: Dict mapping operator_id -> urutan kondisi (nested: int | list[int])
  Example: {42: [2, [0,1]], 57: [1,0]} 
  - 42: condition2 single, lalu condition0&1 dalam AND
  - 57: condition1 single, lalu condition0 single
"""

from query_optimizer.optimization_engine import ParsedQuery
from query_optimizer.query_tree import QueryTree
import random


def analyze_and_operators(query: ParsedQuery) -> dict[int, int]:
    """
    Analisa query untuk mencari semua OPERATOR(AND) dalam FILTER dan jumlah kondisinya.
    
    Returns:
        Dict mapping operator_id -> jumlah kondisi
    """
    operators = {}
    
    def analyze_rec(node: QueryTree):
        if node is None:
            return
        
        # Cek apakah ini FILTER dengan OPERATOR(AND)
        if is_conjunctive_filter(node):
            operator_node = node.childs[1]  # condition tree (OPERATOR(AND))
            num_conditions = len(operator_node.childs)
            if num_conditions >= 2:
                operators[operator_node.id] = num_conditions
        
        # Rekursif ke semua children
        for child in node.childs:
            analyze_rec(child)
    
    analyze_rec(query.query_tree)
    return operators

def generate_random_rule_1_params(num_conditions: int) -> list[int | list[int]]:
        """
        Generate random mixed order untuk kondisi.
        
        Strategi:
        - Random shuffle indices
        - Random grouping: beberapa single, beberapa grouped dalam list
        
        Example output:
        - [2, [0,1]] = condition2 single, lalu (condition0 AND condition1)
        - [1, 0, 2] = semua single
        - [[0,1,2]] = semua dalam satu AND
        """
        indices = list(range(num_conditions))
        random.shuffle(indices)
        
        # Random number of groups
        # 0 groups = semua single
        # 1 group = beberapa grouped, sisanya single
        if num_conditions <= 1:
            return indices
        
        num_groups = random.randint(0, max(1, num_conditions // 2))
        
        if num_groups == 0:
            # Semua single
            return indices
        
        # Buat groups
        result = []
        remaining = indices.copy()
        
        for _ in range(num_groups):
            if len(remaining) < 2:
                break
            
            # Ambil 2-3 kondisi untuk di-group
            group_size = random.randint(2, min(3, len(remaining)))
            group = random.sample(remaining, group_size)
            
            # Remove dari remaining
            for idx in group:
                remaining.remove(idx)
            
            result.append(group)
        
        # Sisanya jadi single
        result.extend(remaining)
        
        # Shuffle final result
        random.shuffle(result)
        
        return result

def copy_rule_1_params(rule_1_params: list[int | list[int]]) -> list[int | list[int]]:
    """Deep copy mixed order structure."""
    return [item.copy() if isinstance(item, list) else item for item in rule_1_params]

def count_conditions_in_rule_1_params(rule_1_params: list[int | list[int]]) -> int:
    """Count total number of conditions in mixed order."""
    count = 0
    for item in rule_1_params:
        if isinstance(item, list):
            count += len(item)
        else:
            count += 1
    return count

def seleksi_konjungtif(query: ParsedQuery, operator_ids_to_split: list[int] | None = None) -> ParsedQuery:
    """
    Transformasi FILTER dengan OPERATOR(AND) menjadi cascaded filters.
    
    Args:
        query: Query yang akan ditransformasi
        operator_ids_to_split: List ID operator AND yang akan dipecah (None = semua)
    """
    if operator_ids_to_split is None:
        # Default: split semua OPERATOR(AND)
        transformed_tree = seleksi_konjungtif_rec(query.query_tree, split_all=True)
    else:
        transformed_tree = seleksi_konjungtif_rec(query.query_tree, split_all=False, ids_to_split=set(operator_ids_to_split))
    
    return ParsedQuery(transformed_tree, query.query)


def seleksi_konjungtif_rec(node: QueryTree, split_all: bool = True, ids_to_split: set[int] | None = None) -> QueryTree:
    """
    Rekursif transformasi seleksi konjungtif dengan kontrol.
    
    Args:
        node: Node yang akan ditransformasi
        split_all: Jika True, split semua AND; jika False, gunakan ids_to_split
        ids_to_split: Set ID operator AND yang akan di-split
    """
    if node is None:
        return None
    
    for i in range(len(node.childs)):
        node.childs[i] = seleksi_konjungtif_rec(node.childs[i], split_all, ids_to_split)
    
    # Transform FILTER dengan OPERATOR(AND) menjadi cascaded filters
    # FILTER structure: child[0] = source, child[1] = condition tree
    if node.is_node_type("FILTER") and len(node.childs) == 2:
        condition = node.childs[1]
        if condition.is_node_type("OPERATOR") and condition.is_node_value("AND"):
            # Cek apakah operator ini harus di-split
            if split_all or (ids_to_split is not None and condition.id in ids_to_split):
                return transform_and_filter(node)
    
    return node


def transform_and_filter(filter_node: QueryTree) -> QueryTree:
    """
    Transformasi FILTER dengan OPERATOR(AND) menjadi chain of cascaded filters.
    
    Input structure:
    FILTER
    ├── source_tree
    └── OPERATOR("AND")
        ├── condition1
        ├── condition2
        └── condition3
    
    Output structure:
    FILTER
    ├── FILTER
    │   ├── FILTER
    │   │   ├── source_tree
    │   │   └── condition1
    │   └── condition2
    └── condition3
    """
    source = filter_node.childs[0]
    and_operator = filter_node.childs[1]
    
    conditions = and_operator.childs
    if len(conditions) < 2:
        return filter_node
    
    current = source
    
    # Build cascaded filters from bottom to top (reverse order)
    for i in range(len(conditions) - 1, -1, -1):
        condition = conditions[i]
        
        # Create new FILTER node
        new_filter = QueryTree("FILTER")
        new_filter.add_child(current)
        new_filter.add_child(condition)
        
        current = new_filter
    
    return current


def is_conjunctive_filter(node: QueryTree) -> bool:
    """
    Cek apakah node adalah FILTER dengan OPERATOR(AND) sebagai condition.
    
    Structure:
    FILTER
    ├── source
    └── OPERATOR("AND")
        └── conditions (>=2)
    """
    if not node.is_node_type("FILTER"):
        return False
    
    if len(node.childs) != 2:
        return False
    
    condition = node.childs[1]
    if not condition.is_node_type("OPERATOR"):
        return False
    
    if not condition.is_node_value("AND"):
        return False
    
    # AND operator must have at least 2 children
    return len(condition.childs) >= 2


def can_transform(node: QueryTree) -> bool:
    """Cek apakah FILTER dengan AND bisa ditransformasi."""
    return is_conjunctive_filter(node)

def cascade_filters(
    query: ParsedQuery, 
    operator_orders: dict[int, list[int | list[int]]] | None = None
) -> ParsedQuery:
    """
    Cascade AND filters dengan kontrol mixed single/grouped conditions.
    
    Args:
        query: Query yang akan ditransformasi
        operator_orders: Dict mapping operator_id -> urutan kondisi (nested structure)
    
    Example:
        operator_orders = {
            42: [2, [0,1]],     # condition2 single, lalu (condition0 AND condition1)
            57: [1, 0],         # condition1 single, lalu condition0 single
            89: [3, [1,0], 2]   # condition3, lalu (condition1 AND condition0), lalu condition2
        }
    
    Interpretation:
    - int: Single condition (akan di-cascade sebagai FILTER tunggal)
    - list[int]: Grouped conditions (tetap dalam OPERATOR(AND))
    """
    def cascade_rec(node: QueryTree) -> QueryTree:
        if node is None:
            return None
        
        for i in range(len(node.childs)):
            node.childs[i] = cascade_rec(node.childs[i])
        
        if is_conjunctive_filter(node):
            operator_node = node.childs[1]
            operator_id = operator_node.id
            
            # Ambil urutan untuk operator ini
            order = operator_orders.get(operator_id) if operator_orders else None
            return cascade_and_mixed(node, order)
        
        return node
    
    transformed_tree = cascade_rec(query.query_tree)
    return ParsedQuery(transformed_tree, query.query)


def cascade_and_mixed(filter_node: QueryTree, order: list[int | list[int]] | None = None) -> QueryTree:
    """
    Cascade FILTER dengan OPERATOR(AND) dengan mixed single/grouped conditions.
    
    Args:
        filter_node: Node FILTER dengan OPERATOR(AND)
        order: Nested order structure (int = single, list[int] = grouped dalam AND)
               Example: [2, [0,1]] = condition2 single, lalu (condition0 AND condition1)
    
    Input:
    FILTER
    ├── source
    └── OPERATOR("AND")
        ├── condition0
        ├── condition1
        └── condition2
    
    Output (with order=[2, [0,1]]):
    FILTER (condition2 single)
    ├── FILTER (condition0 AND condition1)
    │   ├── source
    │   └── OPERATOR("AND")
    │       ├── condition0
    │       └── condition1
    └── condition2
    """
    source = filter_node.childs[0]
    and_operator = filter_node.childs[1]
    conditions = and_operator.childs
    
    # Default: semua single (fully cascaded) dalam urutan terbalik
    if order is None:
        order = list(range(len(conditions)))[::-1]
    
    # Build cascade dari dalam ke luar (reverse order)
    current = clone_tree(source)
    
    for item in reversed(order):
        if isinstance(item, list):
            # Grouped: buat OPERATOR(AND) untuk kondisi-kondisi ini
            if len(item) == 0:
                continue
            elif len(item) == 1:
                # Hanya 1 kondisi, treat as single
                condition = clone_tree(conditions[item[0]])
                new_filter = QueryTree("FILTER")
                new_filter.add_child(current)
                new_filter.add_child(condition)
                current = new_filter
            else:
                # Multiple conditions: buat OPERATOR(AND)
                and_node = QueryTree("OPERATOR", "AND")
                for idx in item:
                    and_node.add_child(clone_tree(conditions[idx]))
                new_filter = QueryTree("FILTER")
                new_filter.add_child(current)
                new_filter.add_child(and_node)
                current = new_filter
        else:
            # Single: cascade sebagai FILTER tunggal
            condition = clone_tree(conditions[item])
            new_filter = QueryTree("FILTER")
            new_filter.add_child(current)
            new_filter.add_child(condition)
            current = new_filter
    
    return current


def uncascade_filters(query: ParsedQuery) -> ParsedQuery:
    """
    Convert cascaded filters kembali ke bentuk FILTER dengan OPERATOR(AND).
    
    Input (cascaded):
    FILTER
    ├── FILTER
    │   ├── FILTER
    │   │   ├── source
    │   │   └── condition1
    │   └── condition2
    └── condition3
    
    Output:
    FILTER
    ├── source
    └── OPERATOR("AND")
        ├── condition1
        ├── condition2
        └── condition3
    """
    def uncascade_rec(node: QueryTree) -> QueryTree:
        if node is None:
            return None
        
        for i in range(len(node.childs)):
            node.childs[i] = uncascade_rec(node.childs[i])
        
        # Detect cascaded filters: FILTER with 2 children where child[0] is also FILTER
        if node.is_node_type("FILTER") and len(node.childs) == 2:
            filters = []
            current = node
            source = None
            
            # Collect all cascaded filters
            while current is not None and current.is_node_type("FILTER") and len(current.childs) == 2:
                condition = current.childs[1]
                filters.append(condition)
                current = current.childs[0]
            
            # If we have 2+ filters and a valid source, create FILTER with OPERATOR(AND)
            if len(filters) >= 2 and current is not None:
                source = current
                
                # Create AND operator with all conditions
                and_operator = QueryTree("OPERATOR", "AND")
                for condition in reversed(filters):  # Reverse to maintain original order
                    and_operator.add_child(condition)
                
                # Create new FILTER with source and AND operator
                new_filter = QueryTree("FILTER")
                new_filter.add_child(source)
                new_filter.add_child(and_operator)
                
                return new_filter
        
        return node
    
    transformed_tree = uncascade_rec(query.query_tree)
    return ParsedQuery(transformed_tree, query.query)


def clone_tree(node: QueryTree) -> QueryTree:
    """Deep clone query tree."""
    if node is None:
        return None
    return node.clone(deep=True)
