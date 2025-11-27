"""
Seleksi Konjungtif

FILTER
├── <source_tree>
└── OPERATOR("AND")
    ├── COMPARISON/condition
    ├── COMPARISON/condition
    └── COMPARISON/condition

Contoh transformasi dengan order = [2, [0,1]]:
FILTER
├── FILTER
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
    operators = {}
    
    def analyze_rec(node: QueryTree):
        if node is None:
            return
        
        if is_conjunctive_filter(node):
            operator_node = node.childs[1]  # condition tree (OPERATOR(AND))
            num_conditions = len(operator_node.childs)
            if num_conditions >= 2:
                operators[operator_node.id] = num_conditions
        
        for child in node.childs:
            analyze_rec(child)
    
    analyze_rec(query.query_tree)
    return operators

def generate_random_rule_1_params(num_conditions: int) -> list[int | list[int]]:
        indices = list(range(num_conditions))
        random.shuffle(indices)
        
        if num_conditions <= 1:
            return indices
        
        num_groups = random.randint(0, max(1, num_conditions // 2))
        
        if num_groups == 0:
            return indices
        
        result = []
        remaining = indices.copy()
        
        for _ in range(num_groups):
            if len(remaining) < 2:
                break
            
            group_size = random.randint(2, min(3, len(remaining)))
            group = random.sample(remaining, group_size)
            
            for idx in group:
                remaining.remove(idx)
            
            result.append(group)
        
        result.extend(remaining)
        
        random.shuffle(result)
        
        return result

def copy_rule_1_params(rule_1_params: list[int | list[int]]) -> list[int | list[int]]:
    return [item.copy() if isinstance(item, list) else item for item in rule_1_params]

def count_conditions_in_rule_1_params(rule_1_params: list[int | list[int]]) -> int:
    count = 0
    for item in rule_1_params:
        if isinstance(item, list):
            count += len(item)
        else:
            count += 1
    return count

def seleksi_konjungtif(query: ParsedQuery, operator_ids_to_split: list[int] | None = None) -> ParsedQuery:
    if operator_ids_to_split is None:
        transformed_tree = seleksi_konjunktif_rec(query.query_tree, split_all=True)
    else:
        transformed_tree = seleksi_konjunktif_rec(query.query_tree, split_all=False, ids_to_split=set(operator_ids_to_split))
    
    return ParsedQuery(transformed_tree, query.query)


def seleksi_konjunktif_rec(node: QueryTree, split_all: bool = True, ids_to_split: set[int] | None = None) -> QueryTree:
    if node is None:
        return None
    
    for i in range(len(node.childs)):
        node.childs[i] = seleksi_konjunktif_rec(node.childs[i], split_all, ids_to_split)
    
    if node.is_node_type("FILTER") and len(node.childs) == 2:
        condition = node.childs[1]
        if condition.is_node_type("OPERATOR") and condition.is_node_value("AND"):
            if split_all or (ids_to_split is not None and condition.id in ids_to_split):
                return transform_and_filter(node)
    
    return node


def transform_and_filter(filter_node: QueryTree) -> QueryTree:
    source = filter_node.childs[0]
    and_operator = filter_node.childs[1]
    
    conditions = and_operator.childs
    if len(conditions) < 2:
        return filter_node
    
    current = source
    
    for i in range(len(conditions) - 1, -1, -1):
        condition = conditions[i]
        
        new_filter = QueryTree("FILTER")
        new_filter.add_child(current)
        new_filter.add_child(condition)
        
        current = new_filter
    
    return current


def is_conjunctive_filter(node: QueryTree) -> bool:
    if not node.is_node_type("FILTER"):
        return False
    
    if len(node.childs) != 2:
        return False
    
    condition = node.childs[1]
    if not condition.is_node_type("OPERATOR"):
        return False
    
    if not condition.is_node_value("AND"):
        return False
    
    return len(condition.childs) >= 2


def can_transform(node: QueryTree) -> bool:
    return is_conjunctive_filter(node)

def cascade_filters(
    query: ParsedQuery, 
    operator_orders: dict[int, list[int | list[int]]] | None = None
) -> ParsedQuery:
    def cascade_rec(node: QueryTree) -> QueryTree:
        if node is None:
            return None
        
        for i in range(len(node.childs)):
            node.childs[i] = cascade_rec(node.childs[i])
        
        if is_conjunctive_filter(node):
            operator_node = node.childs[1]
            operator_id = operator_node.id
            
            order = operator_orders.get(operator_id) if operator_orders else None
            return cascade_and_mixed(node, order)
        
        return node
    
    transformed_tree = cascade_rec(query.query_tree)
    return ParsedQuery(transformed_tree, query.query)


def cascade_and_mixed(filter_node: QueryTree, order: list[int | list[int]] | None = None) -> QueryTree:
    source = filter_node.childs[0]
    and_operator = filter_node.childs[1]
    conditions = and_operator.childs
    
    if order is None:
        order = list(range(len(conditions)))[::-1]
    
    current = clone_tree(source)
    
    for item in reversed(order):
        if isinstance(item, list):
            if len(item) == 0:
                continue
            elif len(item) == 1:
                condition = clone_tree(conditions[item[0]])
                new_filter = QueryTree("FILTER")
                new_filter.add_child(current)
                new_filter.add_child(condition)
                current = new_filter
            else:
                and_node = QueryTree("OPERATOR", "AND")
                for idx in item:
                    and_node.add_child(clone_tree(conditions[idx]))
                new_filter = QueryTree("FILTER")
                new_filter.add_child(current)
                new_filter.add_child(and_node)
                current = new_filter
        else:
            condition = clone_tree(conditions[item])
            new_filter = QueryTree("FILTER")
            new_filter.add_child(current)
            new_filter.add_child(condition)
            current = new_filter
    
    return current


def uncascade_filters(query: ParsedQuery) -> ParsedQuery:
    def uncascade_rec(node: QueryTree) -> QueryTree:
        if node is None:
            return None
        
        for i in range(len(node.childs)):
            node.childs[i] = uncascade_rec(node.childs[i])
        
        if node.is_node_type("FILTER") and len(node.childs) == 2:
            filters = []
            current = node
            source = None
            
            while current is not None and current.is_node_type("FILTER") and len(current.childs) == 2:
                condition = current.childs[1]
                filters.append(condition)
                current = current.childs[0]
            
            if len(filters) >= 2 and current is not None:
                source = current
                
                and_operator = QueryTree("OPERATOR", "AND")
                for condition in reversed(filters):
                    and_operator.add_child(condition)
                
                new_filter = QueryTree("FILTER")
                new_filter.add_child(source)
                new_filter.add_child(and_operator)
                
                return new_filter
        
        return node
    
    transformed_tree = uncascade_rec(query.query_tree)
    return ParsedQuery(transformed_tree, query.query)


def mutate_rule_1_params(params: list[int | list[int]]) -> list[int | list[int]]:
    if not params:
        return params
    
    import random
    
    mutation_type = random.choice(['group', 'ungroup', 'regroup'])
    mutated = [item.copy() if isinstance(item, list) else item for item in params]
    
    if mutation_type == 'group':
        singles = [i for i, item in enumerate(mutated) if not isinstance(item, list)]
        if len(singles) >= 2:
            for i in range(len(singles) - 1):
                idx1 = singles[i]
                idx2 = singles[i + 1]
                if idx2 == idx1 + 1:
                    item1 = mutated[idx1]
                    item2 = mutated[idx2]
                    mutated[idx1:idx2+1] = [[item1, item2]]
                    break
    
    elif mutation_type == 'ungroup':
        groups = [i for i, item in enumerate(mutated) if isinstance(item, list) and len(item) > 1]
        if groups:
            idx = random.choice(groups)
            group = mutated[idx]
            mutated[idx:idx+1] = list(group)
    
    elif mutation_type == 'regroup':
        groups = [i for i, item in enumerate(mutated) if isinstance(item, list) and len(item) >= 2]
        if groups:
            idx = random.choice(groups)
            group = mutated[idx]
            split_point = random.randint(1, len(group) - 1)
            mutated[idx:idx+1] = [group[:split_point], group[split_point:]]
    
    return mutated


def clone_tree(node: QueryTree) -> QueryTree:
    if node is None:
        return None
    return node.clone(deep=True)
