"""
Seleksi Konjungtif & Komutatif (Rule 1 & 2)

Menggabungkan transformasi:
- Rule 1: Cascade/uncascade filters
- Rule 2: Reorder AND conditions

Format params: {operator_id: [condition_ids_order]}
Example: {42: [102, [100, 101]]}
- 42: operator ID
- [102, [100, 101]]: order spec dengan condition IDs
  - 102: single condition
  - [100, 101]: grouped conditions dalam AND
"""

from query_optimizer.optimization_engine import ParsedQuery
from query_optimizer.query_tree import QueryTree
import random


def analyze_and_operators(query: ParsedQuery) -> dict[int, list[int]]:
    """
    Analyze AND operators untuk Rule 1&2.
    
    Returns:
        Dict[operator_id, list[condition_ids]]
        Example: {42: [100, 101, 102]}
    """
    operators = {}
    
    def analyze_rec(node: QueryTree):
        if node is None:
            return
        
        if is_conjunctive_filter(node):
            operator_node = node.childs[1]  # OPERATOR(AND)
            conditions = operator_node.childs
            
            if len(conditions) >= 1:  # Support single condition juga
                # Extract condition IDs
                condition_ids = [cond.id for cond in conditions]
                operators[operator_node.id] = condition_ids
        
        for child in node.childs:
            analyze_rec(child)
    
    analyze_rec(query.query_tree)
    return operators


def generate_random_rule_1_params(condition_ids: list[int]) -> list[int | list[int]]:
    """
    Generate random ordering menggunakan condition IDs.
    
    Args:
        condition_ids: List of condition IDs
    
    Returns:
        list[int | list[int]]: Order specification
        Example: [102, [100, 101]]
    """
    if len(condition_ids) == 1:
        return [condition_ids[0]]
    
    indices = list(range(len(condition_ids)))
    random.shuffle(indices)
    
    num_groups = random.randint(0, max(1, len(condition_ids) // 2))
    
    if num_groups == 0:
        # Semua single
        return [condition_ids[i] for i in indices]
    
    # Buat groups
    result = []
    remaining = indices.copy()
    
    for _ in range(num_groups):
        if len(remaining) < 2:
            break
        
        group_size = random.randint(2, min(3, len(remaining)))
        group_indices = remaining[:group_size]
        remaining = remaining[group_size:]
        
        # Convert indices to condition IDs
        group = [condition_ids[i] for i in group_indices]
        result.append(group)
    
    # Sisanya jadi single
    for i in remaining:
        result.append(condition_ids[i])
    
    random.shuffle(result)
    
    return result


def copy_rule_1_params(rule_1_params: list[int | list[int]]) -> list[int | list[int]]:
    """Deep copy params."""
    return [item.copy() if isinstance(item, list) else item for item in rule_1_params]


def mutate_rule_1_params(params: list[int | list[int]]) -> list[int | list[int]]:
    """Mutate params dengan strategi group/ungroup/reorder."""
    if not params:
        return params
    
    import random
    
    mutation_type = random.choice(['swap', 'group', 'ungroup', 'regroup'])
    mutated = [item.copy() if isinstance(item, list) else item for item in params]
    
    if mutation_type == 'swap' and len(mutated) >= 2:
        # Swap positions
        idx1, idx2 = random.sample(range(len(mutated)), 2)
        mutated[idx1], mutated[idx2] = mutated[idx2], mutated[idx1]
    
    elif mutation_type == 'group':
        # Group adjacent singles
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
        # Ungroup a group
        groups = [i for i, item in enumerate(mutated) if isinstance(item, list) and len(item) > 1]
        if groups:
            idx = random.choice(groups)
            group = mutated[idx]
            mutated[idx:idx+1] = list(group)
    
    elif mutation_type == 'regroup':
        # Split a group
        groups = [i for i, item in enumerate(mutated) if isinstance(item, list) and len(item) >= 2]
        if groups:
            idx = random.choice(groups)
            group = mutated[idx]
            split_point = random.randint(1, len(group) - 1)
            mutated[idx:idx+1] = [group[:split_point], group[split_point:]]
    
    return mutated


def apply_rule1_rule2(
    query: ParsedQuery,
    filter_params: dict[int, list[int | list[int]]]
) -> tuple[ParsedQuery, dict[int, list[int | list[int]]]]:
    """
    Apply Rule 1 & 2: uncascade → reorder → cascade.
    
    Args:
        query: Query to transform
        filter_params: {operator_id: order_spec_with_condition_ids}
    
    Returns:
        (transformed_query, updated_filter_params)
        updated_filter_params has new operator_ids mapped by condition IDs
    """
    if not filter_params:
        return query, filter_params
    
    # Step 1: Uncascade untuk normalize structure
    uncascaded_query = uncascade_filters(query)
    
    # Step 2: Build mapping old_operator_id → condition_ids
    old_operator_to_conditions = {}
    for old_op_id, order_spec in filter_params.items():
        # Extract condition IDs dari order spec
        flat_cond_ids = []
        for item in order_spec:
            if isinstance(item, list):
                flat_cond_ids.extend(item)
            else:
                flat_cond_ids.append(item)
        old_operator_to_conditions[old_op_id] = set(flat_cond_ids)
    
    # Step 3: Analyze uncascaded tree untuk mendapatkan operator IDs baru
    new_analysis = analyze_and_operators(uncascaded_query)
    # new_analysis = {new_op_id: [cond_ids]}
    
    # Step 4: Map params lama ke operator IDs baru by condition IDs
    new_filter_params = {}
    condition_to_new_op = {}
    
    for new_op_id, cond_ids in new_analysis.items():
        cond_set = set(cond_ids)
        condition_to_new_op[frozenset(cond_set)] = new_op_id
    
    for old_op_id, old_order_spec in filter_params.items():
        old_cond_set = old_operator_to_conditions[old_op_id]
        
        # Find matching new operator
        if frozenset(old_cond_set) in condition_to_new_op:
            new_op_id = condition_to_new_op[frozenset(old_cond_set)]
            new_filter_params[new_op_id] = old_order_spec
    
    # Step 5: Reorder conditions
    reorder_orders = {}
    for op_id, order_spec in new_filter_params.items():
        # Flatten to get pure list of condition IDs
        flat = []
        for item in order_spec:
            if isinstance(item, list):
                flat.extend(item)
            else:
                flat.append(item)
        reorder_orders[op_id] = flat
    
    reordered_query = reorder_and_conditions(uncascaded_query, reorder_orders)
    
    # Step 6: Cascade dengan grouping structure
    cascaded_query = cascade_filters(reordered_query, new_filter_params)
    
    return cascaded_query, new_filter_params


def is_conjunctive_filter(node: QueryTree) -> bool:
    """Check if node is FILTER with AND operator."""
    if not node.is_node_type("FILTER"):
        return False
    
    if len(node.childs) != 2:
        return False
    
    condition = node.childs[1]
    if not condition.is_node_type("OPERATOR"):
        return False
    
    if not condition.is_node_value("AND"):
        return False
    
    return len(condition.childs) >= 1  # Support 1+ conditions


def uncascade_filters(query: ParsedQuery) -> ParsedQuery:
    """
    Uncascade cascaded filters into single FILTER with OPERATOR(AND).
    """
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


def reorder_and_conditions(
    query: ParsedQuery,
    operator_orders: dict[int, list[int]]
) -> ParsedQuery:
    """
    Reorder AND conditions menggunakan condition IDs.
    
    Args:
        operator_orders: {operator_id: [condition_ids_order]}
    """
    if not operator_orders:
        return query
    
    def reorder_rec(node: QueryTree) -> QueryTree:
        if node is None:
            return None
        
        for i in range(len(node.childs)):
            node.childs[i] = reorder_rec(node.childs[i])

        if node.is_node_type("OPERATOR") and node.is_node_value("AND"):
            operator_id = node.id
            
            if operator_id in operator_orders:
                order = operator_orders[operator_id]
                conditions = node.childs
                
                if len(order) == len(conditions):
                    # Build mapping condition_id → node
                    id_to_cond = {cond.id: cond for cond in conditions}
                    
                    # Reorder by condition IDs
                    node.childs = [id_to_cond[cond_id] for cond_id in order]

        return node
    
    transformed_tree = reorder_rec(query.query_tree)
    return ParsedQuery(transformed_tree, query.query)


def cascade_filters(
    query: ParsedQuery,
    operator_orders: dict[int, list[int | list[int]]]
) -> ParsedQuery:
    """
    Cascade filters dengan order specification menggunakan condition IDs.
    
    Args:
        operator_orders: {operator_id: order_spec_with_condition_ids}
    """
    def cascade_rec(node: QueryTree) -> QueryTree:
        if node is None:
            return None
        
        for i in range(len(node.childs)):
            node.childs[i] = cascade_rec(node.childs[i])
        
        if is_conjunctive_filter(node):
            operator_node = node.childs[1]
            operator_id = operator_node.id
            
            order = operator_orders.get(operator_id) if operator_orders else None
            
            if order:
                return cascade_and_mixed_by_ids(node, order)
            else:
                # Default: cascade all as singles
                return cascade_and_mixed(node, None)
        
        return node
    
    transformed_tree = cascade_rec(query.query_tree)
    return ParsedQuery(transformed_tree, query.query)


def cascade_and_mixed_by_ids(
    filter_node: QueryTree,
    order: list[int | list[int]]
) -> QueryTree:
    """
    Cascade dengan order specification menggunakan condition IDs.
    
    Args:
        order: list[int | list[int]] where int adalah condition ID
               Example: [102, [100, 101]]
    """
    source = filter_node.childs[0]
    and_operator = filter_node.childs[1]
    conditions = and_operator.childs
    
    # Build mapping: condition_id → condition_node
    id_to_cond = {cond.id: cond for cond in conditions}
    
    current = source
    
    for item in reversed(order):
        if isinstance(item, list):
            # Group of condition IDs
            if len(item) == 0:
                continue
            elif len(item) == 1:
                condition = id_to_cond[item[0]]
                new_filter = QueryTree("FILTER")
                new_filter.add_child(current)
                new_filter.add_child(condition)
                current = new_filter
            else:
                # Multiple conditions in AND
                and_node = QueryTree("OPERATOR", "AND")
                for cond_id in item:
                    and_node.add_child(id_to_cond[cond_id])
                new_filter = QueryTree("FILTER")
                new_filter.add_child(current)
                new_filter.add_child(and_node)
                current = new_filter
        else:
            # Single condition ID
            condition = id_to_cond[item]
            new_filter = QueryTree("FILTER")
            new_filter.add_child(current)
            new_filter.add_child(condition)
            current = new_filter
    
    return current


def cascade_and_mixed(filter_node: QueryTree, order: list[int] | None = None) -> QueryTree:
    """Default cascade (all singles, reversed order)."""
    source = filter_node.childs[0]
    and_operator = filter_node.childs[1]
    conditions = and_operator.childs
    
    if order is None:
        order = list(range(len(conditions)))[::-1]
    
    current = source
    
    for i in order:
        condition = conditions[i]
        new_filter = QueryTree("FILTER")
        new_filter.add_child(current)
        new_filter.add_child(condition)
        current = new_filter
    
    return current