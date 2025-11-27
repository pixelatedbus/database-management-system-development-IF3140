from query_optimizer.optimization_engine import ParsedQuery
from query_optimizer.query_tree import QueryTree


def find_patterns(query: ParsedQuery) -> dict[int, dict]:
    """
    Analisa query untuk mencari semua JOIN yang bisa menerima kondisi dari FILTER.
    
    Returns:
        Dict mapping join_id -> metadata
        metadata = {
            'filter_conditions': [list of condition nodes dari FILTER di atas JOIN],
            'existing_conditions': [list of condition nodes yang sudah ada di JOIN]
        }
    """
    result = {}

    def walk(node: QueryTree):
        if node is None:
            return

        if is_mergeable(node):
            join = node.childs[0]
            filter_condition = node.childs[1]
            
            # Collect condition nodes dari FILTER
            filter_conditions = collect_conditions(filter_condition)
            
            # Collect existing condition nodes dari JOIN (jika ada)
            existing_conditions = []
            if len(join.childs) >= 3:
                existing_conditions = collect_conditions(join.childs[2])
            
            result[join.id] = {
                'filter_conditions': [c.id for c in filter_conditions],
                'existing_conditions': [c.id for c in existing_conditions]
            }

        for child in node.childs:
            walk(child)

    walk(query.query_tree)
    return result


def is_mergeable(node: QueryTree) -> bool:
    if not node or node.type != "FILTER":
        return False
    if len(node.childs) != 2:
        return False
    source = node.childs[0]
    return source and source.type == "JOIN" and len(source.childs) >= 2


def collect_conditions(condition_node: QueryTree) -> list[QueryTree]:
    """
    Collect all condition nodes from a condition tree.
    If condition is OPERATOR(AND), collect all its children.
    Otherwise, return the single condition.
    
    Returns:
        List of condition nodes (COMPARISON, etc.)
    """
    if condition_node is None:
        return []
    
    if condition_node.type == "OPERATOR" and condition_node.val == "AND":
        return list(condition_node.childs)
    else:
        return [condition_node]


def apply_merge(query: ParsedQuery, join_params: dict[int, list[int]] = None) -> ParsedQuery:
    """
    Apply merge transformation based on join_params.
    
    Args:
        query: Query to transform
        join_params: Dict mapping join_id -> [condition_ids_to_merge]
                     Example: {42: [10, 15]} means merge conditions with ID 10 and 15 into JOIN with ID 42
                     Empty list [] means keep FILTER separate (no merge)
    
    Note:
        Rule 4 hanya mengatur WHICH conditions masuk ke JOIN, tidak mengatur:
        - Urutan conditions (order)
        - Cascade (semua conditions di JOIN digabung dengan AND)
    """
    # Clone tree dengan preserve_id=True agar ID tidak berubah
    new_tree = clone(query.query_tree, preserve_id=True)
    
    def walk(node: QueryTree) -> QueryTree:
        if node is None:
            return None

        for i in range(len(node.childs)):
            node.childs[i] = walk(node.childs[i])

        if is_mergeable(node):
            join = node.childs[0]
            # Check if we have params for this JOIN
            if join_params and join.id in join_params:
                condition_ids_to_merge = join_params[join.id]
                return merge_selected_conditions(node, condition_ids_to_merge)

        return node

    transformed_tree = walk(new_tree)
    return ParsedQuery(transformed_tree, query.query)


def merge_selected_conditions(filter_node: QueryTree, condition_ids_to_merge: list[int]) -> QueryTree:
    """
    Args:
        filter_node: FILTER node with JOIN as child[0] and condition as child[1]
        condition_ids_to_merge: List of condition IDs to merge into JOIN
                               Empty list [] = merge nothing (keep FILTER separate)
    
    Returns:
        Modified tree: either merged JOIN or original FILTER with remaining conditions
    
    - Jika semua conditions di-merge -> return JOIN saja (tanpa FILTER)
    - Jika sebagian conditions di-merge -> return FILTER dengan remaining conditions
    - Conditions yang di-merge digabung dengan AND di JOIN (tidak ada ordering)
    """
    join = filter_node.childs[0]
    filter_condition = filter_node.childs[1]
    
    # Collect all conditions dari FILTER
    filter_conditions = collect_conditions(filter_condition)
    
    # If condition_ids_to_merge is empty, keep all in FILTER (no merge)
    if not condition_ids_to_merge:
        return filter_node
    
    # Split conditions by IDs: to_merge vs to_keep_in_filter
    conditions_to_merge = []
    conditions_to_keep = []
    
    for cond in filter_conditions:
        if cond.id in condition_ids_to_merge:
            conditions_to_merge.append(clone(cond, preserve_id=True))
        else:
            conditions_to_keep.append(clone(cond, preserve_id=True))
    
    # Create new JOIN with merged conditions
    new_join = clone(join, preserve_id=True)
    
    # Change JOIN type if merging conditions
    if conditions_to_merge and new_join.val in ("CROSS", "", None):
        new_join.val = "INNER"
    
    # Add conditions to JOIN
    if conditions_to_merge:
        if len(new_join.childs) == 2:
            # No existing condition in JOIN
            if len(conditions_to_merge) == 1:
                new_join.add_child(conditions_to_merge[0])
            else:
                # Multiple conditions -> wrap in AND
                and_node = QueryTree("OPERATOR", "AND")
                for cond in conditions_to_merge:
                    and_node.add_child(cond)
                new_join.add_child(and_node)
        elif len(new_join.childs) >= 3:
            # JOIN already has condition
            existing = new_join.childs[2]
            
            # Collect all conditions to be in JOIN
            all_join_conditions = collect_conditions(existing) + conditions_to_merge
            
            if len(all_join_conditions) == 1:
                new_join.childs[2] = all_join_conditions[0]
            else:
                # Wrap all in AND
                and_node = QueryTree("OPERATOR", "AND")
                for cond in all_join_conditions:
                    and_node.add_child(cond)
                new_join.childs[2] = and_node
    
    # Return based on remaining conditions
    if not conditions_to_keep:
        # All conditions merged -> return JOIN only
        return new_join
    else:
        # Some conditions remain -> return FILTER with remaining conditions
        new_filter = QueryTree("FILTER", "")
        new_filter.add_child(new_join)
        
        if len(conditions_to_keep) == 1:
            new_filter.add_child(conditions_to_keep[0])
        else:
            # Multiple remaining conditions -> wrap in AND
            and_node = QueryTree("OPERATOR", "AND")
            for cond in conditions_to_keep:
                and_node.add_child(cond)
            new_filter.add_child(and_node)
        
        return new_filter


def undo_merge(query: ParsedQuery) -> ParsedQuery:
    def walk(node: QueryTree) -> QueryTree:
        if node is None:
            return None

        for i in range(len(node.childs)):
            node.childs[i] = walk(node.childs[i])

        if node.type == "JOIN" and len(node.childs) == 3:
            condition = node.childs[2]
            # Create new JOIN without condition (convert INNER -> CROSS)
            new_join_type = "CROSS" if node.val == "INNER" else node.val
            new_join = QueryTree("JOIN", new_join_type)
            new_join.add_child(clone(node.childs[0], preserve_id=True))
            new_join.add_child(clone(node.childs[1], preserve_id=True))

            filter_node = QueryTree("FILTER", "")
            filter_node.add_child(new_join)
            filter_node.add_child(clone(condition, preserve_id=True))
            return filter_node

        return node

    new_tree = walk(clone(query.query_tree, preserve_id=True))
    return ParsedQuery(new_tree, query.query)


def generate_params(metadata: dict) -> list[int]:
    """
    Generate random selection of conditions to merge into JOIN.
    
    Args:
        metadata: {
            'filter_conditions': [condition_ids],
            'existing_conditions': [condition_ids]
        }
    
    Returns:
        List of condition IDs to merge into JOIN
    """
    import random
    
    filter_conditions = metadata.get('filter_conditions', [])
    
    if not filter_conditions:
        return []
    
    # Randomly select some conditions to merge (0 to all)
    # Dengan probabilitas lebih tinggi untuk merge sebagian atau semua
    num_to_merge = random.randint(0, len(filter_conditions))
    
    if num_to_merge == 0:
        return []
    
    # Random sample
    return random.sample(filter_conditions, num_to_merge)


def copy_params(params: list[int]) -> list[int]:
    """Deep copy join params."""
    return params.copy() if params else []


def mutate_params(params: list[int]) -> list[int]:
    """
    Mutate join params by:
    - Adding/removing conditions from the merge list
    
    Note: Tidak mengubah urutan karena rule 4 tidak mengatur ordering.
    """
    import random
    
    if not params:
        return params
    
    mutated = params.copy()
    
    mutation_type = random.choice(['add', 'remove', 'replace'])
    
    if mutation_type == 'add':
        # Note: Untuk menambah condition, kita perlu context dari metadata
        # Untuk sekarang, skip add mutation
        pass
    elif mutation_type == 'remove':
        if mutated:
            mutated.pop(random.randint(0, len(mutated) - 1))
    elif mutation_type == 'replace':
        # Replace one condition with another (requires context)
        # Skip for now
        pass
    
    return mutated


def validate_params(params: list[int]) -> bool:
    """Validate join params structure."""
    if not isinstance(params, list):
        return False
    return all(isinstance(x, int) for x in params)


def clone(node: QueryTree, preserve_id: bool = False) -> QueryTree:
    """Clone node dengan option preserve ID."""
    return node.clone(deep=True, preserve_id=preserve_id) if node else None
