from query_optimizer.optimization_engine import ParsedQuery
from query_optimizer.query_tree import QueryTree

def find_patterns(query: ParsedQuery) -> dict[int, dict]:
    result = {}

    def walk(node: QueryTree):
        if node is None:
            return
        if is_reassociable(node):
            inner_join = node.childs[0]
            result[node.id] = {
                'inner_join_id': inner_join.id,
                'outer_condition': len(node.childs) >= 3,
                'inner_condition': len(inner_join.childs) >= 3
            }
        for child in node.childs:
            walk(child)

    walk(query.query_tree)
    return result

def is_reassociable(node: QueryTree) -> bool:
    if not node or node.type != "JOIN":
        return False
    if len(node.childs) < 2:
        return False
    left = node.childs[0]
    if not left or left.type != "JOIN":
        return False
    if len(left.childs) < 2:
        return False
    return True

def _has_right_nested_join(node: QueryTree) -> bool:
    """Check pattern: JOIN(E1, JOIN(E2, E3, cond), cond)."""
    if not node or node.type != "JOIN" or len(node.childs) < 2:
        return False
    right = node.childs[1]
    return bool(right and right.type == "JOIN" and len(right.childs) >= 2)

def apply_associativity(query: ParsedQuery, decisions: dict[int, str] = None) -> ParsedQuery:
    use_default_right = decisions is None
    decision_map = decisions or {}

    def walk(node: QueryTree) -> QueryTree:
        if node is None:
            return None
        for i in range(len(node.childs)):
            node.childs[i] = walk(node.childs[i])

        direction = decision_map.get(node.id, 'right' if use_default_right else 'none')
        if direction == 'right' and is_reassociable(node):
            return reassociate_right(node)
        if direction == 'left' and _has_right_nested_join(node):
            return reassociate_left(node)
        return node

    new_tree = walk(clone(query.query_tree))
    return ParsedQuery(new_tree, query.query)

def reassociate_right(outer_join: QueryTree) -> QueryTree:
    inner_join = outer_join.childs[0]
    e3 = outer_join.childs[1]
    outer_cond = outer_join.childs[2] if len(outer_join.childs) > 2 else None

    e1 = inner_join.childs[0]
    e2 = inner_join.childs[1]
    inner_cond = inner_join.childs[2] if len(inner_join.childs) > 2 else None

    e2_tables = collect_tables(e2)
    e3_tables = collect_tables(e3)

    if outer_cond:
        outer_tables = collect_tables(outer_cond)
        if not outer_tables.issubset(e2_tables | e3_tables):
            return outer_join

    # Rebuild in place to preserve node IDs used by other rules
    inner_join.childs = [e2, e3]
    if outer_cond:
        inner_join.childs.append(outer_cond)
    for ch in inner_join.childs:
        if ch:
            ch.parent = inner_join

    outer_join.childs = [e1, inner_join]
    if inner_cond:
        outer_join.childs.append(inner_cond)
    for ch in outer_join.childs:
        if ch:
            ch.parent = outer_join

    return outer_join

def reassociate_left(outer_join: QueryTree) -> QueryTree:
    if not _has_right_nested_join(outer_join):
        return outer_join

    right_join = outer_join.childs[1]
    e1 = outer_join.childs[0]
    e2 = right_join.childs[0]
    e3 = right_join.childs[1]

    outer_cond = right_join.childs[2] if len(right_join.childs) > 2 else None
    inner_cond = outer_join.childs[2] if len(outer_join.childs) > 2 else None

    # Validate that we are not moving an incompatible condition into the inner join
    if inner_cond:
        inner_tables = collect_tables(e1) | collect_tables(e2)
        cond_tables = collect_tables(inner_cond)
        if not cond_tables.issubset(inner_tables):
            return outer_join

    # Rebuild right_join as the left-deep inner join (keep its ID)
    right_join.childs = [e1, e2]
    if inner_cond:
        right_join.childs.append(inner_cond)
    for ch in right_join.childs:
        if ch:
            ch.parent = right_join

    # Outer join keeps its ID; attach new right child and outer condition
    outer_join.childs = [right_join, e3]
    if outer_cond:
        outer_join.childs.append(outer_cond)
    for ch in outer_join.childs:
        if ch:
            ch.parent = outer_join

    return outer_join

def undo_associativity(query: ParsedQuery) -> ParsedQuery:
    def walk(node: QueryTree) -> QueryTree:
        if node is None:
            return None
        for i in range(len(node.childs)):
            node.childs[i] = walk(node.childs[i])

        if _has_right_nested_join(node):
            return reassociate_left(node)
        return node

    new_tree = walk(clone(query.query_tree))
    return ParsedQuery(new_tree, query.query)

def collect_tables(node: QueryTree) -> set[str]:
    tables = set()

    def walk(n: QueryTree):
        if n is None:
            return
        if n.type == "RELATION":
            tables.add(n.val)
        if n.type == "ALIAS":
            tables.add(n.val)
        if n.type == "TABLE_NAME" and n.childs and n.childs[0].type == "IDENTIFIER":
            tables.add(n.childs[0].val)
        for ch in n.childs:
            walk(ch)

    walk(node)
    return tables

def generate_params(metadata: dict) -> str:
    import random
    return random.choice(['left', 'right', 'none'])

def copy_params(params: str) -> str:
    return params

def mutate_params(params: str) -> str:
    options = ['left', 'right', 'none']
    options.remove(params)
    import random
    return random.choice(options)

def validate_params(params: str) -> bool:
    return isinstance(params, str) and params in {'left', 'right', 'none'}

def clone(node: QueryTree) -> QueryTree:
    return node.clone(deep=True, preserve_id=True) if node else None
