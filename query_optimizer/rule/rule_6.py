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

def apply_associativity(query: ParsedQuery, decisions: dict[int, str] = None) -> ParsedQuery:
    def walk(node: QueryTree) -> QueryTree:
        if node is None:
            return None
        for i in range(len(node.childs)):
            node.childs[i] = walk(node.childs[i])

        if is_reassociable(node):
            direction = 'right'
            if decisions is not None:
                direction = decisions.get(node.id, 'right')
            if direction == 'right':
                return reassociate_right(node)
            elif direction == 'left':
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

    new_inner = QueryTree("JOIN", inner_join.val)
    new_inner.add_child(clone(e2))
    new_inner.add_child(clone(e3))
    if outer_cond:
        new_inner.add_child(clone(outer_cond))

    new_outer = QueryTree("JOIN", outer_join.val)
    new_outer.add_child(clone(e1))
    new_outer.add_child(new_inner)
    if inner_cond:
        new_outer.add_child(clone(inner_cond))

    return new_outer

def reassociate_left(outer_join: QueryTree) -> QueryTree:
    if not outer_join or len(outer_join.childs) < 2:
        return outer_join
    left = outer_join.childs[0]
    if not left or left.type != "JOIN" or len(left.childs) < 2:
        return outer_join

    inner_join = left
    e1 = inner_join.childs[0]
    inner_right = inner_join.childs[1]

    if inner_right.type != "JOIN" or len(inner_right.childs) < 2:
        return outer_join

    e2 = inner_right.childs[0]
    e3 = inner_right.childs[1]
    inner_inner_cond = inner_right.childs[2] if len(inner_right.childs) > 2 else None
    outer_cond = inner_join.childs[2] if len(inner_join.childs) > 2 else None

    new_inner = QueryTree("JOIN", inner_join.val)
    new_inner.add_child(clone(e1))
    new_inner.add_child(clone(e2))
    if outer_cond:
        new_inner.add_child(clone(outer_cond))

    new_outer = QueryTree("JOIN", outer_join.val)
    new_outer.add_child(new_inner)
    new_outer.add_child(clone(e3))
    if inner_inner_cond:
        new_outer.add_child(clone(inner_inner_cond))

    return new_outer

def undo_associativity(query: ParsedQuery) -> ParsedQuery:
    def walk(node: QueryTree) -> QueryTree:
        if node is None:
            return None
        for i in range(len(node.childs)):
            node.childs[i] = walk(node.childs[i])

        if is_reassociable(node):
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
    return node.clone(deep=True) if node else None
