from query_optimizer.optimization_engine import ParsedQuery
from query_optimizer.query_tree import QueryTree


def find_patterns(query: ParsedQuery) -> dict[int, dict]:
    result = {}

    def walk(node: QueryTree):
        if node is None:
            return

        if is_mergeable(node):
            join = node.childs[0]
            has_condition = len(join.childs) >= 3
            result[node.id] = {
                'join_id': join.id,
                'has_condition': has_condition
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


def apply_merge(query: ParsedQuery, decisions: dict[int, bool] = None) -> ParsedQuery:
    def walk(node: QueryTree) -> QueryTree:
        if node is None:
            return None

        for i in range(len(node.childs)):
            node.childs[i] = walk(node.childs[i])

        if is_mergeable(node):
            should_merge = True
            if decisions is not None:
                should_merge = decisions.get(node.id, False)
            if should_merge:
                return merge_nodes(node)

        return node

    new_tree = walk(clone(query.query_tree))
    return ParsedQuery(new_tree, query.query)


def merge_nodes(filter_node: QueryTree) -> QueryTree:
    join = filter_node.childs[0]
    condition = filter_node.childs[1]
    new_join = clone(join)

    if len(new_join.childs) >= 2:
        if new_join.val in ("CROSS", "", None):
            new_join.val = "INNER"

        if len(new_join.childs) == 2:
            new_join.add_child(clone(condition))
            return new_join

        if len(new_join.childs) >= 3:
            existing = new_join.childs[2]
            if existing.type == "OPERATOR" and existing.val == "AND":
                existing.add_child(clone(condition))
            else:
                and_node = QueryTree("OPERATOR", "AND")
                and_node.add_child(clone(existing))
                and_node.add_child(clone(condition))
                new_join.childs[2] = and_node
            return new_join

    return filter_node


def undo_merge(query: ParsedQuery) -> ParsedQuery:
    def walk(node: QueryTree) -> QueryTree:
        if node is None:
            return None

        for i in range(len(node.childs)):
            node.childs[i] = walk(node.childs[i])

        if node.type == "JOIN" and len(node.childs) == 3:
            condition = node.childs[2]
            new_join = QueryTree("JOIN", node.val)
            new_join.add_child(clone(node.childs[0]))
            new_join.add_child(clone(node.childs[1]))

            filter_node = QueryTree("FILTER", "")
            filter_node.add_child(new_join)
            filter_node.add_child(clone(condition))
            return filter_node

        return node

    new_tree = walk(clone(query.query_tree))
    return ParsedQuery(new_tree, query.query)


def generate_params(metadata: dict) -> bool:
    import random
    return random.random() < 0.7


def copy_params(params: bool) -> bool:
    return params


def mutate_params(params: bool) -> bool:
    return not params


def validate_params(params: bool) -> bool:
    return isinstance(params, bool)


def clone(node: QueryTree) -> QueryTree:
    return node.clone(deep=True) if node else None
