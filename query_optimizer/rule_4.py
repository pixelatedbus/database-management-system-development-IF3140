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
    # Build a map of old_id -> decision before cloning
    id_map = {}
    if decisions is not None:
        def build_id_map(node: QueryTree):
            if node is None:
                return
            if node.id in decisions:
                id_map[node.id] = decisions[node.id]
            for child in node.childs:
                build_id_map(child)
        build_id_map(query.query_tree)
    
    # Clone the tree (this will assign new IDs)
    new_tree = clone(query.query_tree)
    
    # Build a map of new nodes to their decisions based on position/structure
    decision_map = {}
    if id_map:
        old_nodes = []
        new_nodes = []
        
        def collect_mergeable_old(node: QueryTree):
            if node is None:
                return
            if is_mergeable(node) and node.id in id_map:
                old_nodes.append((node.id, id_map[node.id]))
            for child in node.childs:
                collect_mergeable_old(child)
        
        def collect_mergeable_new(node: QueryTree):
            if node is None:
                return
            if is_mergeable(node):
                new_nodes.append(node)
            for child in node.childs:
                collect_mergeable_new(child)
        
        collect_mergeable_old(query.query_tree)
        collect_mergeable_new(new_tree)
        
        # Map by position (same order in tree traversal)
        for i, (old_id, decision) in enumerate(old_nodes):
            if i < len(new_nodes):
                decision_map[new_nodes[i].id] = decision
    
    def walk(node: QueryTree) -> QueryTree:
        if node is None:
            return None

        for i in range(len(node.childs)):
            node.childs[i] = walk(node.childs[i])

        if is_mergeable(node):
            should_merge = True
            if decision_map:
                should_merge = decision_map.get(node.id, False)
            if should_merge:
                return merge_nodes(node)

        return node

    transformed_tree = walk(new_tree)
    return ParsedQuery(transformed_tree, query.query)


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
            # Create new JOIN without condition (convert INNER -> CROSS)
            new_join_type = "CROSS" if node.val == "INNER" else node.val
            new_join = QueryTree("JOIN", new_join_type)
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
