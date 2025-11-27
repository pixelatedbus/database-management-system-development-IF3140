from query_optimizer.optimization_engine import ParsedQuery
from query_optimizer.query_tree import QueryTree
from query_optimizer import query_check


def find_patterns(query: ParsedQuery) -> dict[int, dict]:
    result = {}

    def walk(node: QueryTree):
        if node is None:
            return
        if is_pushable(node):
            join = node.childs[0]
            cond = node.childs[1]
            num_conds = 1
            has_and = False
            if cond.type == "OPERATOR" and cond.val == "AND":
                num_conds = len(cond.childs)
                has_and = True
            result[node.id] = {
                'join_id': join.id,
                'num_conditions': num_conds,
                'has_and': has_and
            }
        for child in node.childs:
            walk(child)

    walk(query.query_tree)
    return result


def is_pushable(node: QueryTree) -> bool:
    if not node or node.type != "FILTER":
        return False
    if len(node.childs) != 2:
        return False
    source = node.childs[0]
    if not source or source.type != "JOIN":
        return False
    return len(source.childs) >= 2


def apply_pushdown(query: ParsedQuery, plans: dict[int, dict] = None) -> ParsedQuery:
    def walk(node: QueryTree) -> QueryTree:
        if node is None:
            return None
        
        if is_pushable(node):
            plan = None
            if plans is not None:
                plan = plans.get(node.id)
            if plan is None:
                plan = decide_pushdown(node)
            if plan['distribution'] != 'none':
                pushed = push_filter(node, plan)
                return walk(pushed)
        
        for i in range(len(node.childs)):
            node.childs[i] = walk(node.childs[i])
        
        return node

    new_tree = walk(clone(query.query_tree))
    return ParsedQuery(new_tree, query.query)


def decide_pushdown(filter_node: QueryTree) -> dict:
    join = filter_node.childs[0]
    cond = filter_node.childs[1]
    conditions = cond.childs if cond.type == "OPERATOR" and cond.val == "AND" else [cond]

    left_tables = collect_tables(join.childs[0])
    right_tables = collect_tables(join.childs[1])
    
    available_tables = left_tables | right_tables

    left_idx = []
    right_idx = []

    for idx, c in enumerate(conditions):
        c_tables = collect_tables_in_condition(c, available_tables)
        if c_tables and c_tables.issubset(left_tables) and not c_tables & right_tables:
            left_idx.append(idx)
        elif c_tables and c_tables.issubset(right_tables) and not c_tables & left_tables:
            right_idx.append(idx)

    distribution = 'none'
    if left_idx and not right_idx:
        distribution = 'left'
    elif right_idx and not left_idx:
        distribution = 'right'
    elif left_idx and right_idx:
        distribution = 'both'

    return {
        'distribution': distribution,
        'left_conditions': left_idx,
        'right_conditions': right_idx
    }


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

def collect_tables_in_condition(node: QueryTree, available_tables: set[str]) -> set[str]:
    tables = set()
    metadata = None

    def walk(n: QueryTree):
        nonlocal metadata
        if n is None:
            return
        
        if n.type == "RELATION":
            tables.add(n.val)
        if n.type == "ALIAS":
            tables.add(n.val)
        if n.type == "TABLE_NAME" and n.childs and n.childs[0].type == "IDENTIFIER":
            tables.add(n.childs[0].val)
        if n.type == "COLUMN_REF" and n.childs:
            has_table_name = any(ch.type == "TABLE_NAME" for ch in n.childs)
            
            if not has_table_name:
                col_identifier = None
                for ch in n.childs:
                    if ch.type == "COLUMN_NAME" and ch.childs:
                        for grandchild in ch.childs:
                            if grandchild.type == "IDENTIFIER":
                                col_identifier = grandchild.val
                                break
                    if col_identifier:
                        break
                
                if col_identifier:
                    if metadata is None:
                        metadata = query_check.get_metadata()
                    
                    matching_tables = []
                    for table_name in available_tables:
                        if table_name in metadata["columns"]:
                            if col_identifier in metadata["columns"][table_name]:
                                matching_tables.append(table_name)
                    
                    if len(matching_tables) == 1:
                        tables.add(matching_tables[0])
        
        for ch in n.childs:
            walk(ch)

    walk(node)
    return tables


def push_filter(filter_node: QueryTree, plan: dict) -> QueryTree:
    join = filter_node.childs[0]
    cond = filter_node.childs[1]
    conditions = cond.childs if cond.type == "OPERATOR" and cond.val == "AND" else [cond]

    left_idx = plan.get('left_conditions', [])
    right_idx = plan.get('right_conditions', [])

    left = clone(join.childs[0])
    right = clone(join.childs[1])
    join_cond = clone(join.childs[2]) if len(join.childs) > 2 else None

    if left_idx:
        left = wrap_filter(left, [conditions[i] for i in left_idx if i < len(conditions)])
    if right_idx:
        right = wrap_filter(right, [conditions[i] for i in right_idx if i < len(conditions)])

    new_join = QueryTree("JOIN", join.val)
    new_join.add_child(left)
    new_join.add_child(right)
    if join_cond:
        new_join.add_child(join_cond)

    pushed = set(left_idx) | set(right_idx)
    remaining = [conditions[i] for i in range(len(conditions)) if i not in pushed]
    if remaining:
        return wrap_filter(new_join, remaining)
    return new_join


def wrap_filter(source: QueryTree, conds: list[QueryTree]) -> QueryTree:
    if not conds:
        return source
    filt = QueryTree("FILTER", "")
    filt.add_child(source)
    if len(conds) == 1:
        filt.add_child(clone(conds[0]))
    else:
        op = QueryTree("OPERATOR", "AND")
        for c in conds:
            op.add_child(clone(c))
        filt.add_child(op)
    return filt


def undo_pushdown(query: ParsedQuery) -> ParsedQuery:
    def walk(node: QueryTree) -> QueryTree:
        if node is None:
            return None
        for i in range(len(node.childs)):
            node.childs[i] = walk(node.childs[i])

        if node.type == "JOIN" and len(node.childs) >= 2:
            left = node.childs[0]
            right = node.childs[1]
            join_cond = node.childs[2] if len(node.childs) > 2 else None
            conds = []

            if left.type == "FILTER" and len(left.childs) == 2:
                conds.append(clone(left.childs[1]))
                left = left.childs[0]
            if right.type == "FILTER" and len(right.childs) == 2:
                conds.append(clone(right.childs[1]))
                right = right.childs[0]

            if conds:
                new_join = QueryTree("JOIN", node.val)
                new_join.add_child(left)
                new_join.add_child(right)
                if join_cond:
                    new_join.add_child(join_cond)
                return wrap_filter(new_join, conds)
        return node

    new_tree = walk(clone(query.query_tree))
    return ParsedQuery(new_tree, query.query)


def generate_params(metadata: dict) -> dict:
    import random
    num_conds = metadata.get('num_conditions', 1)
    distribution = random.choice(['left', 'right', 'both', 'none'])
    left_conds = []
    right_conds = []

    if num_conds == 1:
        if distribution == 'left':
            left_conds = [0]
        elif distribution == 'right':
            right_conds = [0]
    else:
        indices = list(range(num_conds))
        random.shuffle(indices)
        if distribution == 'left':
            left_conds = indices
        elif distribution == 'right':
            right_conds = indices
        elif distribution == 'both':
            split = random.randint(1, num_conds - 1)
            left_conds = indices[:split]
            right_conds = indices[split:]

    return {
        'distribution': distribution,
        'left_conditions': left_conds,
        'right_conditions': right_conds
    }


def copy_params(params: dict) -> dict:
    return {
        'distribution': params['distribution'],
        'left_conditions': params['left_conditions'].copy(),
        'right_conditions': params['right_conditions'].copy()
    }


def mutate_params(params: dict) -> dict:
    import random
    mutated = copy_params(params)
    action = random.choice(['change_dist', 'move_cond', 'toggle_cond'])

    if action == 'change_dist':
        options = ['left', 'right', 'both', 'none']
        current = mutated['distribution']
        options.remove(current)
        mutated['distribution'] = random.choice(options)

    elif action == 'move_cond':
        if mutated['left_conditions'] and mutated['right_conditions']:
            if random.random() < 0.5:
                idx = random.choice(mutated['left_conditions'])
                mutated['left_conditions'].remove(idx)
                mutated['right_conditions'].append(idx)
            else:
                idx = random.choice(mutated['right_conditions'])
                mutated['right_conditions'].remove(idx)
                mutated['left_conditions'].append(idx)

    elif action == 'toggle_cond':
        all_conds = set(mutated['left_conditions'] + mutated['right_conditions'])
        if all_conds:
            idx = random.choice(list(all_conds))
            if idx in mutated['left_conditions']:
                mutated['left_conditions'].remove(idx)
            if idx in mutated['right_conditions']:
                mutated['right_conditions'].remove(idx)

    return mutated


def validate_params(params: dict) -> bool:
    if not isinstance(params, dict):
        return False
    required = {'distribution', 'left_conditions', 'right_conditions'}
    if not required.issubset(params.keys()):
        return False
    if params['distribution'] not in {'left', 'right', 'both', 'none'}:
        return False
    left_set = set(params['left_conditions'])
    right_set = set(params['right_conditions'])
    if left_set & right_set:
        return False
    return True


def clone(node: QueryTree) -> QueryTree:
    return node.clone(deep=True) if node else None
