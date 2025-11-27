from query_optimizer.optimization_engine import ParsedQuery
from query_optimizer.query_tree import QueryTree


def get_underlying_join(node: QueryTree) -> QueryTree | None:

    current = node
    while current:
        if current.type == "JOIN":
            return current
        elif current.type == "FILTER":
            if not current.childs:
                return None
            current = current.childs[0]
        else:
            return None
    return None


def is_mergeable(node: QueryTree) -> bool:

    if not node or node.type != "FILTER":
        return False
    if len(node.childs) != 2:
        return False
    
    # Cek apakah source path berujung ke JOIN
    target_join = get_underlying_join(node.childs[0])
    return target_join is not None


def find_patterns(query: ParsedQuery) -> dict[int, dict]:
    """
    Analisa query untuk mencari semua JOIN yang bisa menerima kondisi dari FILTER.
    Support Deep Search: Bisa menemukan JOIN meskipun tertutup banyak FILTER.
    """
    result = {}

    def walk(node: QueryTree):
        if node is None:
            return

        if is_mergeable(node):
            # Gunakan deep search untuk menemukan join target
            join = get_underlying_join(node.childs[0])
            filter_condition = node.childs[1]
            
            # Init entry jika belum ada
            if join.id not in result:
                result[join.id] = {
                    'filter_conditions': [],
                    'existing_conditions': []
                }
                
                # Collect existing conditions di JOIN (hanya sekali per JOIN)
                if len(join.childs) >= 3:
                    existing_conds = collect_conditions(join.childs[2])
                    result[join.id]['existing_conditions'] = [c.id for c in existing_conds]

            # Collect candidates dari FILTER ini dan tambahkan ke list
            filter_conditions = collect_conditions(filter_condition)
            new_candidates = [c.id for c in filter_conditions]
            
            # Append unique candidates
            current_list = result[join.id]['filter_conditions']
            for cand in new_candidates:
                if cand not in current_list:
                    current_list.append(cand)

        for child in node.childs:
            walk(child)

    walk(query.query_tree)
    return result


def collect_conditions(condition_node: QueryTree) -> list[QueryTree]:
    """
    Collect all condition nodes from a condition tree.
    """
    if condition_node is None:
        return []
    
    if condition_node.type == "OPERATOR" and condition_node.val == "AND":
        return list(condition_node.childs)
    else:
        return [condition_node]


def apply_merge(
    query: ParsedQuery,
    join_params: dict[int, list[int]],
    filter_params: dict[int, list[int | list[int]]]
) -> tuple[ParsedQuery, dict[int, list[int]], dict[int, list[int | list[int]]]]:
    
    if not join_params:
        return query, join_params, filter_params
    
    new_tree = clone(query.query_tree, preserve_id=True)
    merged_condition_ids = set()
    
    def walk(node: QueryTree) -> QueryTree:
        nonlocal merged_condition_ids
        
        if node is None:
            return None

        # Post-order traversal (proses anak dulu) penting agar update dari bawah naik ke atas
        for i in range(len(node.childs)):
            node.childs[i] = walk(node.childs[i])

        if is_mergeable(node):
            # Cari target join menggunakan deep search
            target_join = get_underlying_join(node.childs[0])
            
            if target_join and target_join.id in join_params:
                condition_ids_to_merge = join_params[target_join.id]
                
                # Cek apakah FILTER ini punya kondisi yang diminta JOIN
                # Kita hanya memproses kondisi yang dimiliki FILTER node ini saat ini
                current_filter_conditions = collect_conditions(node.childs[1])
                relevant_conditions = [c.id for c in current_filter_conditions if c.id in condition_ids_to_merge]
                
                if relevant_conditions:
                    merged_condition_ids.update(relevant_conditions)
                    return merge_selected_conditions(node, condition_ids_to_merge)

        return node

    transformed_tree = walk(new_tree)
    transformed_query = ParsedQuery(transformed_tree, query.query)
    
    updated_filter_params = adjust_filter_params(filter_params, merged_condition_ids)
    
    return transformed_query, join_params, updated_filter_params


def merge_selected_conditions(filter_node: QueryTree, global_merge_ids: list[int]) -> QueryTree:

    source_child = filter_node.childs[0]
    filter_condition_node = filter_node.childs[1]
    
    # 1. Identifikasi kondisi di level ini yang harus turun
    all_local_conditions = collect_conditions(filter_condition_node)
    
    conditions_to_move_down = []
    conditions_to_stay_here = []
    
    for cond in all_local_conditions:
        if cond.id in global_merge_ids:
            conditions_to_move_down.append(clone(cond, preserve_id=True))
        else:
            conditions_to_stay_here.append(clone(cond, preserve_id=True))
    
    if not conditions_to_move_down:
        return filter_node

    # 2. Modifikasi Source (Subtree) untuk meng-inject kondisi ke JOIN
    # Kita harus mencari JOIN di dalam source_child dan menyuntikkan kondisi
    # Karena kita sudah clone tree di awal proses apply_merge, kita bisa mutate source_child
    
    target_join = get_underlying_join(source_child)
    if not target_join:
        # Should not happen if is_mergeable checked out, but safety first
        return filter_node

    # Ubah type JOIN menjadi INNER jika sebelumnya CROSS/None
    if target_join.val in ("CROSS", "", None):
        target_join.val = "INNER"
    
    # Siapkan kondisi yang akan ditambahkan ke JOIN
    conditions_to_inject = conditions_to_move_down
    
    # Ambil kondisi eksisting di JOIN (jika ada)
    existing_join_conditions = []
    if len(target_join.childs) >= 3:
        existing_join_conditions = collect_conditions(target_join.childs[2])
    
    # Gabungkan
    final_join_conditions = existing_join_conditions + conditions_to_inject
    
    # Reconstruct kondisi di JOIN
    if len(final_join_conditions) == 1:
        # Jika JOIN child < 3, kita expand childs
        if len(target_join.childs) < 3:
            target_join.add_child(final_join_conditions[0])
        else:
            target_join.childs[2] = final_join_conditions[0]
    else:
        # Wrap in AND
        and_node = QueryTree("OPERATOR", "AND")
        for cond in final_join_conditions:
            and_node.add_child(cond)
        
        if len(target_join.childs) < 3:
            target_join.add_child(and_node)
        else:
            target_join.childs[2] = and_node
            
    # 3. Return Node Pengganti
    # Jika masih ada kondisi yang tertinggal di FILTER level ini
    if conditions_to_stay_here:
        new_filter = QueryTree("FILTER", "")
        new_filter.add_child(source_child) # source_child sudah termutasi (JOIN-nya sudah update)
        
        if len(conditions_to_stay_here) == 1:
            new_filter.add_child(conditions_to_stay_here[0])
        else:
            and_node = QueryTree("OPERATOR", "AND")
            for cond in conditions_to_stay_here:
                and_node.add_child(cond)
            new_filter.add_child(and_node)
        return new_filter
    else:
        # Semua kondisi turun ke bawah, FILTER node ini hilang
        return source_child


def adjust_filter_params(
    filter_params: dict[int, list[int | list[int]]],
    merged_condition_ids: set[int]
) -> dict[int, list[int | list[int]]]:
    if not merged_condition_ids:
        return filter_params
    
    updated = {}
    for op_id, order_spec in filter_params.items():
        new_order = []
        for item in order_spec:
            if isinstance(item, list):
                filtered_group = [cid for cid in item if cid not in merged_condition_ids]
                if len(filtered_group) == 1:
                    new_order.append(filtered_group[0])
                elif len(filtered_group) > 1:
                    new_order.append(filtered_group)
            else:
                if item not in merged_condition_ids:
                    new_order.append(item)
        if new_order:
            updated[op_id] = new_order
    return updated


def generate_params(metadata: dict) -> list[int]:
    import random
    filter_conditions = metadata.get('filter_conditions', [])
    if not filter_conditions:
        return []
    # Logic: Randomly select subset of conditions to merge
    num_to_merge = random.randint(0, len(filter_conditions))
    if num_to_merge == 0:
        return []
    return random.sample(filter_conditions, num_to_merge)


def copy_params(params: list[int]) -> list[int]:
    return params.copy() if params else []


def mutate_params(params: list[int]) -> list[int]:
    import random
    if not params: return params
    mutated = params.copy()
    if random.random() < 0.5 and mutated:
        mutated.pop(random.randint(0, len(mutated) - 1))
    return mutated


def validate_params(params: list[int]) -> bool:
    if not isinstance(params, list): return False
    return all(isinstance(x, int) for x in params)


def clone(node: QueryTree, preserve_id: bool = False) -> QueryTree:
    return node.clone(deep=True, preserve_id=preserve_id) if node else None