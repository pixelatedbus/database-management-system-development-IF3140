"""
Seleksi Konjungtif & Komutatif (Rule 1 & 2) - Signature Based
Key Params: frozenset[int] (Set of Condition IDs)

Menggabungkan transformasi:
- Rule 1: Cascade/uncascade filters
- Rule 2: Reorder AND conditions
"""

from query_optimizer.optimization_engine import ParsedQuery
from query_optimizer.query_tree import QueryTree
import random


def analyze_and_operators(query: ParsedQuery) -> dict[frozenset[int], list[int]]:
    """
    Analyze query untuk menemukan kelompok kondisi (Signature).
    Returns: {frozenset(condition_ids): list(condition_ids)}
    """
    # Gunakan temp tree yang di-uncascade untuk melihat kondisi dalam bentuk "Flat"
    temp_tree = query.query_tree.clone(deep=True, preserve_id=True)
    temp_query = ParsedQuery(temp_tree, query.query)
    flat_query = uncascade_filters(temp_query)
    
    signatures = {}
    
    def visit(node: QueryTree):
        if node is None: return
        
        # Deteksi Filter (Single atau Grouped dalam AND)
        if node.is_node_type("FILTER") and len(node.childs) == 2:
            condition = node.childs[1]
            cond_list = []
            
            if condition.is_node_type("OPERATOR") and condition.is_node_value("AND"):
                cond_list = [c.id for c in condition.childs]
            else:
                cond_list = [condition.id]
            
            if cond_list:
                # Key adalah Frozenset (Signature unik konten)
                sig = frozenset(cond_list)
                signatures[sig] = cond_list
                
        for child in node.childs:
            visit(child)

    visit(flat_query.query_tree)
    return signatures


def apply_rule1_rule2(
    query: ParsedQuery,
    filter_params: dict[frozenset[int], list[int | list[int]]]
) -> tuple[ParsedQuery, dict[frozenset[int], list[int | list[int]]]]:
    
    # 1. Selalu uncascade dulu agar struktur konsisten (Flat)
    uncascaded_query = uncascade_filters(query)
    
    # 2. Apply Cascade berdasarkan Signature yang cocok
    def apply_rec(node: QueryTree) -> QueryTree:
        if node is None: return None
        
        # Bottom-up traversal
        for i in range(len(node.childs)):
            node.childs[i] = apply_rec(node.childs[i])
            
        if node.is_node_type("FILTER") and len(node.childs) == 2:
            condition = node.childs[1]
            current_ids = []
            
            # Extract IDs dari node saat ini
            if condition.is_node_type("OPERATOR") and condition.is_node_value("AND"):
                current_ids = [c.id for c in condition.childs]
            else:
                current_ids = [condition.id]
            
            # Buat Signature
            sig = frozenset(current_ids)
            
            # Cek apakah kita punya resep (params) untuk signature ini
            if sig in filter_params:
                order_spec = filter_params[sig]
                # Cascade menggunakan resep tersebut
                return cascade_mixed_signature(node, order_spec)
            else:
                # Jika tidak ada params (misal mutasi baru), cascade default
                return cascade_default(node)
        
        return node

    transformed_tree = apply_rec(uncascaded_query.query_tree)
    
    # Return query baru dan params (params tidak berubah key-nya karena signature based)
    return ParsedQuery(transformed_tree, query.query), filter_params


def uncascade_filters(query: ParsedQuery) -> ParsedQuery:
    """
    Versi AGRESIF: Menggabungkan rantai filter menjadi satu operator AND raksasa.
    Ini penting agar 'Analyze' bisa menangkap semua kondisi sebagai satu kesatuan.
    """
    def collect_chain(node: QueryTree):
        conditions = []
        curr = node
        # Sedot kondisi selama masih ada rantai FILTER
        while curr and curr.is_node_type("FILTER") and len(curr.childs) == 2:
            cond = curr.childs[1]
            if cond.is_node_type("OPERATOR") and cond.is_node_value("AND"):
                for c in cond.childs: 
                    conditions.append(c.clone(deep=True, preserve_id=True))
            else:
                conditions.append(cond.clone(deep=True, preserve_id=True))
            curr = curr.childs[0]
        return conditions, curr

    def walk(node: QueryTree):
        if node is None: return None
        
        if node.is_node_type("FILTER"):
            # Cek apakah ini awal chain
            conds, source = collect_chain(node)
            processed_source = walk(source) # Lanjut ke bawah chain
            
            if not conds: return processed_source
            
            # Selalu bungkus dalam AND untuk konsistensi Signature
            and_op = QueryTree("OPERATOR", "AND")
            for c in conds: and_op.add_child(c)
            
            new_filter = QueryTree("FILTER")
            new_filter.add_child(processed_source)
            new_filter.add_child(and_op)
            return new_filter
            
        for i in range(len(node.childs)):
            node.childs[i] = walk(node.childs[i])
        return node

    new_tree = walk(query.query_tree)
    return ParsedQuery(new_tree, query.query)


def cascade_mixed_signature(filter_node: QueryTree, order_spec: list) -> QueryTree:
    """
    Membangun ulang (Cascade) filter berdasarkan spesifikasi urutan.
    Menggunakan ID matching untuk menemukan node kondisi yang sesuai.
    """
    source = filter_node.childs[0]
    condition_node = filter_node.childs[1]
    
    # Map ID -> Node object agar mudah diambil
    id_map = {}
    if condition_node.is_node_type("OPERATOR") and condition_node.is_node_value("AND"):
        for c in condition_node.childs: id_map[c.id] = c
    else:
        id_map[condition_node.id] = condition_node
        
    current = source
    
    # Build from bottom up (reversed order spec)
    for item in reversed(order_spec):
        new_filter = QueryTree("FILTER")
        new_filter.add_child(current)
        
        child_cond = None
        if isinstance(item, list):
            # Group (AND)
            and_op = QueryTree("OPERATOR", "AND")
            for cid in item:
                if cid in id_map: and_op.add_child(id_map[cid])
            child_cond = and_op
        else:
            # Single
            if item in id_map: child_cond = id_map[item]
            
        if child_cond:
            new_filter.add_child(child_cond)
            current = new_filter
            
    return current


def cascade_default(filter_node: QueryTree) -> QueryTree:
    """Default cascade jika tidak ada params."""
    source = filter_node.childs[0]
    condition_node = filter_node.childs[1]
    conditions = condition_node.childs if condition_node.is_node_value("AND") else [condition_node]
    
    current = source
    for cond in reversed(conditions):
        new_filter = QueryTree("FILTER")
        new_filter.add_child(current)
        new_filter.add_child(cond)
        current = new_filter
    return current


# --- GENERATE & MUTATE (Params Only) ---

def generate_random_rule_1_params(condition_ids: list[int]) -> list:
    if not condition_ids: return []
    if len(condition_ids) == 1: return [condition_ids[0]]
    
    indices = list(range(len(condition_ids)))
    random.shuffle(indices)
    num_groups = random.randint(0, len(condition_ids) // 2)
    
    result = []
    remaining = indices[:]
    
    for _ in range(num_groups):
        if len(remaining) < 2: break
        size = random.randint(2, min(3, len(remaining)))
        grp = [condition_ids[i] for i in remaining[:size]]
        remaining = remaining[size:]
        result.append(grp)
        
    for i in remaining: result.append(condition_ids[i])
    random.shuffle(result)
    return result

def copy_rule_1_params(params):
    return [x.copy() if isinstance(x, list) else x for x in params]

def mutate_rule_1_params(params):
    if not params: return params
    mutated = copy_rule_1_params(params)
    action = random.choice(['swap', 'group', 'ungroup'])
    
    if action == 'swap' and len(mutated) >= 2:
        i1, i2 = random.sample(range(len(mutated)), 2)
        mutated[i1], mutated[i2] = mutated[i2], mutated[i1]
        
    elif action == 'group':
        singles = [i for i, x in enumerate(mutated) if not isinstance(x, list)]
        if len(singles) >= 2:
            i1, i2 = singles[0], singles[1]
            v1, v2 = mutated[i1], mutated[i2]
            if i1 > i2: mutated.pop(i1); mutated.pop(i2)
            else: mutated.pop(i2); mutated.pop(i1)
            mutated.append([v1, v2])
            
    elif action == 'ungroup':
        groups = [i for i, x in enumerate(mutated) if isinstance(x, list)]
        if groups:
            idx = random.choice(groups)
            grp = mutated.pop(idx)
            mutated.extend(grp)
            
    return mutated