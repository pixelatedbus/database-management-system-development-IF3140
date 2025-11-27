"""
Rule 4 (Join ID Key Edition)
Key Params: int (Join Node ID)
Value: list[int] (Selected IDs to merge)

Menggunakan Join ID sebagai anchor karena posisi join relatif stabil,
berbeda dengan Filter yang strukturnya cair (Signature-based).
"""

from query_optimizer.optimization_engine import ParsedQuery
from query_optimizer.query_tree import QueryTree
import random

# --- HELPER FUNCTIONS ---

def get_underlying_join(node: QueryTree) -> QueryTree | None:
    """
    Menelusuri ke bawah melewati tumpukan FILTER untuk menemukan JOIN.
    """
    curr = node
    while curr:
        if curr.type == "JOIN":
            return curr
        elif curr.type == "FILTER":
            if not curr.childs:
                return None
            curr = curr.childs[0]
        else:
            return None
    return None

def collect_conditions(node: QueryTree) -> list[QueryTree]:
    if not node: return []
    if node.type == "OPERATOR" and node.val == "AND":
        return list(node.childs)
    return [node]


# --- MAIN LOGIC ---

def find_patterns(query: ParsedQuery) -> dict[int, dict]:
    """
    Analisa query untuk mencari Join target.
    Returns: {join_id: metadata}
    """
    result = {}
    
    def walk(node: QueryTree):
        if node is None: return

        if node.type == "FILTER" and len(node.childs) == 2:
            # Cek apakah di bawah filter ini ada Join
            join = get_underlying_join(node.childs[0])
            if join:
                # Kumpulkan kandidat kondisi dari filter ini
                filter_conds = collect_conditions(node.childs[1])
                candidates = [c.id for c in filter_conds]
                
                if candidates:
                    # Init entry untuk Join ID ini jika belum ada
                    if join.id not in result:
                        result[join.id] = {
                            'filter_conditions': [],
                            'existing_conditions': []
                        }
                        # Ambil kondisi eksisting di dalam join (hanya sekali init)
                        if len(join.childs) >= 3:
                            exist_conds = collect_conditions(join.childs[2])
                            result[join.id]['existing_conditions'] = [c.id for c in exist_conds]
                    
                    # Tambahkan kandidat unik ke list
                    current_cands = result[join.id]['filter_conditions']
                    for c in candidates:
                        if c not in current_cands:
                            current_cands.append(c)

        for child in node.childs:
            walk(child)

    walk(query.query_tree)
    return result


def apply_merge(
    query: ParsedQuery,
    join_params: dict[int, list[int]],  # Key sekarang INT (Join ID)
    filter_params: dict
) -> tuple[ParsedQuery, dict, dict]:
    
    if not join_params:
        return query, join_params, filter_params
    
    new_tree = query.query_tree.clone(deep=True, preserve_id=True)
    merged_ids = set()
    
    def walk(node: QueryTree) -> QueryTree:
        nonlocal merged_ids
        if node is None: return None
        
        # Post-order traversal (Bottom-up)
        # Penting agar jika ada rantai filter F1 -> F2 -> Join,
        # kita proses F2 dulu, merge ke Join, lalu F1 melihat Join yang sudah ter-merge.
        for i in range(len(node.childs)):
            node.childs[i] = walk(node.childs[i])
            
        if node.type == "FILTER" and len(node.childs) == 2:
            target_join = get_underlying_join(node.childs[0])
            
            # Cek Join ID di params
            if target_join and target_join.id in join_params:
                to_merge_global = join_params[target_join.id]
                
                # Kita hanya bisa merge kondisi yang ADA di node Filter ini
                # Filter ini mungkin hanya memiliki sebagian dari 'to_merge_global'
                # jika filternya terpecah-pecah (cascaded).
                
                return merge_selected(node, to_merge_global, target_join, merged_ids)
                        
        return node

    transformed = walk(new_tree)
    
    # Update filter_params: Hapus ID yang sudah di-merge
    # filter_params tetap menggunakan Signature Key (frozenset) karena dari Rule 1
    updated_filter_params = adjust_filter_params(filter_params, merged_ids)
    
    return ParsedQuery(transformed, query.query), join_params, updated_filter_params


def merge_selected(filter_node, merge_ids_target, target_join, merged_ids_tracker):
    """
    Memindahkan kondisi dari Filter ke Join.
    """
    conds = collect_conditions(filter_node.childs[1])
    
    keep = []
    move = []
    
    for c in conds:
        if c.id in merge_ids_target:
            move.append(c.clone(deep=True, preserve_id=True))
            merged_ids_tracker.add(c.id) # Track bahwa ID ini berhasil dipindah
        else:
            keep.append(c.clone(deep=True, preserve_id=True))
        
    if not move:
        return filter_node
    
    # Inject ke Join
    # Ubah type jadi INNER jika sebelumnya CROSS/None
    if target_join.val in ("CROSS", "", None):
        target_join.val = "INNER"
    
    existing = []
    if len(target_join.childs) >= 3:
        existing = collect_conditions(target_join.childs[2])
        
    final = existing + move
    
    # Rebuild Join Condition Node
    if len(final) == 1:
        cond_node = final[0]
    else:
        cond_node = QueryTree("OPERATOR", "AND")
        for c in final: cond_node.add_child(c)
        
    if len(target_join.childs) < 3:
        target_join.add_child(cond_node)
    else:
        target_join.childs[2] = cond_node
    
    # Rebuild Filter Node (Sisa kondisi yang tidak di-merge)
    if not keep:
        # Jika semua kondisi pindah, Filter node hilang, return childnya (source)
        # Child 0 adalah jalur menuju Join (yang sekarang sudah termodifikasi)
        return filter_node.childs[0]
    
    new_filter = QueryTree("FILTER")
    new_filter.add_child(filter_node.childs[0])
    
    if len(keep) == 1:
        new_filter.add_child(keep[0])
    else:
        and_op = QueryTree("OPERATOR", "AND")
        for c in keep: and_op.add_child(c)
        new_filter.add_child(and_op)
        
    return new_filter


def adjust_filter_params(filter_params: dict, merged_ids: set) -> dict:
    """
    Membersihkan ID yang sudah di-merge dari filter_params.
    Key tetap Signature (frozenset) agar konsisten dengan Rule 1.
    """
    if not merged_ids or not filter_params:
        return filter_params
    
    updated = {}
    for sig, order_spec in filter_params.items():
        new_order = []
        for item in order_spec:
            if isinstance(item, list):
                # Group handling
                kept = [x for x in item if x not in merged_ids]
                if len(kept) == 1: new_order.append(kept[0])
                elif len(kept) > 1: new_order.append(kept)
            else:
                # Single handling
                if item not in merged_ids:
                    new_order.append(item)
        
        # Simpan entry walaupun kosong (opsional, tapi aman untuk kestabilan key)
        updated[sig] = new_order
            
    return updated


# --- GENERATE & MUTATE ---

def generate_params(metadata: dict) -> list[int]:
    """
    Generate list of condition IDs to merge for a specific JOIN ID.
    """
    cands = metadata.get('filter_conditions', [])
    if not cands: return []
    
    # Randomly select subset
    num = random.randint(0, len(cands))
    if num == 0: return []
    return random.sample(cands, num)

def copy_params(p: list[int]) -> list[int]:
    return p.copy() if p else []

def mutate_params(p: list[int]) -> list[int]:
    # Simple mutation: remove random condition
    if p and random.random() < 0.5:
        p_copy = p.copy()
        p_copy.pop(random.randint(0, len(p_copy)-1))
        return p_copy
    return p
    # Note: Untuk menambah kondisi (add mutation), kita butuh metadata (candidates).
    # Implementasi simpel ini hanya support pengurangan/pengacakan subset saat init.
    # Idealnya mutation punya akses ke metadata context.

def validate_params(p: list[int]) -> bool:
    if not isinstance(p, list): return False
    return all(isinstance(x, int) for x in p)