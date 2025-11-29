"""
Demo scenarios for Rule 2 (Seleksi Komutatif - Reordering)
"""

from query_optimizer.optimization_engine import OptimizationEngine
from query_optimizer.rule.rule_1_2 import analyze_and_operators, apply_rule1_rule2

def print_separator(title):
    print("\n" + "="*70)
    print(f"  {title}")
    print("="*70)

def get_signature_and_ids(query):
    signatures = analyze_and_operators(query)
    if not signatures:
        raise Exception("Tidak ditemukan operator AND/Filter pada query ini.")
    
    sig_key = list(signatures.keys())[0]
    condition_ids = signatures[sig_key]
    return sig_key, condition_ids

def print_tree_structure(query, label):
    print(f"\n--- {label} ---")
    print(query.query_tree.tree(show_id=True))

def scenario_1_vertical_reordering():
    print("\n")
    print_separator("SCENARIO 2.1: Vertical Reordering mengubah urutan level filter")
    
    sql = "SELECT * FROM users WHERE col_A = 100 AND col_B > 50 AND col_C = 'active'"
    
    engine = OptimizationEngine()
    query = engine.parse_query(sql)
    
    sig_key, ids = get_signature_and_ids(query)
    
    # ORDER 1: A -> B -> C (A paling atas, C paling bawah/awal)
    order_1 = [ids[0], ids[1], ids[2]]
    params_1 = {sig_key: order_1}

    print(f"Applying Params: {params_1}")
    
    q1, _ = apply_rule1_rule2(query, params_1)
    print_tree_structure(q1, f"Order 1: {order_1} (Top -> Bottom)")
    
    # ORDER 2: C -> B -> A (C paling atas, A paling bawah/awal)
    order_2 = [ids[2], ids[1], ids[0]]
    params_2 = {sig_key: order_2}

    print(f"Applying Params: {params_2}")
    
    q2, _ = apply_rule1_rule2(q1, params_2)
    print_tree_structure(q2, f"Order 2: {order_2} (Top -> Bottom)")

def scenario_2_horizontal_reordering():
    print("\n")
    print_separator("SCENARIO 2.2: Horizontal Reordering mengubah urutan dalam satu level Operator")
    
    sql = "SELECT * FROM users WHERE col_A = 100 AND col_B > 50 AND col_C = 'active'"
    
    engine = OptimizationEngine()
    query = engine.parse_query(sql)
    sig_key, ids = get_signature_and_ids(query)

    # Group A: [A, B, C]
    group_order_1 = [ids[0], ids[1], ids[2]]
    params_1 = {sig_key: [group_order_1]} 

    print(f"Applying Params: {params_1}")
    
    q1, _ = apply_rule1_rule2(query, params_1)
    print_tree_structure(q1, "Horizontal Order 1: [A, B, C]")
    
    # Group B: [C, A, B]
    group_order_2 = [ids[2], ids[0], ids[1]]
    params_2 = {sig_key: [group_order_2]}

    print(f"Applying Params: {params_2}")
    
    q2, _ = apply_rule1_rule2(query, params_2)
    print_tree_structure(q2, "Horizontal Order 2: [C, A, B]")
    
def scenario_3_complex_shuffle():
    print("\n")
    print_separator("SCENARIO 2.3: Complex Shuffle - Kombinasi Vertical & Horizontal Reordering")
    
    sql = "SELECT * FROM users WHERE col_A = 100 AND col_B > 50 AND col_C = 'active' AND col_D < 5"
    
    engine = OptimizationEngine()
    query = engine.parse_query(sql)
    sig_key, ids = get_signature_and_ids(query)

    # A, B, C, D
    a, b, c, d = ids[0], ids[1], ids[2], ids[3]
    
    # Formasi 1: Top=A, Bottom=[B, C, D]
    params_1 = {sig_key: [a, [b, c, d]]}

    print(f"Applying Params: {params_1}")

    q1, _ = apply_rule1_rule2(query, params_1)
    print_tree_structure(q1, "Formasi 1: A di atas, (B,C,D) di bawah")
    
    # Formasi 2: Top=[D, A], Bottom=[C, B]
    params_2 = {sig_key: [[d, a], [c, b]]}

    print(f"Applying Params: {params_2}")

    q2, _ = apply_rule1_rule2(q1, params_2)
    print_tree_structure(q2, "Formasi 2: (D,A) di atas, (C,B) di bawah")