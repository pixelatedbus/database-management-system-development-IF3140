"""
Demo scenarios for Rule 1 (Seleksi Konjungtif)
"""

from query_optimizer.optimization_engine import OptimizationEngine
from query_optimizer.rule.rule_1_2 import (
    analyze_and_operators, 
    apply_rule1_rule2
)

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

def scenario_1_full_cascade():
    print("\n")
    print_separator("SCENARIO 1.1: Cascade - Semua menjadi single (Split)")
    
    sql = "SELECT * FROM users WHERE age > 25 AND status = 'active' AND city = 'Jakarta'"
    print(f"Query: {sql}")
    
    engine = OptimizationEngine()
    query = engine.parse_query(sql)
    
    sig_key, ids = get_signature_and_ids(query)
    
    sorted_ids = sorted(ids) 
    params = {
        sig_key: sorted_ids 
    }
    print("Signaure yang ditemukan:", sig_key)
    print_tree_structure(query, "Initial Tree (1.1)")
    
    print(f"Applying Params: {params}")
    
    new_query, _ = apply_rule1_rule2(query, params)
    
    print_tree_structure(new_query, "Result Tree (1.1)")

def scenario_2_no_cascade():
    print("\n")
    print_separator("SCENARIO 1.2: Uncascade - Semua dibawah 1 operator AND (Grouped)")
    
    sql = "SELECT * FROM users WHERE age > 25 AND status = 'active' AND city = 'Jakarta'"
    
    engine = OptimizationEngine()
    query = engine.parse_query(sql)
    
    sig_key, ids = get_signature_and_ids(query)
    
    params = {
        sig_key: [ids]
    }
    print("Signaure yang ditemukan:", sig_key)
    print_tree_structure(query, "Initial Tree (1.2)")
    
    print(f"Applying Params: {params}")
    
    new_query, _ = apply_rule1_rule2(query, params)
    
    print_tree_structure(new_query, "Result Tree (1.2)")

def scenario_3_mixed_cascade():
    print("\n")
    print_separator("SCENARIO 1.3: Mixed Cascade - Beberapa single, beberapa dibawah AND")
    
    sql = "SELECT * FROM users WHERE age > 25 AND status = 'active' AND city = 'Jakarta'"
    
    engine = OptimizationEngine()
    query = engine.parse_query(sql)
    
    sig_key, ids = get_signature_and_ids(query)
    
    single_id = ids[0]
    grouped_ids = ids[1:]
    
    params = {
        sig_key: [single_id, grouped_ids]
    }
    print("Signaure yang ditemukan:", sig_key)
    print_tree_structure(query, "Initial Tree (1.3)")
    
    print(f"Applying Params: {params}")
    
    new_query, _ = apply_rule1_rule2(query, params)
    
    print_tree_structure(new_query, "Result Tree (1.3)")


def scenario_4_cycle_transitions():
    print("\n")
    print_separator("SCENARIO 1.4: Cycle Transitions (Cascade -> Mixed -> Uncascade)")
    
    sql = "SELECT * FROM users WHERE age > 25 AND status = 'active' AND city = 'Jakarta'"
    
    engine = OptimizationEngine()
    initial_query = engine.parse_query(sql)
    
    sig_key, ids = get_signature_and_ids(initial_query)
    ids = sorted(ids)
    print("Signaure yang ditemukan:", sig_key)
    print_tree_structure(initial_query, "0. Initial Query Tree")

    # TAHAP 1: Ubah ke FULL CASCADE (Split semua)
    params_cascade = { sig_key: ids }
    print(f"\n[TRANSFORM 1] Applying Cascade Params: {params_cascade}")
    
    query_cascade, _ = apply_rule1_rule2(initial_query, params_cascade)
    print_tree_structure(query_cascade, "1. Result: Full Cascade")
    
    # TAHAP 2: Ubah ke MIXED (Sebagian split, sebagian group)
    params_mixed = { sig_key: [ids[0], ids[1:]] } 
    print(f"\n[TRANSFORM 2] Applying Mixed Params: {params_mixed}")
    
    query_mixed, _ = apply_rule1_rule2(query_cascade, params_mixed)
    print_tree_structure(query_mixed, "2. Result: Mixed Structure")

    # TAHAP 3: Ubah ke FULL UNCASCADE (Gabung total)
    params_uncascade = { sig_key: [ids] }
    print(f"\n[TRANSFORM 3] Applying Uncascade Params: {params_uncascade}")
    
    query_uncascade, _ = apply_rule1_rule2(query_mixed, params_uncascade)
    print_tree_structure(query_uncascade, "3. Result: Full Uncascade (Single Operator)")