"""
Demo scenarios for Rule 8 - Projection Push-down over Joins
"""

import sys
from unittest.mock import patch, MagicMock

# Mock metadata agar bisa bekerja tanpa database asli
MOCK_METADATA = {
    "columns": {
        "users": ["id", "name", "email", "age", "city", "status"],
        "orders": ["order_id", "user_id", "amount", "date", "status"],
        "products": ["product_id", "name", "price", "category"]
    }
}

mock_query_check = MagicMock()
mock_query_check.get_metadata.side_effect = lambda: MOCK_METADATA
mock_query_check.check_query.side_effect = lambda *args, **kwargs: None # Mock check_query to do nothing

# Patch sys.modules before importing OptimizationEngine
with patch.dict(sys.modules, {'query_optimizer.query_check': mock_query_check}):
    from query_optimizer.optimization_engine import OptimizationEngine
    from query_optimizer.rule.rule_8 import push_projection_over_joins, undo_rule8

def print_separator(title):
    print("\n" + "="*80)
    print(f"  {title}")
    print("="*80)

def print_tree_structure(query, label):
    print(f"\n--- {label} ---")
    print(query.query_tree.tree(show_id=True))

def scenario_1_basic_pushdown():
    print("\n")
    print_separator("SCENARIO 1: Basic Pushdown")
    print("- Query meminta 'users.name' dan 'orders.amount'.")
    print("- Rule 8 harus menyisipkan PROJECT node sebelum JOIN untuk membuang kolom tak terpakai.")
    
    sql = "SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id"
    print(f"Query: {sql}")
    
    engine = OptimizationEngine()
    query = engine.parse_query(sql)
    print_tree_structure(query, "Original Tree (8.1)")
    
    optimized_query = push_projection_over_joins(query)
    
    print_tree_structure(optimized_query, "Result Tree (8.1)")
    print("Yang terjadi:")
    print("1. Pada cabang 'users': Terbuat PROJECT (name, id). 'email/age' dibuang sebelum join.")
    print("2. Pada cabang 'orders': Terbuat PROJECT (amount, user_id).")
    print("3. Kolom 'id' dan 'user_id' tetap ada karena dibutuhkan untuk kondisi JOIN.")

def scenario_2_join_key_preservation():
    print("\n")
    print_separator("SCENARIO 2: Join Key Preservation")
    print("- Query hanya meminta 'users.name'. Kolom dari 'orders' tidak diminta di SELECT,")
    print("- tetapi 'orders' diperlukan untuk filter JOIN.")
    
    sql = "SELECT users.name FROM users JOIN orders ON users.id = orders.user_id"
    
    engine = OptimizationEngine()
    query = engine.parse_query(sql)
    print_tree_structure(query, "Original Tree (8.2)")
    
    optimized_query = push_projection_over_joins(query)
    
    print_tree_structure(optimized_query, "Result Tree (8.2)")
    print("Yang terjadi:")
    print("1. Cabang 'users': PROJECT (name, id).")
    print("2. Cabang 'orders': PROJECT (user_id).")
    print("   Meskipun tidak ada kolom orders di SELECT, 'user_id' tetap diproyeksikan")
    print("   karena dibutuhkan oleh kondisi JOIN (users.id = orders.user_id).")

def scenario_3_nested_joins():
    print("\n")
    print_separator("SCENARIO 3: Pushdown pada Nested Joins (3 Tables)")
    print("- Optimasi harus bisa masuk ke dalam struktur join bertingkat.")
    
    sql = "SELECT users.name, products.price FROM users JOIN orders ON users.id = orders.user_id JOIN products ON orders.product_id = products.id"
    
    engine = OptimizationEngine()
    query = engine.parse_query(sql)
    print_tree_structure(query, "Original Tree (8.3)")
    
    optimized_query = push_projection_over_joins(query)
    
    print_tree_structure(optimized_query, "Result Tree (8.3)")
    print("Yang terjadi:")
    print("Proyeksi harus turun ke masing-masing tabel sumber (users, orders, products).")
    print("Tabel 'orders' hanya berfungsi sebagai jembatan (bridge table), jadi hanya key-nya yang diambil.")

def scenario_4_undo_optimization():
    print("\n")
    print_separator("SCENARIO 4: Undo Optimization")
    print("- Mengembalikan struktur tree ke bentuk semula (menghapus Projection di bawah Join).")
    
    sql = "SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id"
    engine = OptimizationEngine()
    query = engine.parse_query(sql)
    print_tree_structure(query, "Original Tree (8.4)")
    
    # 1. Optimize
    opt_query = push_projection_over_joins(query)
    print_tree_structure(opt_query, "Step 1: Optimized")
    
    # 2. Undo
    restored_query = undo_rule8(opt_query)
    print_tree_structure(restored_query, "Step 2: Restored (Undo)")
    
    print("Analisis:")
    print("Node PROJECT yang disisipkan di antara JOIN dan RELATION telah dihapus.")

def scenario_5_star_query():
    print("\n")
    print_separator("SCENARIO 5: SELECT * (Negative Test)")
    print("Jika SELECT *, biasanya tidak ada pushdown projection karena semua kolom dibutuhkan.")
    print("(Kecuali optimizer melakukan ekspansi * menjadi list kolom, rule ini mengecek 'val == *')")
    
    sql = "SELECT * FROM users JOIN orders ON users.id = orders.user_id"
    
    engine = OptimizationEngine()
    query = engine.parse_query(sql)
    print_tree_structure(query, "Original Tree (8.5)")

    optimized_query = push_projection_over_joins(query)
    
    print_tree_structure(optimized_query, "Result Tree (8.5)")