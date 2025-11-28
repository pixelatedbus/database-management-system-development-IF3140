import sys
import random

# ==========================================
# FIX: Force UTF-8 Encoding for Windows Output
# ==========================================
try:
    sys.stdout.reconfigure(encoding='utf-8')
except AttributeError:
    # Fallback for older Python versions or specific environments
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from query_optimizer.genetic_optimizer import GeneticOptimizer, Individual
from query_optimizer.optimization_engine import OptimizationEngine, ParsedQuery
from query_optimizer.rule_params_manager import get_rule_params_manager

# ==========================================
# 0. DEFINISI SKEMA (Untuk Referensi)
# ==========================================
dummy_tables = {
    "users": ["id", "name", "email"],
    "profiles": ["id", "user_id", "bio"],
    "orders": ["id", "user_id", "amount", "total", "product_id"],
    "products": ["id", "name", "category", "price", "stock", "description", "discount"],
    "employees": ["id", "name", "salary", "department", "bonus"],
    "accounts": ["id", "balance"],
    "logs": ["id", "message"],
    "payroll": ["salary"]
}

# ==========================================
# 1. SETUP & QUERY AWAL
# ==========================================

# Query Kompleks: 4 Table Joins + Multiple Filters
# Menggunakan tabel users, profiles, orders, dan products sesuai skema dummy_tables
sql = """
SELECT u.name, prod.name, o.total 
FROM users u 
JOIN profiles prof ON u.id = prof.user_id 
JOIN orders o ON u.id = o.user_id 
JOIN products prod ON o.product_id = prod.id 
WHERE u.email LIKE '%@gmail.com' 
AND prod.category = 'Electronics' 
AND o.total > 100000 
AND prof.bio IS NOT NULL
"""

print(f"Executing Query: {sql}\n")

engine = OptimizationEngine()
query = engine.parse_query(sql)
optimizer_helper = GeneticOptimizer() 
mgr = get_rule_params_manager()

print("==================================================")
print(" 1. QUERY TREE AWAL (BASE)")
print("==================================================")
print(query.query_tree.tree(show_id=True))

# Analisa Rules (Manual Step)
ops = mgr.get_registered_operations()
base_analysis = {}
for op in ops:
    base_analysis[op] = mgr.analyze_query(query, op)

# ==========================================
# 2. POPULASI GENERASI 1
# ==========================================
print("\n==================================================")
print(" 2. GENERASI 1 (INITIAL POPULATION)")
print("==================================================")

population_size = 20
population = []

# Generate Random Population
for _ in range(population_size):
    params = {}
    for op, metadata in base_analysis.items():
        params[op] = {}
        for key, meta in metadata.items():
            params[op][key] = mgr.generate_random_params(op, meta)
    population.append(Individual(params, query))

# Hitung Fitness Gen 1
for ind in population:
    if ind.fitness is None:
        ind.fitness = engine.get_cost(ind.query).total_cost

# Sort by Fitness (Ascending - Cost terendah terbaik)
population.sort(key=lambda x: x.fitness)

def print_individual(title, ind):
    print(f"\n--- {title} ---")
    print(f"Fitness (Cost): {ind.fitness}")
    print(f"Params Applied: {ind.operation_params}")
    print(ind.query.query_tree.tree(show_id=True))

# Tampilkan 5 Child Pertama
print("\n>>> 5 Sample Children dari Generasi 1:")
for i in range(5):
    print_individual(f"Child {i+1}", population[i])

# Tampilkan Child Terbaik Gen 1
print("\n>>> BEST Child Generasi 1:")
print_individual("Best Gen 1", population[0])

# ==========================================
# 3. SIMULASI CROSSOVER & MUTASI (3 PASANG)
# ==========================================
print("\n==================================================")
print(" 3. DEMO CROSSOVER & MUTASI (3 PAIRS)")
print("==================================================")

# Ambil sample parent dari populasi terbaik
parents_pool = population[:10]
demo_pairs = [
    (parents_pool[0], parents_pool[1]), # Pair 1
    (parents_pool[2], parents_pool[3]), # Pair 2
    (parents_pool[4], parents_pool[5])  # Pair 3 (Will Mutate)
]

for i, (p1, p2) in enumerate(demo_pairs):
    print(f"\n################ PASANGAN KE-{i+1} ################")
    print(f"Parent A (Cost: {p1.fitness}) Params: {p1.operation_params}")
    print(f"Parent B (Cost: {p2.fitness}) Params: {p2.operation_params}")
    
    # Lakukan Crossover
    c1, c2 = optimizer_helper._crossover(p1, p2, query)
    
    # Khusus Pasangan ke-3, kita paksa Mutasi pada Child 1
    mutation_note = ""
    if i == 2:
        print("\n[!] MUTASI DIPICU PADA CHILD A...")
        original_params = str(c1.operation_params)
        c1 = optimizer_helper._mutate(c1)
        mutation_note = f" (MUTATED!)\nBefore: {original_params}\nAfter : {c1.operation_params}"
    
    # Hitung fitness child untuk display
    c1.fitness = engine.get_cost(c1.query).total_cost
    c2.fitness = engine.get_cost(c2.query).total_cost
    
    print(f"\n-> Hasil Crossover Child A{mutation_note}:")
    print(f"   Cost: {c1.fitness}")
    print(f"   Tree: \n{c1.query.query_tree.tree(show_id=True)}")
    
    print(f"\n-> Hasil Crossover Child B:")
    print(f"   Cost: {c2.fitness}")
    print(f"   Tree: \n{c2.query.query_tree.tree(show_id=True)}")
    
    # Masukkan ke populasi demo selanjutnya
    population.append(c1)
    population.append(c2)

# ==========================================
# 4. 5 CHILD SELANJUTNYA (HASIL CROSSOVER)
# ==========================================
print("\n==================================================")
print(" 4. 5 CHILD BARU (HASIL DARI PROSES DI ATAS)")
print("==================================================")
# Mengambil 5 individu terakhir yang ditambahkan ke list population (anak-anak baru)
new_children = population[-6:] 
for i, child in enumerate(new_children[:5]):
    print_individual(f"New Child {i+1}", child)


# ==========================================
# 5. EVOLUTION LOOP (STEP PER 10 GENERASI)
# ==========================================
print("\n==================================================")
print(" 5. EVOLUSI (MONITORING PER 10 GENERASI)")
print("==================================================")

total_generations = 30
# Reset populasi ke ukuran tetap untuk memulai loop yang adil
population = population[:population_size] 

for g in range(1, total_generations + 1):
    # Elitism
    next_pop = population[:2]
    
    while len(next_pop) < population_size:
        p1, p2 = random.sample(population[:10], 2)
        c1, c2 = optimizer_helper._crossover(p1, p2, query)
        
        # Chance mutation
        if random.random() < 0.2: c1 = optimizer_helper._mutate(c1)
        if random.random() < 0.2: c2 = optimizer_helper._mutate(c2)
        
        # Calculate fitness immediately for sorting
        c1.fitness = engine.get_cost(c1.query).total_cost
        c2.fitness = engine.get_cost(c2.query).total_cost
        
        next_pop.extend([c1, c2])
    
    population = next_pop[:population_size]
    population.sort(key=lambda x: x.fitness)
    
    # Print setiap 10 generasi
    if g % 10 == 0:
        best = population[0]
        print(f"\n>>> Generasi {g} Best Fitness: {best.fitness}")
        print(f"Params: {best.operation_params}")
        # Tampilkan tree ringkas (opsional, uncomment jika ingin tree penuh tiap 10 gen)
        # print(best.query.query_tree.tree(show_id=True))

# ==========================================
# 6. HASIL AKHIR
# ==========================================
final_best = population[0]
print("\n==================================================")
print(" 6. FINAL OPTIMIZED RESULT")
print("==================================================")
print_individual("FINAL BEST INDIVIDUAL", final_best)