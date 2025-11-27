from query_optimizer.genetic_optimizer import GeneticOptimizer, Individual
from query_optimizer.optimization_engine import OptimizationEngine
from query_optimizer.rule_params_manager import get_rule_params_manager

# Example SQL query for testing
sql = "SELECT users.email FROM users JOIN profiles ON users.id = profiles.user_id WHERE users.age > 25 AND users.status = 'active' AND profiles.bio IS NOT NULL"

# Initialize engine and parse query
engine = OptimizationEngine()
query = engine.parse_query(sql)

print("=== Query Tree Awal ===")
print(query.query_tree.tree(show_id=True))

# --- MANUAL STEP 1: Analisa Rules ---
# Karena method _analyze_query_for_rules sudah di-inline ke optimize,
# kita panggil RuleParamsManager secara manual untuk testing.
mgr = get_rule_params_manager()
ops = mgr.get_registered_operations()

rule_analysis = {}
for op in ops:
    rule_analysis[op] = mgr.analyze_query(query, op)

print("\n=== Rule Analysis Result ===")
print(rule_analysis)

# --- MANUAL STEP 2: Initialize Population ---
# Membangun populasi manual untuk testing
population_size = 20
population = []

for _ in range(population_size):
    params = {}
    for op, metadata in rule_analysis.items():
        params[op] = {}
        for key, meta in metadata.items():
            # Key bisa berupa int (NodeID) atau frozenset (Signature)
            # Generate random params untuk setiap key yang ditemukan
            params[op][key] = mgr.generate_random_params(op, meta)
    
    # Buat individual baru
    ind = Individual(params, query)
    population.append(ind)

print(f"\n=== First Generation ({len(population)} individuals) ===")
# Tampilkan contoh 1 individu pertama
for i, individual in enumerate(population[:1]):
    print(f"\nChild {i+1}:")
    print("Params:", individual.operation_params)
    print("Fitness:", individual.fitness) # Akan None karena belum dievaluasi
    print("Tree Transformation:")
    print(individual.query.query_tree.tree(show_id=True))

# Jika ingin mengetes optimasi penuh:
# optimizer = GeneticOptimizer(population_size=20, generations=1)
# optimized_query = optimizer.optimize(query)