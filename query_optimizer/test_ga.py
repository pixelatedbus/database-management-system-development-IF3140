
from query_optimizer.genetic_optimizer import GeneticOptimizer, Individual
from query_optimizer.optimization_engine import OptimizationEngine

# Example SQL query for testing
sql = "SELECT users.email FROM users JOIN profiles ON users.id = profiles.user_id WHERE users.age > 25 AND users.status = 'active' AND profiles.bio IS NOT NULL"

# Initialize engine and parse query
engine = OptimizationEngine()
query = engine.parse_query(sql)

# Analyze rules for the query
optimizer = GeneticOptimizer(population_size=20, generations=1)
rule_analysis = optimizer._analyze_query_for_rules(query)

print("params", rule_analysis)
print(query.query_tree.tree(show_id=True))

# Initialize Genetic Algorithm optimizer
optimizer = GeneticOptimizer(population_size=20, generations=1)


# Analyze rules for the query
rule_analysis = optimizer._analyze_query_for_rules(query)

# Generate the first population (generation 0)
population = optimizer._initialize_population(query, rule_analysis)



print("First generation (5 children):")
for i, individual in enumerate(population[:5]):
    print(f"\nChild {i+1}:")
    print("Params:", individual.operation_params)
    print("Fitness:", individual.fitness)
    print("Tree:")
    print(individual.query.query_tree.tree(show_id=True))

