"""
Genetic Algorithm
"""

import random
from typing import Callable, Any
from query_optimizer.optimization_engine import ParsedQuery, OptimizationEngine
from query_optimizer.rule_1 import (
    cascade_filters,
    uncascade_filters,
    clone_tree
)
from query_optimizer.rules_registry import (
    get_all_rules
)
from query_optimizer.rule_params_manager import get_rule_params_manager


class Individual:
    """
    Kromosom dalam populasi GA yang merepresentasikan satu solusi query.
    
    Attributes:
        rule_params: Dict[rule_name, Dict[node_id, params]]
                     Example: {
                         'rule_1': {42: [2, [0,1]], 57: [1, 0]},
                         'rule_2': {123: {...}},
                         ...
                     }
        applied_rules: List of rule names to apply in order
        fitness: Fitness value (lower is better)
    """
    
    def __init__(
        self, 
        rule_params: dict[str, dict[int, Any]],
        base_query: ParsedQuery,
        applied_rules: list[str] | None = None,
        lazy_eval: bool = False
    ):
        self.rule_params = rule_params
        self.applied_rules = applied_rules or []
        self.base_query = base_query
        self._query_cache = None
        self.fitness: float | None = None
        
        if not lazy_eval:
            self._query_cache = self._apply_transformations(base_query)
    
    @property
    def query(self) -> ParsedQuery:
        """Lazy evaluation of query transformations."""
        if self._query_cache is None:
            self._query_cache = self._apply_transformations(self.base_query)
        return self._query_cache
    
    def _apply_transformations(self, base_query: ParsedQuery) -> ParsedQuery:
        """Terapkan transformations ke query berdasarkan rule params."""
        cloned_tree = clone_tree(base_query.query_tree)
        current_query = ParsedQuery(cloned_tree, base_query.query)
        
        # Terapkan optimization rules
        if self.applied_rules:
            all_rules = dict(get_all_rules())
            for rule_name in self.applied_rules:
                if rule_name in all_rules:
                    try:
                        current_query = all_rules[rule_name](current_query)
                    except:
                        pass
        
        # Apply Rule 1 if present (Seleksi Konjungtif)
        if 'rule_1' in self.rule_params:
            query_and = uncascade_filters(current_query)
            if self.rule_params['rule_1']:
                current_query = cascade_filters(query_and, operator_orders=self.rule_params['rule_1'])
            else:
                current_query = query_and
        
        # TODO: Apply other rules (rule_2, rule_3, etc.) when implemented
        
        return current_query
    
    def __repr__(self):
        return f"Individual(fitness={self.fitness}, params={self.rule_params}, rules={self.applied_rules})"


class GeneticOptimizer:
    """
    Args:
        population_size: Jumlah individu dalam populasi
        generations: Jumlah generasi evolusi
        mutation_rate: Probabilitas mutasi (0.0 - 1.0)
        crossover_rate: Probabilitas crossover (0.0 - 1.0)
        elitism: Jumlah individu terbaik yang dipertahankan tiap generasi
        fitness_func: Fungsi untuk menghitung fitness (cost function)
    """
    
    def __init__(
        self,
        population_size: int = 50,
        generations: int = 100,
        mutation_rate: float = 0.1,
        crossover_rate: float = 0.8,
        elitism: int = 2,
        fitness_func: Callable[[ParsedQuery], float] | None = None
    ):
        self.population_size = population_size
        self.generations = generations
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.elitism = elitism
        self.fitness_func = fitness_func or self._default_fitness
        
        self.best_individual: Individual | None = None
        self.best_fitness: float = float('inf')
        self.history: list[dict] = []
        
        self._rule_analysis_cache: dict[str, dict[int, Any]] | None = None
    
    def _default_fitness(self, query: ParsedQuery) -> float:
        """Fungsi fitness default menggunakan cost dari OptimizationEngine (lebih rendah lebih baik)."""
        engine = OptimizationEngine()
        cost = engine.get_cost(query)
        return float(cost)
    
    def optimize(self, query: ParsedQuery) -> ParsedQuery:
        """Jalankan GA untuk mencari struktur query optimal."""
        
        self._rule_analysis_cache = self._analyze_query_for_rules(query)
        
        population = self._initialize_population(query, self._rule_analysis_cache)
        
        for generation in range(self.generations):
            # Evaluasi fitness
            for individual in population:
                if individual.fitness is None:
                    individual.fitness = self.fitness_func(individual.query)
            
            population.sort(key=lambda ind: ind.fitness)
            
            # Track individu terbaik
            if population[0].fitness < self.best_fitness:
                self.best_fitness = population[0].fitness
                self.best_individual = population[0]
            
            # Catat statistik generasi
            self.history.append({
                'generation': generation,
                'best_fitness': population[0].fitness,
                'avg_fitness': sum(ind.fitness for ind in population) / len(population),
                'worst_fitness': population[-1].fitness
            })
            
            # Buat generasi berikutnya
            next_population = []
            next_population.extend(population[:self.elitism])  # Elitism
            
            while len(next_population) < self.population_size:
                parent1 = self._tournament_selection(population)
                parent2 = self._tournament_selection(population)
                
                if random.random() < self.crossover_rate:
                    child1, child2 = self._crossover(parent1, parent2, query)
                else:
                    child1 = self._crossover(parent1, parent1, query)
                    child2 = self._crossover(parent2, parent2, query)
                
                if random.random() < self.mutation_rate:
                    child1 = self._mutate(child1, query, self._rule_analysis_cache)
                if random.random() < self.mutation_rate:
                    child2 = self._mutate(child2, query, self._rule_analysis_cache)
                
                next_population.append(child1)
                if len(next_population) < self.population_size:
                    next_population.append(child2)
            
            population = next_population
        
        # Evaluasi final
        for individual in population:
            if individual.fitness is None:
                individual.fitness = self.fitness_func(individual.query)
        
        population.sort(key=lambda ind: ind.fitness)
        self.best_individual = population[0]
        self.best_fitness = population[0].fitness
        
        return self.best_individual.query
    
    def _analyze_query_for_rules(self, query: ParsedQuery) -> dict[str, dict[int, Any]]:
        """
        Analisa query untuk semua rules yang ter-register.
        
        Returns:
            Dict[rule_name, Dict[node_id, metadata]]
        """
        manager = get_rule_params_manager()
        analysis_results = {}
        
        for rule_name in manager.get_registered_rules():
            analysis_results[rule_name] = manager.analyze_query(query, rule_name)
        
        return analysis_results
    
    def _initialize_population(
        self,
        base_query: ParsedQuery,
        rule_analysis: dict[str, dict[int, Any]]
    ) -> list[Individual]:
        """Inisialisasi populasi dengan random params untuk semua rules."""
        population = []
        manager = get_rule_params_manager()
        all_rule_names = [name for name, _ in get_all_rules()]
        
        for _ in range(self.population_size):
            # Generate random params untuk setiap rule
            rule_params = {}
            
            for rule_name, analysis_data in rule_analysis.items():
                rule_params[rule_name] = {}
                for node_id, metadata in analysis_data.items():
                    params = manager.generate_random_params(rule_name, metadata)
                    rule_params[rule_name][node_id] = params
            
            # Random pilih rules untuk diterapkan
            num_rules = random.randint(1, min(3, len(all_rule_names)))
            applied_rules = random.sample(all_rule_names, num_rules)
            
            individual = Individual(rule_params, base_query, applied_rules)
            population.append(individual)
        
        return population
    
    def _tournament_selection(
        self,
        population: list[Individual],
        tournament_size: int = 3
    ) -> Individual:
        """Seleksi individu menggunakan tournament selection."""
        tournament = random.sample(population, min(tournament_size, len(population)))
        return min(tournament, key=lambda ind: ind.fitness)
    
    def _crossover(
        self,
        parent1: Individual,
        parent2: Individual,
        base_query: ParsedQuery
    ) -> tuple[Individual, Individual]:
        """
        Crossover dua parent. Optimized: simple uniform crossover.
        """
        import copy
        
        # Simple uniform crossover per rule
        child1_params = {}
        child2_params = {}
        
        all_rules = set(parent1.rule_params.keys()) | set(parent2.rule_params.keys())
        
        for rule_name in all_rules:
            if random.random() < 0.5:
                child1_params[rule_name] = copy.deepcopy(parent1.rule_params.get(rule_name, {}))
                child2_params[rule_name] = copy.deepcopy(parent2.rule_params.get(rule_name, {}))
            else:
                child1_params[rule_name] = copy.deepcopy(parent2.rule_params.get(rule_name, {}))
                child2_params[rule_name] = copy.deepcopy(parent1.rule_params.get(rule_name, {}))
        
        # Simple applied_rules crossover
        if random.random() < 0.5:
            child1_rules = parent1.applied_rules.copy()
            child2_rules = parent2.applied_rules.copy()
        else:
            child1_rules = parent2.applied_rules.copy()
            child2_rules = parent1.applied_rules.copy()
        
        # Lazy evaluation
        child1 = Individual(child1_params, base_query, child1_rules, lazy_eval=True)
        child2 = Individual(child2_params, base_query, child2_rules, lazy_eval=True)
        
        return child1, child2
    
    def _mutate(
        self,
        individual: Individual,
        base_query: ParsedQuery,
        rule_analysis: dict[str, dict[int, Any]]
    ) -> Individual:
        """
        Mutasi individu. Optimized: hanya copy yang dimutate.
        """
        import copy
        
        mutation_type = random.choice(['params', 'rules'])
        
        # Shallow copy dulu (fast)
        mutated_params = {k: v.copy() for k, v in individual.rule_params.items()}
        mutated_rules = individual.applied_rules.copy()
        
        if mutation_type == 'params' and mutated_params:
            manager = get_rule_params_manager()
            rule_name = random.choice(list(mutated_params.keys()))
            node_params = mutated_params[rule_name]
            
            if node_params:
                node_id = random.choice(list(node_params.keys()))
                # Deep copy hanya rule yang dimutate
                mutated_params[rule_name] = copy.deepcopy(mutated_params[rule_name])
                # Use rule-specific mutation
                mutated_params[rule_name][node_id] = manager.mutate_params(
                    rule_name,
                    mutated_params[rule_name][node_id]
                )
        
        elif mutation_type == 'rules':
            all_rule_names = [name for name, _ in get_all_rules()]
            rule_mutation = random.choice(['add', 'remove', 'replace'])
            
            if rule_mutation == 'add' and len(mutated_rules) < 5:
                available_rules = [r for r in all_rule_names if r not in mutated_rules]
                if available_rules:
                    mutated_rules.append(random.choice(available_rules))
            
            elif rule_mutation == 'remove' and mutated_rules:
                mutated_rules.pop(random.randint(0, len(mutated_rules) - 1))
            
            elif rule_mutation == 'replace' and mutated_rules:
                idx = random.randint(0, len(mutated_rules) - 1)
                available_rules = [r for r in all_rule_names if r not in mutated_rules]
                if available_rules:
                    mutated_rules[idx] = random.choice(available_rules)
        
        # Lazy evaluation
        return Individual(mutated_params, base_query, mutated_rules, lazy_eval=True)
    
    def get_statistics(self) -> dict:
        """Dapatkan statistik optimasi."""
        return {
            'best_fitness': self.best_fitness,
            'best_params': self.best_individual.rule_params if self.best_individual else None,
            'best_rules': self.best_individual.applied_rules if self.best_individual else None,
            'generations': len(self.history),
            'history': self.history
        }
    
    def print_progress(self):
        """Tampilkan progress optimasi."""
        if not self.history:
            print("No optimization run yet.")
            return
        
        print("\n=== Genetic Algorithm Optimization Results ===")
        print(f"Generations: {len(self.history)}")
        print(f"Population Size: {self.population_size}")
        print(f"Best Fitness: {self.best_fitness:.2f}")
        
        if self.best_individual:
            print(f"\nBest Rule Parameters:")
            for rule_name, node_params in self.best_individual.rule_params.items():
                print(f"\n  {rule_name}:")
                for node_id, params in node_params.items():
                    print(f"    Node {node_id}: {params}")
                    # Special formatting for rule_1
                    if rule_name == 'rule_1' and isinstance(params, list):
                        explanations = []
                        for item in params:
                            if isinstance(item, list):
                                explanations.append(f"({', '.join(map(str, item))} dalam AND)")
                            else:
                                explanations.append(f"{item} single")
                        if explanations:
                            print(f"      → {' -> '.join(explanations)}")
            print(f"\nApplied Rules: {self.best_individual.applied_rules}")
        
        print("\nProgress:")
        print("Gen | Best    | Average | Worst")
        print("----|---------|---------|--------")
        
        for i, record in enumerate(self.history):
            if i % max(1, len(self.history) // 10) == 0 or i == len(self.history) - 1:
                print(f"{record['generation']:3d} | "
                      f"{record['best_fitness']:7.2f} | "
                      f"{record['avg_fitness']:7.2f} | "
                      f"{record['worst_fitness']:7.2f}")


def optimize_query_genetic(
    query: ParsedQuery,
    population_size: int = 50,
    generations: int = 100,
    mutation_rate: float = 0.1,
    fitness_func: Callable[[ParsedQuery], float] | None = None
) -> tuple[ParsedQuery, dict]:
    """Optimasi query menggunakan GA (convenience function)."""
    ga = GeneticOptimizer(
        population_size=population_size,
        generations=generations,
        mutation_rate=mutation_rate,
        fitness_func=fitness_func
    )
    
    optimized_query = ga.optimize(query)
    stats = ga.get_statistics()
    
    ga.print_progress()
    
    return optimized_query, stats
