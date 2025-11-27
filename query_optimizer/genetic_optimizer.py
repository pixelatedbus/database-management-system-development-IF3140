"""
Genetic Algorithm
"""

import random
from typing import Callable, Any
from query_optimizer.optimization_engine import ParsedQuery, OptimizationEngine
from query_optimizer.rule.rule_1 import (
    cascade_filters,
    uncascade_filters
)
from query_optimizer.rule.rule_2 import reorder_and_conditions
from query_optimizer.rule import rule_4
from query_optimizer.rule import rule_5
from query_optimizer.rule_params_manager import get_rule_params_manager


class Individual:
    """
    Kromosom
    Attributes:
        operation_params: Dict[operation_type, Dict[node_id, order]]
                         Example: {
                             'filter_params': {
                                 42: [2, [0, 1]],  # Unified order: cond2 single, [0,1] grouped
                                 57: [1, 0]        # All singles
                             },
                             'join_params': {
                                 33: {
                                    filtered_conditon: []  # Keep separate (no merge)
                                    existing_condition: [5, 8]  # conditions merged in JOIN 33
                                 }
                                 42: {
                                    filtered_conditon: [10, 15]  # conditions to merge into JOIN 42
                                    existing_condition: []  # Keep separate (no merge)
                                 }
                             }
                         }
        fitness: Fitness value (lower is better)
    """
    
    def __init__(
        self, 
        operation_params: dict[str, dict[int, Any]],
        base_query: ParsedQuery,
        lazy_eval: bool = False
    ):
        self.operation_params = operation_params
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
        """
        Terapkan transformations ke query berdasarkan operation params.
        """
        cloned_tree = base_query.query_tree.clone(deep=True, preserve_id=True)
        current_query = ParsedQuery(cloned_tree, base_query.query)

        filter_params = self.operation_params.get('filter_params', {})
        join_child_params = self.operation_params.get('join_child_params', {})
        join_params = self.operation_params.get('join_params', {})
        
        # Step 1: Apply Rule 1&2 (filter operations)
        if filter_params:
            from query_optimizer.rule.rule_1 import apply_rule1_rule2
            current_query, filter_params = apply_rule1_rule2(current_query, filter_params)
        
        # Step 2: Apply Rule 5 (join child order)
        if join_child_params:
            from query_optimizer.rule.rule_5 import apply_join_commutativity
            current_query, join_child_params = apply_join_commutativity(current_query, join_child_params)
        
        # Step 3: Apply Rule 4 (join merge)
        if join_params:
            current_query, join_params, filter_params = rule_4.apply_merge(
                current_query, join_params, filter_params
            )
        
        # Update operation_params dengan adjusted params
        self.operation_params = {
            'filter_params': filter_params,
            'join_child_params': join_child_params,
            'join_params': join_params
        }
        
        return current_query
    
    def __repr__(self):
        return f"Individual(fitness={self.fitness}, params={self.operation_params})"


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

    def _update_params(self, individual: Individual) -> Individual:
        new_analysis = self._analyze_query_for_rules(individual.query)
        
        updated_params = {}
        manager = get_rule_params_manager()
        
        for operation_name, new_metadata in new_analysis.items():
            updated_params[operation_name] = {}
            
            for node_id, metadata in new_metadata.items():
                old_params = self._find_matching_old_params(
                    individual.operation_params.get(operation_name, {}),
                    node_id,
                    metadata
                )
                
                if old_params is not None:
                    updated_params[operation_name][node_id] = \
                        self._adjust_params_to_metadata(old_params, metadata, operation_name)
                else:
                    updated_params[operation_name][node_id] = \
                        manager.generate_random_params(operation_name, metadata)
        
        return Individual(updated_params, individual.base_query, lazy_eval=False)
    
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
                    child1, _ = self._crossover(parent1, parent1, query)
                    child2, _ = self._crossover(parent2, parent2, query)
                
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
        Analisa query untuk semua operations yang ter-register.
        
        Returns:
            Dict[operation_name, Dict[node_id, metadata]]
        """
        manager = get_rule_params_manager()
        analysis_results = {}
        
        for operation_name in manager.get_registered_operations():
            analysis_results[operation_name] = manager.analyze_query(query, operation_name)
        
        return analysis_results
    
    def _initialize_population(
        self,
        base_query: ParsedQuery,
        rule_analysis: dict[str, dict[int, Any]]
    ) -> list[Individual]:
        """Inisialisasi populasi dengan random params untuk semua operations."""
        population = []
        manager = get_rule_params_manager()
        
        for _ in range(self.population_size):
            # Generate random params untuk setiap operation
            operation_params = {}
            
            for operation_name, analysis_data in rule_analysis.items():
                operation_params[operation_name] = {}
                for node_id, metadata in analysis_data.items():
                    params = manager.generate_random_params(operation_name, metadata)
                    operation_params[operation_name][node_id] = params
            
            individual = Individual(operation_params, base_query)
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
        
        # Simple uniform crossover per operation
        child1_params = {}
        child2_params = {}
        
        all_operations = set(parent1.operation_params.keys()) | set(parent2.operation_params.keys())
        
        for operation_name in all_operations:
            if random.random() < 0.5:
                child1_params[operation_name] = copy.deepcopy(parent1.operation_params.get(operation_name, {}))
                child2_params[operation_name] = copy.deepcopy(parent2.operation_params.get(operation_name, {}))
            else:
                child1_params[operation_name] = copy.deepcopy(parent2.operation_params.get(operation_name, {}))
                child2_params[operation_name] = copy.deepcopy(parent1.operation_params.get(operation_name, {}))
        
        # Lazy evaluation
        child1 = Individual(child1_params, base_query, lazy_eval=True)
        child2 = Individual(child2_params, base_query, lazy_eval=True)
        
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
        
        # Shallow copy dulu (fast)
        mutated_params = {k: v.copy() for k, v in individual.operation_params.items()}
        
        if mutated_params:
            manager = get_rule_params_manager()
            operation_name = random.choice(list(mutated_params.keys()))
            node_params = mutated_params[operation_name]
            
            if node_params:
                node_id = random.choice(list(node_params.keys()))
                # Deep copy hanya operation yang dimutate
                mutated_params[operation_name] = copy.deepcopy(mutated_params[operation_name])
                # Use operation-specific mutation
                mutated_params[operation_name][node_id] = manager.mutate_params(
                    operation_name,
                    mutated_params[operation_name][node_id]
                )
        
        # Lazy evaluation
        return Individual(mutated_params, base_query, lazy_eval=True)
    
    def get_ga_statistics(self) -> dict:
        """Dapatkan statistik optimasi."""
        return {
            'best_fitness': self.best_fitness,
            'best_params': self.best_individual.operation_params if self.best_individual else None,
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
            print("\nBest Operation Parameters:")
            for operation_name, node_params in self.best_individual.operation_params.items():
                print(f"\n  {operation_name}:")
                for node_id, order in node_params.items():
                    print(f"    Node {node_id}: {order}")
                    # Special formatting for filter_params (unified order format)
                    if operation_name == 'filter_params' and isinstance(order, list):
                        # Show the unified order explanation
                        explanations = []
                        for item in order:
                            if isinstance(item, list):
                                explanations.append(f"({', '.join(map(str, item))} dalam AND)")
                            else:
                                explanations.append(f"{item} single")
                        if explanations:
                            print(f"      → Order: {' -> '.join(explanations)}")
                    # Special formatting for join_params (condition selection)
                    elif operation_name == 'join_params' and isinstance(order, list):
                        if order:
                            print(f"      → Merge conditions: {order}")
                        else:
                            print("      → Keep separate (no merge)")
        
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
    stats = ga.get_ga_statistics()
    
    ga.print_progress()
    
    return optimized_query, stats
