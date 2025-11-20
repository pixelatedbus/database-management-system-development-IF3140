"""
Genetic Algorithm
"""

import random
from typing import Callable
from query_optimizer.optimization_engine import ParsedQuery, OptimizationEngine
from query_optimizer.seleksi_konjungtif import (
    cascade_filters,
    uncascade_filters,
    is_conjunctive_filter,
    clone_tree
)
from query_optimizer.rules_registry import (
    get_all_rules
)


class Individual:
    """Kromosom dalam populasi GA yang merepresentasikan satu solusi query."""
    
    def __init__(
        self, 
        filter_orders: dict[int, list[int]], 
        base_query: ParsedQuery,
        applied_rules: list[str] | None = None
    ):
        self.filter_orders = filter_orders
        self.applied_rules = applied_rules or []
        self.query = self._apply_orders(base_query)
        self.fitness: float | None = None
    
    def _apply_orders(self, base_query: ParsedQuery) -> ParsedQuery:
        """Terapkan rules dan filter order ke query."""
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
        
        # Uncascade dulu, baru cascade dengan order baru
        query_and = uncascade_filters(current_query)
        
        if self.filter_orders:
            first_order = list(self.filter_orders.values())[0]
            result = cascade_filters(query_and, first_order)
            return result
        
        return query_and
    
    def __repr__(self):
        return f"Individual(orders={self.filter_orders}, rules={self.applied_rules}, fitness={self.fitness})"


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
    
    def _default_fitness(self, query: ParsedQuery) -> float:
        """Fungsi fitness default menggunakan cost dari OptimizationEngine (lebih rendah lebih baik)."""
        engine = OptimizationEngine()
        cost = engine.get_cost(query)
        return float(cost)
    
    def optimize(self, query: ParsedQuery) -> ParsedQuery:
        """Jalankan GA untuk mencari struktur query optimal."""
        
        filter_info = self._analyze_filters(query)
        if not filter_info:
            return query
        
        population = self._initialize_population(query, filter_info)
        
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
                    child1 = Individual(parent1.filter_orders.copy(), query, parent1.applied_rules.copy())
                    child2 = Individual(parent2.filter_orders.copy(), query, parent2.applied_rules.copy())
                
                if random.random() < self.mutation_rate:
                    child1 = self._mutate(child1, query, filter_info)
                if random.random() < self.mutation_rate:
                    child2 = self._mutate(child2, query, filter_info)
                
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
    
    def _analyze_filters(self, query: ParsedQuery) -> dict[int, int]:
        """Analisa query untuk mencari filter AND dan jumlah kondisinya."""
        filters = {}
        filter_id = [0]
        
        def analyze(node):
            if node is None:
                return
            
            if is_conjunctive_filter(node):
                num_conditions = len(node.childs) - 1
                if num_conditions >= 2:
                    filters[filter_id[0]] = num_conditions
                    filter_id[0] += 1
            
            for child in node.childs:
                analyze(child)
        
        analyze(query.query_tree)
        return filters
    
    def _initialize_population(
        self,
        base_query: ParsedQuery,
        filter_info: dict[int, int]
    ) -> list[Individual]:
        """Inisialisasi populasi dengan random filter order dan rules."""
        population = []
        all_rule_names = [name for name, _ in get_all_rules()]
        
        for _ in range(self.population_size):
            filter_orders = {}
            for filter_id, num_conditions in filter_info.items():
                order = list(range(num_conditions))
                random.shuffle(order)
                filter_orders[filter_id] = order
            
            num_rules = random.randint(1, min(3, len(all_rule_names)))
            applied_rules = random.sample(all_rule_names, num_rules)
            
            individual = Individual(filter_orders, base_query, applied_rules)
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
        """Crossover dua parent menggunakan OX untuk permutasi dan mixing untuk rules."""
        child1_orders = {}
        child2_orders = {}
        
        for filter_id in parent1.filter_orders.keys():
            p1_order = parent1.filter_orders[filter_id]
            p2_order = parent2.filter_orders[filter_id]
            
            size = len(p1_order)
            if size < 2:
                child1_orders[filter_id] = p1_order.copy()
                child2_orders[filter_id] = p2_order.copy()
                continue
            
            cx_point1 = random.randint(0, size - 1)
            cx_point2 = random.randint(cx_point1 + 1, size)
            
            c1_order = self._order_crossover(p1_order, p2_order, cx_point1, cx_point2)
            c2_order = self._order_crossover(p2_order, p1_order, cx_point1, cx_point2)
            
            child1_orders[filter_id] = c1_order
            child2_orders[filter_id] = c2_order
        
        # Mixing rules dari kedua parent
        all_rules = set(parent1.applied_rules + parent2.applied_rules)
        if all_rules:
            num_rules1 = random.randint(1, min(len(all_rules), 3))
            child1_rules = random.sample(list(all_rules), num_rules1)
            num_rules2 = random.randint(1, min(len(all_rules), 3))
            child2_rules = random.sample(list(all_rules), num_rules2)
        else:
            child1_rules = []
            child2_rules = []
        
        child1 = Individual(child1_orders, base_query, child1_rules)
        child2 = Individual(child2_orders, base_query, child2_rules)
        
        return child1, child2
    
    def _order_crossover(
        self,
        parent1: list[int],
        parent2: list[int],
        cx1: int,
        cx2: int
    ) -> list[int]:
        """Order crossover (OX) untuk permutasi."""
        size = len(parent1)
        child = [-1] * size
        child[cx1:cx2] = parent1[cx1:cx2]
        
        p2_idx = cx2
        c_idx = cx2
        while -1 in child:
            if parent2[p2_idx % size] not in child:
                child[c_idx % size] = parent2[p2_idx % size]
                c_idx += 1
            p2_idx += 1
        
        return child
    
    def _mutate(
        self,
        individual: Individual,
        base_query: ParsedQuery,
        filter_info: dict[int, int]
    ) -> Individual:
        """Mutasi individu dengan swap filter order atau ubah rules."""
        mutated_orders = {}
        
        for filter_id, order in individual.filter_orders.items():
            new_order = order.copy()
            if len(new_order) >= 2:
                idx1, idx2 = random.sample(range(len(new_order)), 2)
                new_order[idx1], new_order[idx2] = new_order[idx2], new_order[idx1]
            mutated_orders[filter_id] = new_order
        
        mutated_rules = individual.applied_rules.copy()
        all_rule_names = [name for name, _ in get_all_rules()]
        mutation_type = random.choice(['add', 'remove', 'replace'])
        
        if mutation_type == 'add' and len(mutated_rules) < 5:
            available_rules = [r for r in all_rule_names if r not in mutated_rules]
            if available_rules:
                mutated_rules.append(random.choice(available_rules))
        
        elif mutation_type == 'remove' and len(mutated_rules) > 0:
            mutated_rules.pop(random.randint(0, len(mutated_rules) - 1))
        
        elif mutation_type == 'replace' and len(mutated_rules) > 0:
            idx = random.randint(0, len(mutated_rules) - 1)
            available_rules = [r for r in all_rule_names if r not in mutated_rules]
            if available_rules:
                mutated_rules[idx] = random.choice(available_rules)
        
        return Individual(mutated_orders, base_query, mutated_rules)
    
    def get_statistics(self) -> dict:
        """Dapatkan statistik optimasi."""
        return {
            'best_fitness': self.best_fitness,
            'best_orders': self.best_individual.filter_orders if self.best_individual else None,
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
            print(f"\nBest Filter Orders:")
            for filter_id, order in self.best_individual.filter_orders.items():
                print(f"  Filter {filter_id}: {order}")
        
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
