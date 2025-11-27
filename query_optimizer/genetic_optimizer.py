"""
Genetic Algorithm - Hybrid Support
- Filter Params: Uses Signature-Based Crossover
- Join/Other Params: Uses ID-Based Crossover (Dictionary Match)
"""

import random
from typing import Callable, Any
from query_optimizer.optimization_engine import ParsedQuery, OptimizationEngine
from query_optimizer.rule_params_manager import get_rule_params_manager
from query_optimizer.rule import rule_1_2, rule_4, rule_5, rule_6

class Individual:
    def __init__(self, operation_params, base_query, lazy_eval=False):
        self.operation_params = operation_params
        self.base_query = base_query
        self._query_cache = None
        self.fitness = None
        if not lazy_eval:
             self._query_cache = self._apply_transformations(base_query)
             
    @property
    def query(self):
        if self._query_cache is None:
            self._query_cache = self._apply_transformations(self.base_query)
        return self._query_cache
        
    def _apply_transformations(self, base_query):
        # Selalu clone dari base clean
        new_tree = base_query.query_tree.clone(deep=True, preserve_id=True)
        q = ParsedQuery(new_tree, base_query.query)
        
        # Get params (Mutable copies)
        fp = self.operation_params.get('filter_params', {})
        jp = self.operation_params.get('join_params', {})
        jcp = self.operation_params.get('join_child_params', {})
        jap = self.operation_params.get('join_associativity_params', {})
        
        # Apply Rules
        # Rule 1 (Filter) - Key: Signature (frozenset)
        if fp:
            q, fp = rule_1_2.apply_rule1_rule2(q, fp)
            self.operation_params['filter_params'] = fp
            
        # Rule 5 (Join Swap) - Key: Int
        if jcp:
            q, jcp = rule_5.apply_join_commutativity(q, jcp)
            self.operation_params['join_child_params'] = jcp

        # Rule 6 (Associativity) - Key: Int
        if jap:
            q = rule_6.apply_associativity(q, jap)
            
        # Rule 4 (Join Merge) - Key: Int
        if jp:
            q, jp, fp = rule_4.apply_merge(q, jp, fp)
            self.operation_params['join_params'] = jp
            self.operation_params['filter_params'] = fp
            
        return q

class GeneticOptimizer:
    def __init__(self, population_size=50, generations=100, mutation_rate=0.1):
        self.pop_size = population_size
        self.gens = generations
        self.mut_rate = mutation_rate
        self.history = []
        
    def optimize(self, query):
        mgr = get_rule_params_manager()
        ops = mgr.get_registered_operations()
        
        # Analyze Base Query
        # Hasilnya akan mixed: Filter punya key frozenset, Join punya key Int
        base_analysis = {}
        for op in ops:
            base_analysis[op] = mgr.analyze_query(query, op)
            
        # Init Pop
        pop = []
        for _ in range(self.pop_size):
            params = {}
            for op, metadata in base_analysis.items():
                params[op] = {}
                for key, meta in metadata.items():
                    # Key akan otomatis mengikuti hasil analyze (Signature atau Int)
                    params[op][key] = mgr.generate_random_params(op, meta)
            pop.append(Individual(params, query))
            
        # Evolution Loop
        for g in range(self.gens):
            # Fitness Eval
            for ind in pop:
                if ind.fitness is None:
                    # Ganti dengan cost function asli Anda
                    eng = OptimizationEngine()
                    ind.fitness = float(eng.get_cost(ind.query))
            
            pop.sort(key=lambda x: x.fitness)
            self.history.append({'gen': g, 'best': pop[0].fitness})
            
            next_pop = pop[:2] # Elitism
            
            while len(next_pop) < self.pop_size:
                p1, p2 = random.sample(pop[:10], 2)
                c1, c2 = self._crossover(p1, p2, query)
                
                if random.random() < self.mut_rate: c1 = self._mutate(c1)
                if random.random() < self.mut_rate: c2 = self._mutate(c2)
                
                next_pop.extend([c1, c2])
            pop = next_pop[:self.pop_size]
            
        return pop[0].query, self.history

    def _crossover(self, p1, p2, base_query):
        """
        Hybrid Crossover:
        - filter_params: Menggunakan Signature Logic (Key: frozenset)
        - join_params dll: Menggunakan ID Logic (Key: int)
        """
        c1_params = {}
        c2_params = {}
        
        all_ops = set(p1.operation_params.keys()) | set(p2.operation_params.keys())
        
        for op in all_ops:
            c1_params[op] = {}
            c2_params[op] = {}
            
            p1_data = p1.operation_params.get(op, {})
            p2_data = p2.operation_params.get(op, {})
            
            # Logic sama untuk keduanya karena kita sudah pakai Signature Key untuk filter
            # Key untuk filter sekarang adalah frozenset, Key untuk join adalah int.
            # Karena frozenset dan int keduanya hashable, kita bisa pakai set union biasa.
            
            all_keys = set(p1_data.keys()) | set(p2_data.keys())
            
            for k in all_keys:
                # Ambil gen
                val1 = p1_data.get(k)
                val2 = p2_data.get(k)
                
                if val1 is None: val1 = val2
                if val2 is None: val2 = val1
                
                # Swap coin flip
                if random.random() < 0.5:
                    c1_params[op][k] = val1 
                    c2_params[op][k] = val2
                else:
                    c1_params[op][k] = val2
                    c2_params[op][k] = val1
                    
        return Individual(c1_params, base_query, lazy_eval=True), Individual(c2_params, base_query, lazy_eval=True)

    def _mutate(self, ind):
        new_params = {op: {k: v for k,v in data.items()} for op, data in ind.operation_params.items()}
        mgr = get_rule_params_manager()
        
        if not new_params: return ind
        
        # Pick operation
        op = random.choice(list(new_params.keys()))
        data = new_params[op]
        
        if data:
            # Pick node (Key bisa int atau frozenset, tidak masalah)
            key = random.choice(list(data.keys()))
            data[key] = mgr.mutate_params(op, data[key])
            
        return Individual(new_params, ind.base_query, lazy_eval=True)