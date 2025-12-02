"""
Genetic Algorithm - Hybrid Support
- Filter Params: Uses Signature-Based Crossover
- Join/Other Params: Uses ID-Based Crossover (Dictionary Match)
"""

import random
import copy
from typing import Callable, Any
from query_optimizer.optimization_engine import ParsedQuery, OptimizationEngine
from query_optimizer.rule_params_manager import get_rule_params_manager
from query_optimizer.rule import rule_1_2, rule_4, rule_5, rule_6

class Individual:
    def __init__(self, operation_params, base_query, lazy_eval=False, genealogy=None):
        self.operation_params = operation_params
        self.base_query = base_query
        self._query_cache = None
        self.fitness = None
        self.genealogy = genealogy or {} 
        
        if not lazy_eval:
             self._query_cache = self._apply_transformations(base_query)
             
    @property
    def query(self):
        if self._query_cache is None:
            self._query_cache = self._apply_transformations(self.base_query)
        return self._query_cache

    def _inject_join_params_into_filter(self, filter_params, join_params):
        fp_complete = {k: copy.deepcopy(v) for k, v in filter_params.items()}
        
        join_target_ids = set()
        for target_list in join_params.values():
            join_target_ids.update(target_list)
            
        if not join_target_ids:
            return fp_complete
            
        for sig, order_spec in fp_complete.items():
            existing_ids = set()
            for item in order_spec:
                if isinstance(item, list):
                    existing_ids.update(item)
                else:
                    existing_ids.add(item)
            
            missing_ids = [x for x in sig if x in join_target_ids and x not in existing_ids]
            
            for mid in missing_ids:
                order_spec.append(mid)
                
        return fp_complete
        
    def _apply_transformations(self, base_query):
        new_tree = base_query.query_tree.clone(deep=True, preserve_id=True)
        q = ParsedQuery(new_tree, base_query.query)
        
        fp_raw = self.operation_params.get('filter_params', {})
        jp = self.operation_params.get('join_params', {})
        jcp = self.operation_params.get('join_child_params', {})
        jap = self.operation_params.get('join_associativity_params', {})
        
        fp_for_rule1 = self._inject_join_params_into_filter(fp_raw, jp)

        if fp_for_rule1:
            q, _ = rule_1_2.apply_rule1_rule2(q, fp_for_rule1)
            
        if jp:
            q, jp, fp_clean = rule_4.apply_merge(q, jp, fp_for_rule1)
            self.operation_params['join_params'] = jp
            self.operation_params['filter_params'] = fp_clean
        else:
            self.operation_params['filter_params'] = fp_for_rule1
        
        if jap:
            q = rule_6.apply_associativity(q, jap)
        
        if jcp:
            q, jcp = rule_5.apply_join_commutativity(q, jcp)
            self.operation_params['join_child_params'] = jcp
            
        return q

class GeneticOptimizer:
    def __init__(self, population_size=50, generations=100, mutation_rate=0.1, elitism=2):
        self.pop_size = population_size
        self.gens = generations
        self.mut_rate = mutation_rate
        self.elitism = elitism
        self.history = []
        
    def optimize(self, query):
        mgr = get_rule_params_manager()
        ops = mgr.get_registered_operations()
        
        base_analysis = {}
        for op in ops:
            base_analysis[op] = mgr.analyze_query(query, op)
            
        pop = []
        for _ in range(self.pop_size):
            params = {}
            for op, metadata in base_analysis.items():
                params[op] = {}
                for key, meta in metadata.items():
                    params[op][key] = mgr.generate_random_params(op, meta)
            pop.append(Individual(params, query))
            
        for g in range(self.gens):
            for ind in pop:
                if ind.fitness is None:
                    eng = OptimizationEngine()
                    ind.fitness = eng.get_cost(ind.query)
            
            pop.sort(key=lambda x: x.fitness)
            self.history.append({'gen': g, 'best': pop[0].fitness})
            
            next_pop = pop[:self.elitism]
            
            while len(next_pop) < self.pop_size:
                p1, p2 = random.sample(pop[:10], 2)
                c1, c2 = self._crossover(p1, p2, query)
                
                if random.random() < self.mut_rate: c1 = self._mutate(c1)
                if random.random() < self.mut_rate: c2 = self._mutate(c2)
                
                next_pop.extend([c1, c2])
            pop = next_pop[:self.pop_size]
            
        return pop[0].query, self.history

    def _crossover(self, p1, p2, base_query):
        c1_params = {}
        c2_params = {}
        c1_genealogy = {}
        c2_genealogy = {}
        
        coupled_ops = {'filter_params', 'join_params'}
        
        all_ops = set(p1.operation_params.keys()) | set(p2.operation_params.keys())
        
        inherit_group_from_p1 = random.choice([True, False])
        
        source_label_1 = "Parent A" if inherit_group_from_p1 else "Parent B"
        source_label_2 = "Parent B" if inherit_group_from_p1 else "Parent A"

        for op in coupled_ops:
            if op in all_ops:
                c1_params[op] = {}
                c2_params[op] = {}
                
                p1_data = p1.operation_params.get(op, {})
                p2_data = p2.operation_params.get(op, {})
                
                if inherit_group_from_p1:
                    c1_params[op] = {k: v for k,v in p1_data.items()}
                    c2_params[op] = {k: v for k,v in p2_data.items()}
                else:
                    c1_params[op] = {k: v for k,v in p2_data.items()}
                    c2_params[op] = {k: v for k,v in p1_data.items()}
                
                c1_genealogy[op] = f"[COUPLED] All from {source_label_1}"
                c2_genealogy[op] = f"[COUPLED] All from {source_label_2}"

        independent_ops = all_ops - coupled_ops
        
        for op in independent_ops:
            c1_params[op] = {}
            c2_params[op] = {}
            c1_genealogy[op] = {}
            c2_genealogy[op] = {}
            
            p1_data = p1.operation_params.get(op, {})
            p2_data = p2.operation_params.get(op, {})
            
            all_keys = set(p1_data.keys()) | set(p2_data.keys())
            
            for k in all_keys:
                val1 = p1_data.get(k)
                val2 = p2_data.get(k)
                
                src1 = "Parent A"
                src2 = "Parent B"
                
                if val1 is None: 
                    val1 = val2
                    src1 = "Parent B"
                if val2 is None: 
                    val2 = val1
                    src2 = "Parent A"
                
                if random.random() < 0.5:
                    c1_params[op][k] = val1 
                    c1_genealogy[op][k] = src1
                    
                    c2_params[op][k] = val2
                    c2_genealogy[op][k] = src2
                else:
                    c1_params[op][k] = val2
                    c1_genealogy[op][k] = src2

                    c2_params[op][k] = val1
                    c2_genealogy[op][k] = src1
                    
        return (Individual(c1_params, base_query, lazy_eval=True, genealogy=c1_genealogy), 
                Individual(c2_params, base_query, lazy_eval=True, genealogy=c2_genealogy))

    def _mutate(self, ind):
        new_params = {op: {k: v for k,v in data.items()} for op, data in ind.operation_params.items()}
        new_genealogy = ind.genealogy.copy()
        
        mgr = get_rule_params_manager()
        
        if not new_params: return ind
        
        op = random.choice(list(new_params.keys()))
        data = new_params[op]
        
        if data:
            key = random.choice(list(data.keys()))
            data[key] = mgr.mutate_params(op, data[key])
            
            if isinstance(new_genealogy.get(op), dict):
                new_genealogy[op][key] = "MUTATED"
            else:
                current_info = new_genealogy.get(op, "")
                new_genealogy[op] = f"{current_info} + MUTATED ({key})"
            
        return Individual(new_params, ind.base_query, lazy_eval=True, genealogy=new_genealogy)
