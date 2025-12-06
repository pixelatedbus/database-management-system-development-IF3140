"""
Main class for query optimization
"""

from __future__ import annotations
from typing import Optional, Callable
from storage_manager.storage_manager import StorageManager
from .tokenizer import Tokenizer
from .parser import Parser
from .query_tree import QueryTree
from .query_check import check_query
from .cost import CostCalculator

class OptimizationError(Exception):
    """Exception raised for errors in the optimization process."""
    pass

class ParsedQuery:
    def __init__(self, query_tree:QueryTree, query:str):
        self.query_tree: QueryTree = query_tree
        self.query: str = query

class OptimizationEngine:
    
    _instance: Optional[OptimizationEngine] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if hasattr(self, '_initialized'):
            return
        
        self._initialized = True
        self.query_tree: QueryTree = None
        self.optimized_tree: QueryTree = None
        self.original_sql: str = ""
        self.storage = StorageManager()
        self.statistics = self.storage.get_stats()
        self.cost_calculator = CostCalculator(self.statistics)
    
    def parse_query(self, sql: str) -> ParsedQuery:
        self.original_sql = sql
        
        tokenizer = Tokenizer(sql)
        
        parser = Parser(tokenizer)
        query_tree = parser.parse()
        
        check_query(query_tree)

        result = ParsedQuery(query_tree, sql)
        
        return result
    
    def optimize_query(
        self, 
        query_tree: Optional[ParsedQuery] = None,
        use_genetic: bool = True,
        population_size: int = 50,
        generations: int = 100,
        mutation_rate: float = 0.1,
        elitism: int = 2,
        # fitness_func: Optional[Callable[[ParsedQuery], float]] = None
    ) -> ParsedQuery:
        """
        Apply optimization rules to the query tree.
        
        Args:
            query_tree: Query tree to optimize. If None, uses stored query_tree
            use_genetic: Whether to use Genetic Algorithm optimization
            population_size: GA population size (default: 50)
            generations: GA number of generations (default: 100)
            mutation_rate: GA mutation rate (default: 0.1)
            elitism: GA elitism count (default: 2)
        
        Returns:
            ParsedQuery: Optimized query tree        
        """

        if query_tree is None:
            if self.query_tree is None:
                raise OptimizationError("No query tree available for optimization.")
            else:
                query_tree = ParsedQuery(self.query_tree, self.original_sql)
        
        # Rule 3: Projection elimination
        from .rule.rule_3 import seleksi_proyeksi
        query_tree = seleksi_proyeksi(query_tree)
        
        # Rule 7: Filter pushdown over joins
        from .rule.rule_7 import apply_pushdown
        query_tree = apply_pushdown(query_tree)
        
        # Rule 8: Projection push-down over joins
        from .rule.rule_8 import push_projection_over_joins
        query_tree = push_projection_over_joins(query_tree)
        
        if use_genetic:
            from .genetic_optimizer import GeneticOptimizer
            
            ga = GeneticOptimizer(
                population_size=population_size,
                generations=generations,
                mutation_rate=mutation_rate,
                elitism=elitism or 2,
                # fitness_func=fitness_func or self._default_fitness_func
            )
            
            optimized_tree, history = ga.optimize(query_tree)
            self.optimized_tree = optimized_tree.query_tree
        else:
            optimized_tree = query_tree
            self.optimized_tree = optimized_tree.query_tree

        return optimized_tree
    
    def get_cost(self, query_tree:ParsedQuery) -> float:
        total_cost = self.cost_calculator.get_cost(query_tree.query_tree).total_cost
        return total_cost
    
    def reset(self) -> None:
        self.query_tree = None
        self.optimized_tree = None
        self.original_sql = ""
    
    def debug(self) -> None:
        print("Original SQL:", self.original_sql)
        print("Parsed Query Tree:", self.query_tree.tree() if self.query_tree else None)
        print("Optimized Query Tree:", self.optimized_tree.tree() if self.optimized_tree else None)

