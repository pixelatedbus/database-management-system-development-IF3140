"""
Main class for query optimization
"""

from __future__ import annotations
from typing import Optional, Callable, TYPE_CHECKING
from .tokenizer import Tokenizer
from .parser import Parser
from .query_tree import QueryTree
from .query_check import check_query

if TYPE_CHECKING:
    from .genetic_optimizer import GeneticOptimizer

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
        """Singleton pattern"""
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
    
    def parse_query(self, sql: str) -> ParsedQuery:
        """
        Parse SQL query string into a QueryTree.
        """
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
        crossover_rate: float = 0.8,
        elitism: int = 2,
        fitness_func: Optional[Callable[[ParsedQuery], float]] = None
    ) -> ParsedQuery:
        """
        Apply optimization rules to the query tree.
        
        Args:
            query_tree: Query tree to optimize. If None, uses stored query_tree
            use_genetic: Whether to use Genetic Algorithm optimization
            population_size: GA population size (default: 50)
            generations: GA number of generations (default: 100)
            mutation_rate: GA mutation rate (default: 0.1)
            crossover_rate: GA crossover rate (default: 0.8)
            elitism: GA elitism count (default: 2)
            fitness_func: Custom fitness function. If None, uses default cost function
        
        Returns:
            ParsedQuery: Optimized query tree
        
        Examples:
            # Basic optimization with default GA settings
            optimized = engine.optimize_query(query)
            
            # Quick optimization with smaller parameters
            optimized = engine.optimize_query(query, population_size=20, generations=30)
            
            # Without GA (returns original query)
            optimized = engine.optimize_query(query, use_genetic=False)
            
            # With custom fitness function
            def my_fitness(q): return len(q.query_tree.find_nodes_by_type("FILTER"))
            optimized = engine.optimize_query(query, fitness_func=my_fitness)
        """

        if query_tree is None:
            if self.query_tree is None:
                raise OptimizationError("No query tree available for optimization.")
            else:
                query_tree = ParsedQuery(self.query_tree, self.original_sql)
        
        # Apply Rule 3 (projection elimination) ONCE before genetic algorithm
        from .rule_3 import seleksi_proyeksi
        query_tree = seleksi_proyeksi(query_tree)
        
        if use_genetic:
            # Lazy import to avoid circular dependency
            from .genetic_optimizer import GeneticOptimizer
            
            # Use Genetic Algorithm for optimization
            ga = GeneticOptimizer(
                population_size=population_size,
                generations=generations,
                mutation_rate=mutation_rate,
                crossover_rate=crossover_rate,
                elitism=elitism,
                fitness_func=fitness_func or self._default_fitness_func
            )
            
            optimized_tree = ga.optimize(query_tree)
            self.optimized_tree = optimized_tree.query_tree
        else:
            # Return original query unchanged
            optimized_tree = query_tree
            self.optimized_tree = optimized_tree.query_tree

        return optimized_tree
    
    def _default_fitness_func(self, query: ParsedQuery) -> float:
        """Default fitness function using cost estimation."""
        return float(self.get_cost(query))
    
    def get_cost(self, query_tree:ParsedQuery) -> int:
        """
        Only simulation until proper implementation.
        """
        import random
        
        # Dummy implementation with random cost
        # Base cost on tree structure
        node_count = 0
        filter_count = 0
        operator_count = 0
        join_count = 0
        
        def count_nodes(node):
            nonlocal node_count, filter_count, operator_count, join_count
            if node is None:
                return
            
            node_count += 1
            if node.type == "FILTER":
                filter_count += 1
            elif node.type in {"OPERATOR", "OPERATOR_S"}:
                operator_count += 1
            elif node.type == "JOIN":
                join_count += 1
            
            for child in node.childs:
                count_nodes(child)
        
        count_nodes(query_tree.query_tree)
        
        # Random cost with some structure dependency
        base_cost = 100
        filter_cost = filter_count * 40
        operator_cost = operator_count * 30  # Logical operators slightly cheaper
        join_cost = join_count * 150
        
        cost = base_cost + filter_cost + operator_cost + join_cost
        
        return cost
    
    def reset(self) -> None:
        """
        Reset the engine state.
        """
        self.query_tree = None
        self.optimized_tree = None
        self.original_sql = ""
    
    def debug(self) -> None:
        print("Original SQL:", self.original_sql)
        print("Parsed Query Tree:", self.query_tree.tree() if self.query_tree else None)
        print("Optimized Query Tree:", self.optimized_tree.tree() if self.optimized_tree else None)

