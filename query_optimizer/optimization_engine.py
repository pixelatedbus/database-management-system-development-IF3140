"""
Main class for query optimization
"""

from __future__ import annotations
from typing import Optional
from query_optimizer.tokenizer import Tokenizer
from query_optimizer.parser import Parser
from query_optimizer.query_tree import QueryTree
from query_optimizer.query_check import check_query

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
    
    def optimize_query(self, query_tree: Optional[ParsedQuery] = None) -> ParsedQuery:
        """
        Apply optimization rules to the query tree.
        """

        if query_tree is None:
            if self.query_tree is None:
                raise OptimizationError("No query tree available for optimization.")
            else:
                query_tree = ParsedQuery(self.query_tree, self.original_sql)
        # TODO: Implement optimization rules here
        # For now, return the original tree unchanged
        optimized_tree = query_tree
        self.optimized_tree = optimized_tree.query_tree

        return optimized_tree
    
    def get_cost(self, query_tree:ParsedQuery) -> int:
        """
        Calculate estimated execution cost for the query tree.
        """
        # TODO: Implement proper cost estimation here
        cost = 0
        
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

