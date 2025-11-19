from query_optimizer.optimization_engine import OptimizationEngine
from query_optimizer.query_tree import QueryTree

class AdapterOptimizer:
    def __init__(self):
        self.optimization_engine = OptimizationEngine()
    
    def parse_optimized_query(self, query: str) -> QueryTree:
        tree = self.optimization_engine.parse_query(query)
        optimized_tree = self.optimization_engine.optimize_query(tree)
        return optimized_tree