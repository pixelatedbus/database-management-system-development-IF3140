import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from query_optimizer.optimization_engine import OptimizationEngine, ParsedQuery
from query_optimizer.query_tree import QueryTree

class AdapterOptimizer:

    def __init__(self):
        self.optimization_engine = OptimizationEngine()
    
    def parse_optimized_query(self, query: str) -> ParsedQuery:
        parsed_query = self.optimization_engine.parse_query(query)
        optimized_query = self.optimization_engine.optimize_query(parsed_query)
        return optimized_query
    
    def can_pushdown_projection(self, query_tree: QueryTree, source: QueryTree) -> bool:
        """
        Determine if projection can be pushed down to storage layer.
        Only simple RELATION sources allow pushdown.
        
        Args:
            query_tree: The PROJECT node
            source: The source node (last child of PROJECT)
            
        Returns:
            True if projection can be pushed down
        """
        return source.type == "RELATION"
    
    def can_pushdown_filter(self, source: QueryTree) -> bool:
        """
        Determine if filter can be pushed down to storage layer.
        Only simple RELATION sources allow pushdown.
        
        Args:
            source: The source node (first child of FILTER)
            
        Returns:
            True if filter can be pushed down
        """
        return source.type == "RELATION"
    
    def get_execution_method(self, query_tree: QueryTree) -> str:
        """
        Determine the execution method for a query tree node.
        Can be extended to consider indexes, statistics, etc.
        
        Args:
            query_tree: The query tree node
            
        Returns:
            Execution method string (sequential_search, hash_index, btree_index, nested_loop, etc.)
        """
        node_type = query_tree.type
        
        if hasattr(query_tree, 'method') and query_tree.method:
            # Use method specified in the tree
            return query_tree.method
        
        # Default methods based on node type
        if node_type in ["RELATION", "SELECT"]:
            return "sequential_search"
        elif node_type == "JOIN":
            return "nested_loop"
        else:
            return ""
    
    def should_use_in_memory_filtering(self, condition_types: list) -> bool:
        """
        Determine if conditions require in-memory filtering.
        Complex conditions like IN, EXISTS, BETWEEN cannot be pushed to storage.
        
        Args:
            condition_types: List of condition node types
            
        Returns:
            True if in-memory filtering is required
        """
        complex_conditions = {
            "IN_EXPR", "NOT_IN_EXPR", 
            "EXISTS_EXPR", "NOT_EXISTS_EXPR",
            "BETWEEN_EXPR", "NOT_BETWEEN_EXPR",
            "OPERATOR"  # AND/OR/NOT may contain complex nested conditions
        }
        
        return any(ct in complex_conditions for ct in condition_types)
    
def print_query_tree(query_tree, indent=0):
    """Print query tree structure recursively"""
    if query_tree is None:
        return
    
    prefix = "  " * indent
    node_info = f"{query_tree.type}"
    
    if query_tree.val:
        node_info += f" [{query_tree.val}]"
    
    print(f"{prefix}{node_info}")
    
    for child in query_tree.childs:
        print_query_tree(child, indent + 1)

# test
if __name__ == "__main__":
    adapter = AdapterOptimizer()
    query = "SELECT id, name FROM users WHERE id = 10;"
    
    # Parse query (returns ParsedQuery object)
    parsed_query = adapter.parse_optimized_query(query)
    
    print("="*70)
    print("PARSED QUERY")
    print("="*70)
    print(f"Original SQL: {parsed_query.query}")
    print(f"\nQuery Tree Structure:")
    print_query_tree(parsed_query.query_tree)
