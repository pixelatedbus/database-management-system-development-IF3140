"""
Rule Parameters Manager

Centralized management untuk semua rule parameters dalam Genetic Algorithm.
Setiap rule memiliki:
- analyze: Extract rule-specific info dari query
- generate_random: Generate random params
- copy: Deep copy params
- mutate: Mutate params
- validate: Check if params valid
"""

from typing import Any, Callable
from query_optimizer.optimization_engine import ParsedQuery
import random


class RuleParamsManager:
    """Manager untuk handle semua rule parameters dalam GA."""
    
    def __init__(self):
        self._rules = {}
        self._register_default_rules()
    
    def register_rule(
        self,
        rule_name: str,
        analyze_func: Callable[[ParsedQuery], dict[int, Any]],
        generate_func: Callable[[Any], Any],
        copy_func: Callable[[Any], Any],
        mutate_func: Callable[[Any], Any],
        validate_func: Callable[[Any], bool] = None
    ):
        """
        Register rule dengan semua handler functions.
        
        Args:
            rule_name: Nama rule (e.g., "rule_1", "rule_2")
            analyze_func: (query) -> dict[id, metadata]
            generate_func: (metadata) -> params
            copy_func: (params) -> copied_params
            mutate_func: (params) -> mutated_params
            validate_func: (params) -> bool
        """
        self._rules[rule_name] = {
            'analyze': analyze_func,
            'generate': generate_func,
            'copy': copy_func,
            'mutate': mutate_func,
            'validate': validate_func or (lambda x: True)
        }
    
    def analyze_query(self, query: ParsedQuery, rule_name: str) -> dict[int, Any]:
        """Analisa query untuk rule tertentu."""
        if rule_name not in self._rules:
            return {}
        return self._rules[rule_name]['analyze'](query)
    
    def generate_random_params(self, rule_name: str, metadata: Any) -> Any:
        """Generate random params untuk rule."""
        if rule_name not in self._rules:
            return None
        return self._rules[rule_name]['generate'](metadata)
    
    def copy_params(self, rule_name: str, params: Any) -> Any:
        """Deep copy params."""
        if rule_name not in self._rules:
            return None
        return self._rules[rule_name]['copy'](params)
    
    def mutate_params(self, rule_name: str, params: Any) -> Any:
        """Mutate params."""
        if rule_name not in self._rules:
            return params
        return self._rules[rule_name]['mutate'](params)
    
    def validate_params(self, rule_name: str, params: Any) -> bool:
        """Validate params."""
        if rule_name not in self._rules:
            return True
        return self._rules[rule_name]['validate'](params)
    
    def get_registered_rules(self) -> list[str]:
        """Get list of registered rule names."""
        return list(self._rules.keys())
    
    def _register_default_rules(self):
        """Register default rules."""
        # Rule 1 - Seleksi Konjungtif (Cascading)
        from query_optimizer.rule_1 import (
            analyze_and_operators,
            generate_random_rule_1_params,
            copy_rule_1_params,
            mutate_rule_1_params,
        )
        
        self.register_rule(
            rule_name='rule_1',
            analyze_func=analyze_and_operators,
            generate_func=generate_random_rule_1_params,
            copy_func=copy_rule_1_params,
            mutate_func=mutate_rule_1_params
        )
        
        # Rule 2 - Seleksi Komutatif (Reordering)
        from query_optimizer.rule_2 import (
            analyze_and_operators_for_reorder,
            generate_random_rule_2_params,
            copy_rule_2_params,
            mutate_rule_2_params,
        )
        
        def validate_rule_2(params: list[int]) -> bool:
            """Validate rule 2 params - requires metadata for num_conditions."""
            # Basic validation: must be a list
            return isinstance(params, list) and len(params) > 0
        
        self.register_rule(
            rule_name='rule_2',
            analyze_func=analyze_and_operators_for_reorder,
            generate_func=generate_random_rule_2_params,
            copy_func=copy_rule_2_params,
            mutate_func=mutate_rule_2_params,
            validate_func=validate_rule_2
        )


# Global instance
_manager = RuleParamsManager()


def get_rule_params_manager() -> RuleParamsManager:
    """Get global rule params manager instance."""
    return _manager


# Example: How to register Rule 2, 3, etc.
"""
# Rule 2 Example (hypothetical):
def analyze_rule_2(query: ParsedQuery) -> dict[int, Any]:
    # Analyze query for Rule 2
    return {node_id: metadata}

def generate_rule_2_params(metadata: Any) -> Any:
    # Generate random params for Rule 2
    return some_params

def copy_rule_2_params(params: Any) -> Any:
    # Deep copy Rule 2 params
    return copied_params

def mutate_rule_2_params(params: Any) -> Any:
    # Mutate Rule 2 params
    return mutated_params

# Register Rule 2
manager = get_rule_params_manager()
manager.register_rule(
    rule_name='rule_2',
    analyze_func=analyze_rule_2,
    generate_func=generate_rule_2_params,
    copy_func=copy_rule_2_params,
    mutate_func=mutate_rule_2_params
)
"""
