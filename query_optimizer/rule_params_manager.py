"""
Rule Parameters Manager

Centralized management untuk semua operation parameters dalam Genetic Algorithm.
Menggunakan parameter umum berdasarkan tipe operasi:
- filter_params: Parameter untuk filter operations (Rule 1 + 2 - AND operators)
- join_params: Parameter untuk join operations (Rule 4 - Push selection into joins)

Struktur params menggunakan format:
{
    'filter_params': {
        operator_id: [2, [0, 1]]  # UNIFIED ORDER: mixed single/grouped conditions
                                   # int (single) | list[int] (grouped dalam AND)
                                   # [2, [0,1]] = cond2 single, lalu (cond0 AND cond1) grouped
                                   # [1, 0, 2]  = semua single (reorder tanpa cascade)
    },
    'join_params': {
        join_node_id: [10, 15]  # CONDITION SELECTION: kondisi mana yang masuk ke JOIN
                                # [10, 15] = merge conditions dengan ID 10 dan 15 ke JOIN
                                # [] = tidak ada kondisi yang di-merge (keep FILTER separate)
    }
}

Format filter_params menggabungkan:
- Seleksi Komutatif (reordering): mengatur urutan kondisi
- Seleksi Konjunktif (cascading): menentukan grouping dalam AND

Contoh filter_params:
- [0, 1, 2]     → Semua single, order original (no reorder, full cascade)
- [2, 1, 0]     → Semua single, reversed (reorder, full cascade)
- [2, [0, 1]]   → cond2 single cascade, cond0&1 tetap grouped
- [[0, 1, 2]]   → Semua dalam satu AND (reorder dulu, tapi no cascade)

Format join_params mengontrol WHICH conditions masuk ke JOIN:
- [10, 15]: Merge conditions dengan ID 10 dan 15 dari FILTER ke JOIN
- []: Tidak merge kondisi apapun (keep FILTER separate)
- [10, 15, 20]: Merge 3 conditions ke JOIN

Note: Rule 4 HANYA mengatur WHICH conditions masuk ke JOIN.
TIDAK mengatur:
- Urutan conditions (order) - semua conditions di JOIN digabung dengan AND tanpa ordering
- Cascade - semua conditions di JOIN digabung dalam satu OPERATOR(AND)

Contoh join_params:
- {42: [10, 15]}  → Merge conditions 10 dan 15 ke JOIN dengan ID 42
- {57: []}        → Keep FILTER separate untuk JOIN dengan ID 57
- {88: [5, 8, 12]} → Merge conditions 5, 8, dan 12 ke JOIN dengan ID 88
"""

from typing import Any, Literal
from query_optimizer.optimization_engine import ParsedQuery
import random


OperationType = Literal['filter_params', 'join_params', 'join_child_params']


class RuleParamsManager:
    """Manager untuk handle semua operation parameters dalam GA."""
    
    def __init__(self):
        self._operations = {}
        self._register_default_operations()
    
    def register_operation(
        self,
        operation_name: OperationType,
        analyze_func,
        generate_func,
        copy_func,
        mutate_func,
        validate_func=None
    ):
        """
        Register operation dengan semua handler functions.
        
        Args:
            operation_name: Nama operasi (e.g., "filter_params", "join_params")
            analyze_func: (query) -> dict[id, metadata]
            generate_func: (metadata) -> params
            copy_func: (params) -> copied_params
            mutate_func: (params) -> mutated_params
            validate_func: (params) -> bool
        """
        self._operations[operation_name] = {
            'analyze': analyze_func,
            'generate': generate_func,
            'copy': copy_func,
            'mutate': mutate_func,
            'validate': validate_func or (lambda x: True)
        }
    
    def analyze_query(self, query: ParsedQuery, operation_name: OperationType) -> dict[int, Any]:
        """Analisa query untuk operation tertentu."""
        if operation_name not in self._operations:
            return {}
        return self._operations[operation_name]['analyze'](query)
    
    def generate_random_params(self, operation_name: OperationType, metadata: Any) -> Any:
        """Generate random params untuk operation."""
        if operation_name not in self._operations:
            return None
        return self._operations[operation_name]['generate'](metadata)
    
    def copy_params(self, operation_name: OperationType, params: Any) -> Any:
        """Deep copy params."""
        if operation_name not in self._operations:
            return None
        return self._operations[operation_name]['copy'](params)
    
    def mutate_params(self, operation_name: OperationType, params: Any) -> Any:
        """Mutate params."""
        if operation_name not in self._operations:
            return params
        return self._operations[operation_name]['mutate'](params)
    
    def validate_params(self, operation_name: OperationType, params: Any) -> bool:
        """Validate params."""
        if operation_name not in self._operations:
            return True
        return self._operations[operation_name]['validate'](params)
    
    def get_registered_operations(self) -> list[OperationType]:
        """Get list of registered operation names."""
        return list(self._operations.keys())
    
    def _register_default_operations(self):
        """Register default operations."""
        # Filter operations (Rule 1 + Rule 2)
        from query_optimizer.rule.rule_1_2 import (
            analyze_and_operators,
            generate_random_rule_1_params,
            copy_rule_1_params,
            mutate_rule_1_params,
        )
        
        def generate_filter_params(condition_ids: list[int]) -> list[int | list[int]]:
            """Generate random filter params dari condition IDs."""
            return generate_random_rule_1_params(condition_ids)
        
        def validate_filter_params(params: list[int | list[int]]) -> bool:
            """Validate filter params structure."""
            if not isinstance(params, list):
                return False
            for item in params:
                if isinstance(item, list):
                    if not all(isinstance(x, int) for x in item):
                        return False
                elif not isinstance(item, int):
                    return False
            return True
        
        self.register_operation(
            operation_name='filter_params',
            analyze_func=analyze_and_operators,  # Returns {op_id: [cond_ids]}
            generate_func=generate_filter_params,  # Input: [cond_ids]
            copy_func=copy_rule_1_params,
            mutate_func=mutate_rule_1_params,
            validate_func=validate_filter_params
        )
        
        # Join operations (Rule 4: Push selection into joins)
        from query_optimizer.rule import rule_4
        
        def analyze_joins(query: ParsedQuery) -> dict[int, Any]:
            """Analyze FILTER-JOIN patterns for rule 4.
            
            Returns:
                Dict[join_id, metadata]
                metadata = {
                    'filter_conditions': [condition_ids],
                    'existing_conditions': [condition_ids]
                }
            """
            return rule_4.find_patterns(query)
        
        def generate_join_params(metadata: dict) -> list[int]:
            """Generate selection of conditions to merge into JOIN.
            
            Args:
                metadata: {
                    'filter_conditions': [condition_ids],
                    'existing_conditions': [condition_ids]
                }
            
            Returns:
                list[int]: List of condition IDs to merge into JOIN
            """
            return rule_4.generate_params(metadata)
        
        def copy_join_params(params: list[int]) -> list[int]:
            """Deep copy join params."""
            return rule_4.copy_params(params)
        
        def mutate_join_params(params: list[int]) -> list[int]:
            """Mutate join params by adding/removing conditions."""
            return rule_4.mutate_params(params)
        
        def validate_join_params(params: list[int]) -> bool:
            """Validate join params structure."""
            return rule_4.validate_params(params)
        
        self.register_operation(
            operation_name='join_params',
            analyze_func=analyze_joins,
            generate_func=generate_join_params,
            copy_func=copy_join_params,
            mutate_func=mutate_join_params,
            validate_func=validate_join_params
        )
        
        # Join child order operations (Rule 5: Join commutativity)
        from query_optimizer.rule import rule_5
        
        def analyze_join_children(query: ParsedQuery) -> dict[int, Any]:
            """Find all JOIN nodes untuk rule 5.
            
            Returns:
                Dict[join_id, metadata]
                metadata = {
                    'left_child': node,
                    'right_child': node,
                    'join_type': str
                }
            """
            return rule_5.find_join_nodes(query)
        
        def generate_join_child_order(metadata: dict) -> bool:
            """Generate random bool: swap children or not.
            
            Args:
                metadata: JOIN node metadata
            
            Returns:
                bool: True = swap, False = keep original
            """
            return rule_5.generate_join_child_params(metadata)
        
        def copy_join_child_order(params: bool) -> bool:
            """Copy join child order params."""
            return rule_5.copy_join_child_params(params)
        
        def mutate_join_child_order(params: bool) -> bool:
            """Mutate join child order by flipping."""
            return rule_5.mutate_join_child_params(params)
        
        def validate_join_child_order(params: bool) -> bool:
            """Validate join child order params."""
            return rule_5.validate_join_child_params(params)
        
        self.register_operation(
            operation_name='join_child_params',
            analyze_func=analyze_join_children,
            generate_func=generate_join_child_order,
            copy_func=copy_join_child_order,
            mutate_func=mutate_join_child_order,
            validate_func=validate_join_child_order
        )


# Global instance
_manager = RuleParamsManager()


def get_rule_params_manager() -> RuleParamsManager:
    """Get global rule params manager instance."""
    return _manager
