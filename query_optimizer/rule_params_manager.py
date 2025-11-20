"""
Rule Parameters Manager

Centralized management untuk semua operation parameters dalam Genetic Algorithm.
Menggunakan parameter umum berdasarkan tipe operasi:
- filter_params: Parameter untuk filter operations (AND operators)
- join_params: Parameter untuk join operations (kosong untuk saat ini)

Struktur params menggunakan format UNIFIED ORDER:
{
    'filter_params': {
        node_id: [2, [0, 1]]  # Mixed order: int (single) | list (grouped dalam AND)
                              # [2, [0,1]] = cond2 single, lalu (cond0 AND cond1) grouped
                              # [1, 0, 2]  = semua single (reorder tanpa cascade)
    },
    'join_params': {}  # Untuk future implementation
}

Format order ini menggabungkan:
- Seleksi Komutatif (reordering): mengatur urutan kondisi
- Seleksi Konjunktif (cascading): menentukan grouping dalam AND

Contoh:
- [0, 1, 2]     → Semua single, order original (no reorder, full cascade)
- [2, 1, 0]     → Semua single, reversed (reorder, full cascade)
- [2, [0, 1]]   → cond2 single cascade, cond0&1 tetap grouped
- [[0, 1, 2]]   → Semua dalam satu AND (reorder dulu, tapi no cascade)
"""

from typing import Any, Literal
from query_optimizer.optimization_engine import ParsedQuery
import random


OperationType = Literal['filter_params', 'join_params']


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
        from query_optimizer.rule_1 import (
            analyze_and_operators,
            copy_rule_1_params,
            mutate_rule_1_params,
        )
        from query_optimizer.rule_2 import (
            generate_random_rule_2_params,
            mutate_rule_2_params,
        )
        
        def generate_filter_params(num_conditions: int) -> list[int | list[int]]:
            """
            Generate random filter params dengan unified order format.
            
            Format: Mixed list of int | list[int]
            - int: single condition (akan di-cascade sebagai FILTER tunggal)
            - list: grouped conditions (tetap dalam AND)
            
            Example outputs:
            - [0, 1, 2]     : All single, no grouping
            - [2, [0, 1]]   : cond2 single, [cond0, cond1] grouped
            - [[0, 1, 2]]   : All grouped in one AND
            """
            indices = list(range(num_conditions))
            random.shuffle(indices)
            
            # Random grouping
            if num_conditions <= 1:
                return indices
            
            num_groups = random.randint(0, max(1, num_conditions // 2))
            
            if num_groups == 0:
                # Semua single
                return indices
            
            # Buat groups
            result = []
            remaining = indices.copy()
            
            for _ in range(num_groups):
                if len(remaining) < 2:
                    break
                
                # Ambil 2-3 kondisi untuk di-group
                group_size = random.randint(2, min(3, len(remaining)))
                group = remaining[:group_size]
                remaining = remaining[group_size:]
                result.append(group)
            
            # Sisanya jadi single
            result.extend(remaining)
            
            return result
        
        def copy_filter_params(params: list[int | list[int]]) -> list[int | list[int]]:
            """Deep copy filter params."""
            return copy_rule_1_params(params)
        
        def mutate_filter_params(params: list[int | list[int]]) -> list[int | list[int]]:
            """
            Mutate filter params dengan strategi campuran:
            - Reorder: swap positions (dari mutate_rule_2)
            - Group/ungroup: gabung/pisah conditions (dari mutate_rule_1)
            """
            # Randomly choose mutation strategy
            if random.random() < 0.5:
                # Strategy 1: Mutate as cascade (group/ungroup)
                return mutate_rule_1_params(params)
            else:
                # Strategy 2: Flatten, mutate as permutation, then reconstruct groups
                # Flatten to get all indices
                flat = []
                for item in params:
                    if isinstance(item, list):
                        flat.extend(item)
                    else:
                        flat.append(item)
                
                # Apply permutation mutation
                mutated_flat = mutate_rule_2_params(flat)
                
                # Reconstruct with similar grouping structure
                result = []
                idx = 0
                for item in params:
                    if isinstance(item, list):
                        group_size = len(item)
                        result.append(mutated_flat[idx:idx+group_size])
                        idx += group_size
                    else:
                        result.append(mutated_flat[idx])
                        idx += 1
                
                return result
        
        def validate_filter_params(params: list[int | list[int]]) -> bool:
            """Validate filter params structure."""
            if not isinstance(params, list):
                return False
            # Check all indices present
            flat = []
            for item in params:
                if isinstance(item, list):
                    flat.extend(item)
                else:
                    flat.append(item)
            return len(flat) > 0 and len(set(flat)) == len(flat)
        
        self.register_operation(
            operation_name='filter_params',
            analyze_func=analyze_and_operators,  # Same analysis for both rules
            generate_func=generate_filter_params,
            copy_func=copy_filter_params,
            mutate_func=mutate_filter_params,
            validate_func=validate_filter_params
        )
        
        # Join operations (placeholder for future)
        def analyze_joins(query: ParsedQuery) -> dict[int, Any]:
            """Placeholder: Analyze join operations."""
            # TODO: Implement when join optimization rules are added
            return {}
        
        def generate_join_params(metadata: Any) -> dict:
            """Placeholder: Generate join params."""
            return {}
        
        def copy_join_params(params: dict) -> dict:
            """Placeholder: Copy join params."""
            import copy
            return copy.deepcopy(params)
        
        def mutate_join_params(params: dict) -> dict:
            """Placeholder: Mutate join params."""
            return params
        
        self.register_operation(
            operation_name='join_params',
            analyze_func=analyze_joins,
            generate_func=generate_join_params,
            copy_func=copy_join_params,
            mutate_func=mutate_join_params
        )


# Global instance
_manager = RuleParamsManager()


def get_rule_params_manager() -> RuleParamsManager:
    """Get global rule params manager instance."""
    return _manager
