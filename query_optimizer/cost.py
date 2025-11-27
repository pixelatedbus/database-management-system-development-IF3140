from typing import Dict, Optional, Tuple
from dataclasses import dataclass
from query_optimizer.query_tree import QueryTree
from storage_manager.models import Statistic
import math


@dataclass
class CostResult:
    """Hasil perhitungan cost untuk sebuah query plan.
    
    Atribut:
        io_cost: Cost I/O dalam jumlah block accesses
        cpu_cost: Cost CPU (reserved untuk implementasi future, default 0)
        estimated_cardinality: Estimasi jumlah tuple hasil
        estimated_blocks: Estimasi jumlah blok yang dibutuhkan hasil
    """
    io_cost: float
    cpu_cost: float = 0.0
    estimated_cardinality: int = 0
    estimated_blocks: int = 0
    
    @property
    def total_cost(self) -> float:
        return self.io_cost + self.cpu_cost
    
    def __add__(self, other: 'CostResult') -> 'CostResult':
        return CostResult(
            io_cost=self.io_cost + other.io_cost,
            cpu_cost=self.cpu_cost + other.cpu_cost,
            estimated_cardinality=max(self.estimated_cardinality, other.estimated_cardinality),
            estimated_blocks=max(self.estimated_blocks, other.estimated_blocks)
        )


class CostCalculator:
    
    def __init__(self, statistics: Dict[str, Statistic]):

        self.statistics = statistics
        
        self.SEQUENTIAL_IO_COST = 1.0
        self.RANDOM_IO_COST = 1.5
        self.WRITE_COST = 2.0
        
        self.CPU_COST_PER_TUPLE = 0.0
    
    def get_cost(self, query_tree: QueryTree) -> CostResult:

        node_type = query_tree.type
        
        if node_type == "RELATION":
            return self._cost_scan(query_tree)
        elif node_type == "PROJECT":
            return self._cost_project(query_tree)
        elif node_type == "FILTER":
            return self._cost_filter(query_tree)
        elif node_type == "JOIN":
            return self._cost_join(query_tree)
        elif node_type == "SORT":
            return self._cost_sort(query_tree)
        elif node_type == "LIMIT":
            return self._cost_limit(query_tree)
        elif node_type == "ALIAS":
            return self._cost_alias(query_tree)
        elif node_type in ["UPDATE_QUERY", "INSERT_QUERY", "DELETE_QUERY"]:
            return self._cost_modification(query_tree)
        elif node_type == "CREATE_TABLE":
            return CostResult(io_cost=1.0)
        elif node_type == "DROP_TABLE":
            return CostResult(io_cost=1.0)
        elif node_type == "BEGIN_TRANSACTION":
            return self._cost_transaction(query_tree)
        else:
            return self._cost_generic(query_tree)
    
    def _cost_scan(self, node: QueryTree, index_info: Optional[dict] = None) -> CostResult:

        table_name = node.val
        
        if table_name not in self.statistics:
            return CostResult(io_cost=1000.0, estimated_cardinality=1000, estimated_blocks=100)
        
        stats = self.statistics[table_name]
        
        if index_info:
            return self._cost_index_scan(table_name, stats, index_info)
        
        io_cost = stats.b_r * self.SEQUENTIAL_IO_COST
        
        return CostResult(
            io_cost=io_cost,
            cpu_cost=stats.n_r * self.CPU_COST_PER_TUPLE,
            estimated_cardinality=stats.n_r,
            estimated_blocks=stats.b_r
        )
    
    def _cost_project(self, node: QueryTree) -> CostResult:

        # Child terakhir adalah source data
        if not node.childs:
            return CostResult(io_cost=0.0)
        
        source = node.childs[-1]
        return self.get_cost(source)
    
    def _cost_filter(self, node: QueryTree) -> CostResult:

        if len(node.childs) < 2:
            return CostResult(io_cost=0.0)
        
        source = node.childs[0]
        condition = node.childs[1]
        
        if source.type == "RELATION":
            table_name = source.val
            if table_name in self.statistics:
                stats = self.statistics[table_name]
                
                index_info = self._find_usable_index(condition, table_name, stats)
                
                if index_info:
                    return self._cost_index_scan(table_name, stats, index_info)
        
        source_cost = self.get_cost(source)
        
        selectivity = self._estimate_selectivity(condition, source)
        
        # Cost tetap sama (pipelined), tapi cardinality berkurang
        return CostResult(
            io_cost=source_cost.io_cost,
            cpu_cost=source_cost.cpu_cost,
            estimated_cardinality=int(source_cost.estimated_cardinality * selectivity),
            estimated_blocks=int(math.ceil(source_cost.estimated_blocks * selectivity))
        )
    
    def _estimate_selectivity(self, condition: QueryTree, source: QueryTree) -> float:

        node_type = condition.type
        
        # Comparison operators
        if node_type == "COMPARISON":
            op = condition.val
            if op == "=":
                # Equality: 1/V(A,r) jika ada statistik distinct values
                return self._selectivity_equality(condition, source)
            elif op in ["<", ">", "<=", ">="]:
                # Range: asumsi uniform distribution, default 1/3
                return 0.33
            elif op in ["!=", "<>"]:
                # Not equal: 1 - (1/V(A,r))
                eq_sel = self._selectivity_equality(condition, source)
                return 1.0 - eq_sel
        
        elif node_type == "OPERATOR":
            op = condition.val
            if op == "AND":
                # Conjunction: multiply selectivities
                sel = 1.0
                for child in condition.childs:
                    sel *= self._estimate_selectivity(child, source)
                return sel
            elif op == "OR":
                if len(condition.childs) >= 2:
                    sel1 = self._estimate_selectivity(condition.childs[0], source)
                    sel2 = self._estimate_selectivity(condition.childs[1], source)
                    return sel1 + sel2 - (sel1 * sel2)
            elif op == "NOT":
                if condition.childs:
                    return 1.0 - self._estimate_selectivity(condition.childs[0], source)
        
        # Special conditions
        elif node_type == "IS_NULL_EXPR":
            return 0.01  # Asumsi 1% NULL
        elif node_type == "IS_NOT_NULL_EXPR":
            return 0.99
        elif node_type == "LIKE_EXPR":
            return 0.05  # Asumsi 5% match untuk pattern
        elif node_type == "BETWEEN_EXPR":
            return 0.25  # Asumsi 25% untuk range
        elif node_type == "IN_EXPR":
            if len(condition.childs) > 1:
                list_node = condition.childs[1]
                if list_node.type == "LIST":
                    n_values = len(list_node.childs)
                    return min(0.5, n_values * 0.05)  # Max 50%
            return 0.1
        
        return 0.1
    
    def _selectivity_equality(self, comparison: QueryTree, source: QueryTree) -> float:
        if len(comparison.childs) < 1:
            return 0.1
        
        # Coba extract column name dari left operand
        left = comparison.childs[0]
        column_name = self._extract_column_name(left)
        
        if not column_name:
            return 0.1
        
        # Coba dapat V(A,r) dari statistik
        table_name = self._extract_table_name(source)
        if table_name and table_name in self.statistics:
            stats = self.statistics[table_name]
            if column_name in stats.V_a_r:
                v_a_r = stats.V_a_r[column_name]
                if v_a_r > 0:
                    return 1.0 / v_a_r
        
        return 0.1  # Default
    
    def _cost_join(self, node: QueryTree) -> CostResult:

        if len(node.childs) < 2:
            return CostResult(io_cost=0.0)
        
        left = node.childs[0]
        right = node.childs[1]
        join_type = node.val
        
        left_cost = self.get_cost(left)
        right_cost = self.get_cost(right)
        
        if join_type == "NATURAL":
            return self._cost_natural_join(left_cost, right_cost)
        
        # Join dengan ON condition
        if len(node.childs) >= 3:
            condition = node.childs[2]
            return self._cost_conditional_join(left_cost, right_cost, condition, left, right)
        
        # Cross product (no condition)
        return self._cost_cross_product(left_cost, right_cost)
    
    def _cost_natural_join(self, left_cost: CostResult, right_cost: CostResult) -> CostResult:
        """Cost untuk natural join (block nested loop join)."""
        io_cost = left_cost.io_cost + right_cost.io_cost
        io_cost += left_cost.estimated_blocks * right_cost.estimated_blocks
        
        estimated_card = int(math.sqrt(left_cost.estimated_cardinality * right_cost.estimated_cardinality))
        
        return CostResult(
            io_cost=io_cost,
            estimated_cardinality=estimated_card,
            estimated_blocks=int(math.ceil(estimated_card / 10))
        )
    
    def _cost_conditional_join(self, left_cost: CostResult, right_cost: CostResult, 
                               condition: QueryTree, left_node: QueryTree, right_node: QueryTree) -> CostResult:

        is_equijoin, left_col, right_col = self._is_equijoin(condition)
        
        if is_equijoin and right_node.type == "RELATION":
            right_table = right_node.val
            if right_table in self.statistics:
                right_stats = self.statistics[right_table]
                
                # Cek apakah right_col punya index
                if right_col in right_stats.indexes:
                    index_info = right_stats.indexes[right_col]
                    # Gunakan Index Nested Loop Join
                    return self._cost_index_nested_loop_join(
                        left_cost, right_stats, index_info, right_col
                    )
        
        if is_equijoin and left_node.type == "RELATION":
            left_table = left_node.val
            if left_table in self.statistics:
                left_stats = self.statistics[left_table]
                
                if left_col in left_stats.indexes:
                    index_info = left_stats.indexes[left_col]
                    return self._cost_index_nested_loop_join(
                        right_cost, left_stats, index_info, left_col
                    )
        
        bnlj_cost = self._cost_block_nested_loop_join(left_cost, right_cost)
        hash_join_cost = self._cost_hash_join(left_cost, right_cost)
        
        if hash_join_cost.io_cost < bnlj_cost.io_cost:
            return hash_join_cost
        return bnlj_cost
    
    def _cost_cross_product(self, left_cost: CostResult, right_cost: CostResult) -> CostResult:
        io_cost = left_cost.io_cost + right_cost.io_cost
        io_cost += left_cost.estimated_blocks * right_cost.estimated_blocks
        
        estimated_card = left_cost.estimated_cardinality * right_cost.estimated_cardinality
        
        return CostResult(
            io_cost=io_cost,
            estimated_cardinality=estimated_card,
            estimated_blocks=int(math.ceil(estimated_card / 10))
        )
    
    def _cost_sort(self, node: QueryTree) -> CostResult:

        if not node.childs:
            return CostResult(io_cost=0.0)
        
        source = node.childs[-1]
        source_cost = self.get_cost(source)
        
        b_r = source_cost.estimated_blocks
        M = 10  # Asumsi 10 blocks memory buffer (bisa di-config)
        
        if b_r <= M:
            io_cost = source_cost.io_cost + (2 * b_r)
        else:
            # External merge sort
            num_passes = math.ceil(math.log(b_r / M, M - 1)) if b_r > M else 0
            io_cost = source_cost.io_cost + (2 * b_r * (num_passes + 1))
        
        return CostResult(
            io_cost=io_cost,
            estimated_cardinality=source_cost.estimated_cardinality,
            estimated_blocks=source_cost.estimated_blocks
        )
    
    def _cost_limit(self, node: QueryTree) -> CostResult:
 
        if not node.childs:
            return CostResult(io_cost=0.0)
        
        limit_value = int(node.val) if node.val.isdigit() else 100
        source = node.childs[0]
        source_cost = self.get_cost(source)
        
        if source_cost.estimated_cardinality > 0:
            ratio = min(1.0, limit_value / source_cost.estimated_cardinality)
            io_cost = source_cost.io_cost * ratio
            estimated_blocks = int(math.ceil(source_cost.estimated_blocks * ratio))
        else:
            io_cost = source_cost.io_cost
            estimated_blocks = source_cost.estimated_blocks
        
        return CostResult(
            io_cost=io_cost,
            estimated_cardinality=min(limit_value, source_cost.estimated_cardinality),
            estimated_blocks=estimated_blocks
        )
    
    def _cost_alias(self, node: QueryTree) -> CostResult:
        if node.childs:
            return self.get_cost(node.childs[0])
        return CostResult(io_cost=0.0)
    
    def _cost_modification(self, node: QueryTree) -> CostResult:
        node_type = node.type
        
        if node_type == "INSERT_QUERY":
            return CostResult(io_cost=self.WRITE_COST, estimated_cardinality=1)
        
        elif node_type == "DELETE_QUERY":
            # DELETE: scan + delete
            if node.childs:
                source = node.childs[0]
                source_cost = self.get_cost(source)
                
                # Jika ada WHERE, cari filter node
                delete_cost = source_cost.io_cost
                if len(node.childs) > 1:
                    filter_node = node.childs[1]
                    filter_cost = self.get_cost(filter_node)
                    delete_cost = filter_cost.io_cost
                
                # Tambah cost untuk delete operation
                delete_cost += source_cost.estimated_blocks * self.WRITE_COST
                
                return CostResult(io_cost=delete_cost)
        
        elif node_type == "UPDATE_QUERY":
            # UPDATE: scan + modify
            if node.childs:
                source = node.childs[0]
                source_cost = self.get_cost(source)
                
                update_cost = source_cost.io_cost
                for child in node.childs:
                    if child.type == "FILTER":
                        filter_cost = self.get_cost(child)
                        update_cost = filter_cost.io_cost
                        break
                
                # Tambah cost untuk write
                update_cost += source_cost.estimated_blocks * self.WRITE_COST
                
                return CostResult(io_cost=update_cost)
        
        return CostResult(io_cost=1.0)
    
    def _cost_transaction(self, node: QueryTree) -> CostResult:
        """Cost untuk transaction (sum of all statements)."""
        total_cost = CostResult(io_cost=0.0)
        
        for child in node.childs:
            if child.type != "COMMIT":
                child_cost = self.get_cost(child)
                total_cost = total_cost + child_cost
        
        return total_cost
    
    def _cost_generic(self, node: QueryTree) -> CostResult:
        """Generic cost calculation untuk node types lainnya."""
        if not node.childs:
            return CostResult(io_cost=0.0)
        
        # Akumulasi cost dari semua children
        total_cost = CostResult(io_cost=0.0)
        for child in node.childs:
            child_cost = self.get_cost(child)
            total_cost = total_cost + child_cost
        
        return total_cost
    
    # Helper methods
    
    def _find_usable_index(self, condition: QueryTree, table_name: str, 
                          stats: Statistic) -> Optional[dict]:

        if not hasattr(stats, 'indexes') or not stats.indexes:
            return None
        
        if condition.type == "COMPARISON":
            op = condition.val
            if len(condition.childs) >= 2:
                left = condition.childs[0]
                right = condition.childs[1]
                
                col_name = self._extract_column_name(left)
                if not col_name or col_name not in stats.indexes:
                    return None
                
                index_info = stats.indexes[col_name]
                index_type = index_info.get("type")
                
                if index_type == "hash" and op == "=":
                    if right.type.startswith("LITERAL"):
                        selectivity = 1.0 / stats.V_a_r.get(col_name, 100)
                        return {
                            "column": col_name,
                            "type": "hash",
                            "operator": op,
                            "selectivity": selectivity
                        }
                
                if index_type == "btree" and op in ["=", "<", ">", "<=", ">=", "!="]:
                    if right.type.startswith("LITERAL"):
                        if op == "=":
                            selectivity = 1.0 / stats.V_a_r.get(col_name, 100)
                        elif op in ["<", ">", "<=", ">="]:
                            selectivity = 0.33  # Default range selectivity
                        else:
                            selectivity = 1.0 - (1.0 / stats.V_a_r.get(col_name, 100))
                        
                        return {
                            "column": col_name,
                            "type": "btree",
                            "height": index_info.get("height", 3),
                            "operator": op,
                            "selectivity": selectivity
                        }
        
        elif condition.type == "BETWEEN_EXPR":
            if len(condition.childs) >= 1:
                col_expr = condition.childs[0]
                col_name = self._extract_column_name(col_expr)
                
                if col_name and col_name in stats.indexes:
                    index_info = stats.indexes[col_name]
                    if index_info.get("type") == "btree":
                        return {
                            "column": col_name,
                            "type": "btree",
                            "height": index_info.get("height", 3),
                            "operator": "BETWEEN",
                            "selectivity": 0.25
                        }
        
        elif condition.type == "IN_EXPR":
            if len(condition.childs) >= 2:
                col_expr = condition.childs[0]
                list_node = condition.childs[1]
                
                col_name = self._extract_column_name(col_expr)
                if col_name and col_name in stats.indexes:
                    index_info = stats.indexes[col_name]
                    
                    if list_node.type == "LIST":
                        n_values = len(list_node.childs)
                        selectivity = min(0.5, n_values / stats.V_a_r.get(col_name, 100))
                        
                        return {
                            "column": col_name,
                            "type": index_info.get("type"),
                            "height": index_info.get("height", 3),
                            "operator": "IN",
                            "selectivity": selectivity,
                            "n_values": n_values
                        }
        
        elif condition.type == "OPERATOR" and condition.val == "AND":
            best_index = None
            best_selectivity = 1.0
            
            for child in condition.childs:
                index_info = self._find_usable_index(child, table_name, stats)
                if index_info and index_info["selectivity"] < best_selectivity:
                    best_index = index_info
                    best_selectivity = index_info["selectivity"]
            
            return best_index
        
        return None
    
    def _cost_index_scan(self, table_name: str, stats: Statistic, index_info: dict) -> CostResult:
        index_type = index_info["type"]
        selectivity = index_info["selectivity"]
        operator = index_info["operator"]
        print(index_info)
        
        estimated_tuples = int(stats.n_r * selectivity)
        estimated_blocks = max(1, int(math.ceil(estimated_tuples / stats.f_r)))
        
        if index_type == "hash":
            index_access_cost = 1.0 * self.RANDOM_IO_COST
            
            if operator == "IN":
                n_values = index_info.get("n_values", 1)
                index_access_cost *= n_values
            
            data_access_cost = estimated_blocks * self.SEQUENTIAL_IO_COST
            
            total_io_cost = index_access_cost + data_access_cost
            
        elif index_type == "btree":
            height = index_info.get("height", 3)
            
            if operator == "=":
                index_access_cost = (height + 1) * self.RANDOM_IO_COST
                data_access_cost = estimated_blocks * self.SEQUENTIAL_IO_COST
                total_io_cost = index_access_cost + data_access_cost
                
            elif operator in ["<", ">", "<=", ">=", "BETWEEN"]:
                index_access_cost = (height + 1) * self.RANDOM_IO_COST
                
                leaf_scan_cost = max(1, estimated_blocks // 10) * self.SEQUENTIAL_IO_COST # Ini asumsi blocking factor indexnya itu 10 kali lebih kecil
                data_access_cost = estimated_blocks * self.RANDOM_IO_COST # Ini asumsi clustered index kalau primary harusnya sequential cost
                
                total_io_cost = index_access_cost + leaf_scan_cost + data_access_cost
                
            elif operator == "IN":
                n_values = index_info.get("n_values", 1)
                index_access_cost = n_values * (height + 1) * self.RANDOM_IO_COST
                data_access_cost = estimated_blocks * self.SEQUENTIAL_IO_COST
                total_io_cost = index_access_cost + data_access_cost
                
            else:
                index_access_cost = (height + 1) * self.RANDOM_IO_COST
                data_access_cost = estimated_blocks * self.SEQUENTIAL_IO_COST
                total_io_cost = index_access_cost + data_access_cost
        else:
            return CostResult(
                io_cost=stats.b_r * self.SEQUENTIAL_IO_COST,
                estimated_cardinality=stats.n_r,
                estimated_blocks=stats.b_r
            )
        
        return CostResult(
            io_cost=total_io_cost,
            cpu_cost=estimated_tuples * self.CPU_COST_PER_TUPLE,
            estimated_cardinality=estimated_tuples,
            estimated_blocks=estimated_blocks
        )
    
    def _cost_index_nested_loop_join(self, outer_cost: CostResult, inner_stats: Statistic,
                                     index_info: dict, join_column: str) -> CostResult:

        index_type = index_info.get("type")
        n_outer = outer_cost.estimated_cardinality
        
        outer_scan_cost = outer_cost.io_cost
        
        if index_type == "hash":
            index_lookup_cost = n_outer * 1.0 * self.RANDOM_IO_COST

            selectivity = 1.0 / inner_stats.V_a_r.get(join_column, 100)
            estimated_matches_per_lookup = max(1, int(inner_stats.n_r * selectivity))
            total_matches = n_outer * estimated_matches_per_lookup
            data_blocks_needed = int(math.ceil(total_matches / inner_stats.f_r))
            data_access_cost = data_blocks_needed * self.SEQUENTIAL_IO_COST
            
            total_io_cost = outer_scan_cost + index_lookup_cost + data_access_cost
            
        elif index_type == "btree":
            height = index_info.get("height", 3)
            
            index_lookup_cost = n_outer * (height + 1) * self.RANDOM_IO_COST
            
            selectivity = 1.0 / inner_stats.V_a_r.get(join_column, 100)
            estimated_matches_per_lookup = max(1, int(inner_stats.n_r * selectivity))
            total_matches = n_outer * estimated_matches_per_lookup
            data_blocks_needed = int(math.ceil(total_matches / inner_stats.f_r))
            data_access_cost = data_blocks_needed * self.SEQUENTIAL_IO_COST
            
            total_io_cost = outer_scan_cost + index_lookup_cost + data_access_cost
        else:
            return self._cost_block_nested_loop_join(outer_cost, 
                CostResult(io_cost=inner_stats.b_r, estimated_cardinality=inner_stats.n_r, 
                          estimated_blocks=inner_stats.b_r))
        
        join_selectivity = 1.0 / max(
            inner_stats.V_a_r.get(join_column, 100),
            100
        )
        estimated_output = int(outer_cost.estimated_cardinality * inner_stats.n_r * join_selectivity)
        estimated_output_blocks = int(math.ceil(estimated_output / inner_stats.f_r))
        
        return CostResult(
            io_cost=total_io_cost,
            estimated_cardinality=estimated_output,
            estimated_blocks=estimated_output_blocks
        )
    
    def _cost_block_nested_loop_join(self, left_cost: CostResult, 
                                     right_cost: CostResult) -> CostResult:

        read_outer_cost = left_cost.io_cost
        

        write_outer_cost = left_cost.estimated_blocks * self.WRITE_COST
        
        nested_cost = left_cost.estimated_blocks * right_cost.estimated_blocks * self.SEQUENTIAL_IO_COST
        
        total_io_cost = read_outer_cost + write_outer_cost + nested_cost
        
        selectivity = 0.1
        estimated_card = int(left_cost.estimated_cardinality * right_cost.estimated_cardinality * selectivity)
        
        return CostResult(
            io_cost=total_io_cost,
            estimated_cardinality=estimated_card,
            estimated_blocks=int(math.ceil(estimated_card / 10))
        )
    
    def _cost_hash_join(self, left_cost: CostResult, right_cost: CostResult) -> CostResult:

        M = 100  # Memory buffer size (blocks) - bisa di-config
        

        if left_cost.estimated_blocks <= right_cost.estimated_blocks:
            build_cost = left_cost
            probe_cost = right_cost
        else:
            build_cost = right_cost
            probe_cost = left_cost
        
        if build_cost.estimated_blocks <= M:
            io_cost = build_cost.io_cost
            io_cost += build_cost.estimated_blocks * self.WRITE_COST
            io_cost += probe_cost.io_cost
            
        else:
            partitioning_cost = 2 * (build_cost.estimated_blocks + probe_cost.estimated_blocks)
            join_cost = build_cost.estimated_blocks + probe_cost.estimated_blocks
            io_cost = (partitioning_cost + join_cost) * self.SEQUENTIAL_IO_COST
        
        selectivity = 0.1
        estimated_card = int(left_cost.estimated_cardinality * right_cost.estimated_cardinality * selectivity)
        
        return CostResult(
            io_cost=io_cost,
            estimated_cardinality=estimated_card,
            estimated_blocks=int(math.ceil(estimated_card / 10))
        )
    
    def _is_equijoin(self, condition: QueryTree) -> Tuple[bool, Optional[str], Optional[str]]:
        if condition.type == "COMPARISON" and condition.val == "=":
            if len(condition.childs) >= 2:
                left = condition.childs[0]
                right = condition.childs[1]
                
                left_col = self._extract_column_name(left)
                right_col = self._extract_column_name(right)
                
                if left_col and right_col:
                    return True, left_col, right_col
        
        return False, None, None
    
    def _extract_column_name(self, node: QueryTree) -> Optional[str]:
        if node.type == "COLUMN_REF":
            for child in node.childs:
                if child.type == "COLUMN_NAME":
                    for grandchild in child.childs:
                        if grandchild.type == "IDENTIFIER":
                            return grandchild.val
        elif node.type == "IDENTIFIER":
            return node.val
        
        return None
    
    def _extract_table_name(self, node: QueryTree) -> Optional[str]:
        """Extract table name dari query tree node."""
        if node.type == "RELATION":
            return node.val
        elif node.type == "ALIAS" and node.childs:
            return self._extract_table_name(node.childs[0])
        elif node.childs:
            for child in node.childs:
                table_name = self._extract_table_name(child)
                if table_name:
                    return table_name
        
        return None