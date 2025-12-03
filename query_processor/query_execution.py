import sys
import os
import traceback
import logging
logger = logging.getLogger(__name__)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


class TransactionAbortedException(Exception):
    def __init__(self, transaction_id: int, reason: str):
        self.transaction_id = transaction_id
        self.reason = reason
        super().__init__(f"Transaction {transaction_id} aborted: {reason}")

from query_optimizer.query_tree import QueryTree

# Try relative import first (when used as package), fallback to direct import (when used standalone)
try:
    from .adapter_storage import (
        AdapterStorage, 
        Condition, 
        DataRetrieval, 
        ColumnDefinition,
        ForeignKey
    )
    from .adapter_optimizer import AdapterOptimizer
    from .transaction_buffer import TransactionBuffer
except ImportError:
    from adapter_storage import (
        AdapterStorage,
        Condition,
        DataRetrieval,
        ColumnDefinition,
        ForeignKey
    )
    from adapter_optimizer import AdapterOptimizer
    from transaction_buffer import TransactionBuffer

class QueryExecution:
    def __init__(self, storage_adapter=None, ccm_adapter=None, storage_manager=None, frm_adapter=None):
        self.ccm_adapter = ccm_adapter
        self.frm_adapter = frm_adapter
        self.transaction_buffer = TransactionBuffer()
        
        # Initialize storage adapter
        if storage_adapter:
            self.storage_adapter = storage_adapter
            self.storage_manager = storage_adapter.sm
        elif storage_manager:
            self.storage_adapter = AdapterStorage(storage_manager=storage_manager)
            self.storage_manager = storage_manager
        else:
            # Use project root data directory by default
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
            data_dir = os.path.join(project_root, 'data')
            self.storage_adapter = AdapterStorage(data_dir=data_dir)
            self.storage_manager = self.storage_adapter.sm
        
        # Initialize optimizer adapter for optimization decisions
        self.optimizer_adapter = AdapterOptimizer()
        
        # Track limit values per transaction for thread safety
        self.limit = {} 
    
    # for select and join methods
    def _get_execution_method(self, node: QueryTree) -> str:
        """Delegate to optimizer adapter for execution method decisions"""
        return self.optimizer_adapter.get_execution_method(node)
        
    def execute_node(self, query_tree: QueryTree, transaction_id: int = None) -> list[dict] | None:
        node_type = query_tree.type
        
        if node_type == "PROJECT":
            return self.execute_project(query_tree, transaction_id)
        elif node_type == "FILTER":
            return self.execute_filter(query_tree, transaction_id)
        elif node_type == "SORT":
            return self.execute_sort(query_tree, transaction_id)
        elif node_type == "LIMIT":
            return self.execute_limit(query_tree, transaction_id)
        elif node_type == "RELATION":
            return self.execute_relation(query_tree, transaction_id)
        elif node_type == "ALIAS":
            return self.execute_alias(query_tree, transaction_id)
        elif node_type == "JOIN":
            return self.execute_join(query_tree, transaction_id)
        elif node_type == "UPDATE_QUERY":
            return self.execute_update(query_tree, transaction_id)
        elif node_type == "INSERT_QUERY":
            return self.execute_insert(query_tree, transaction_id)
        elif node_type == "DELETE_QUERY":
            return self.execute_delete(query_tree, transaction_id)
        elif node_type == "BEGIN_TRANSACTION":
            return self.execute_transaction(query_tree, transaction_id)
        elif node_type == "CREATE_TABLE":
            return self.execute_create_table(query_tree, transaction_id)
        elif node_type == "DROP_TABLE":
            return self.execute_drop_table(query_tree, transaction_id)
        else:
            raise ValueError(f"Unsupported node type: {node_type}")
        
    def execute_project(self, query_tree: QueryTree, transaction_id: int) -> list[dict]:
        """
        Execute PROJECT node (SELECT clause)
        Structure:
        - If value="*": PROJECT("*") with 1 child (source)
        - Else: PROJECT with N COLUMN_REF children + 1 source child (last)
        """
        print(f"\n[PROJECT] Executing SELECT...")
        
        # Get source (last child)
        source = query_tree.childs[-1]
        
        # if simple, pushdown
        if source.type == "RELATION":
            table_name = source.val
            
            # Validate READ access with CCM
            if self.ccm_adapter and transaction_id:
                response = self.ccm_adapter.validate_action(transaction_id, table_name, 'read')
                if not response.allowed:
                    logger.info(f"[PROJECT] CCM denied READ access: {response.message}")
                    # Check if transaction was aborted (deadlock prevention: Wait-Die)
                    msg_lower = response.message.lower()
                    if "aborted" in msg_lower or "died" in msg_lower:
                        raise TransactionAbortedException(transaction_id, response.message)
                    return []
                # Log the object access
                self.ccm_adapter.log_object(transaction_id, table_name)
            
            if query_tree.val == "*":
                logger.info(f"[PROJECT] SELECT * from '{table_name}' (optimized - direct storage call)")
                try:
                    
                    data_retrieval = DataRetrieval(
                        table=table_name,
                        column=[],  # Empty = all columns
                        conditions=[]
                    )
                    source_data = self.storage_manager.read_block(data_retrieval)
                    
                    # Apply buffered operations for this transaction
                    if transaction_id:
                        source_data = self._apply_buffered_operations(source_data, transaction_id, table_name)
                        logger.info(f"[PROJECT] After applying buffer: {len(source_data)} rows")
                    
                    # Apply limit if set for this transaction
                    if transaction_id in self.limit:
                        source_data = source_data[:self.limit[transaction_id]]
                        del self.limit[transaction_id]
                    
                    logger.info(f"[PROJECT] Retrieved {len(source_data)} rows")
                    return source_data
                
                except Exception as e:
                    logger.info(f"[PROJECT] Error: {e}")
                    return []
            
            # SELECT
            columns = []
            for i in range(len(query_tree.childs) - 1):  # All except last (source)
                col_ref = query_tree.childs[i]
                column_name = self.extract_column_name(col_ref)
                columns.append(column_name)
            
            logger.info(f"[PROJECT] SELECT {', '.join(columns)} from '{table_name}' (optimized - projection pushed down)")
            try:
                
                # Use storage adapter with projection pushdown
                source_data = self.storage_adapter.read_data(
                    table_name=table_name,
                    columns=columns,
                    conditions=[],
                    transaction_id=transaction_id
                )
                
                # Apply buffered operations for this transaction
                if transaction_id:
                    # First get full rows with buffer applied
                    full_data_retrieval = DataRetrieval(
                        table=table_name,
                        column=[],
                        conditions=[]
                    )
                    full_data = self.storage_manager.read_block(full_data_retrieval)
                    full_data = self._apply_buffered_operations(full_data, transaction_id, table_name)
                    
                    # Then project to requested columns
                    source_data = []
                    for row in full_data:
                        projected_row = {col: row.get(col) for col in columns if col in row}
                        source_data.append(projected_row)
                    
                    logger.info(f"[PROJECT] After applying buffer: {len(source_data)} rows")
                
                # Apply limit if set for this transaction
                if transaction_id in self.limit:
                    source_data = source_data[:self.limit[transaction_id]]
                    del self.limit[transaction_id]
                
                logger.info(f"[PROJECT] Retrieved {len(source_data)} rows with {len(columns)} columns")
                return source_data
            except Exception as e:
                logger.info(f"[PROJECT] Error: {e}")
                return []
        
        # Complex source, query processor handles it
        source_data = self.execute_node(source, transaction_id)
        
        # Apply limit if set for this transaction
        if transaction_id in self.limit:
            source_data = source_data[:self.limit[transaction_id]]
            del self.limit[transaction_id]
        
        if not source_data:
            print("[PROJECT] No data from source")
            return []
        
        if query_tree.val == "*":
            logger.info(f"[PROJECT] SELECT * - returning all columns ({len(source_data)} rows)")
            return source_data
        
        columns = []
        for i in range(len(query_tree.childs) - 1):
            col_ref = query_tree.childs[i]
            column_name = self.extract_column_name(col_ref)
            columns.append(column_name)
        
        logger.info(f"[PROJECT] SELECT columns: {columns} (in-memory projection)")
        
        projected_data = []
        for row in source_data:
            projected_row = {}
            for col in columns:
                if col in row:
                    projected_row[col] = row[col]
                else:
                    logger.info(f"[PROJECT] Warning: Column '{col}' not found in row")
            projected_data.append(projected_row)
        
        logger.info(f"[PROJECT] Projected {len(projected_data)} rows")
        return projected_data
    
    def execute_filter(self, query_tree: QueryTree, transaction_id: int) -> list[dict]:
        """
        Execute FILTER node (WHERE clause)
        Structure: FILTER with 2 children: [source, condition_tree]
        """
        print(f"\n[FILTER] Executing WHERE clause...")
        
        if len(query_tree.childs) != 2:
            raise ValueError(f"FILTER must have exactly 2 children (source + condition)")
        
        source = query_tree.childs[0]
        condition_tree = query_tree.childs[1]
        
        # Try to push down to storage manager if source is a simple RELATION
        if source.type == "RELATION":
            table_name = source.val
            method = self._get_execution_method(source)
            condition_str = self.condition_tree_to_string(condition_tree)

            # Validate READ access with CCM
            if self.ccm_adapter and transaction_id:
                response = self.ccm_adapter.validate_action(transaction_id, table_name, 'read')
                if not response.allowed:
                    logger.info(f"[FILTER] CCM denied READ access: {response.message}")
                    # Check if transaction was aborted (deadlock prevention: Wait-Die)
                    msg_lower = response.message.lower()
                    if "aborted" in msg_lower or "died" in msg_lower:
                        raise TransactionAbortedException(transaction_id, response.message)
                    return []
                # Log the object access
                self.ccm_adapter.log_object(transaction_id, table_name)

            # Pushdown if possible            
            try:
                conditions = self.condition_tree_to_conditions(condition_tree)
                logger.info(f"[FILTER] -> STORAGE MANAGER: Filter on table '{table_name}' with {len(conditions)} conditions using method: {method}")
                logger.info(f"[FILTER]    Condition: {condition_str}")
                
                # TODO: Use method attribute to leverage indexes when available
                data_retrieval = DataRetrieval(
                    table=table_name,
                    column=[],  # Empty = all columns
                    conditions=conditions
                )
                
                filtered_data = self.storage_manager.read_block(data_retrieval)
                logger.info(f"[FILTER] Retrieved {len(filtered_data)} rows (filter pushed down via {method})")
                
                # Apply buffered operations for this transaction
                if transaction_id:
                    filtered_data = self._apply_buffered_operations(filtered_data, transaction_id, table_name)
                    logger.info(f"[FILTER] After applying buffer: {len(filtered_data)} rows")
                
                return filtered_data
                
            except ValueError as e:
                # Complex condition (OR/NOT) - fallback to in-memory filtering
                logger.info(f"[FILTER] Cannot push down condition (complex OR/NOT logic): {e}")
                logger.info(f"[FILTER] Falling back to in-memory filtering")
        
        # Fallback: Execute source then filter in memory
        source_data = self.execute_node(source, transaction_id)
        
        if not source_data:
            print("[FILTER] No data from source")
            return []
        
        filtered_data = []
        for row in source_data:
            if self.evaluate_condition(condition_tree, row):
                filtered_data.append(row)
        
        logger.info(f"[FILTER] Filtered {len(source_data)} -> {len(filtered_data)} rows (in-memory)")
        return filtered_data
    
    def execute_sort(self, query_tree: QueryTree, transaction_id: int) -> list[dict]:
        """
        Execute SORT node (ORDER BY clause)
        Structure: SORT with 2 children: [COLUMN_REF, source]
        Value: "ASC" or "DESC"
        """
        print(f"\n[SORT] Executing ORDER BY...")
        
        if len(query_tree.childs) != 2:
            raise ValueError(f"SORT must have exactly 2 children (column + source)")
        
        # Get column to sort by, get direction get data
        col_ref = query_tree.childs[0]
        column_name = self.extract_column_name(col_ref)
        direction = query_tree.val if query_tree.val else "ASC"
        reverse = (direction.upper() == "DESC")
        source = query_tree.childs[1]
        source_data = self.execute_node(source, transaction_id)
        
        if not source_data:
            print("[SORT] No data to sort")
            return []
        
        logger.info(f"[SORT] Sorting by '{column_name}' {direction}")
        
        # Sort data
        try:
            sorted_data = sorted(source_data, key=lambda row: row.get(column_name, ""), reverse=reverse)
            logger.info(f"[SORT] Sorted {len(sorted_data)} rows")
            return sorted_data
        except Exception as e:
            logger.info(f"[SORT] Error sorting: {e}")
            return source_data
    
    def execute_limit(self, query_tree: QueryTree, transaction_id: int) -> list[dict]:
        """
        Execute LIMIT node
        Structure: LIMIT with 1 child (source), value = limit number
        """
        print(f"\n[LIMIT] Executing LIMIT...")
        
        limit_value = int(query_tree.val)
        self.limit[transaction_id] = limit_value
        logger.info(f"[LIMIT] Setting limit to {limit_value} for transaction {transaction_id}")
        
        # Execute source
        source = query_tree.childs[0]
        return self.execute_node(source, transaction_id)
    
    def execute_relation(self, query_tree: QueryTree, transaction_id: int) -> list[dict]:
        """
        Execute RELATION node (table reference)
        Value: table name
        Uses method attribute to determine access strategy
        """
        table_name = query_tree.val
        method = self._get_execution_method(query_tree)
        
        print(f"\n[RELATION] Accessing table '{table_name}' using method: {method}")
        logger.info(f"[RELATION] -> STORAGE MANAGER: Scan table '{table_name}' with transaction_id={transaction_id}")
        
        # Validate READ access with CCM
        if self.ccm_adapter and transaction_id:
            response = self.ccm_adapter.validate_action(transaction_id, table_name, 'read')
            if not response.allowed:
                logger.info(f"[RELATION] CCM denied READ access: {response.message}")
                # Check if transaction was aborted (deadlock prevention: Wait-Die)
                msg_lower = response.message.lower()
                if "aborted" in msg_lower or "died" in msg_lower:
                    raise TransactionAbortedException(transaction_id, response.message)
                return []
            # Log the object access
            self.ccm_adapter.log_object(transaction_id, table_name)
        
        # Call storage manager to read all rows
        # TODO: Implement different access methods (hash_index, btree_index) when available
        data_retrieval = DataRetrieval(
            table=table_name,
            column=[],  # Empty = all columns
            conditions=[]  # No conditions = all rows
        )
        
        try:
            data = self.storage_manager.read_block(data_retrieval)
            logger.info(f"[RELATION] Retrieved {len(data)} rows from '{table_name}' via {method}")
            
            # Apply buffered operations for this transaction
            if transaction_id:
                data = self._apply_buffered_operations(data, transaction_id, table_name)
                logger.info(f"[RELATION] After applying buffer: {len(data)} rows")
            
            return data
        except Exception as e:
            logger.info(f"[RELATION] Error reading from storage manager: {e}")
            logger.info(f"[RELATION] Returning empty result")
            return []
    
    def execute_alias(self, query_tree: QueryTree, transaction_id: int) -> list[dict]:
        """
        Execute ALIAS node (table alias)
        Structure: ALIAS with 1 child (what is being aliased)
        Value: alias name
        """
        alias_name = query_tree.val
        print(f"\n[ALIAS] Applying alias '{alias_name}'")
        
        source = query_tree.childs[0]
        return self.execute_node(source, transaction_id)
    
    def execute_join(self, query_tree: QueryTree, transaction_id: int) -> list[dict]:
        """
        Execute JOIN node using method specified in node
        Structure:
        - NATURAL: JOIN("NATURAL") with 2 children (left, right relations)
        - INNER: JOIN("INNER") with 3 children (left, right, condition)
        """
        join_type = query_tree.val
        join_method = self._get_execution_method(query_tree)
        
        print(f"\n[JOIN] Executing {join_type} JOIN using method: {join_method}")
        
        left = query_tree.childs[0]
        right = query_tree.childs[1]
        left_table = self.extract_table_name(left)
        right_table = self.extract_table_name(right)
        
        logger.info(f"[JOIN] Left table: '{left_table}'")
        logger.info(f"[JOIN] Right table: '{right_table}'")
        
        # Execute both sides
        left_data = self.execute_node(left, transaction_id)
        right_data = self.execute_node(right, transaction_id)
        
        if not left_data:
            logger.info(f"[JOIN] Left table empty, returning empty result")
            return []
        if not right_data:
            logger.info(f"[JOIN] Right table empty, returning empty result")
            return []
        
        logger.info(f"[JOIN] Left rows: {len(left_data)}, Right rows: {len(right_data)}")
        
        if join_method == "nested_loop":
            result = self._execute_nested_loop_join(join_type, left_data, right_data, query_tree, transaction_id)
        elif join_method == "hash_join":
            # TODO: Implement hash join algorithm
            logger.info(f"[JOIN] Hash join not yet implemented, falling back to nested loop")
            result = self._execute_nested_loop_join(join_type, left_data, right_data, query_tree, transaction_id)
        elif join_method == "merge_join":
            # TODO: Implement merge join algorithm
            logger.info(f"[JOIN] Merge join not yet implemented, falling back to nested loop")
            result = self._execute_nested_loop_join(join_type, left_data, right_data, query_tree, transaction_id)
        else:
            logger.info(f"[JOIN] Unknown join method '{join_method}', using nested loop")
            result = self._execute_nested_loop_join(join_type, left_data, right_data, query_tree, transaction_id)
        
        return result
    
    def _execute_nested_loop_join(self, join_type: str, left_data: list[dict], right_data: list[dict], 
                                   query_tree: QueryTree, transaction_id: int) -> list[dict]:
        """
        Execute nested loop join algorithm (O(n*m) complexity)
        """
        result = []
        
        if join_type == "NATURAL":
            logger.info(f"[JOIN] NATURAL JOIN - finding common columns...")
            
            if not left_data or not right_data:
                return []
            
            left_cols = set(left_data[0].keys())
            right_cols = set(right_data[0].keys())
            common_cols = left_cols & right_cols
            
            if not common_cols:
                logger.info(f"[JOIN] No common columns found, performing cartesian product")
                for left_row in left_data:
                    for right_row in right_data:
                        merged_row = {**left_row, **right_row}
                        result.append(merged_row)
            else:
                logger.info(f"[JOIN] Common columns: {common_cols}")
                
                for left_row in left_data:
                    for right_row in right_data:
                        match = all(
                            left_row.get(col) == right_row.get(col) 
                            for col in common_cols
                        )
                        
                        if match:
                            merged_row = {**left_row, **right_row}
                            result.append(merged_row)
            
            logger.info(f"[JOIN] NATURAL JOIN produced {len(result)} rows (nested loop)")
            return result
            
        elif join_type == "INNER":
            if len(query_tree.childs) < 3:
                raise ValueError("INNER JOIN requires join condition")
            
            condition = query_tree.childs[2]
            condition_str = self.condition_tree_to_string(condition)
            logger.info(f"[JOIN] INNER JOIN ON {condition_str}")
            
            matched_count = 0
            for left_row in left_data:
                for right_row in right_data:
                    merged_row = {**left_row, **right_row}
                    
                    if self.evaluate_condition(condition, merged_row):
                        result.append(merged_row)
                        matched_count += 1
            
            logger.info(f"[JOIN] INNER JOIN produced {len(result)} rows (nested loop, checked {len(left_data) * len(right_data)} combinations)")
            return result
        
        elif join_type == "CROSS":
            logger.info(f"[JOIN] CROSS JOIN - cartesian product")
            
            for left_row in left_data:
                for right_row in right_data:
                    merged_row = {**left_row, **right_row}
                    result.append(merged_row)
            
            logger.info(f"[JOIN] CROSS JOIN produced {len(result)} rows (nested loop)")
            return result
        
        return []
        
    def execute_update(self, query_tree: QueryTree, transaction_id: int) -> None:
        """
        Execute UPDATE_QUERY node
        Structure: UPDATE_QUERY with children: [RELATION, ASSIGNMENT+, FILTER?]
        
        Uses READ-MODIFY-WRITE pattern to support expressions like SET y = y + 1
        """
        logger.info(f"\n[UPDATE] Executing UPDATE statement...")
        
        if len(query_tree.childs) < 2:
            raise ValueError("UPDATE requires at least RELATION and ASSIGNMENT")
        
        relation = query_tree.childs[0]
        table_name = relation.val
        
        # Get assignments
        assignment_exprs = []
        i = 1
        while i < len(query_tree.childs) and query_tree.childs[i].type == "ASSIGNMENT":
            assignment = query_tree.childs[i]
            col_ref = assignment.childs[0]
            value_expr = assignment.childs[1]
            
            column_name = self.extract_column_name(col_ref)
            assignment_exprs.append((column_name, value_expr))
            i += 1
        
        # Get WHERE condition if exists
        conditions = []
        if i < len(query_tree.childs) and query_tree.childs[i].type == "FILTER":
            filter_node = query_tree.childs[i]
            condition_str = self.condition_tree_to_string(filter_node.childs[1])
            logger.info(f"[UPDATE] WHERE condition: {condition_str}")
            try:
                conditions = self.condition_tree_to_conditions(filter_node.childs[1])
            except ValueError as e:
                logger.info(f"[UPDATE] Complex condition not supported for UPDATE: {e}")
                return 0
        
        logger.info(f"[UPDATE] Using READ-MODIFY-WRITE pattern for expression support")
        logger.info(f"[UPDATE]    Table: '{table_name}'")
        logger.info(f"[UPDATE]    Assignments: {len(assignment_exprs)} columns")
        if conditions:
            logger.info(f"[UPDATE]    WHERE: {len(conditions)} condition(s)")
        logger.info(f"[UPDATE]    Transaction ID: {transaction_id}")
        
        # Validate WRITE access with CCM
        if self.ccm_adapter and transaction_id:
            response = self.ccm_adapter.validate_action(transaction_id, table_name, 'write')
            if not response.allowed:
                logger.info(f"[UPDATE] CCM denied WRITE access: {response.message}")
                # Check if transaction was aborted (deadlock prevention: Wait-Die)
                msg_lower = response.message.lower()
                if "aborted" in msg_lower or "died" in msg_lower:
                    raise TransactionAbortedException(transaction_id, response.message)
                return 0
            # Log the object access
            self.ccm_adapter.log_object(transaction_id, table_name)
        
        try:
            # STEP 1: READ (with buffered operations applied)
            logger.info(f"[UPDATE] STEP 1: Reading matching rows with buffered operations...")
            # Read from storage
            data_retrieval = DataRetrieval(
                table=table_name,
                column=[],
                conditions=conditions
            )
            storage_rows = self.storage_manager.read_block(data_retrieval)
            
            # Apply buffered operations to see uncommitted writes in this transaction
            if transaction_id:
                matching_rows = self._apply_buffered_operations(storage_rows, transaction_id, table_name)
                logger.info(f"[UPDATE]    After applying buffer: {len(matching_rows)} row(s)")
            else:
                matching_rows = storage_rows
            
            logger.info(f"[UPDATE]    Found {len(matching_rows)} matching row(s)")
            
            if not matching_rows:
                logger.info(f"[UPDATE] No rows to update")
                return 0
            
            # STEP 2: MODIFY
            logger.info(f"[UPDATE] STEP 2: Evaluating expressions per row...")
            rows_to_update = []
            for row in matching_rows:
                updated_row = row.copy()
                for col_name, value_expr in assignment_exprs:
                    new_value = self.evaluate_value_expression(value_expr, row)
                    updated_row[col_name] = new_value
                    logger.info(f"[UPDATE]    Row {row.get('id', '?')}: {col_name} = {row.get(col_name)} → {new_value}")
                rows_to_update.append(updated_row)
            
            # Log to FRM
            if self.frm_adapter and transaction_id:
                self.frm_adapter.log_write(
                    transaction_id=transaction_id,
                    query=f"UPDATE {table_name}",
                    table_name=table_name,
                    old_data=matching_rows,  # Original rows for rollback
                    new_data=rows_to_update  # Modified rows
                )
            
            # STEP 3: Buffer updates instead of writing immediately
            logger.info(f"[UPDATE] STEP 3: Buffering updated rows...")
            
            if transaction_id:
                # Get all columns from original matching rows
                all_columns = list(matching_rows[0].keys()) if matching_rows else []
                
                for idx, new_row in enumerate(rows_to_update):
                    # Create unique condition for this specific row
                    row_conditions = []
                    for col in all_columns:
                        if col in matching_rows[idx]:
                            row_conditions.append(Condition(column=col, operation="=", operand=matching_rows[idx][col]))
                    
                    # Buffer the update
                    self.transaction_buffer.buffer_update(
                        transaction_id=transaction_id,
                        table_name=table_name,
                        old_data=matching_rows[idx],
                        new_data=new_row,
                        conditions=row_conditions
                    )
                
                logger.info(f"[UPDATE] Buffered {len(rows_to_update)} row(s) (will write on COMMIT)")
                return len(rows_to_update)
            else:
                # No transaction - write directly
                rows_affected = 0
                all_columns = list(matching_rows[0].keys()) if matching_rows else []
                
                for idx, row in enumerate(rows_to_update):
                    row_conditions = []
                    for col in all_columns:
                        if col in matching_rows[idx]:
                            row_conditions.append(Condition(column=col, operation="=", operand=matching_rows[idx][col]))
                    
                    affected = self.storage_adapter.write_data(
                        table_name=table_name,
                        columns=all_columns,
                        values=[row[col] for col in all_columns],
                        conditions=row_conditions,
                        transaction_id=transaction_id
                    )
                    rows_affected += affected
                
                logger.info(f"[UPDATE] Updated {rows_affected} row(s) (no transaction)")
                return rows_affected
            
        except Exception as e:
            logger.info(f"[UPDATE] Error: {e}")
            # for debugging lmao
            import traceback
            traceback.print_exc()
            return 0
    
    def execute_insert(self, query_tree: QueryTree, transaction_id: int) -> int:
        """
        Execute INSERT_QUERY node
        Structure: INSERT_QUERY with children: [RELATION, COLUMN_LIST, VALUES_CLAUSE]
        """
        print(f"\n[INSERT] Executing INSERT statement...")
        
        if len(query_tree.childs) != 3:
            raise ValueError("INSERT requires RELATION, COLUMN_LIST, and VALUES_CLAUSE")
        
        relation = query_tree.childs[0]
        table_name = relation.val
        
        column_list = query_tree.childs[1]
        columns = [self.extract_identifier(child) for child in column_list.childs]
        
        values_clause = query_tree.childs[2]
        values = [self.extract_literal_value(child) for child in values_clause.childs]
        
        logger.info(f"[INSERT] -> STORAGE MANAGER: INSERT INTO '{table_name}'")
        logger.info(f"[INSERT]    Columns: {columns}")
        logger.info(f"[INSERT]    Values: {values}")
        logger.info(f"[INSERT]    Transaction ID: {transaction_id}")
        
        # Validate WRITE access with CCM
        if self.ccm_adapter and transaction_id:
            response = self.ccm_adapter.validate_action(transaction_id, table_name, 'write')
            if not response.allowed:
                logger.info(f"[INSERT] CCM denied WRITE access: {response.message}")
                # Check if transaction was aborted (deadlock prevention: Wait-Die)
                msg_lower = response.message.lower()
                if "aborted" in msg_lower or "died" in msg_lower:
                    raise TransactionAbortedException(transaction_id, response.message)
                return 0
            # Log the object access
            self.ccm_adapter.log_object(transaction_id, table_name)
        
        try:
            # Prepare new data for FRM logging
            new_row = dict(zip(columns, values))
            
            # Log to FRM
            if self.frm_adapter and transaction_id:
                self.frm_adapter.log_write(
                    transaction_id=transaction_id,
                    query=f"INSERT INTO {table_name}",
                    table_name=table_name,
                    old_data=None,  # No old data for INSERT
                    new_data=[new_row]
                )
            
            # Buffer the write operation instead of writing to storage immediately
            if transaction_id:
                self.transaction_buffer.buffer_insert(transaction_id, table_name, new_row)
                logger.info(f"[INSERT] Buffered 1 row (will write on COMMIT)")
                return 1
            else:
                # No transaction - write directly (for DDL operations)
                rows_affected = self.storage_adapter.write_data(
                    table_name=table_name,
                    columns=columns,
                    values=values,
                    conditions=[],
                    transaction_id=transaction_id
                )
                logger.info(f"[INSERT] Inserted {rows_affected} row(s) (no transaction)")
                return rows_affected
        except Exception as e:
            logger.info(f"[INSERT] Error: {e}")
            return 0
    
    def execute_delete(self, query_tree: QueryTree, transaction_id: int) -> int:
        """
        Execute DELETE_QUERY node
        Structure: DELETE_QUERY with children: [RELATION, FILTER?]
        """
        print(f"\n[DELETE] Executing DELETE statement...")
        
        if len(query_tree.childs) < 1:
            raise ValueError("DELETE requires RELATION")
        
        relation = query_tree.childs[0]
        table_name = relation.val
        
        conditions = []
        if len(query_tree.childs) > 1 and query_tree.childs[1].type == "FILTER":
            filter_node = query_tree.childs[1]
            condition_str = self.condition_tree_to_string(filter_node.childs[1])
            logger.info(f"[DELETE] WHERE condition: {condition_str}")
            try:
                conditions = self.condition_tree_to_conditions(filter_node.childs[1])
            except ValueError as e:
                logger.info(f"[DELETE] Complex condition not supported for DELETE: {e}")
                return 0
        
        logger.info(f"[DELETE] -> STORAGE MANAGER: DELETE FROM '{table_name}'")
        if conditions:
            logger.info(f"[DELETE]    WHERE {len(conditions)} condition(s)")
        else:
            logger.info(f"[DELETE]    (All rows)")
        logger.info(f"[DELETE]    Transaction ID: {transaction_id}")
        
        # Validate WRITE access with CCM
        if self.ccm_adapter and transaction_id:
            response = self.ccm_adapter.validate_action(transaction_id, table_name, 'write')
            if not response.allowed:
                logger.info(f"[DELETE] CCM denied WRITE access: {response.message}")
                # Check if transaction was aborted (deadlock prevention: Wait-Die)
                msg_lower = response.message.lower()
                if "aborted" in msg_lower or "died" in msg_lower:
                    raise TransactionAbortedException(transaction_id, response.message)
                return 0
            # Log the object access
            self.ccm_adapter.log_object(transaction_id, table_name)
        
        try:
            # Read rows before deletion for FRM logging
            data_retrieval = DataRetrieval(
                table=table_name,
                column=[],
                conditions=conditions
            )
            old_rows = self.storage_manager.read_block(data_retrieval)
            
            # Log to FRM
            if self.frm_adapter and transaction_id:
                self.frm_adapter.log_write(
                    transaction_id=transaction_id,
                    query=f"DELETE FROM {table_name}",
                    table_name=table_name,
                    old_data=old_rows,  # For rollback
                    new_data=None  # No new data for DELETE
                )
            
            if transaction_id:
                # Buffer deletions
                for row in old_rows:
                    self.transaction_buffer.buffer_delete(
                        transaction_id=transaction_id,
                        table_name=table_name,
                        row_data=row,
                        conditions=conditions
                    )
                logger.info(f"[DELETE] Buffered {len(old_rows)} row(s) for deletion (will delete on COMMIT)")
                return len(old_rows)
            else:
                # No transaction - delete directly
                rows_affected = self.storage_adapter.delete_data(
                    table_name=table_name,
                    conditions=conditions,
                    transaction_id=transaction_id
                )
                logger.info(f"[DELETE] Deleted {rows_affected} row(s) (no transaction)")
                return rows_affected
        except Exception as e:
            logger.info(f"[DELETE] Error: {e}")
            return 0
    
    # TODO: implement transaction management with ccm
    def execute_transaction(self, query_tree: QueryTree, transaction_id: int) -> None:
        print(f"\n[TRANSACTION] Executing BEGIN TRANSACTION...")
        logger.info(f"[TRANSACTION] -> CCM: Start transaction")
        
        # Execute all statements in transaction
        for child in query_tree.childs:
            if child.type == "COMMIT":
                logger.info(f"[TRANSACTION] -> CCM: COMMIT transaction")
                break
            else:
                self.execute_node(child, transaction_id)
        
        return None
    
    # TODO: TEStING!!!! DAN INTEGRASIIN LEBIH BAIK
    def execute_create_table(self, query_tree: QueryTree, transaction_id: int) -> None:
        print(f"\n[CREATE TABLE] Executing CREATE TABLE statement...")
        
        if len(query_tree.childs) < 2:
            raise ValueError("CREATE TABLE requires table name and column definitions")
        
        table_name = query_tree.childs[0].val
        
        col_def_list = query_tree.childs[1]
        if col_def_list.type != "COLUMN_DEF_LIST":
            raise ValueError(f"Expected COLUMN_DEF_LIST, got {col_def_list.type}")
        
        logger.info(f"[CREATE TABLE] Table name: '{table_name}'")
        logger.info(f"[CREATE TABLE] Columns: {len(col_def_list.childs)}")
        
        columns = []
        primary_keys = []
        foreign_keys = []
        
        for col_def in col_def_list.childs:
            if col_def.type != "COLUMN_DEF":
                continue
            
            # Get column name (first child - IDENTIFIER)
            col_name = col_def.childs[0].val
            
            # Get data type (second child - DATA_TYPE)
            data_type_node = col_def.childs[1]
            data_type_str = data_type_node.val
            
            # Parse data type and size
            # Format: "INTEGER", "FLOAT", "VARCHAR(50)", "CHAR(10)"
            data_type = data_type_str.split('(')[0].upper()
            size = None
            if '(' in data_type_str:
                size_str = data_type_str.split('(')[1].rstrip(')')
                size = int(size_str)
            
            # Check for constraints (third child if exists)
            is_primary_key = False
            has_foreign_key = False
            
            if len(col_def.childs) > 2:
                constraint = col_def.childs[2]
                
                if constraint.type == "PRIMARY_KEY":
                    is_primary_key = True
                    primary_keys.append(col_name)
                    logger.info(f"[CREATE TABLE]   - {col_name} {data_type_str} PRIMARY KEY")
                
                elif constraint.type == "FOREIGN_KEY":
                    has_foreign_key = True
                    # FOREIGN_KEY contains: [REFERENCES(table), IDENTIFIER(column)]
                    ref_table = constraint.childs[0].val
                    ref_column = constraint.childs[1].val
                    
                    foreign_keys.append(ForeignKey(
                        column=col_name,
                        references_table=ref_table,
                        references_column=ref_column,
                        on_delete="RESTRICT",
                        on_update="RESTRICT"
                    ))
                    logger.info(f"[CREATE TABLE]   - {col_name} {data_type_str} FOREIGN KEY REFERENCES {ref_table}({ref_column})")
                else:
                    logger.info(f"[CREATE TABLE]   - {col_name} {data_type_str}")
            else:
                logger.info(f"[CREATE TABLE]   - {col_name} {data_type_str}")
            
            # Create ColumnDefinition
            columns.append(ColumnDefinition(
                name=col_name,
                data_type=data_type,
                size=size,
                is_primary_key=is_primary_key,
                is_nullable=not is_primary_key,
                default_value=None
            ))
        
        logger.info(f"[CREATE TABLE] -> STORAGE MANAGER: Create table '{table_name}' with {len(columns)} columns")
        if primary_keys:
            logger.info(f"[CREATE TABLE]    Primary keys: {primary_keys}")
        if foreign_keys:
            logger.info(f"[CREATE TABLE]    Foreign keys: {len(foreign_keys)}")
        logger.info(f"[CREATE TABLE]    Transaction ID: {transaction_id}")
        
        try:
            self.storage_adapter.create_table(
                table_name=table_name,
                columns=columns,
                primary_keys=primary_keys if primary_keys else None,
                foreign_keys=foreign_keys if foreign_keys else None
            )
            logger.info(f"[CREATE TABLE] ✓ Table '{table_name}' created successfully")
            return None
        except Exception as e:
            logger.info(f"[CREATE TABLE] Error: {e}")
            raise
    
    def execute_drop_table(self, query_tree: QueryTree, transaction_id: int) -> None:
        print(f"\n[DROP TABLE] Executing DROP TABLE statement...")
        
        if len(query_tree.childs) < 1:
            raise ValueError("DROP TABLE requires table name")
        
        # Get table name
        table_name = query_tree.childs[0].val
        
        # Get drop behavior (CASCADE/RESTRICT)
        behavior = query_tree.val if query_tree.val else "RESTRICT"
        
        logger.info(f"[DROP TABLE] Table name: '{table_name}'")
        logger.info(f"[DROP TABLE] Behavior: {behavior}")
        logger.info(f"[DROP TABLE] -> STORAGE MANAGER: Drop table '{table_name}'")
        logger.info(f"[DROP TABLE]    Transaction ID: {transaction_id}")
        
        # Note: Storage manager implements RESTRICT by default
        # CASCADE is not implemented in storage manager yet
        if behavior == "CASCADE":
            logger.info(f"[DROP TABLE] Warning: CASCADE not fully implemented in storage manager")
        
        # Use storage adapter for table drop
        try:
            self.storage_adapter.drop_table(table_name)
            logger.info(f"[DROP TABLE] ✓ Table '{table_name}' dropped successfully")
            return None
        except Exception as e:
            logger.info(f"[DROP TABLE] Error: {e}")
            raise
        
    def extract_column_name(self, col_ref: QueryTree) -> str:
        if col_ref.type != "COLUMN_REF":
            raise ValueError(f"Expected COLUMN_REF, got {col_ref.type}")
        
        column_name_node = col_ref.childs[0]  # COLUMN_NAME
        if column_name_node.type != "COLUMN_NAME":
            raise ValueError(f"Expected COLUMN_NAME, got {column_name_node.type}")
        
        identifier = column_name_node.childs[0]  # IDENTIFIER
        return identifier.val
    
    def extract_identifier(self, node: QueryTree) -> str:
        if node.type == "IDENTIFIER":
            return node.val
        elif node.type == "COLUMN_NAME" and node.childs:
            return node.childs[0].val
        return node.val
    
    def extract_literal_value(self, node: QueryTree):
        if node.type == "LITERAL_NUMBER":
            try:
                return int(node.val) if '.' not in node.val else float(node.val)
            except:
                return node.val
        elif node.type == "LITERAL_STRING":
            return node.val
        elif node.type == "LITERAL_BOOLEAN":
            return node.val.upper() == "TRUE"
        elif node.type == "LITERAL_NULL":
            return None
        return node.val

    # Evaluate value expression tree against a single row    
    def evaluate_value_expression(self, expr: QueryTree, row: dict):
        if expr.type.startswith("LITERAL_"):
            return self.extract_literal_value(expr)
        elif expr.type == "COLUMN_REF":
            col_name = self.extract_column_name(expr)
            return row.get(col_name)
        elif expr.type == "ARITH_EXPR":
            operator = expr.val
            left = self.evaluate_value_expression(expr.childs[0], row)
            right = self.evaluate_value_expression(expr.childs[1], row)
            
            if operator == "+":
                return left + right
            elif operator == "-":
                return left - right
            elif operator == "*":
                return left * right
            elif operator == "/":
                return left / right if right != 0 else None
            elif operator == "%":
                return left % right if right != 0 else None
        
        return None
    
    def evaluate_condition(self, condition: QueryTree, row: dict) -> bool:
        if condition.type == "COMPARISON":
            operator = condition.val
            left_val = self.evaluate_value_expression(condition.childs[0], row)
            right_val = self.evaluate_value_expression(condition.childs[1], row)
            
            if operator == "=":
                return left_val == right_val
            elif operator in ["<>", "!="]:
                return left_val != right_val
            elif operator == ">":
                return left_val > right_val
            elif operator == ">=":
                return left_val >= right_val
            elif operator == "<":
                return left_val < right_val
            elif operator == "<=":
                return left_val <= right_val
        
        elif condition.type == "OPERATOR":
            operator = condition.val
            
            if operator == "AND":
                return all(self.evaluate_condition(child, row) for child in condition.childs)
            elif operator == "OR":
                return any(self.evaluate_condition(child, row) for child in condition.childs)
            elif operator == "NOT":
                return not self.evaluate_condition(condition.childs[0], row)
        
        elif condition.type == "IS_NULL_EXPR":
            col_ref = condition.childs[0]
            col_name = self.extract_column_name(col_ref)
            return row.get(col_name) is None
        
        elif condition.type == "IS_NOT_NULL_EXPR":
            col_ref = condition.childs[0]
            col_name = self.extract_column_name(col_ref)
            return row.get(col_name) is not None
        
        elif condition.type == "IN_EXPR":
            # IN_EXPR has 2 children: [COLUMN_REF, LIST or subquery]
            col_ref = condition.childs[0]
            col_name = self.extract_column_name(col_ref)
            col_value = row.get(col_name)
            
            list_or_subquery = condition.childs[1]
            
            if list_or_subquery.type == "LIST":
                # IN with literal list: id IN (1, 2, 3)
                values = [self.extract_literal_value(child) for child in list_or_subquery.childs]
                return col_value in values
            else:
                # IN with subquery: id IN (SELECT ...)
                # Execute subquery and check if col_value is in results
                subquery_results = self.execute_node(list_or_subquery, None)
                if not subquery_results:
                    return False
                
                # Extract values from first column of subquery results
                first_col = list(subquery_results[0].keys())[0] if subquery_results[0] else None
                if first_col:
                    subquery_values = [r[first_col] for r in subquery_results]
                    return col_value in subquery_values
                return False
        
        elif condition.type == "NOT_IN_EXPR":
            # NOT IN - inverse of IN_EXPR
            col_ref = condition.childs[0]
            col_name = self.extract_column_name(col_ref)
            col_value = row.get(col_name)
            
            list_or_subquery = condition.childs[1]
            
            if list_or_subquery.type == "LIST":
                values = [self.extract_literal_value(child) for child in list_or_subquery.childs]
                return col_value not in values
            else:
                # NOT IN with subquery
                subquery_results = self.execute_node(list_or_subquery, None)
                if not subquery_results:
                    return True  # NOT IN empty set is true
                
                first_col = list(subquery_results[0].keys())[0] if subquery_results[0] else None
                if first_col:
                    subquery_values = [r[first_col] for r in subquery_results]
                    return col_value not in subquery_values
                return True
        
        elif condition.type == "EXISTS_EXPR":
            # EXISTS checks if subquery returns any rows
            # EXISTS_EXPR has 1 child: subquery
            subquery = condition.childs[0]
            subquery_results = self.execute_node(subquery, None)
            return len(subquery_results) > 0
        
        elif condition.type == "NOT_EXISTS_EXPR":
            # NOT EXISTS checks if subquery returns no rows
            subquery = condition.childs[0]
            subquery_results = self.execute_node(subquery, None)
            return len(subquery_results) == 0
        
        elif condition.type == "BETWEEN_EXPR":
            # BETWEEN_EXPR has 3 children: [COLUMN_REF, lower_bound, upper_bound]
            col_ref = condition.childs[0]
            col_name = self.extract_column_name(col_ref)
            col_value = row.get(col_name)
            
            lower_bound = self.evaluate_value_expression(condition.childs[1], row)
            upper_bound = self.evaluate_value_expression(condition.childs[2], row)
            
            return lower_bound <= col_value <= upper_bound
        
        elif condition.type == "NOT_BETWEEN_EXPR":
            col_ref = condition.childs[0]
            col_name = self.extract_column_name(col_ref)
            col_value = row.get(col_name)
            
            lower_bound = self.evaluate_value_expression(condition.childs[1], row)
            upper_bound = self.evaluate_value_expression(condition.childs[2], row)
            
            return not (lower_bound <= col_value <= upper_bound)
        
        return True
    
    # FOR DEBUGGING
    def condition_tree_to_string(self, condition: QueryTree) -> str:
        if condition.type == "COMPARISON":
            left = self.value_expr_to_string(condition.childs[0])
            right = self.value_expr_to_string(condition.childs[1])
            return f"{left} {condition.val} {right}"
        
        elif condition.type == "OPERATOR":
            operator = condition.val
            if operator == "NOT":
                return f"NOT ({self.condition_tree_to_string(condition.childs[0])})"
            else:
                parts = [self.condition_tree_to_string(child) for child in condition.childs]
                return f" {operator} ".join([f"({p})" for p in parts])
        
        elif condition.type == "IS_NULL_EXPR":
            col = self.extract_column_name(condition.childs[0])
            return f"{col} IS NULL"
        
        elif condition.type == "IS_NOT_NULL_EXPR":
            col = self.extract_column_name(condition.childs[0])
            return f"{col} IS NOT NULL"
        
        elif condition.type == "IN_EXPR":
            col = self.extract_column_name(condition.childs[0])
            list_or_subquery = condition.childs[1]
            if list_or_subquery.type == "LIST":
                values = [self.value_expr_to_string(v) for v in list_or_subquery.childs]
                return f"{col} IN ({', '.join(values)})"
            else:
                return f"{col} IN (subquery)"
        
        elif condition.type == "NOT_IN_EXPR":
            col = self.extract_column_name(condition.childs[0])
            list_or_subquery = condition.childs[1]
            if list_or_subquery.type == "LIST":
                values = [self.value_expr_to_string(v) for v in list_or_subquery.childs]
                return f"{col} NOT IN ({', '.join(values)})"
            else:
                return f"{col} NOT IN (subquery)"
        
        elif condition.type == "EXISTS_EXPR":
            return "EXISTS (subquery)"
        
        elif condition.type == "NOT_EXISTS_EXPR":
            return "NOT EXISTS (subquery)"
        
        elif condition.type == "BETWEEN_EXPR":
            col = self.extract_column_name(condition.childs[0])
            lower = self.value_expr_to_string(condition.childs[1])
            upper = self.value_expr_to_string(condition.childs[2])
            return f"{col} BETWEEN {lower} AND {upper}"
        
        elif condition.type == "NOT_BETWEEN_EXPR":
            col = self.extract_column_name(condition.childs[0])
            lower = self.value_expr_to_string(condition.childs[1])
            upper = self.value_expr_to_string(condition.childs[2])
            return f"{col} NOT BETWEEN {lower} AND {upper}"
        
        return "<condition>"
    
    def value_expr_to_string(self, expr: QueryTree) -> str:
        if expr.type.startswith("LITERAL_"):
            return str(expr.val)
        elif expr.type == "COLUMN_REF":
            return self.extract_column_name(expr)
        elif expr.type == "ARITH_EXPR":
            left = self.value_expr_to_string(expr.childs[0])
            right = self.value_expr_to_string(expr.childs[1])
            return f"({left} {expr.val} {right})"
        return "<expr>"
        
    def condition_tree_to_conditions(self, condition: QueryTree) -> list[Condition]:
        conditions = []
        
        if condition.type == "COMPARISON":
            col_name = self.extract_column_name(condition.childs[0])
            operator = condition.val
            operand_val = self.evaluate_value_expression(condition.childs[1], {})
            conditions.append(Condition(column=col_name, operation=operator, operand=operand_val))
        
        elif condition.type == "OPERATOR":
            if condition.val == "AND":
                for child in condition.childs:
                    conditions.extend(self.condition_tree_to_conditions(child))
            elif condition.val == "OR":
                raise ValueError(f"OR conditions cannot be represented as List[Condition]")
            elif condition.val == "NOT":
                raise ValueError(f"NOT conditions cannot be represented as List[Condition]")
        
        elif condition.type == "IS_NULL_EXPR":
            col_name = self.extract_column_name(condition.childs[0])
            conditions.append(Condition(column=col_name, operation="=", operand=None))
        
        elif condition.type == "IS_NOT_NULL_EXPR":
            col_name = self.extract_column_name(condition.childs[0])
            conditions.append(Condition(column=col_name, operation="<>", operand=None))
        
        elif condition.type in ["IN_EXPR", "NOT_IN_EXPR"]:
            # IN/NOT IN with list or subquery cannot be pushed down to storage manager
            raise ValueError(f"{condition.type} cannot be represented as List[Condition] - use in-memory filtering")
        
        elif condition.type in ["EXISTS_EXPR", "NOT_EXISTS_EXPR"]:
            # EXISTS/NOT EXISTS with subquery cannot be pushed down
            raise ValueError(f"{condition.type} requires subquery execution - use in-memory filtering")
        
        elif condition.type in ["BETWEEN_EXPR", "NOT_BETWEEN_EXPR"]:
            # BETWEEN requires range check, not supported in simple Condition format
            raise ValueError(f"{condition.type} requires range check - use in-memory filtering")
        
        # Other complex expressions are supported in-memory
        # but cannot be represented as simple Condition objects for storage manager pushdown
        
        return conditions
    
    def extract_table_name(self, node: QueryTree) -> str:
        if node.type == "RELATION":
            return node.val
        elif node.type == "ALIAS":
            if node.childs and node.childs[0].type == "RELATION":
                return node.childs[0].val
            return node.val
        elif node.type == "PROJECT":
            source = node.childs[-1]
            return self.extract_table_name(source)
        elif node.type == "FILTER":
            source = node.childs[0]
            return self.extract_table_name(source)
        elif node.type == "SORT":
            source = node.childs[-1]
            return self.extract_table_name(source)
        elif node.type == "LIMIT":
            source = node.childs[-1]
            return self.extract_table_name(source)
        elif node.type == "JOIN":
            left_table = self.extract_table_name(node.childs[0])
            right_table = self.extract_table_name(node.childs[1])
            return f"{left_table},{right_table}"
        else:
            raise ValueError(f"Cannot extract table name from node type {node.type}")
    
    def _apply_buffered_operations(self, storage_data: list[dict], 
                                    transaction_id: int, table_name: str) -> list[dict]:
        if not transaction_id:
            return storage_data
        
        # Start with storage data (committed baseline)
        result = list(storage_data)  # Copy to avoid modifying original
        
        # Get all buffered operations for this transaction
        ops = self.transaction_buffer.get_buffered_operations(transaction_id)
        
        for op in ops:
            if op.table_name != table_name:
                continue
            
            if op.operation_type == "INSERT":
                # Add new rows from buffered INSERTs
                result.append(op.data.copy())
            
            elif op.operation_type == "UPDATE":
                # Replace matching rows with updated data
                for i, row in enumerate(result):
                    if self._row_matches_data(row, op.old_data):
                        result[i] = op.data.copy()
            
            elif op.operation_type == "DELETE":
                # Remove matching rows from buffered DELETEs
                result = [
                    row for row in result 
                    if not self._row_matches_data(row, op.data)
                ]
        
        return result
    
    def _row_matches_data(self, row: dict, target_data: dict) -> bool:
        if not target_data:
            return False
        
        return all(row.get(k) == v for k, v in target_data.items())


def main():
    """Test query execution with sample queries"""
    print("="*70)
    print("Query Execution Engine - Test Mode")
    print("="*70)
    
    from adapter_optimizer import AdapterOptimizer
    
    executor = QueryExecution()
    adapter = AdapterOptimizer()
    
    # Test 1: Simple SELECT
    print("\n\n" + "="*70)
    print("TEST 1: Simple SELECT")
    print("="*70)
    query1 = "SELECT id, name FROM users WHERE age > 25;"
    parsed1 = adapter.parse_optimized_query(query1)
    result1 = executor.execute_node(parsed1.query_tree)
    print(f"\n[RESULT] {len(result1)} rows returned")
    for row in result1[:3]:  # Show first 3 rows
        print(f"  {row}")
    
    # Test 2: SELECT * 
    print("\n\n" + "="*70)
    print("TEST 2: SELECT * (all columns)")
    print("="*70)
    query2 = "SELECT * FROM users WHERE city = 'Jakarta';"
    parsed2 = adapter.parse_optimized_query(query2)
    result2 = executor.execute_node(parsed2.query_tree)
    print(f"\n[RESULT] {len(result2)} rows returned")
    for row in result2[:3]:
        print(f"  {row}")
    
    # Test 3: Complex WHERE with AND
    print("\n\n" + "="*70)
    print("TEST 3: Complex WHERE with AND")
    print("="*70)
    query3 = "SELECT name FROM users WHERE age > 25 AND city = 'Jakarta';"
    parsed3 = adapter.parse_optimized_query(query3)
    result3 = executor.execute_node(parsed3.query_tree)
    print(f"\n[RESULT] {len(result3)} rows returned")
    for row in result3:
        print(f"  {row}")
    
    # Test 4: UPDATE
    print("\n\n" + "="*70)
    print("TEST 4: UPDATE")
    print("="*70)
    query4 = "UPDATE users SET age = 30 WHERE id = 1;"
    parsed4 = adapter.parse_optimized_query(query4)
    executor.execute_node(parsed4.query_tree)
    
    # Test 5: INSERT
    print("\n\n" + "="*70)
    print("TEST 5: INSERT")
    print("="*70)
    query5 = "INSERT INTO users (id, name, age) VALUES (10, 'Eve', 27);"
    parsed5 = adapter.parse_optimized_query(query5)
    executor.execute_node(parsed5.query_tree)
    
    # Test 6: DELETE
    print("\n\n" + "="*70)
    print("TEST 6: DELETE")
    print("="*70)
    query6 = "DELETE FROM users WHERE id = 2;"
    parsed6 = adapter.parse_optimized_query(query6)
    executor.execute_node(parsed6.query_tree)
    
    print("\n\n" + "="*70)
    print("All tests completed!")
    print("="*70)

if __name__ == "__main__":
    main()