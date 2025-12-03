from storage_manager.storage_manager import StorageManager
from storage_manager.models import (
    Condition, 
    DataRetrieval, 
    DataWrite, 
    DataDeletion,
    DataUpdate,
    ColumnDefinition,
    ForeignKey
)

# Re-export for query_execution to use
__all__ = [
    'AdapterStorage',
    'Condition',
    'DataRetrieval', 
    'DataWrite',
    'DataDeletion',
    'DataUpdate',
    'ColumnDefinition',
    'ForeignKey'
]

class AdapterStorage:
    """
    Adapter for Storage Manager operations.
    Provides a clean interface between query execution and storage layer.
    """
    
    def __init__(self, storage_manager=None, data_dir=None):
        """Initialize with either an existing storage manager or create a new one"""
        if storage_manager:
            self.sm = storage_manager
        else:
            self.sm = StorageManager(data_dir=data_dir) if data_dir else StorageManager()
    
    def read_data(self, table_name, columns=None, conditions=None, transaction_id=None):
        """
        Read data from storage with optional projection and filtering.
        
        Args:
            table_name: Name of the table to read from
            columns: List of column names to project (empty = all columns)
            conditions: List of Condition objects for filtering
            transaction_id: Optional transaction ID for concurrency control
            
        Returns:
            List of dictionaries representing rows
        """
        data_retrieval = DataRetrieval(
            table=table_name,
            column=columns or [],
            conditions=conditions or []
        )
        
        # Try to pass transaction_id if supported, otherwise call without it
        try:
            return self.sm.read_block(data_retrieval, transaction_id=transaction_id)
        except TypeError:
            # Fallback for storage managers that don't support transaction_id parameter
            return self.sm.read_block(data_retrieval)
    
    def write_data(self, table_name, columns, values, conditions=None, transaction_id=None):
        """
        Write data to storage (INSERT if conditions empty, UPDATE if conditions present).
        
        Args:
            table_name: Name of the table to write to
            columns: List of column names
            values: List of values corresponding to columns
            conditions: List of Condition objects for UPDATE (empty = INSERT)
            transaction_id: Optional transaction ID for concurrency control
            
        Returns:
            Number of rows affected
        """
        data_write = DataWrite(
            table=table_name,
            column=columns,
            new_value=values,
            conditions=conditions or []
        )
        
        # Try to pass transaction_id if supported, otherwise call without it
        try:
            return self.sm.write_block(data_write, transaction_id=transaction_id)
        except TypeError:
            # Fallback for storage managers that don't support transaction_id parameter
            return self.sm.write_block(data_write)
    
    def delete_data(self, table_name, conditions=None, transaction_id=None):
        """
        Delete data from storage.
        
        Args:
            table_name: Name of the table to delete from
            conditions: List of Condition objects for filtering (empty = delete all)
            transaction_id: Optional transaction ID for concurrency control
            
        Returns:
            List of deleted rows
        """
        data_deletion = DataDeletion(
            table=table_name,
            conditions=conditions or []
        )
        
        # Try to pass transaction_id if supported, otherwise call without it
        try:
            return self.sm.delete_block(data_deletion, transaction_id=transaction_id)
        except TypeError:
            # Fallback for storage managers that don't support transaction_id parameter
            return self.sm.delete_block(data_deletion)
    
    def create_table(self, table_name, columns, primary_keys=None, foreign_keys=None):
        """
        Create a new table in storage.
        
        Args:
            table_name: Name of the table to create
            columns: List of ColumnDefinition objects
            primary_keys: List of primary key column names
            foreign_keys: List of ForeignKey objects
        """
        return self.sm.create_table(
            table_name=table_name,
            columns=columns,
            primary_keys=primary_keys,
            foreign_keys=foreign_keys
        )
    
    def drop_table(self, table_name):
        """
        Drop a table from storage.
        
        Args:
            table_name: Name of the table to drop
        """
        return self.sm.drop_table(table_name)
    
    def batch_update_data(self, table_name, old_data_list, new_data_list, transaction_id=None):
        """
        Batch update data using old/new data matching (optimized for FRM/transactions).
        
        This method uses the storage manager's update_by_old_new_data which:
        1. Matches rows by PRIMARY KEY first (efficient)
        2. Falls back to exact match if no PK match
        3. Performs all updates in one operation
        
        Args:
            table_name: Name of the table to update
            old_data_list: List of dictionaries representing old row states
            new_data_list: List of dictionaries representing new row states
            transaction_id: Optional transaction ID for concurrency control
            
        Returns:
            Number of rows updated
            
        Example:
            adapter.batch_update_data(
                "users",
                [{"id": 1, "status": "inactive"}],
                [{"id": 1, "status": "active"}]
            )
        """
        data_update = DataUpdate(
            table=table_name,
            old_data=old_data_list,
            new_data=new_data_list
        )
        
        # Try to pass transaction_id if supported, otherwise call without it
        try:
            return self.sm.update_by_old_new_data(data_update, transaction_id=transaction_id)
        except TypeError:
            # Fallback for storage managers that don't support transaction_id parameter
            return self.sm.update_by_old_new_data(data_update)
    
    # Legacy methods for backward compatibility
    def storage_select(self, table_name, conds=None, projections=None):
        """Legacy method - use read_data instead"""
        return self.read_data(table_name, projections, conds)

    def storage_insert(self, table_name, row_dict):
        """Legacy method - use write_data instead"""
        cols = list(row_dict.keys())
        vals = list(row_dict.values())
        return self.write_data(table_name, cols, vals, conditions=[])

    def storage_update(self, table_name, set_dict, condition_tuple):
        """Legacy method - use write_data instead"""
        col, op, val = condition_tuple
        cond = Condition(col, op, val)
        return self.write_data(
            table_name,
            list(set_dict.keys()),
            list(set_dict.values()),
            conditions=[cond]
        )

    def storage_delete(self, table_name, condition_tuple):
        """Legacy method - use delete_data instead"""
        col, op, val = condition_tuple
        cond = Condition(col, op, val)
        return self.delete_data(table_name, conditions=[cond])

