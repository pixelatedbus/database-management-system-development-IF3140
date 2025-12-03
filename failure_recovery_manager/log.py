import json
from datetime import datetime
from typing import Dict, Optional, List
from storage_manager.models import (
    Condition, 
    DataWrite 
)

class actiontype(enumerate):
    start = 0
    write = 1
    commit = 2
    abort = 3
    checkpoint = 4

class log:
    def __init__(
            self, 
            transaction_id: str | int, 
            action: actiontype, 
            timestamp: datetime, 
            old_data: Dict = None, 
            new_data: Dict = None, 
            table_name : str = None
        ) -> None:
        self.transaction_id = transaction_id
        self.action = action
        self.timestamp = timestamp
        self.old_data = old_data
        self.new_data = new_data
        self.table_name = table_name
    
    def __str__(self):
        return(
            f"transaction_id:{self.transaction_id}, "
            f"action:{self.action}, "
            f"timestamp:{self.timestamp.strftime('%Y-%m-%d_%H-%M-%S')}, "
            f"old_data:{self.old_data}, "
            f"new_data:{self.new_data}, "
            f"table_name:{self.table_name}, "
        )
    
    def display(self):
        data = {
            "transaction_id":self.transaction_id,
            "action":self.action,
            "timestamp":str(self.timestamp.strftime('%Y-%m-%d_%H-%M-%S')),
            "old_data":self.old_data,
            "new_data":self.new_data,
            "table_name":self.table_name
        }
        print(json.dumps(data, indent=4))

    def to_datawrite_undo(self, pks: Optional[List[str]] = None) -> DataWrite:
        if self.table_name is None:
            raise ValueError("No table_name in log")

        if self.old_data is None:
            raise ValueError("no old_data in log")

        columns = list(self.old_data.keys())
        values = [self.old_data[c] for c in columns]

        conditions: List[Condition] = []

        # might be wrong
        if pks:
            for col in pks:
                src_dict = self.new_data or self.old_data
                if src_dict is None or col not in src_dict:
                    raise ValueError(f"primary key {col} not in data")
                conditions.append(Condition(col, "=", src_dict[col]))

        return DataWrite(
            table=self.table_name,
            column=columns,
            new_value=values,
            conditions=conditions,
        )
    
    def to_datawrite_redo(self, pks: Optional[List[str]] = None) -> DataWrite:
        if self.table_name is None:
            raise ValueError("No table_name in log")

        if self.new_data is None:
            raise ValueError("no new_data in log")

        columns = list(self.new_data.keys())
        values = [self.new_data[c] for c in columns]

        conditions: List[Condition] = []

        # might be wrong
        if pks:
            for col in pks:
                src_dict = self.new_data or self.new_data
                if src_dict is None or col not in src_dict:
                    raise ValueError(f"primary key {col} not in data")
                conditions.append(Condition(col, "=", src_dict[col]))

        return DataWrite(
            table=self.table_name,
            column=columns,
            new_value=values,
            conditions=conditions,
        )
