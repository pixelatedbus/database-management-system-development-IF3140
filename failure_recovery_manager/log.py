import json
from datetime import datetime
from typing import Dict, Optional, List
from storage_manager.models import (
    Condition, 
    DataWrite,
    DataDeletion,
    DataUpdate
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
    
    def to_data_undo(self) -> List[DataWrite | DataDeletion | DataUpdate]:
        # anomaly
        if len(self.old_data) == 0 and len(self.new_data) == 0:
            return []
        # insertion
        elif len(self.old_data) == 0:
            return [DataDeletion(
                table = self.table_name,
                conditions = [
                    Condition(c, "=", row[c]) for c in row.keys()
                ]
            ) for row in self.new_data]
        # deletion
        elif len(self.new_data) == 0:
            return [DataWrite(
                table = self.table_name,
                column = row.keys(),
                conditions = [], # cause insert
                new_value = [row[c] for c in row.keys()]
            ) for row in self.old_data]
        # update
        else:
            return [DataUpdate(
                table = self.table_name,
                old_data = self.new_data,
                new_data = self.old_data
            )]
    
    def to_data_redo(self):
        # anomaly
        if len(self.old_data) == 0 and len(self.new_data) == 0:
            return []
        # insertion
        elif len(self.old_data) == 0:
            return [DataWrite(
                table = self.table_name,
                column = row.keys(),
                conditions = [], # cause insert
                new_value = [row[c] for c in row.keys()]
            ) for row in self.new_data]
        # deletion
        elif len(self.new_data) == 0:
            return [DataDeletion(
                table = self.table_name,
                conditions = [
                    Condition(c, "=", row[c]) for c in row.keys()
                ]
            ) for row in self.old_data]
        # update
        else:
            return [DataUpdate(
                table = self.table_name,
                old_data = self.old_data,
                new_data = self.new_data
            )]
