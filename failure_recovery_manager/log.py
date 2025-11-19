import json
from datetime import datetime
from typing import Dict

class actiontype(enumerate):
    start = 0
    write = 1
    commit = 2
    abort = 3

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
            f"timestamp:{self.timestamp}, "
            f"old_data:{self.old_data}, "
            f"new_data:{self.new_data}, "
            f"table_name:{self.table_name}, "
        )
    
    def display(self):
        data = {
            "transaction_id":self.transaction_id,
            "action":self.action,
            "timestamp":str(self.timestamp),
            "old_data":self.old_data,
            "new_data":self.new_data,
            "table_name":self.table_name
        }
        print(json.dumps(data, indent=4))
