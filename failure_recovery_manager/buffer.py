import sys
from copy import deepcopy

class table:
        class table_type(enumerate):
                data = 1
        
        def __init__(self, name: str, data: list[dict], type: table_type = table_type.data):
                self.name : str = name
                self.data : list[dict] = data
                self.type : table.table_type = type
        

class buffer:
        # Max buffer size is 128 MB
        def __init__(self, max_size = 128 * 1024 * 1024):
                self.tables : list[table] = []
                self.current_size = 0
                self.max_size_bytes = max_size
