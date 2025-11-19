from collections import defaultdict

class HashIndex:

    def __init__(self, table_name : str, column_name : str):
        self.index = defaultdict(list)
        self.table_name = table_name
        self.column_name = column_name

    def insert(self, key: str, record_id: int):
        self.index[key].append(record_id)

    def search(self, key):
        return self.index.get(key, [])
    
    def save(self, filepath: str):
        #TODO: Save index to a file for persistance

        return NotImplementedError("TODO")
    
    def load(self, filepath: str):
        #TODO: load an index from a file for persistance

        return NotImplementedError("TODO")