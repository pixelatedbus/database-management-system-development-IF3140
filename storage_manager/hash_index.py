from collections import defaultdict
import pickle
import os

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
        #save index ke binary file agar persistence 
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'wb') as f:
            pickle.dump(self.index, f)
    
    def load(self, filepath: str):
        #Load index dari binary, jika tidak ada buat baru kosongan
        if not os.path.exists(filepath):
            self.index = defaultdict(list)
            return
        with open(filepath, 'rb') as f:
            self.index = pickle.load(f)