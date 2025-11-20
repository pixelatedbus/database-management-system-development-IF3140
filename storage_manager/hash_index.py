from collections import defaultdict
import pickle
import os

class HashIndex:
    # hash index buat optimasi query equality (WHERE col = value)
    # nyimpen mapping dari value ke list record_id

    def __init__(self, table_name : str, column_name : str):
        # inisialisasi hash index
        self.index = defaultdict(list)
        self.table_name = table_name
        self.column_name = column_name

    def insert(self, key: str, record_id: int):
        # masukin key-value pair ke index
        # key bisa duplikat (multiple records dengan value yang sama)
        self.index[key].append(record_id)

    def search(self, key):
        # cari record_ids yang match dengan key
        # return empty list kalo ga ada
        return self.index.get(key, [])

    def save(self, filepath: str):
        # save index ke binary file pake pickle
        dir_path = os.path.dirname(filepath)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)
        with open(filepath, 'wb') as f:
            pickle.dump(self.index, f)

    def load(self, filepath: str):
        # load index dari binary file
        # kalo file ga exist, bikin index kosong
        if not os.path.exists(filepath):
            self.index = defaultdict(list)
            return
        with open(filepath, 'rb') as f:
            self.index = pickle.load(f)