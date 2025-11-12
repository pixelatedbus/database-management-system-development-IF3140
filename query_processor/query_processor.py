import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from misc import *
from query_optimizer import *

class QueryProcessor:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(QueryProcessor, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        pass

    def execute_query(self, query:str):
        try:
            self._get_query_tree(query)
        except Exception as e:
            print(e) # could be more sophisticated...
        
        # TODO: continue
        ...
    
    # ---------------------------- private methods ----------------------------
    def _get_query_tree(self, query:str):
        tok = Tokenizer(query)
        parser = Parser(tok)

        query_tree = parser.parse() # query tree is generated
        check_query(query_tree) # query tree validation

        # for testing purposes
        print(query_tree)
        print(query_tree.tree())

        return query_tree

    def _execute(self, query_tree:QueryTree):
        if len(query_tree.childs) == 0:
            print(query_tree.val) # placeholder. diganti dengan interaksi ke komponen lain untuk eksekusi instruksi (?)
            ... # perbarui row hasil eksekusi di node ybs. jadi attr query_tree (?)
            query_tree.parent.childs = [query_tree] # 1 child representasi row hasil eksekusi
        else:
            ... # placeholder. diganti dengan interaksi ke komponen lain untuk eksekusi instruksi (?)
            ... # perbarui row hasil eksekusi di node ybs. jadi attr query_tree (?)

# Unit Testing
def main():
    print("Hello, World")
    processor = QueryProcessor()
    processor.execute_query("SELECT * FROM users WHERE name='mifune';")

if __name__ == "__main__":
    main()