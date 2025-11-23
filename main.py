import sys
import os
import datetime
import threading
import time

# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from storage_manager import Rows, Condition
from query_processor.query_processor import QueryProcessor

class ClientThread(threading.Thread):
    def __init__(self, client_id, processor, scenario):
        super().__init__()
        self.client_id = client_id
        self.processor = processor
        self.scenario = scenario
        
    def run(self):
        print(f"\n--- Client {self.client_id} Dimulai ---")
        for query in self.scenario:
            print(f"[C{self.client_id}] Mengirim: {query}")
            result = self.processor.execute_query(query, self.client_id)
            status = "OK" if result.success else "ERROR"
            print(f"[C{self.client_id}] Status: {status} - {result.message}")
            if not result.success and "aborted" in result.message:
                break 
            time.sleep(0.1)
        print(f"--- Client {self.client_id} Selesai ---")

def main():
    
    processor = QueryProcessor()
    
    scenario_1 = [
        "BEGIN_TRANSACTION",
        "SELECT id, name FROM users WHERE name='mifune';",
        "COMMIT"
    ]
    
    scenario_2 = [
        "BEGIN_TRANSACTION",
        "UPDATE users SET name='Zelda' WHERE id=2;",
        "COMMIT"
    ]
    
    client_a = ClientThread(1, processor, scenario_1)
    client_b = ClientThread(2, processor, scenario_2)

    client_a.start()
    client_b.start()

    client_a.join()
    client_b.join()

if __name__ == "__main__":
    main()