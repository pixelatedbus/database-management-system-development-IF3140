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

def populate_test_users(processor):
    print("\n=== Populating test_users ===")
    processor.execute_query("BEGIN TRANSACTION", client_id=0)
    processor.execute_query("DROP TABLE test_users", client_id=0)
    processor.execute_query("CREATE TABLE test_users (id INTEGER, name VARCHAR(50), age INTEGER)", client_id=0)
    processor.execute_query("INSERT INTO test_users (id, name, age) VALUES (1, 'mifune', 25)", client_id=0)
    processor.execute_query("INSERT INTO test_users (id, name, age) VALUES (2, 'link', 30)", client_id=0)
    processor.execute_query("INSERT INTO test_users (id, name, age) VALUES (3, 'samus', 28)", client_id=0)
    processor.execute_query("INSERT INTO test_users (id, name, age) VALUES (4, 'mario', 35)", client_id=0)
    processor.execute_query("COMMIT", client_id=0)
    print("=== test_users ready ===\n")

def main():
    
    processor = QueryProcessor()
    populate_test_users(processor)
    
    scenario_1 = [
        "BEGIN TRANSACTION",
        "SELECT * FROM test_users WHERE id < 3",
        "COMMIT"
    ]
    
    scenario_2 = [
        "BEGIN TRANSACTION",
        "UPDATE test_users SET age=31 WHERE id=2",
        "COMMIT"
    ]
    
    client_a = ClientThread(1, processor, scenario_1)
    client_b = ClientThread(2, processor, scenario_2)

    client_a.start()
    time.sleep(0.5)  # Let client_a acquire lock first
    client_b.start()

    client_a.join()
    client_b.join()

if __name__ == "__main__":
    main()