import sys
import os
import threading
import time
import logging

logging.basicConfig(
    level=logging.CRITICAL, # ubah jadi critical kalo mau ga muncul debug, jadi info kalo mau muncul
    format="[%(levelname)s] %(message)s"
)

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

            time.sleep(0.5) 
            
            print(f"[C{self.client_id}] Mengirim: {query}")
            result = self.processor.execute_query(query, self.client_id)
            status = "OK" if result.success else "ERROR"
            print(f"[C{self.client_id}] Status: {status} - {result.message}")
            
            if not result.success and "aborted" in result.message.lower():
                print(f"[C{self.client_id}] Transaksi Dibatalkan (Rollback) oleh Sistem.")
                break 
                
        print(f"--- Client {self.client_id} Selesai ---")



def main():
    
    processor = QueryProcessor()

    scenario_deadlock_1 = [
        "BEGIN TRANSACTION",
        "UPDATE students SET age = 30 WHERE id = 1", 
        "UPDATE courses SET credits = 4 WHERE id = 1",
        "COMMIT"
    ]
    
    scenario_deadlock_2 = [
        "BEGIN TRANSACTION",
        "UPDATE courses SET credits = 3 WHERE id = 1",
        "UPDATE students SET age = 35 WHERE id = 1",
        "COMMIT"
    ]
    
    client_a = ClientThread(1, processor, scenario_deadlock_1)
    client_b = ClientThread(2, processor, scenario_deadlock_2)

    client_a.start()
    time.sleep(0.5)  # biar client_a dapat kunci dulu
    client_b.start()

    client_a.join()
    client_b.join()

if __name__ == "__main__":
    main()
