import sys
import os
import threading
import time
import logging
from concurrency_control_manager.src.enums import AlgorithmType


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



def run_test_with_algorithm(algorithm_type: AlgorithmType):
    """Run deadlock test scenario with specified algorithm"""
    print("\n" + "=" * 80)
    print(f"TESTING ALGORITHM: {algorithm_type.value}")
    print("=" * 80)
    
    processor = QueryProcessor()
    processor.adapter_ccm.set_algorithm(algorithm=algorithm_type)
    
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
    
    print(f"\n{'=' * 80}")
    print(f"COMPLETED: {algorithm_type.value}")
    print(f"{'=' * 80}\n")
    
    # Wait a bit before next test
    time.sleep(2)

def main():
    print("\n" + "#" * 80)
    print("CONCURRENCY CONTROL MANAGER - ALGORITHM TESTING")
    print("Testing all algorithms with deadlock scenario")
    print("#" * 80)
    
    # Test all algorithms
    algorithms = [
        AlgorithmType.LockBased,
        AlgorithmType.TimestampBased,
        AlgorithmType.ValidationBased,
        AlgorithmType.MVCC
    ]
    
    for algo in algorithms:
        try:
            run_test_with_algorithm(algo)
        except Exception as e:
            print(f"\n[ERROR] Failed to test {algo.value}: {e}\n")
    
    print("\n" + "#" * 80)
    print("ALL TESTS COMPLETED")
    print("#" * 80)


if __name__ == "__main__":
    main()
