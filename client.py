import threading
import time

class Client(threading.Thread):
    def __init__(self, client_id: int, query_processor, transaction_scenario: list):
        super().__init__()
        self.client_id = client_id
        self.qp = query_processor
        self.scenario = transaction_scenario

    def run(self):
        """Metode yang dijalankan saat thread dimulai."""
        print(f"--- Client {self.client_id} (Thread) Dimulai ---")
        
        for step, query in enumerate(self.scenario):
            print(f"[Client {self.client_id}] Langkah {step+1}: Mengirim query: {query}")
            
            result = self.qp.execute_query(query, self.client_id)
            
            # Asumsi: Query Processor mengembalikan ExecutionResult
            print(f"[Client {self.client_id}] Hasil: {result.message}")
            
            
        print(f"--- Client {self.client_id} (Thread) Selesai ---")

class ExecutionResult:
    def __init__(self, message: str, success: bool = True):
        self.message = message
        self.success = success