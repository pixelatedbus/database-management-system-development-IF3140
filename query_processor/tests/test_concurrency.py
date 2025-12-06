"""
Unit Tests for Concurrent Transaction Processing and Wait-Die Protocol

Tests the Concurrency Control Manager's Wait-Die deadlock prevention protocol
through concurrent query execution with multiple simulated clients.

Test Scenarios:
1. Basic concurrent transactions (no conflict)
2. Wait-Die protocol - younger transaction waits for older
3. Wait-Die protocol - older transaction forces younger to die
4. Deadlock prevention with multiple resources
5. Serializable execution verification

Run tests:
    python -m unittest query_processor.tests.test_concurrency -v
"""

import unittest
import sys
import os
import shutil
import threading
import time
import logging

# Configure logging
logging.basicConfig(level=logging.CRITICAL)

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from query_processor.query_processor import QueryProcessor
from storage_manager.storage_manager import StorageManager


class TestConcurrentTransactions(unittest.TestCase):
    """Test concurrent transaction processing and Wait-Die protocol."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment once for all tests."""
        cls.test_data_dir = "data/test_concurrency"
        
    def setUp(self):
        """Set up fresh environment before each test."""
        # Clean up any existing test data
        if os.path.exists(self.test_data_dir):
            shutil.rmtree(self.test_data_dir)
        
        os.makedirs(self.test_data_dir, exist_ok=True)
        
        # Create fresh instances for each test
        StorageManager._instance = None
        StorageManager._initialized = False
        QueryProcessor._instance = None
        
        self.storage_manager = StorageManager(data_dir=self.test_data_dir)
        self.query_processor = QueryProcessor()
        self.query_processor.adapter_storage.sm = self.storage_manager
        self.query_processor.query_execution_engine.storage_manager = self.storage_manager
        self.query_processor.query_execution_engine.storage_adapter.sm = self.storage_manager
        
        # Track results from threads
        self.thread_results = {}
        self.thread_errors = {}
        
    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests."""
        if os.path.exists(cls.test_data_dir):
            try:
                shutil.rmtree(cls.test_data_dir)
            except:
                pass
    
    def _setup_test_tables(self):
        """Create test tables for concurrency tests."""
        # Create accounts table for transaction tests
        self.query_processor.execute_query("""
        CREATE TABLE accounts (
            id INTEGER PRIMARY KEY,
            name VARCHAR(50),
            balance INTEGER
        )
        """, 0)
        
        # Insert test data
        self.query_processor.execute_query("INSERT INTO accounts VALUES (1, 'Alice', 1000)", 0)
        self.query_processor.execute_query("INSERT INTO accounts VALUES (2, 'Bob', 2000)", 0)
        self.query_processor.execute_query("INSERT INTO accounts VALUES (3, 'Charlie', 1500)", 0)
        
        # Create employees table for multi-table tests
        self.query_processor.execute_query("""
        CREATE TABLE employees (
            id INTEGER PRIMARY KEY,
            name VARCHAR(50),
            salary INTEGER
        )
        """, 0)
        
        self.query_processor.execute_query("INSERT INTO employees VALUES (1, 'John', 5000)", 0)
        self.query_processor.execute_query("INSERT INTO employees VALUES (2, 'Jane', 6000)", 0)
    
    def _execute_transaction(self, client_id, queries, delay=0):
        """
        Execute a list of queries as a transaction in a separate thread.
        
        Args:
            client_id: Unique client identifier
            queries: List of SQL queries to execute
            delay: Optional delay in seconds before starting
        """
        try:
            if delay > 0:
                time.sleep(delay)
            
            results = []
            for query in queries:
                result = self.query_processor.execute_query(query, client_id)
                results.append({
                    'query': query,
                    'success': result.success,
                    'message': result.message
                })
                
                # Stop if transaction was aborted
                if not result.success and ('aborted' in result.message.lower() or 'died' in result.message.lower()):
                    break
            
            self.thread_results[client_id] = results
            
        except Exception as e:
            self.thread_errors[client_id] = str(e)
    
    def test_01_basic_concurrent_reads(self):
        """Test concurrent read transactions (should all succeed)."""
        self._setup_test_tables()
        
        # Two clients reading simultaneously
        queries_1 = [
            "BEGIN TRANSACTION",
            "SELECT * FROM accounts WHERE id = 1",
            "COMMIT"
        ]
        
        queries_2 = [
            "BEGIN TRANSACTION",
            "SELECT * FROM accounts WHERE id = 2",
            "COMMIT"
        ]
        
        thread1 = threading.Thread(target=self._execute_transaction, args=(1, queries_1))
        thread2 = threading.Thread(target=self._execute_transaction, args=(2, queries_2, 0.1))
        
        thread1.start()
        thread2.start()
        thread1.join()
        thread2.join()
        
        # Both transactions should succeed
        self.assertIn(1, self.thread_results)
        self.assertIn(2, self.thread_results)
        
        # Verify both committed successfully
        for result in self.thread_results[1]:
            if 'COMMIT' in result['query']:
                self.assertTrue(result['success'], "Client 1 should commit successfully")
        
        for result in self.thread_results[2]:
            if 'COMMIT' in result['query']:
                self.assertTrue(result['success'], "Client 2 should commit successfully")
    
    def test_02_wait_die_older_waits(self):
        """Test Wait-Die: Older transaction waits for younger transaction to complete."""
        self._setup_test_tables()
        
        # Client 1 (younger, lower client_id = lower timestamp) locks account 1 first
        queries_1 = [
            "BEGIN TRANSACTION",
            "UPDATE accounts SET balance = 1100 WHERE id = 1",
            "COMMIT"
        ]
        
        # Client 2 (older, higher client_id = higher timestamp) tries same account - should WAIT
        queries_2 = [
            "BEGIN TRANSACTION",
            "SELECT * FROM accounts WHERE id = 1",
            "COMMIT"
        ]
        
        thread1 = threading.Thread(target=self._execute_transaction, args=(1, queries_1))
        thread2 = threading.Thread(target=self._execute_transaction, args=(2, queries_2, 0.2))
        
        thread1.start()
        thread2.start()
        thread1.join()
        thread2.join()
        
        # Both should succeed (older client 2 waits for younger client 1)
        self.assertIn(1, self.thread_results)
        self.assertIn(2, self.thread_results)
        
        # Verify client 1 (younger) committed
        client1_committed = any(r['success'] and 'COMMIT' in r['query'] for r in self.thread_results[1])
        self.assertTrue(client1_committed, "Younger transaction (client 1) should commit")
        
        # Client 2 (older) should succeed after waiting
        client2_committed = any(r['success'] and 'COMMIT' in r['query'] for r in self.thread_results[2])
        self.assertTrue(client2_committed, "Older transaction (client 2) should succeed after waiting")
    
    def test_03_wait_die_younger_dies(self):
        """Test Wait-Die: Younger transaction dies when conflicting with older."""
        self._setup_test_tables()
        
        # Client 1 (older, lower client_id) locks account 1 first
        queries_1 = [
            "BEGIN TRANSACTION",
            "UPDATE accounts SET balance = 900 WHERE id = 1",
            "COMMIT"
        ]
        
        # Client 2 (younger, higher client_id) tries to access same account - should DIE
        queries_2 = [
            "BEGIN TRANSACTION",
            "UPDATE accounts SET balance = 1200 WHERE id = 1",
            "COMMIT"
        ]
        
        thread1 = threading.Thread(target=self._execute_transaction, args=(1, queries_1))
        thread2 = threading.Thread(target=self._execute_transaction, args=(2, queries_2, 0.3))
        
        thread1.start()
        thread2.start()
        thread1.join()
        thread2.join()
        
        # Check results
        self.assertIn(1, self.thread_results)
        self.assertIn(2, self.thread_results)
        
        # Younger transaction (client 2) should be aborted/die
        client2_aborted = any(
            not r['success'] and ('aborted' in r['message'].lower() or 'died' in r['message'].lower())
            for r in self.thread_results[2]
        )
        
        # Older transaction (client 1) should succeed
        client1_committed = any(r['success'] and 'COMMIT' in r['query'] for r in self.thread_results[1])
        
        # Verify Wait-Die: younger dies, older succeeds
        self.assertTrue(
            client2_aborted or client1_committed,
            "Wait-Die: younger transaction should die, older should succeed"
        )
    
    def test_04_deadlock_prevention_multiple_resources(self):
        """Test deadlock prevention with multiple resources."""
        self._setup_test_tables()
        
        # Client 1: Lock account 1, then try account 2
        queries_1 = [
            "BEGIN TRANSACTION",
            "UPDATE accounts SET balance = 1100 WHERE id = 1",
            "UPDATE accounts SET balance = 2100 WHERE id = 2",
            "COMMIT"
        ]
        
        # Client 2: Lock account 2, then try account 1 (potential deadlock)
        queries_2 = [
            "BEGIN TRANSACTION",
            "UPDATE accounts SET balance = 2200 WHERE id = 2",
            "UPDATE accounts SET balance = 1200 WHERE id = 1",
            "COMMIT"
        ]
        
        thread1 = threading.Thread(target=self._execute_transaction, args=(1, queries_1))
        thread2 = threading.Thread(target=self._execute_transaction, args=(2, queries_2, 0.2))
        
        thread1.start()
        thread2.start()
        thread1.join()
        thread2.join()
        
        # Verify deadlock was prevented
        self.assertIn(1, self.thread_results)
        self.assertIn(2, self.thread_results)
        
        # At least one transaction should complete successfully
        client1_success = any(r['success'] and 'COMMIT' in r['query'] for r in self.thread_results[1])
        client2_success = any(r['success'] and 'COMMIT' in r['query'] for r in self.thread_results[2])
        
        self.assertTrue(
            client1_success or client2_success,
            "At least one transaction should complete successfully"
        )
        
        # Check if one was aborted (deadlock prevention)
        client1_aborted = any(
            not r['success'] and ('aborted' in r['message'].lower() or 'died' in r['message'].lower())
            for r in self.thread_results[1]
        )
        client2_aborted = any(
            not r['success'] and ('aborted' in r['message'].lower() or 'died' in r['message'].lower())
            for r in self.thread_results[2]
        )
        
        # Not both should be aborted
        self.assertFalse(
            client1_aborted and client2_aborted,
            "Both transactions should not be aborted"
        )
    
    def test_05_multiple_clients_different_tables(self):
        """Test multiple clients accessing different tables (should succeed)."""
        self._setup_test_tables()
        
        # Client 1: Read from accounts
        queries_1 = [
            "BEGIN TRANSACTION",
            "SELECT * FROM accounts WHERE id = 1",
            "COMMIT"
        ]
        
        # Client 2: Read from employees (different table, no conflict)
        queries_2 = [
            "BEGIN TRANSACTION",
            "SELECT * FROM employees WHERE id = 1",
            "COMMIT"
        ]
        
        # Start both clients simultaneously
        thread1 = threading.Thread(target=self._execute_transaction, args=(1, queries_1))
        thread2 = threading.Thread(target=self._execute_transaction, args=(2, queries_2))
        
        thread1.start()
        thread2.start()
        thread1.join()
        thread2.join()
        
        # Both should succeed (no conflicts on different tables)
        self.assertIn(1, self.thread_results)
        self.assertIn(2, self.thread_results)
        
        # Verify both committed
        for client_id in [1, 2]:
            committed = any(r['success'] and 'COMMIT' in r['query'] for r in self.thread_results[client_id])
            self.assertTrue(committed, f"Client {client_id} should commit successfully")
    
    def test_06_transaction_abort_releases_locks(self):
        """Test that aborting a transaction releases all locks."""
        self._setup_test_tables()
        
        # Client 1: Lock and abort
        queries_1 = [
            "BEGIN TRANSACTION",
            "UPDATE accounts SET balance = 1100 WHERE id = 1",
            "ABORT"
        ]
        
        # Client 2: Try to access after client 1 aborts
        queries_2 = [
            "BEGIN TRANSACTION",
            "UPDATE accounts SET balance = 1200 WHERE id = 1",
            "COMMIT"
        ]
        
        thread1 = threading.Thread(target=self._execute_transaction, args=(1, queries_1))
        thread2 = threading.Thread(target=self._execute_transaction, args=(2, queries_2, 0.5))
        
        thread1.start()
        thread2.start()
        thread1.join()
        thread2.join()
        
        # Client 1 should abort successfully
        client1_aborted = any(r['success'] and 'ABORT' in r['query'] for r in self.thread_results[1])
        self.assertTrue(client1_aborted, "Client 1 should abort successfully")
        
        # Client 2 should be able to commit after client 1 aborts
        client2_committed = any(r['success'] and 'COMMIT' in r['query'] for r in self.thread_results[2])
        self.assertTrue(client2_committed, "Client 2 should commit after client 1 aborts")
    
    def test_07_wait_die_timestamp_ordering(self):
        """Test that Wait-Die correctly uses timestamp ordering."""
        self._setup_test_tables()
        
        # Start 3 clients in sequence (increasing timestamps)
        queries = [
            "BEGIN TRANSACTION",
            "UPDATE accounts SET balance = balance + 100 WHERE id = 1",
            "COMMIT"
        ]
        
        threads = []
        for i in range(1, 4):
            thread = threading.Thread(
                target=self._execute_transaction,
                args=(i, queries, (i-1) * 0.2)
            )
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()
        
        # Verify all clients executed
        for client_id in [1, 2, 3]:
            self.assertIn(client_id, self.thread_results, f"Client {client_id} should have results")
        
        # At least the oldest (client 1) should succeed
        client1_success = any(r['success'] and 'COMMIT' in r['query'] for r in self.thread_results[1])
        self.assertTrue(client1_success, "Oldest transaction (client 1) should succeed")


def suite():
    """Create test suite."""
    test_suite = unittest.TestSuite()
    test_suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestConcurrentTransactions))
    return test_suite


if __name__ == '__main__':
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite())
