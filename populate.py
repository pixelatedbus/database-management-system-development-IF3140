from query_processor.query_processor import QueryProcessor

processor = QueryProcessor()

print("\n=== Populating test_users ===")
processor.execute_query("BEGIN TRANSACTION", client_id=0)
processor.execute_query("DROP TABLE test_users", client_id=0)
processor.execute_query("CREATE TABLE test_users (id INTEGER, name VARCHAR(50), age INTEGER)", client_id=0)
processor.execute_query("INSERT INTO test_users (id, name, age) VALUES (1, 'mifune', 25)", client_id=0)
processor.execute_query("INSERT INTO test_users (id, name, age) VALUES (2, 'link', 30)", client_id=0)
processor.execute_query("INSERT INTO test_users (id, name, age) VALUES (3, 'samus', 28)", client_id=0)
processor.execute_query("INSERT INTO test_users (id, name, age) VALUES (4, 'mario', 35)", client_id=0)
processor.execute_query("DROP TABLE test_products", client_id=0)
processor.execute_query("CREATE TABLE test_products (id INTEGER, product_name VARCHAR(50), price INTEGER)", client_id=0)
processor.execute_query("INSERT INTO test_products (id, product_name, price) VALUES (1, 'alkohol', 25)", client_id=0)
processor.execute_query("COMMIT", client_id=0)
print("=== test_users ready ===\n")

