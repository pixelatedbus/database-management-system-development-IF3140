import sys
import os
import logging
import threading
import time

logging.basicConfig(
    level=logging.CRITICAL,  # Change to INFO for debug output
    format="[%(levelname)s] %(message)s"
)

from storage_manager import Rows
from query_processor.query_processor import QueryProcessor


def print_banner():
    """Print welcome banner"""
    print("=" * 70)
    print("  DATABASE MANAGEMENT SYSTEM - POSTGREZQL")
    print("  IF3140 - STEI ITB")
    print("=" * 70)
    print()


def print_help():
    """Print available commands"""
    print("\nAvailable commands:")
    print("  BEGIN TRANSACTION           - Start a new transaction")
    print("  COMMIT                      - Commit current transaction")
    print("  ABORT                       - Abort current transaction")
    print("  SELECT ...                  - Execute SELECT query")
    print("  INSERT INTO ...             - Execute INSERT query")
    print("  UPDATE ...                  - Execute UPDATE query")
    print("  DELETE FROM ...             - Execute DELETE query")
    print("  CREATE TABLE ...            - Create a new table")
    print("  DROP TABLE ...              - Drop a table")
    print("  \\h or help                  - Show this help message")
    print("  \\q or quit                  - Exit the client")
    print()


def format_results(result):
    if not result.success:
        return f"ERROR: {result.message}"
    
    if result.data and len(result.data.rows) > 0:
        rows = result.data.rows
        
        # Get column names from first row
        if rows:
            columns = list(rows[0].keys())
            
            # Calculate column widths
            col_widths = {}
            for col in columns:
                col_widths[col] = max(len(str(col)), max(len(str(row.get(col, ''))) for row in rows))
            
            # Print header
            header = " | ".join(str(col).ljust(col_widths[col]) for col in columns)
            separator = "-+-".join("-" * col_widths[col] for col in columns)
            
            output = [header, separator]
            
            # Print rows
            for row in rows:
                row_str = " | ".join(str(row.get(col, '')).ljust(col_widths[col]) for col in columns)
                output.append(row_str)
            
            output.append(f"\n({len(rows)} row{'s' if len(rows) != 1 else ''})")
            return "\n".join(output)
        else:
            return "Query returned no rows."
    
    return result.message

def run_transaction(qp, client_id, statements, sleep_time=5):
    print(f"[INFO] Transaction {client_id} scheduled. Sleeping {sleep_time} seconds before processing...")
    time.sleep(10)
    print(f"[INFO] Transaction {client_id} starting...")

    for query in statements:
        result = qp.execute_query(query, client_id)
        print(format_results(result))
        print()


def main():
    print_banner()
    print("Type 'help' or '\\h' for help, '\\q' to quit.\n")
    print("End your input with a semicolon (;).")
    
    # client id: 0 untuk single query, >=1 untuk transaction

    processor = QueryProcessor()
    client_id = 1  # Single client in CLI mode
    transaction_active = False

    buffer = "" # handling multiline

    # handling transaksi konkuren
    transaction_statements = []
    transaction_client_id = None
    
    while True:
        try:
            # Show prompt
            if transaction_active:
                prompt = "dbms*> "
            else:
                prompt = "dbms> "
            
            line = input(prompt).strip()
            buffer += " " + line

            if ";" not in buffer:
                continue
            
            # handling multi-statement
            statements = buffer.split(";")
            buffer = statements[-1].strip() # sisa
            statements = [s.strip() for s in statements[:-1] if s.strip()] # jaga-jaga

            for query in statements:
                # Skip empty lines
                if not query:
                    continue
            
                # Handle special commands
                if query.lower() in ['\\q', 'quit', 'exit']:
                    if transaction_active:
                        print("Warning: Transaction still active. Committing...")
                        result = processor.execute_query("COMMIT", client_id)
                    print("\nGoodbye!")
                    sys.exit()
            
                if query.lower() in ['\\h', 'help']:
                    print_help()
                    continue
            
                query_upper = query.upper().strip()
                # handling transaksi
                if query_upper == "BEGIN TRANSACTION":
                    if transaction_active:
                        print("[ERROR] Already inside a transaction...")
                        continue

                    result = processor.execute_query(query, client_id)
                    print(format_results(result), '\n')

                    if result.success:
                        transaction_active = True
                        transaction_statements = []
                        transaction_client_id = client_id
                        client_id += 1
                    
                    continue

                if transaction_active:
                    if query_upper not in ["COMMIT", "ABORT"]:
                        transaction_statements.append(query)
                        print(f"(queued) {query} \n") # sanity check, boleh dihapus
                        continue

                    if query_upper == "COMMIT":
                        print(f"[INFO] Spawning transaction thread for T{transaction_client_id}...\n")
                        
                        stmts_copy = transaction_statements.copy()
                        thread = threading.Thread(target=run_transaction, args=(processor, transaction_client_id, stmts_copy), daemon=True)
                        thread.start()
                    
                        transaction_active = False
                        transaction_statements = []
                        transaction_client_id = None
                        continue
                    
                    if query_upper == "ABORT":
                        result = processor.execute_query(query, transaction_client_id)
                        print(format_results(result), "\n")
                        transaction_active = False
                        transaction_statements = []
                        transaction_client_id = None
                        continue

                # Execute query (normal)
                result = processor.execute_query(query, 0) # untuk single query (non transaction)
                # Display results
                print(format_results(result))
                print()
            
        except KeyboardInterrupt:
            print("\n\nInterrupted. Use '\\q' to quit.")
            if transaction_active:
                print("Transaction still active. Use COMMIT or ABORT.")
        except EOFError:
            print("\n\nGoodbye!")
            break
        except Exception as e:
            print(f"Error: {e}")
            print()


if __name__ == "__main__":
    main()