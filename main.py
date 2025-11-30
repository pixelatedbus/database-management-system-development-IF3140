import sys
import os
import logging

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


def main():
    print_banner()
    print("Type 'help' or '\\h' for help, '\\q' to quit.\n")
    
    processor = QueryProcessor()
    client_id = 1  # Single client in CLI mode
    transaction_active = False
    
    while True:
        try:
            # Show prompt
            if transaction_active:
                prompt = "dbms*> "
            else:
                prompt = "dbms> "
            
            # Get input
            query = input(prompt).strip()
            
            # Skip empty lines
            if not query:
                continue
            
            # Handle special commands
            if query.lower() in ['\\q', 'quit', 'exit']:
                if transaction_active:
                    print("Warning: Transaction still active. Committing...")
                    result = processor.execute_query("COMMIT", client_id)
                print("\nGoodbye!")
                break
            
            if query.lower() in ['\\h', 'help']:
                print_help()
                continue
            
            # Execute query
            result = processor.execute_query(query, client_id)
            
            # Update transaction state
            query_upper = query.upper().strip()
            if query_upper == "BEGIN TRANSACTION":
                if result.success:
                    transaction_active = True
            elif query_upper in ["COMMIT", "ABORT"]:
                if result.success or "aborted" in result.message.lower():
                    transaction_active = False
            
            # Check if transaction was aborted by system
            if not result.success and ("aborted" in result.message.lower() or "died" in result.message.lower()):
                transaction_active = False
            
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