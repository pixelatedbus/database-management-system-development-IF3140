import socket
import json
import sys

HOST = '127.0.0.1'
PORT = 5433


def format_results(response):
    """Format query results for display"""
    if not response.get('success'):
        return f"ERROR: {response.get('message', 'Unknown error')}"
    
    data = response.get('data', [])
    
    if data and len(data) > 0:
        # Get column names
        columns = list(data[0].keys())
        
        # Calculate column widths
        col_widths = {}
        for col in columns:
            col_widths[col] = max(len(str(col)), max(len(str(row.get(col, ''))) for row in data))
        
        # Print header
        header = " | ".join(str(col).ljust(col_widths[col]) for col in columns)
        separator = "-+-".join("-" * col_widths[col] for col in columns)
        
        output = [header, separator]
        
        # Print rows
        for row in data:
            row_str = " | ".join(str(row.get(col, '')).ljust(col_widths[col]) for col in columns)
            output.append(row_str)
        
        output.append(f"\n({len(data)} row{'s' if len(data) != 1 else ''})")
        return "\n".join(output)
    
    return response.get('message', 'Query executed successfully')


def main():
    """Main client function"""
    print("=" * 70)
    print("  DATABASE CLIENT - POSTGREZQL")
    print("  IF3140 - STEI ITB")
    print("=" * 70)
    print(f"\nConnecting to {HOST}:{PORT}...")
    
    try:
        # Connect to server
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((HOST, PORT))
        print("Connected to database server!")
        print("\nType your SQL queries or 'quit' to exit.\n")
        
        transaction_active = False
        
        while True:
            try:
                # Show prompt based on transaction state
                if transaction_active:
                    prompt = "dbms*> "
                else:
                    prompt = "dbms> "
                
                # Get user input
                query = input(prompt).strip()
                
                if not query:
                    continue
                
                # Check for quit command
                if query.lower() in ['quit', 'exit', '\\q']:
                    print("\nGoodbye!")
                    return
                
                # Remove trailing semicolon if present
                if query.endswith(';'):
                    query = query[:-1].strip()
                
                # Track transaction state
                query_upper = query.upper().strip()
                if query_upper == "BEGIN TRANSACTION":
                    transaction_active = True
                elif query_upper in ["COMMIT", "ABORT"]:
                    transaction_active = False
                
                # Send query to server immediately
                request = json.dumps({'query': query})
                # Send with length prefix
                request_bytes = request.encode('utf-8')
                client_socket.sendall(len(request_bytes).to_bytes(4, 'big') + request_bytes)
                
                # Receive response with length prefix
                length_bytes = client_socket.recv(4)
                if not length_bytes:
                    print("Connection closed by server")
                    break
                response_length = int.from_bytes(length_bytes, 'big')
                
                # Receive full response
                response_data = b''
                while len(response_data) < response_length:
                    chunk = client_socket.recv(min(4096, response_length - len(response_data)))
                    if not chunk:
                        break
                    response_data += chunk
                
                response = json.loads(response_data.decode('utf-8'))
                
                # Display results
                print(format_results(response))
                print()
            
            except KeyboardInterrupt:
                print("\n\nInterrupted. Type 'quit' to exit.")
            except EOFError:
                print("\n\nGoodbye!")
                break
    
    except ConnectionRefusedError:
        print(f"ERROR: Could not connect to server at {HOST}:{PORT}")
        print("Make sure the server is running.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        try:
            client_socket.close()
        except:
            pass


if __name__ == "__main__":
    main()
