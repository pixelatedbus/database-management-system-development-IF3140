import socket
import json
import sys

HOST = '127.0.0.1'
PORT = 5433


def format_results(response):
    if not response.get('success'):
        return f"ERROR: {response.get('message', 'Unknown error')}"
    
    data = response.get('data', [])
    
    if data and len(data) > 0:
        columns = list(data[0].keys())
        
        col_widths = {}
        for col in columns:
            col_widths[col] = max(len(str(col)), max(len(str(row.get(col, ''))) for row in data))
        
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
        print("\nType your SQL queries (end with ';' to execute) or 'quit' to exit.\n")
        
        transaction_active = False
        query_buffer = []
        
        while True:
            try:
                if query_buffer:
                    if transaction_active:
                        prompt = "dbms*-> "
                    else:
                        prompt = "dbms-> "
                else:
                    if transaction_active:
                        prompt = "dbms*> "
                    else:
                        prompt = "dbms> "
                
                line = input(prompt).strip()
                
                if not line:
                    if not query_buffer:
                        continue
                    query_buffer.append("")
                    continue
                
                if not query_buffer and line.lower() in ['quit', 'exit', '\\q']:
                    print("\nGoodbye!")
                    return
                
                query_buffer.append(line)
                
                if line.endswith(';'):
                    query = ' '.join(query_buffer).strip()
                    query = query[:-1].strip()
                    query_buffer = []                    
                    query_upper = query.upper().strip()
                    if query_upper == "BEGIN TRANSACTION":
                        transaction_active = True
                    elif query_upper in ["COMMIT", "ABORT"]:
                        transaction_active = False
                    
                    # Send query to server
                    request = json.dumps({'query': query})
                    request_bytes = request.encode('utf-8')
                    client_socket.sendall(len(request_bytes).to_bytes(4, 'big') + request_bytes)
                    
                    length_bytes = client_socket.recv(4)
                    if not length_bytes:
                        print("Connection closed by server")
                        break
                    response_length = int.from_bytes(length_bytes, 'big')
                    
                    response_data = b''
                    while len(response_data) < response_length:
                        chunk = client_socket.recv(min(4096, response_length - len(response_data)))
                        if not chunk:
                            break
                        response_data += chunk
                    
                    response = json.loads(response_data.decode('utf-8'))
                    
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
