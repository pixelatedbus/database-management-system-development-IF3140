import socket
import threading
import json
import logging

from query_processor.query_processor import QueryProcessor

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(message)s"
)

HOST = '127.0.0.1'
PORT = 5433

processor = QueryProcessor()
client_counter = 1
client_counter_lock = threading.Lock()


def handle_client(conn, addr, client_id):
    logging.info(f"Client {client_id} connected from {addr}")
    
    try:
        while True:
            length_bytes = conn.recv(4)
            if not length_bytes:
                break
            
            request_length = int.from_bytes(length_bytes, 'big')
            
            data = b''
            while len(data) < request_length:
                chunk = conn.recv(min(4096, request_length - len(data)))
                if not chunk:
                    break
                data += chunk
            
            if not data:
                break
            
            request_str = data.decode('utf-8')
            logging.info(f"Client {client_id} sent: {request_str[:100]}...")
            
            try:
                request = json.loads(request_str)
                query = request.get('query', '')
                
                if not query:
                    response = {
                        'success': False,
                        'message': 'No query provided'
                    }
                else:
                    result = processor.execute_query(query, client_id)
                    
                    response = {
                        'success': result.success,
                        'message': result.message,
                        'data': []
                    }
                    
                    if result.data and hasattr(result.data, 'rows'):
                        response['data'] = result.data.rows
                
            except json.JSONDecodeError:
                response = {
                    'success': False,
                    'message': 'Invalid JSON format'
                }
            except Exception as e:
                response = {
                    'success': False,
                    'message': f'Error: {str(e)}'
                }
            
            response_bytes = json.dumps(response).encode('utf-8')
            conn.sendall(len(response_bytes).to_bytes(4, 'big') + response_bytes)
    
    except Exception as e:
        logging.error(f"Error handling client {client_id}: {e}")
    finally:
        conn.close()
        logging.info(f"Client {client_id} disconnected")


def main():
    global client_counter
    
    print("=" * 70)
    print("  DATABASE SERVER - POSTGREZQL")
    print("  IF3140 - STEI ITB")
    print("=" * 70)
    print(f"\nStarting server on {HOST}:{PORT}...")
    
    # Create socket
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    try:
        server_socket.bind((HOST, PORT))
        server_socket.listen(5)
        print(f"Server listening on {HOST}:{PORT}")
        print("Waiting for connections...\n")
        
        while True:
            # Accept new connection
            conn, addr = server_socket.accept()
            
            # Assign client ID
            with client_counter_lock:
                current_client_id = client_counter
                client_counter += 1
            
            # Create thread for client
            client_thread = threading.Thread(
                target=handle_client,
                args=(conn, addr, current_client_id),
                daemon=True
            )
            client_thread.start()
    
    except KeyboardInterrupt:
        print("\n\nShutting down server...")
    except Exception as e:
        print(f"Server error: {e}")
    finally:
        server_socket.close()
        print("Server stopped.")


if __name__ == "__main__":
    main()
