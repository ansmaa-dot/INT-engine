import socket

def start_server(host='0.0.0.0', port=2333):
    # Create a TCP/IP socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        # Allow reusing the address to avoid "Address already in use" errors
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        # Bind the socket to the port
        server_socket.bind((host, port))
        
        # Listen for incoming connections (queue up to 5 connections)
        server_socket.listen(5)
        print(f"[*] Server listening on {host}:{port}...")

        while True:
            # Wait for a connection
            client_socket, client_address = server_socket.accept()
            print(f"[+] Connection accepted from {client_address[0]}:{client_address[1]}")
            
            with client_socket:
                while True:
                    # Receive data (up to 1024 bytes)
                    data = client_socket.recv(1024)
                    if not data:
                        break  # Client disconnected
                    
                    #message = data.decode('utf-8').strip()
                    print(f"[{client_address[0]}] Received: {data}")
                    
                    # Echo the message back to the client
                    #response = f"Echo: {message}\n"
                    #client_socket.sendall(response.encode('utf-8'))
                
                print(f"[-] Connection closed with {client_address[0]}:{client_address[1]}")

if __name__ == "__main__":
    try:
        start_server()
    except KeyboardInterrupt:
        print("\n[*] Server shutting down.")
