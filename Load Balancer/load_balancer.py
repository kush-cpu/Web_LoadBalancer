import socket
import threading

class LoadBalancer:
    def __init__(self, backend_servers):
        self.backend_servers = backend_servers
        self.server_index = 0
        self.lock = threading.Lock()  # For thread safety when picking backend server
    
    def get_next_server(self):
        with self.lock:
            server = self.backend_servers[self.server_index]
            self.server_index = (self.server_index + 1) % len(self.backend_servers)
            return server
    
    def forward(self, client_socket, backend_server):
        try:
            backend_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            backend_socket.connect(backend_server)
            
            # Forward client request to backend server
            threading.Thread(target=self.forward_to_backend, args=(client_socket, backend_socket)).start()
            threading.Thread(target=self.forward_to_client, args=(backend_socket, client_socket)).start()
        except Exception as e:
            print(f"Error connecting to backend server {backend_server}: {e}")
    
    def forward_to_backend(self, client_socket, backend_socket):
        try:
            while True:
                data = client_socket.recv(4096)
                if not data:
                    break
                backend_socket.sendall(data)
        finally:
            client_socket.close()
            backend_socket.close()
    
    def forward_to_client(self, backend_socket, client_socket):
        try:
            while True:
                data = backend_socket.recv(4096)
                if not data:
                    break
                client_socket.sendall(data)
        finally:
            client_socket.close()
            backend_socket.close()
    
    def handle_client(self, client_socket):
        # Select the next server using round-robin
        backend_server = self.get_next_server()
        print(f"Forwarding request to {backend_server}")
        self.forward(client_socket, backend_server)

    def start(self, host='0.0.0.0', port=8080):
        # Start the load balancer server
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((host, port))
        server_socket.listen(5)
        print(f"Load Balancer listening on {host}:{port}")
        
        while True:
            client_socket, addr = server_socket.accept()
            print(f"Connection received from {addr}")
            threading.Thread(target=self.handle_client, args=(client_socket,)).start()


if __name__ == "__main__":
    # List of backend servers as (IP, Port) tuples
    backend_servers = [
        ('127.0.0.1', 8081),
        ('127.0.0.1', 8082),
        ('127.0.0.1', 8083),
    ]
    
    load_balancer = LoadBalancer(backend_servers)
    load_balancer.start(port=8080)
