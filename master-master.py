import socket
import threading
import json
import time

class MasterCoordinator:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.neighbors = []  # lista de masters vizinhos
        self.connections = []  # conexões ativas (sockets)
        self.lock = threading.Lock()

        # socket servidor
        self.socket_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket_server.bind((self.host, self.port))
        self.socket_server.listen(5)

        print(f"[Coordinator] Master pronto em {self.host}:{self.port}")

        # thread para escutar conexões de entrada
        threading.Thread(target=self.listen_for_masters, daemon=True).start()

    def add_neighbor(self, neighbor_host, neighbor_port):
        """Adiciona um Master vizinho manualmente"""
        self.neighbors.append((neighbor_host, neighbor_port))

    def connect_to_neighbors(self):
        """Tenta conectar a todos os vizinhos"""
        for neighbor in self.neighbors:
            threading.Thread(target=self._connect_to_master, args=neighbor, daemon=True).start()

    def _connect_to_master(self, neighbor_host, neighbor_port):
        """Mantém uma conexão ativa com um master vizinho"""
        while True:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((neighbor_host, neighbor_port))
                print(f"[Coordinator] Conectado a Master {neighbor_host}:{neighbor_port}")

                with self.lock:
                    self.connections.append(s)

                # Envia mensagem inicial de identificação
                hello = {"type": "hello", "sender_host": self.host, "sender_port": self.port}
                s.sendall(json.dumps(hello).encode("utf-8"))

                # Loop ouvindo mensagens desse vizinho
                while True:
                    data = s.recv(1024)
                    if not data:
                        break
                    message = json.loads(data.decode("utf-8"))
                    print(f"[Coordinator] Mensagem recebida de {neighbor_host}:{neighbor_port} → {message}")

            except Exception as e:
                print(f"[Coordinator] Falha ao conectar {neighbor_host}:{neighbor_port} ({e}), tentando novamente...")
                time.sleep(3)  # espera antes de tentar de novo
            finally:
                try:
                    s.close()
                except:
                    pass
                time.sleep(3)  # reconecta depois

    def listen_for_masters(self):
        """Escuta conexões de outros Masters"""
        while True:
            client_socket, addr = self.socket_server.accept()
            print(f"[Coordinator] Conexão recebida de Master {addr}")
            threading.Thread(target=self.handle_master_connection, args=(client_socket,), daemon=True).start()

    def handle_master_connection(self, client_socket):
        """Processa mensagens de outros Masters"""
        try:
            while True:
                data = client_socket.recv(1024)
                if not data:
                    break
                message = json.loads(data.decode("utf-8"))
                print(f"[Coordinator] Mensagem recebida: {message}")

                if message.get("type") == "loan_request":
                    self.handle_loan_request(client_socket, message)

        except Exception as e:
            print(f"[Coordinator] Erro: {e}")
        finally:
            client_socket.close()

    def request_worker(self, neighbor_host, neighbor_port):
        """Pede um worker emprestado a outro Master"""
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((neighbor_host, neighbor_port))
            msg = {"type": "loan_request", "sender_host": self.host, "sender_port": self.port}
            s.sendall(json.dumps(msg).encode("utf-8"))
            response = s.recv(1024)
            print(f"[Coordinator] Resposta de {neighbor_host}:{neighbor_port} → {response.decode()}")
            s.close()
        except Exception as e:
            print(f"[Coordinator] Falha ao pedir worker: {e}")

    def handle_loan_request(self, client_socket, message):
        """Responde a um pedido de Worker"""
        print(f"[Coordinator] Pedido de empréstimo recebido de {message['sender_host']}:{message['sender_port']}")
        response = {"type": "loan_response", "status": "denied"}
        client_socket.sendall(json.dumps(response).encode("utf-8"))

    def shutdown(self):
        print("[Coordinator] Encerrando conexões...")
        with self.lock:
            for c in self.connections:
                try:
                    c.close()
                except:
                    pass
        self.socket_server.close()


if __name__ == "__main__":
    host = "10.62.217.22"
    port = 5001

    master = MasterCoordinator(host, port)

    # Exemplo: adicionar vizinhos
    #master.add_neighbor("10.62.217.209", 5000)
    master.add_neighbor("10.62.217.203", 5000)    
 

    # conecta automaticamente aos vizinhos
    master.connect_to_neighbors()

    print("[Coordinator] Aguardando conexões de outros Masters... (Ctrl+C para sair)")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        master.shutdown()
        print("[Coordinator] Finalizado.")
