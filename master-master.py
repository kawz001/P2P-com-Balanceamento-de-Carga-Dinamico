import socket
import threading
import json
import time
import uuid

class MasterCoordinator:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.server_id = str(uuid.uuid4())  # gera um UUID único
        self.neighbors = []  # (host, port)
        self.socket_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket_server.bind((self.host, self.port))
        self.socket_server.listen(5)

        print(f"[Coordinator] Master {self.server_id} pronto em {self.host}:{self.port}")
        threading.Thread(target=self.listen_for_masters, daemon=True).start()

    def add_neighbor(self, neighbor_host, neighbor_port):
        """Adiciona um vizinho manualmente"""
        self.neighbors.append((neighbor_host, neighbor_port))

    def listen_for_masters(self):
        """Escuta conexões de outros Masters"""
        while True:
            client_socket, addr = self.socket_server.accept()
            print(f"[Coordinator] Conexão recebida de {addr}")
            threading.Thread(target=self.handle_master_connection, args=(client_socket,), daemon=True).start()

    def handle_master_connection(self, client_socket):
        """Processa mensagens recebidas"""
        try:
            while True:
                data = client_socket.recv(1024)
                if not data:
                    break

                message = json.loads(data.decode("utf-8"))

                print(f"[Coordinator] Mensagem recebida: {message}")

                if message.get("TASK") == "HEARTBEAT":
                    # Envia resposta
                    response = {
                        "SERVER_ID": "ID4 RECEBIDO",
                        "TASK": "HEARTBEAT",
                        "RESPONSE": "ALIVE"
                    }
                    client_socket.sendall(json.dumps(response).encode("utf-8"))
                    print(f"[Coordinator] Respondeu HEARTBEAT → {response}")

        except Exception as e:
            print(f"[Coordinator] Erro: {e}")
        finally:
            client_socket.close()

    def send_heartbeat(self):
        """Envia heartbeat para todos os vizinhos"""
        for neighbor_host, neighbor_port in self.neighbors:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((neighbor_host, neighbor_port))

                msg = {
                    "SERVER_ID": "ID4 DO KAWEZAO",
                    "TASK": "HEARTBEAT"
                }
                s.sendall(json.dumps(msg).encode("utf-8"))
                print(f"[Coordinator] Enviou HEARTBEAT → {neighbor_host}:{neighbor_port}")

                # Espera resposta
                response = s.recv(1024)
                if response:
                    print(f"[Coordinator] Resposta de {neighbor_host}:{neighbor_port} → {response.decode()}")

                s.close()

            except Exception as e:
                print(f"[Coordinator] Falha ao enviar HEARTBEAT para {neighbor_host}:{neighbor_port} → {e}")


if __name__ == "__main__":
    host = "10.62.217.22"
    port = 5000


    master = MasterCoordinator(host, port)

    # exemplo de vizinhos (ajuste as portas conforme seu setup)
    master.add_neighbor("10.62.217.209", 5000)
    master.add_neighbor("10.62.217.203", 5000)
    master.add_neighbor("10.62.217.199", 8765)

    try:
        while True:
            master.send_heartbeat()  # envia heartbeat a cada 5s
            time.sleep(5)
    except KeyboardInterrupt:
        print("[Coordinator] Finalizado.")
