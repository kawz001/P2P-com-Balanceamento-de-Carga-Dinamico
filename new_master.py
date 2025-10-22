import socket
import threading
import json
import time
import uuid
import random

class MasterCoordinator:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.server_id = "4B"
        self.neighbors = []  # (host, port)
        self.socket_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket_server.bind((self.host, self.port))
        self.socket_server.listen(5)

        # controle de tasks e workers
        self.pending_tasks = 20
        self.threshold = 10
        self.workers = {}  # {uuid: {"status": "idle", "socket": socket}}

        print(f"[Coordinator] Master {self.server_id[:8]} pronto em {self.host}:{self.port}")
        threading.Thread(target=self.listen_for_masters, daemon=True).start()

    # ----------------------------------------
    # CONEXÕES E MENSAGENS
    # ----------------------------------------
    def add_neighbor(self, neighbor_host, neighbor_port):
        self.neighbors.append((neighbor_host, neighbor_port))

    def listen_for_masters(self):
        """Escuta conexões de outros Masters e Workers"""
        while True:
            client_socket, addr = self.socket_server.accept()
            threading.Thread(target=self.handle_master_connection, args=(client_socket,), daemon=True).start()

    def handle_master_connection(self, client_socket):
        """Recebe mensagens de Masters e Workers"""
        try:
            while True:
                data = client_socket.recv(4096)
                if not data:
                    break
                message = json.loads(data.decode("utf-8"))
                task = message.get("TASK")

                print(f"[Coordinator] Mensagem recebida: {message}")

                # --- HEARTBEAT ENTRE MASTERS ---
                if task == "HEARTBEAT":
                    response = {
                        "SERVER_ID": self.server_id,
                        "TASK": "HEARTBEAT",
                        "RESPONSE": "ALIVE"
                    }
                    client_socket.sendall(json.dumps(response).encode("utf-8"))

                # --- SOLICITAÇÃO DE WORKERS ---
                elif task == "WORKER_REQUEST":
                    self.handle_worker_request(client_socket, message)

                # --- RECEBEU REDIRECIONAMENTO DE WORKER ---
                elif task == "REDIRECT":
                    self.handle_worker_redirect(message)

                # --- WORKER SE REGISTROU AQUI ---
                elif message.get("WORKER") == "ALIVE":
                    worker_uuid = message.get("WORKER_UUID")
                    self.workers[worker_uuid] = {"status": "idle", "socket": client_socket}
                    print(f"[Coordinator] Novo Worker {worker_uuid} conectado. Agora pertence a este Master.")
                    self.confirm_worker_assignment(client_socket, worker_uuid)
                    self.send_task_to_worker(client_socket, worker_uuid)

                # --- WORKER CONCLUIU UMA TAREFA ---
                elif message.get("type") == "task_completed":
                    worker_uuid = message.get("worker_uuid")
                    task_id = message.get("task_id")

                    if self.pending_tasks > 0:
                        self.pending_tasks -= 1
                    if worker_uuid in self.workers:
                        self.workers[worker_uuid]["status"] = "idle"

                    print(f"[Coordinator] ✅ Worker {worker_uuid} concluiu a tarefa {task_id}.")
                    print(f"[Coordinator] 📉 Tarefas pendentes agora: {self.pending_tasks}")

                    if self.pending_tasks > 0:
                        self.send_task_to_worker(client_socket, worker_uuid)

        except Exception as e:
            print(f"[Coordinator] Erro na conexão: {e}")
        finally:
            client_socket.close()

    # ----------------------------------------
    # HEARTBEAT
    # ----------------------------------------
    def send_heartbeat(self):
        while True:
            for neighbor_host, neighbor_port in self.neighbors:
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.settimeout(2)
                    s.connect((neighbor_host, neighbor_port))
                    msg = {"SERVER_ID": self.server_id, "TASK": "HEARTBEAT"}
                    s.sendall(json.dumps(msg).encode("utf-8"))
                    response = s.recv(1024)
                    if response:
                        print(f"[Coordinator] Resposta de {neighbor_host}:{neighbor_port} → {response.decode()}")
                    s.close()
                except Exception as e:
                    print(f"[Coordinator] Falha ao enviar HEARTBEAT para {neighbor_host}:{neighbor_port} → {e}")
            time.sleep(5)

    # ----------------------------------------
    # GERENCIAMENTO DE SATURAÇÃO
    # ----------------------------------------
    def simulate_task_generation(self):
        """Simula chegada de tarefas e detecção de saturação"""
        while True:
            time.sleep(random.randint(3, 6))
            print(f"[Load] Tasks pendentes: {self.pending_tasks}")
            if self.pending_tasks >= self.threshold:
                print("[ALERTA] Saturação detectada. Solicitando suporte...")
                threading.Thread(target=self.request_support_from_neighbors, daemon=True).start()

    def request_support_from_neighbors(self):
        """Envia WORKER_REQUEST a todos os vizinhos"""
        for neighbor_host, neighbor_port in self.neighbors:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((neighbor_host, neighbor_port))
                payload = {"MASTER": self.server_id, "TASK": "WORKER_REQUEST"}
                s.sendall(json.dumps(payload).encode("utf-8"))
                response = s.recv(4096)
                if response:
                    message = json.loads(response.decode("utf-8"))
                    if message.get("RESPONSE") == "AVAILABLE":
                        workers = message.get("WORKERS", [])
                        print(f"[SUPORTE] Recebido {len(workers)} workers disponíveis de {neighbor_host}")
                    else:
                        print(f"[SUPORTE] Master {neighbor_host} não possui workers disponíveis.")
                s.close()
            except Exception as e:
                print(f"[SUPORTE] Erro ao contatar {neighbor_host}:{neighbor_port} → {e}")

    # ----------------------------------------
    # SUPORTE ENTRE MASTERS
    # ----------------------------------------
    def handle_worker_request(self, client_socket, message):
        requester_master_id = message.get("MASTER")
        requester_host, requester_port = client_socket.getpeername()  # Pega IP e porta de quem pediu
        available_workers = [w for w, info in self.workers.items() if info["status"] == "idle"]

        if available_workers:
            response = {
                "MASTER": self.server_id,
                "RESPONSE": "AVAILABLE",
                "WORKERS": [{"WORKER_UUID": w} for w in available_workers]
            }
            client_socket.sendall(json.dumps(response).encode("utf-8"))
            print(f"[SUPORTE] Enviando resposta positiva com {len(available_workers)} workers para {requester_host}:{requester_port}.")

            # Agora o redirecionamento é dinâmico — volta para o Master que pediu
            for w in available_workers:
                self.redirect_worker_to_master(w, requester_host, requester_port)

        else:
            response = {"MASTER": self.server_id, "RESPONSE": "UNAVAILABLE"}
            client_socket.sendall(json.dumps(response).encode("utf-8"))
            print(f"[SUPORTE] Resposta negativa para {requester_master_id} - sem workers disponíveis.")

    def redirect_worker_to_master(self, worker_uuid, target_master_host, target_master_port):
        """Envia comando real de redirecionamento ao Worker"""
        if worker_uuid not in self.workers:
            print(f"[REDIRECIONAMENTO] Worker {worker_uuid} não encontrado.")
            return

        payload = {
            "TASK": "REDIRECT",
            "MASTER_REDIRECT": target_master_host,
            "MASTER_REDIRECT_PORT": target_master_port
        }

        try:
            worker_socket = self.workers[worker_uuid]["socket"]
            worker_socket.sendall(json.dumps(payload).encode("utf-8"))
            print(f"[REDIRECIONAMENTO] Worker {worker_uuid} redirecionado para {target_master_host}:{target_master_port}")
        except Exception as e:
            print(f"[REDIRECIONAMENTO] Falha ao redirecionar Worker {worker_uuid}: {e}")

    # ----------------------------------------
    # ASSIGNAÇÃO DE WORKERS
    # ----------------------------------------
    def confirm_worker_assignment(self, client_socket, worker_uuid):
        message = {
            "TASK": "ASSIGN_MASTER",
            "MESSAGE": f"Agora você pertence ao Master {self.server_id[:8]}"
        }
        try:
            client_socket.sendall(json.dumps(message).encode("utf-8"))
            print(f"[Coordinator] Confirmação enviada ao Worker {worker_uuid}")
        except Exception as e:
            print(f"[Coordinator] Erro ao confirmar Worker {worker_uuid}: {e}")

    def send_task_to_worker(self, client_socket, worker_uuid):
        """Envia uma tarefa simulada ao Worker"""
        if self.pending_tasks <= 0:
            return

        task = {
            "task_id": str(uuid.uuid4())[:8],
            "workload": random.randint(3, 6)
        }
        message = {"type": "new_task", "task": task}
        try:
            self.pending_tasks -= 1
            self.workers[worker_uuid]["status"] = "busy"
            client_socket.sendall(json.dumps(message).encode("utf-8"))
            print(f"[Coordinator] 🧩 Enviada task {task['task_id']} ao Worker {worker_uuid}. Pendentes: {self.pending_tasks}")
        except Exception as e:
            print(f"[Coordinator] Erro ao enviar task para Worker {worker_uuid}: {e}")


# ----------------------------------------
# EXECUÇÃO
# ----------------------------------------
if __name__ == "__main__":
    host = "10.62.217.207"
    port = 5000

    master = MasterCoordinator(host, port)
    master.add_neighbor("10.62.217.22", 5000)

    threading.Thread(target=master.send_heartbeat, daemon=True).start()
    threading.Thread(target=master.simulate_task_generation, daemon=True).start()

    while True:
        time.sleep(1)
