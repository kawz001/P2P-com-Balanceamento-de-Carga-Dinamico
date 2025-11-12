import socket
import threading
import json
import time
import uuid
import random
import logging

# --------------------------- CONFIGURAÇÃO DE LOG ---------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("MasterCoordinator")

class MasterCoordinator:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.server_id = str(uuid.uuid4())  # usado como SERVER_UUID
        self.neighbors = []  # (host, port)
        self.socket_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket_server.bind((self.host, self.port))
        self.socket_server.listen(5)

        self.pending_tasks = 20
        self.threshold = 10
        self.workers = {}      # {uuid: {"status": "idle", "socket": socket, "owner": server_uuid}}
        self.transfers = {}    # {worker_uuid: lender_server_uuid}

        logger.info(f"Master {self.server_id[:8]} pronto em {self.host}:{self.port}")
        threading.Thread(target=self.listen_for_masters, daemon=True).start()

    # --------------------------- CONEXÕES ENTRE MASTERS ---------------------------

    def add_neighbor(self, neighbor_host, neighbor_port):
        self.neighbors.append((neighbor_host, neighbor_port))

    def listen_for_masters(self):
        while True:
            client_socket, addr = self.socket_server.accept()
            threading.Thread(target=self.handle_master_connection, args=(client_socket,), daemon=True).start()

    def handle_master_connection(self, client_socket):
        try:
            buffer = ""
            while True:
                data = client_socket.recv(4096)
                if not data:
                    break
                buffer += data.decode('utf-8')
                while '\n' in buffer:
                    line, buffer = buffer.split('\n', 1)
                    if not line.strip():
                        continue
                    try:
                        message = json.loads(line)
                    except Exception as e:
                        logger.error(f"JSON inválido recebido: {e} | raw: {line}")
                        continue

                    task = message.get("TASK")
                    logger.info(f"Mensagem recebida: {message}")

                    if task == "HEARTBEAT":
                        response = {"SERVER_UUID": self.server_id, "TASK": "HEARTBEAT", "RESPONSE": "ALIVE"}
                        payload = json.dumps(response, ensure_ascii=False) + '\n'
                        client_socket.sendall(payload.encode('utf-8'))

                    elif task == "WORKER_REQUEST":
                        self.handle_worker_request(client_socket, message)

                    elif task == "REDIRECT_CONFIRM":
                        self.handle_redirect_confirm(message)

                    elif task == "COMMAND_RELEASE":
                        workers = message.get("WORKERS_UUID") or message.get("WORKERS", [])
                        requester = message.get("SERVER_UUID")
                        logger.info(f"Recebida notificação COMMAND_RELEASE de {requester} para {workers}")
                        ack = {"SERVER_UUID": self.server_id, "RESPONSE": "RELEASE_ACK", "WORKERS_UUID": workers}
                        payload = json.dumps(ack, ensure_ascii=False) + '\n'
                        client_socket.sendall(payload.encode('utf-8'))

                    elif message.get("WORKER") == "ALIVE":
                        worker_uuid = message.get("WORKER_UUID")
                        owner = message.get("SERVER_UUID") or f"{self.host}:{self.port}"
                        self.workers[worker_uuid] = {"status": "idle", "socket": client_socket, "owner": owner}
                        logger.info(f"Novo Worker {worker_uuid} conectado. Owner: {owner}")
                        self.confirm_worker_assignment(client_socket, worker_uuid)
                        # enviar primeira task se houver
                        self.send_task_to_worker(None, worker_uuid)

                    elif message.get("type") == "task_completed" or message.get("STATUS") == "OK":
                        if message.get('type') == 'task_completed':
                            worker_uuid = message.get("worker_uuid")
                            task_id = message.get("task_id")
                        else:
                            worker_uuid = message.get("WORKER_UUID")
                            task_id = message.get("TASK")

                        if self.pending_tasks > 0:
                            self.pending_tasks -= 1
                        if worker_uuid in self.workers:
                            self.workers[worker_uuid]["status"] = "idle"

                        logger.info(f"✅ Worker {worker_uuid} concluiu a tarefa {task_id}. Pendentes: {self.pending_tasks}")

                        # Enviar ACK de status
                        ack = {"STATUS": "ACK"}
                        client_socket.sendall(json.dumps(ack, ensure_ascii=False).encode('utf-8') + b'\n')

        except Exception as e:
            logger.error(f"Erro na conexão: {e}")
        finally:
            try:
                client_socket.close()
            except:
                pass

    # --------------------------- HEARTBEAT ENTRE SERVIDORES ---------------------------

    def send_heartbeat(self):
        while True:
            for neighbor_host, neighbor_port in self.neighbors:
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.settimeout(2)
                    s.connect((neighbor_host, neighbor_port))
                    msg = {"SERVER_UUID": self.server_id, "TASK": "HEARTBEAT"}
                    payload = json.dumps(msg, ensure_ascii=False) + '\n'
                    s.sendall(payload.encode('utf-8'))
                    response = s.recv(4096)
                    if response:
                        logger.info(f"Resposta de {neighbor_host}:{neighbor_port} → {response.decode()}")
                    s.close()
                except Exception as e:
                    logger.warning(f"Falha ao enviar HEARTBEAT → {e}")
            time.sleep(5)

    # --------------------------- GERAÇÃO DE TASKS ---------------------------

    def simulate_task_generation(self):
        while True:
            time.sleep(random.randint(3, 10))
            self.pending_tasks += 1
            logger.info(f"Tasks pendentes: {self.pending_tasks}")
            if self.pending_tasks >= self.threshold:
                logger.warning("Saturação detectada. Solicitando suporte...")
                threading.Thread(target=self.request_support_from_neighbors, daemon=True).start()

    def continuous_dispatcher(self):
        """
        Mantém o envio contínuo de tarefas para workers ociosos.
        Garante que o master continue despachando mesmo após todos
        os workers terminarem as tasks anteriores.
        """
        while True:
            try:
                if self.pending_tasks > 0:
                    idle_workers = [w for w, info in self.workers.items() if info["status"] == "idle"]
                    for w in idle_workers:
                        if self.pending_tasks <= 0:
                            break
                        logger.debug(f"Despachando automaticamente nova task para {w}")
                        self.send_task_to_worker(None, w)
                time.sleep(2)
            except Exception as e:
                logger.error(f"Erro no continuous_dispatcher: {e}")
                time.sleep(2)

    # --------------------------- SOLICITAÇÃO DE WORKERS ENTRE SERVERS ---------------------------

    def request_support_from_neighbors(self):
        for neighbor_host, neighbor_port in self.neighbors:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((neighbor_host, neighbor_port))
                payload = {"MASTER": self.server_id, "TASK": "WORKER_REQUEST",
                           "REQUESTOR_INFO": {"ip": self.host, "port": self.port}}
                s.sendall(json.dumps(payload, ensure_ascii=False).encode('utf-8') + b'\n')
                response = s.recv(4096)
                if response:
                    message = json.loads(response.decode("utf-8"))
                    if message.get("RESPONSE") == "AVAILABLE":
                        workers = message.get("WORKERS_UUID", [])
                        logger.info(f"{len(workers)} workers disponíveis em {neighbor_host}")
                        confirm_payload = {
                            "TASK": "REDIRECT_CONFIRM",
                            "WORKERS": [{"WORKER_UUID": w} for w in workers],
                            "TARGET_HOST": self.host,
                            "TARGET_PORT": self.port
                        }
                        s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        s2.connect((neighbor_host, neighbor_port))
                        s2.sendall(json.dumps(confirm_payload, ensure_ascii=False).encode('utf-8') + b'\n')
                        s2.close()
                    else:
                        logger.info(f"Master {neighbor_host} sem workers disponíveis.")
                s.close()
            except Exception as e:
                logger.error(f"Erro ao contatar {neighbor_host}:{neighbor_port} → {e}")

    def handle_worker_request(self, client_socket, message):
        requester_info = message.get("REQUESTOR_INFO")
        requester_master_id = message.get("MASTER") or (
            requester_info and f"{requester_info.get('ip')}:{requester_info.get('port')}"
        )
        available_workers = [w for w, info in self.workers.items() if info["status"] == "idle"]

        if available_workers:
            response = {"SERVER_UUID": self.server_id, "RESPONSE": "AVAILABLE", "WORKERS_UUID": available_workers}
        else:
            response = {"SERVER_UUID": self.server_id, "RESPONSE": "UNAVAILABLE"}

        payload = json.dumps(response, ensure_ascii=False) + '\n'
        client_socket.sendall(payload.encode('utf-8'))
        logger.info(f"Resposta WORKER_REQUEST para {requester_master_id}: {response}")

    def handle_redirect_confirm(self, message):
        workers = message.get("WORKERS", []) or message.get("WORKERS_UUID", [])
        target_host = message.get("TARGET_HOST")
        target_port = message.get("TARGET_PORT")
        for w_info in workers:
            worker_uuid = w_info.get("WORKER_UUID") if isinstance(w_info, dict) else w_info
            self.redirect_worker_to_master(worker_uuid, target_host, target_port)

    def redirect_worker_to_master(self, worker_uuid, target_master_host, target_master_port):
        if worker_uuid not in self.workers:
            logger.warning(f"Worker {worker_uuid} não encontrado.")
            return
        payload = {"TASK": "REDIRECT", "SERVER_REDIRECT": {"ip": target_master_host, "port": target_master_port}}
        try:
            worker_socket = self.workers[worker_uuid].get("socket")
            if not self._socket_is_alive(worker_socket):
                logger.warning(f"Socket do worker {worker_uuid} inativo.")
                return
            payload_txt = json.dumps(payload, ensure_ascii=False) + '\n'
            worker_socket.sendall(payload_txt.encode('utf-8'))
            self.workers[worker_uuid]["status"] = "transferred"
            self.transfers[worker_uuid] = self.server_id
            logger.info(f"Worker {worker_uuid} redirecionado para {target_master_host}:{target_master_port}")
        except Exception as e:
            logger.error(f"Falha ao redirecionar {worker_uuid}: {e}")

    # --------------------------- TAREFAS E WORKERS ---------------------------

    def confirm_worker_assignment(self, client_socket, worker_uuid):
        message = {"TASK": "ASSIGN_MASTER", "MESSAGE": f"Agora você pertence ao Master {self.server_id[:8]}"}
        payload = json.dumps(message, ensure_ascii=False) + '\n'
        client_socket.sendall(payload.encode('utf-8'))

    def send_task_to_worker(self, client_socket, worker_uuid):
        """Envia uma task ao worker."""
        if self.pending_tasks <= 0:
            return

        worker_socket = None
        if worker_uuid in self.workers:
            worker_socket = self.workers[worker_uuid].get("socket")
        if not worker_socket and client_socket:
            worker_socket = client_socket

        if not self._socket_is_alive(worker_socket):
            logger.warning(f"Socket inválido para {worker_uuid}")
            return

        task = {"task_id": str(uuid.uuid4())[:8], "workload": random.randint(2, 5)}
        message = {"type": "new_task", "task": task}
        payload = json.dumps(message, ensure_ascii=False) + '\n'
        try:
            self.pending_tasks -= 1
            self.workers[worker_uuid]["status"] = "busy"
            worker_socket.sendall(payload.encode('utf-8'))
            logger.info(f"🧩 Enviada task {task['task_id']} ao Worker {worker_uuid}. Pendentes: {self.pending_tasks}")
        except Exception as e:
            logger.error(f"Erro ao enviar task para Worker {worker_uuid}: {e}")
            self.workers[worker_uuid]["status"] = "idle"

    def _socket_is_alive(self, sock):
        try:
            return sock and sock.fileno() >= 0
        except Exception:
            return False


# --------------------------- MAIN ---------------------------
if __name__ == "__main__":
    host = "10.62.218.30"
    port = 5000
    master = MasterCoordinator(host, port)
    #master.add_neighbor("127.0.0.1", 5001)

    threading.Thread(target=master.send_heartbeat, daemon=True).start()
    threading.Thread(target=master.simulate_task_generation, daemon=True).start()
    threading.Thread(target=master.continuous_dispatcher, daemon=True).start()  # 👈 DESPACHO CONTÍNUO

    while True:
        time.sleep(1)
