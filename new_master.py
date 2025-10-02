import json
import socket
import threading
import time
from queue import Queue


class Master:
    def __init__(self, host, port, threshold=5, farm=None, heartbeat_interval=5, timeout=15):
        self.host = "10.62.217.209"
        self.port = 9001
        self.workers = farm if farm is not None else {}
        self.task_queue = Queue()
        self.task_counter = 0
        self.threshold = threshold
        self.socket_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket_server.bind((self.host, self.port))
        self.socket_server.listen(5)
        self.neighbor_masters = []

        # Controle do heartbeat
        self.last_seen = {}  # ultimo timestamp de resposta de cada worker
        self.heartbeat_interval = heartbeat_interval
        self.timeout = timeout

        print(f"Master inicializado em {self.host}:{self.port}")

        # Thread que fica monitorando heartbeats
        threading.Thread(target=self.monitor_heartbeats, daemon=True).start()

    def add_neighbor(self, neighbor_host, neighbor_port):
        self.neighbor_masters.append((neighbor_host, neighbor_port))

    def handle_client_connection(self, client_socket):
        try:
            while True:
                data = client_socket.recv(1024)
                if not data:
                    break
                message = json.loads(data.decode('utf-8'))

                # Registro de Worker
                if message.get("type") == "register":
                    worker_port = message["worker_port"]
                    self.workers[worker_port] = client_socket
                    self.last_seen[worker_port] = time.time()
                    print(f"Novo Worker registrado: {worker_port}")

                # Worker concluiu tarefa
                elif message.get("type") == "task_completed":
                    print(f"Tarefa {message['task_id']} concluída pelo Worker {message['worker_port']}")

                # Resposta de heartbeat
                elif message.get("TASK") == "HEARTBEAT" and message.get("RESPONSE") == "ALIVE":
                    worker_id = message["SERVER_ID"]
                    self.last_seen[worker_id] = time.time()
                    print(f"Heartbeat recebido do Worker {worker_id}")

                # Pedido de empréstimo
                elif message.get("type") == "loan_request":
                    self.handle_loan_request(client_socket, message)

        except Exception as e:
            print(f"Erro ao lidar com cliente: {e}")
        finally:
            client_socket.close()

#Empréstimo para outro WOrker
    def handle_loan_request(self, client_socket, message):
        print(f"Recebeu pedido de empréstimo de Worker de {message['sender_port']}")
        if self.workers:
            worker_port_to_loan = list(self.workers.keys())[0]
            worker_socket_to_loan = self.workers.pop(worker_port_to_loan)

            # Notifica o Worker para se reconectar
            loan_message = {
                "type": "reassign_master",
                "new_master_host": message['sender_host'],
                "new_master_port": message['sender_port']
            }
            worker_socket_to_loan.sendall(json.dumps(loan_message).encode('utf-8') + b'\n')

            # Responde ao Master vizinho
            response = {"type": "loan_response", "status": "accepted", "worker_port": worker_port_to_loan}
            client_socket.sendall(json.dumps(response).encode('utf-8') + b'\n')
            print(f"Worker {worker_port_to_loan} emprestado para o Master {message['sender_port']}")
        else:
            response = {"type": "loan_response", "status": "denied"}
            client_socket.sendall(json.dumps(response).encode('utf-8') + b'\n')
            print("Não há Workers para emprestar.")

#Conexões TCP
    def listen_for_connections(self):
        while True:
            client_socket, addr = self.socket_server.accept()
            print(f"Conexão de {addr[0]}:{addr[1]} estabelecida.")
            client_handler = threading.Thread(target=self.handle_client_connection, args=(client_socket,))
            client_handler.start()
#Criar tarefa e colocar na queue
    def generate_tasks(self, num_tasks):
        for i in range(num_tasks):
            task = {"task_id": self.task_counter, "workload": 1}
            self.task_queue.put(task)
            self.task_counter += 1
        print(f"Geradas {num_tasks} novas tarefas.")

#Retira da queue e manda pros wprkers
    def distribute_tasks(self):
        while not self.task_queue.empty():
            task = self.task_queue.get()
            if not self.workers: # Se nao tiver workers na fila chama threshold
                print("Sem Workers disponíveis. Carga alta.")
                if self.task_queue.qsize() > self.threshold:
                    self.initiate_consensual_protocol()
                self.task_queue.put(task)  # devolve a tarefa
                break

            worker_ports = list(self.workers.keys())
            worker_socket = self.workers[worker_ports[0]]

            message = {"type": "new_task", "task": task}
            try:
                worker_socket.sendall(json.dumps(message).encode('utf-8') + b'\n')
            except Exception as e:
                print(f"Erro ao enviar tarefa para o Worker: {e}. Removendo Worker.")
                self.workers.pop(worker_ports[0])

# Pedir ajuda aos Master vizinhos e se aceitar redirecionar o worker
    def initiate_consensual_protocol(self):
        print(f"Carga > threshold ({self.threshold}). Iniciando protocolo de consenso.")
        for neighbor_host, neighbor_port in self.neighbor_masters:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((neighbor_host, neighbor_port))
                request = {
                    "type": "loan_request",
                    "sender_host": self.host,
                    "sender_port": self.port
                }
                s.sendall(json.dumps(request).encode('utf-8') + b'\n')
                response_data = s.recv(1024)
                response = json.loads(response_data.decode('utf-8'))

                if response["status"] == "accepted":
                    print(f"Master vizinho {neighbor_port} aceitou o pedido. Worker será redirecionado.")
                    break
            except Exception as e:
                print(f"Erro ao conectar com Master vizinho {neighbor_port}: {e}")

# batidas
    def send_heartbeat(self):
#Envia heartbeat para todos os Workers registrados
        for worker_port, worker_socket in list(self.workers.items()):
            try:
                msg = {"SERVER_ID": worker_port, "TASK": "HEARTBEAT"}
                worker_socket.sendall(json.dumps(msg).encode("u#tf-8"))
            except:
                print(f"Falha ao enviar heartbeat para Worker {worker_port}")
                self.workers.pop(worker_port, None)

    def monitor_heartbeats(self):
#Thread que envia heartbeats LOOP e verifica timeouts
        while True:
            self.send_heartbeat()
            now = time.time()
            for worker_port in list(self.last_seen.keys()):
                if now - self.last_seen[worker_port] > self.timeout:
                    print(f"Worker {worker_port} não respondeu no timeout. Removendo da lista.")
                    self.workers.pop(worker_port, None)
                    self.last_seen.pop(worker_port, None)
            time.sleep(self.heartbeat_interval)
