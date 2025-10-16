import time
import json
import socket
import threading
import uuid
from queue import Queue

class Worker:
    def __init__(self, host, port, master_host, master_port):
        self.host = host
        self.port = port
        self.master_host = master_host
        self.master_port = master_port
        self.worker_uuid = str(uuid.uuid4())
        self.is_connected = False
        self.socket = None
        self.task_queue = Queue()
        self.active_tasks = 0
        self.max_tasks = 2
        print(f"[Worker] Iniciado {self.worker_uuid[:8]} em {self.host}:{self.port}")

    # --------------------------------
    # CONEXÃO COM MASTER
    # --------------------------------
    def connect_to_master(self):
        """Conecta-se ao Master e registra"""
        while not self.is_connected:
            try:
                self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.socket.connect((self.master_host, self.master_port))
                self.is_connected = True
                print(f"[Worker] Conectado ao Master {self.master_host}:{self.master_port}")
                self.register_to_master()
                threading.Thread(target=self.listen_for_messages, daemon=True).start()
                threading.Thread(target=self.task_scheduler, daemon=True).start()
            except Exception as e:
                print(f"[Worker] Falha ao conectar: {e}. Tentando novamente em 5s...")
                time.sleep(5)

    def register_to_master(self):
        """Envia payload de registro ao Master"""
        payload = {
            "MASTER": self.master_host,
            "MASTER_ORIGIN": self.master_host,
            "WORKER": "ALIVE",
            "WORKER_UUID": self.worker_uuid
        }
        self.send_message(payload)
        print(f"[Worker] Registrado no Master {self.master_host}:{self.master_port}")

    # --------------------------------
    # RECEBIMENTO DE MENSAGENS
    # --------------------------------
    def listen_for_messages(self):
        """Escuta mensagens vindas do Master"""
        while self.is_connected:
            try:
                data = self.socket.recv(4096)
                if not data:
                    print("[Worker] Conexão perdida com Master.")
                    self.reconnect()
                    break

                message = json.loads(data.decode("utf-8"))
                self.handle_message(message)

            except Exception as e:
                print(f"[Worker] Erro ao receber dados: {e}")
                self.reconnect()

    def handle_message(self, message):
        """Processa mensagens recebidas do Master"""
        task_type = message.get("TASK")

        # --- Redirecionamento de Master ---
        if task_type == "REDIRECT":
            new_master = message.get("MASTER_REDIRECT")
            print(f"[Worker] 🔄 Recebeu redirecionamento → Novo Master: {new_master}")
            self.disconnect()
            self.master_host = new_master
            print(f"[Worker] Reconectando ao novo Master {new_master}...")
            self.connect_to_master()

        # --- Mensagem de confirmação ---
        elif task_type == "ASSIGN_MASTER":
            msg = message.get("MESSAGE", "")
            print(f"[Worker] 📩 Mensagem do Master: {msg}")

        # --- Nova tarefa recebida ---
        elif message.get("type") == "new_task":
            task = message["task"]
            print(f"[Worker] 📦 Recebeu nova tarefa {task['task_id']}")
            self.task_queue.put(task)

        # --- Heartbeat ---
        elif task_type == "HEARTBEAT":
            response = {
                "SERVER_ID": self.port,
                "TASK": "HEARTBEAT",
                "RESPONSE": "ALIVE"
            }
            self.send_message(response)

        else:
            print(f"[Worker] Mensagem desconhecida recebida: {message}")

    # --------------------------------
    # AGENDADOR DE TAREFAS
    # --------------------------------
    def task_scheduler(self):
        """Monitora a fila e executa tarefas disponíveis"""
        while True:
            if not self.task_queue.empty() and self.active_tasks < self.max_tasks:
                task = self.task_queue.get()
                threading.Thread(target=self.process_task, args=(task,), daemon=True).start()
            time.sleep(0.5)

    # --------------------------------
    # PROCESSAMENTO DE TAREFAS
    # --------------------------------
    def process_task(self, task):
        """Executa uma tarefa simulada"""
        self.active_tasks += 1
        print(f"[Worker] 🧩 Iniciando tarefa {task['task_id']} ({self.active_tasks}/{self.max_tasks})")
        time.sleep(task.get("workload", 3))
        print(f"[Worker] ✅ Tarefa {task['task_id']} concluída.")
        self.active_tasks -= 1

        # Notifica o Master
        self.send_message({
            "type": "task_completed",
            "task_id": task["task_id"],
            "worker_uuid": self.worker_uuid
        })

    # --------------------------------
    # ENVIO E CONEXÃO
    # --------------------------------
    def send_message(self, message):
        """Envia mensagens ao Master"""
        if self.is_connected:
            try:
                self.socket.sendall(json.dumps(message).encode("utf-8"))
            except Exception as e:
                print(f"[Worker] Erro ao enviar mensagem: {e}")
                self.reconnect()

    def disconnect(self):
        """Fecha conexão"""
        try:
            if self.socket:
                self.socket.close()
        except:
            pass
        self.is_connected = False
        print("[Worker] Desconectado do Master.")

    def reconnect(self):
        """Tenta reconectar ao Master"""
        self.disconnect()
        print("[Worker] Tentando reconectar...")
        time.sleep(5)
        self.connect_to_master()

# --------------------------------
# EXECUÇÃO
# --------------------------------
if __name__ == "__main__":
    worker = Worker(
        host="10.62.217.209",
        port=5070,
        master_host="10.62.217.22",
        master_port=5000
    )

    worker.connect_to_master()

    while True:
        time.sleep(1)
