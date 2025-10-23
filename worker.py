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

    def connect_to_master(self):
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
        payload = {
            "MASTER": self.master_host,
            "MASTER_ORIGIN": self.master_host,
            "WORKER": "ALIVE",
            "WORKER_UUID": self.worker_uuid
        }
        self.send_message(payload)

    def listen_for_messages(self):
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
        task_type = message.get("TASK")

        if task_type == "REDIRECT":
            new_master_host = message.get("MASTER_REDIRECT")
            new_master_port = message.get("MASTER_REDIRECT_PORT")
            print(f"[Worker] 🔄 Redirecionado → {new_master_host}:{new_master_port}")
            self.disconnect()
            self.master_host = new_master_host
            self.master_port = new_master_port
            self.connect_to_master()

        elif task_type == "ASSIGN_MASTER":
            print(f"[Worker] 📩 {message.get('MESSAGE','')}")

        elif message.get("type") == "new_task":
            task = message["task"]
            print(f"[Worker] 📦 Nova tarefa {task['task_id']}")
            self.task_queue.put(task)

        elif task_type == "HEARTBEAT":
            response = {"SERVER_ID": self.port, "TASK": "HEARTBEAT", "RESPONSE": "ALIVE"}
            self.send_message(response)
        else:
            print(f"[Worker] Mensagem desconhecida: {message}")

    def task_scheduler(self):
        while True:
            if not self.task_queue.empty() and self.active_tasks < self.max_tasks:
                task = self.task_queue.get()
                threading.Thread(target=self.process_task, args=(task,), daemon=True).start()
            time.sleep(0.5)

    def process_task(self, task):
        self.active_tasks += 1
        print(f"[Worker] 🧩 Executando tarefa {task['task_id']}")
        time.sleep(task.get("workload", 3))
        print(f"[Worker] ✅ Tarefa {task['task_id']} concluída.")
        self.active_tasks -= 1
        self.send_message({
            "type": "task_completed",
            "task_id": task["task_id"],
            "worker_uuid": self.worker_uuid
        })

    def send_message(self, message):
        if self.is_connected:
            try:
                self.socket.sendall(json.dumps(message).encode("utf-8"))
            except Exception as e:
                print(f"[Worker] Erro ao enviar mensagem: {e}")
                self.reconnect()

    def disconnect(self):
        try:
            if self.socket:
                self.socket.close()
        except:
            pass
        self.is_connected = False
        print("[Worker] Desconectado do Master.")

    def reconnect(self):
        self.disconnect()
        print("[Worker] Tentando reconectar...")
        time.sleep(5)
        self.connect_to_master()

if __name__ == "__main__":
    worker = Worker(
        host="10.62.217.22",
        port=5070,
        master_host="10.62.217.207",
        master_port=5000
    )
    worker.connect_to_master()
    while True:
        time.sleep(1)
