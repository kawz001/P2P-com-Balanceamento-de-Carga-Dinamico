import time
import json
import socket
import threading
from queue import Queue

class Worker:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.master_host = "10.62.217.209"
        self.master_port = 5000   # tem que bater com o Master
        self.is_connected = False
        self.socket = None
        print(f"Worker inicializado em {self.host}:{self.port}")

    def connect_to_master(self, master_host, master_port):
        self.master_host = master_host
        self.master_port = master_port
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((master_host, master_port))
            self.is_connected = True
            print(f"Worker conectado ao Master em {master_host}:{master_port}")
            self.send_message({"type": "register", "worker_port": self.port})
        except Exception as e:
            print(f"Erro ao conectar ao Master: {e}")
            self.is_connected = False

    def send_message(self, message):
        if self.is_connected:
            try:
                self.socket.sendall(json.dumps(message).encode('utf-8'))
            except Exception as e:
                print(f"Erro ao enviar mensagem: {e}")
                self.disconnect()

    def process_task(self, task):
        print(f"Worker processando tarefa: {task['task_id']}")
        time.sleep(task['workload'])
        print(f"Tarefa {task['task_id']} concluída.")
        self.send_message({
            "type": "task_completed",
            "task_id": task['task_id'],
            "worker_port": self.port
        })

    def listen_for_tasks(self):
        while self.is_connected:
            try:
                data = self.socket.recv(1024)
                if not data:
                    print("Conexão com o Master perdida.")
                    self.is_connected = False
                    break

                message = json.loads(data.decode('utf-8'))

                if message.get("type") == "new_task":
                    self.process_task(message["task"])

                elif message.get("type") == "reassign_master":
                    new_master_host = message["new_master_host"]
                    new_master_port = message["new_master_port"]
                    print(f"Recebeu instrução para se reconectar a novo Master em {new_master_host}:{new_master_port}")
                    self.disconnect()
                    self.connect_to_master(new_master_host, new_master_port)

                elif message.get("TASK") == "HEARTBEAT":
                    response = {
                        "SERVER_ID": self.port,
                        "TASK": "HEARTBEAT",
                        "RESPONSE": "ALIVE"
                    }
                    self.send_message(response)

            except Exception as e:
                print(f"Erro ao receber dados: {e}")
                self.is_connected = False

    def disconnect(self):
        if self.socket:
            self.socket.close()
            self.is_connected = False
            print("Worker desconectado do Master.")


if __name__ == "__main__":
    worker = Worker("10.62.217.207", 5069)
    worker.connect_to_master("10.62.217.22", 5000)   # mesma porta do Master

    threading.Thread(target=worker.listen_for_tasks, daemon=True).start()

    while True:
        time.sleep(1)
