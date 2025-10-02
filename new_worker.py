import time
import json
import socket
import threading
from queue import Queue

#Conexão de ip, porta e defs
class Worker:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.master_host = None
        self.master_port = None
        self.is_connected = False
        self.socket = None
        print(f"Worker inicializado em {self.host}:{self.port}")

#Faz o Socket TCP e conecta ao Master e envia mensagem quando sucesso
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

#Enviar um Json mara o Master indicando
    def send_message(self, message):
        if self.is_connected:
            try:
                self.socket.sendall(json.dumps(message).encode('utf-8'))
            except Exception as e:
                print(f"Erro ao enviar mensagem: {e}")
                self.disconnect()

#Exucução da tarefa recebida
def process_task(self, task):
        print(f"Worker processando tarefa: {task['task_id']}")
        # Simula o processamento da tarefa
        time.sleep(task['workload'])
        print(f"Tarefa {task['task_id']} concluída.")
        self.send_message({
            "type": "task_completed",
            "task_id": task['task_id'],
            "worker_port": self.port
        })

def listen_for_tasks(self):
        #Fica ouvindo o Master para receber tarefas reatribuições ou heartbeats
        while self.is_connected:
            try:
                data = self.socket.recv(1024)
                if not data:
                    print("Conexão com o Master perdida.")
                    self.is_connected = False
                    break

                message = json.loads(data.decode('utf-8'))

                # Nova tarefa recebida
                if message.get("type") == "new_task":
                    self.process_task(message["task"])

                # Redirecionamento para outro Master
                elif message.get("type") == "reassign_master":
                    new_master_host = message["new_master_host"]
                    new_master_port = message["new_master_port"]
                    print(f"Recebeu instrução para se reconectar a novo Master em {new_master_host}:{new_master_port}")
                    self.disconnect()
                    self.connect_to_master(new_master_host, new_master_port)

                # Heartbeat recebido responder com "ALIVE"
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
#Fecha o Socket e desconecta
def disconnect(self):
        if self.socket:
            self.socket.close()
            self.is_connected = False
            print("Worker desconectado do Master.")
