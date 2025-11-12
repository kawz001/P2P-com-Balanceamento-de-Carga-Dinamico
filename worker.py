import time
import json
import socket
import threading
import uuid
from queue import Queue
import logging

# --------------------------- CONFIGURAÇÃO DE LOG ---------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("Worker")

class Worker:
    def __init__(self, host, port, master_host, master_port):
        self.host = host
        self.port = port
        self.master_host = master_host
        self.master_port = master_port
        self.worker_uuid = str(uuid.uuid4())
        self.original_master = None  # guarda "ip:port" do master original se for emprestado
        self.is_connected = False
        self.socket = None
        self.task_queue = Queue()
        self.active_tasks = 0
        self.max_tasks = 2
        logger.info(f"Iniciado {self.worker_uuid[:8]} em {self.host}:{self.port}")

    def connect_to_master(self):
        while not self.is_connected:
            try:
                self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.socket.connect((self.master_host, self.master_port))
                self.is_connected = True
                logger.info(f"Conectado ao Master {self.master_host}:{self.master_port}")
                self.register_to_master()
                threading.Thread(target=self.listen_for_messages, daemon=True).start()
                threading.Thread(target=self.task_scheduler, daemon=True).start()
            except Exception as e:
                logger.warning(f"Falha ao conectar: {e}. Tentando novamente em 5s...")
                time.sleep(5)

    def register_to_master(self):
        # Envia registro para o master atual. Se este worker for emprestado, inclui SERVER_UUID do dono original.
        payload = {
            "WORKER": "ALIVE",
            "WORKER_UUID": self.worker_uuid
        }
        # Se temos um master de origem e for diferente do atual, enviamos SERVER_UUID para indicar empréstimo
        if self.original_master and (self.original_master != f"{self.master_host}:{self.master_port}"):
            payload["SERVER_UUID"] = self.original_master
        self.send_message(payload)
        logger.info(f"Registro enviado ao master ({self.master_host}:{self.master_port}): {payload}")

    def listen_for_messages(self):
        # Lê do socket em streaming; usa '\n' como delimitador de mensagens JSON.
        buffer = ""
        while self.is_connected:
            try:
                data = self.socket.recv(4096)
                if not data:
                    logger.warning("Conexão perdida com Master.")
                    self.reconnect()
                    break
                buffer += data.decode('utf-8')
                while '\n' in buffer:
                    line, buffer = buffer.split('\n', 1)
                    if not line.strip():
                        continue
                    try:
                        message = json.loads(line)
                        logger.info(f"Mensagem recebida (parsed): {message}")
                        self.handle_message(message)
                    except Exception as e:
                        logger.error(f"Mensagem inválida recebida: {e} | raw: {line}")
            except Exception as e:
                logger.error(f"Erro ao receber dados: {e}")
                self.reconnect()

    def handle_message(self, message):
        task_type = message.get("TASK")

        # Suporta tanto payloads antigos quanto os do PDF (SERVER_REDIRECT, SERVER_RETURN etc.)
        if task_type == "REDIRECT":
            # Accept both formats: {'MASTER_REDIRECT','MASTER_REDIRECT_PORT'} or {'SERVER_REDIRECT': {'ip', 'port'}}
            sr = message.get("SERVER_REDIRECT")
            if sr:
                new_master_host = sr.get("ip")
                new_master_port = sr.get("port")
            else:
                new_master_host = message.get("MASTER_REDIRECT")
                new_master_port = message.get("MASTER_REDIRECT_PORT")
            logger.info(f"🔄 Redirecionado → {new_master_host}:{new_master_port}")
            # Mark original master if not already set
            if not self.original_master:
                try:
                    self.original_master = f"{self.master_host}:{self.master_port}"
                except Exception:
                    self.original_master = None
            self.disconnect()
            self.master_host = new_master_host
            self.master_port = new_master_port
            # connect_to_master fará o registro (incluindo SERVER_UUID se for emprestado)
            self.connect_to_master()

        elif task_type == "RETURN":
            # Ordem para retornar ao master original. SERVER_RETURN contains ip/port
            sr = message.get("SERVER_RETURN")
            if sr:
                return_ip = sr.get("ip")
                return_port = sr.get("port")
                logger.info(f"🔙 Ordem de retorno recebida. Voltando para {return_ip}:{return_port}")
                # Atualiza master para o original
                self.disconnect()
                self.master_host = return_ip
                self.master_port = return_port
                # clear original_master since we returned
                self.original_master = None
                self.connect_to_master()
            else:
                logger.warning("Payload RETURN inválido: sem SERVER_RETURN")

        elif task_type == "ASSIGN_MASTER":
            logger.info(f"📩 {message.get('MESSAGE', '')}")

        elif message.get("type") == "new_task":
            task = message["task"]
            logger.info(f"📦 Nova tarefa recebida (id={task.get('task_id')} workload={task.get('workload')})")
            self.task_queue.put(task)

        elif task_type == "HEARTBEAT":
            # Responder com o formato pedido no PDF: SERVER_UUID e TASK=HEARTBEAT
            response = {"SERVER_UUID": f"{self.host}:{self.port}", "TASK": "HEARTBEAT", "RESPONSE": "ALIVE"}
            self.send_message(response)

        elif message.get("STATUS"):
            # Recebe confirmação de status (ACK) ou comandos genéricos
            logger.info(f"Status message recebida: {message}")

        else:
            logger.warning(f"Mensagem desconhecida: {message}")

    def task_scheduler(self):
        while True:
            if not self.task_queue.empty() and self.active_tasks < self.max_tasks:
                task = self.task_queue.get()
                threading.Thread(target=self.process_task, args=(task,), daemon=True).start()
            time.sleep(0.5)

    def process_task(self, task):
        self.active_tasks += 1
        logger.info(f"🧩 Executando tarefa {task['task_id']} (workload={task.get('workload')}) — active_tasks={self.active_tasks}")
        time.sleep(task.get("workload", 3))
        logger.info(f"✅ Tarefa {task['task_id']} concluída.")
        self.active_tasks -= 1
        # Reporta status conforme especificação: STATUS / TASK / WORKER_UUID
        status_payload = {"STATUS": "OK", "TASK": task.get("task_id") or task.get("task_type", "QUERY"), "WORKER_UUID": self.worker_uuid}
        self.send_message(status_payload)
        # Também envia evento compatível antigo para masters que esperam 'task_completed'
        try:
            self.send_message({
                "type": "task_completed",
                "task_id": task["task_id"],
                "worker_uuid": self.worker_uuid
            })
        except Exception:
            pass

    def send_message(self, message):
        # Adiciona delimitador '\n' para framing e envia JSON seguro
        if self.is_connected:
            try:
                payload = json.dumps(message, ensure_ascii=False) + '\n'
                # log do envio para rastreio
                try:
                    logger.debug(f"Enviando ao master ({self.master_host}:{self.master_port}): {message}")
                except:
                    pass
                self.socket.sendall(payload.encode('utf-8'))
            except Exception as e:
                logger.error(f"Erro ao enviar mensagem: {e} | payload: {message}")
                self.reconnect()
        else:
            logger.warning("Tentativa de enviar mensagem sem conexão ativa.")

    def disconnect(self):
        try:
            if self.socket:
                self.socket.close()
        except:
            pass
        self.is_connected = False
        logger.info("Desconectado do Master.")

    def reconnect(self):
        self.disconnect()
        logger.info("Tentando reconectar...")
        time.sleep(5)
        self.connect_to_master()

if __name__ == "__main__":
    worker = Worker(
        host="127.0.0.1",
        port=5070,
        master_host="127.0.0.1",
        master_port=5000
    )
    worker.connect_to_master()
    while True:
        time.sleep(1)
