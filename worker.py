
import socket
import threading
import json
import time
import uuid
import random
import logging
import os

# ---------------------------
# CONFIGURAÇÃO
# ---------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("WORKER")

WORKER_UUID = f"WORKER-{str(uuid.uuid4())[:8]}"
STATE_FILE = "worker_state.json"

HOST = "10.62.217.209"
PORT = random.randint(5000, 5000)
DEFAULT_MASTER = {"ip": "10.62.217.207", "port": 5000}

HEARTBEAT_INTERVAL = 8
RECONNECT_DELAY = 3
EXECUTION_TIME = (2, 5)

# ---------------------------
# VARIÁVEIS DE ESTADO
# ---------------------------
lock = threading.Lock()
current_master = DEFAULT_MASTER.copy()
running = True
metrics = {"executadas": 0, "falhas": 0}

# ---------------------------
# PERSISTÊNCIA
# ---------------------------
def salvar_estado():
    """Salva o master atual em disco."""
    with lock:
        with open(STATE_FILE, "w") as f:
            json.dump(current_master, f)

def carregar_estado():
    """Carrega o master anterior (se existir)."""
    global current_master
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                current_master = json.load(f)
                logger.info(f"💾 [RECUPERADO] Último master: {current_master['ip']}:{current_master['port']}")
        except Exception:
            logger.warning("⚠️ [RECUPERADO] Falha ao carregar estado, usando padrão.")

# ---------------------------
# UTILITÁRIOS
# ---------------------------
def enviar_json(sock, obj):
    sock.sendall((json.dumps(obj) + "\n").encode())

def receber_json(sock, timeout=10):
    sock.settimeout(timeout)
    data = b""
    try:
        while b"\n" not in data:
            chunk = sock.recv(4096)
            if not chunk:
                break
            data += chunk
    except socket.timeout:
        return None
    if not data:
        return None
    try:
        return json.loads(data.decode().split("\n")[0])
    except Exception:
        return None

# ---------------------------
# EXECUÇÃO DE TAREFAS
# ---------------------------
def executar_tarefa(task_data):
    logger.info(f"⚙️ [EXECUÇÃO] Iniciando tarefa com dados: {task_data}")
    time.sleep(random.uniform(*EXECUTION_TIME))
    sucesso = random.choice([True, True, True, False])
    if sucesso:
        logger.info("✅ [EXECUÇÃO] Tarefa concluída com sucesso!")
        metrics["executadas"] += 1
        return "OK"
    else:
        logger.warning("⚠️ [EXECUÇÃO] Falha durante a tarefa!")
        metrics["falhas"] += 1
        return "NOK"

# ---------------------------
# CONEXÃO COM MASTER
# ---------------------------
def ciclo_worker():
    """Loop principal do worker (envia ALIVE e executa tarefas)."""
    global current_master
    while running:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(10)
                s.connect((current_master["ip"], current_master["port"]))
                payload = {
                    "WORKER": "ALIVE",
                    "WORKER_UUID": WORKER_UUID,
                    "port": PORT
                }
                enviar_json(s, payload)
                logger.info(f"📡 [ENVIO] Pedido de tarefa enviado a {current_master['ip']}:{current_master['port']}")
                resp = receber_json(s, timeout=10)

                if not resp:
                    logger.warning("💤 [TIMEOUT] Nenhuma resposta do Master.")
                    time.sleep(RECONNECT_DELAY)
                    continue

                if resp.get("TASK") == "QUERY":
                    tarefa = resp.get("USER")
                    status = executar_tarefa(tarefa)
                    with socket.socket() as report:
                        report.connect((current_master["ip"], current_master["port"]))
                        enviar_json(report, {"WORKER_UUID": WORKER_UUID, "TASK": "QUERY", "STATUS": status})
                        ack = receber_json(report, timeout=5)
                        if ack and ack.get("STATUS") == "ACK":
                            logger.info("📬 [ACK] Status confirmado pelo Master.")
                elif resp.get("TASK") == "NO_TASK":
                    logger.info("📭 [NO_TASK] Nenhuma tarefa disponível.")
                    time.sleep(3)
                else:
                    logger.debug(f"❓ [DESCONHECIDO] Payload inesperado: {resp}")

        except Exception as e:
            logger.error(f"🔴 [ERRO] Falha de comunicação com Master: {e}")
            time.sleep(RECONNECT_DELAY)

# ---------------------------
# HEARTBEAT CORRIGIDO
# ---------------------------
def heartbeat_loop():
    """Envia heartbeat compatível com o Master."""
    global current_master
    while running:
        try:
            with socket.socket() as s:
                s.settimeout(5)
                s.connect((current_master["ip"], current_master["port"]))
                enviar_json(s, {"TASK": "WORKER_HEARTBEAT", "WORKER_UUID": WORKER_UUID})
                resp = receber_json(s, timeout=5)
                if resp and resp.get("RESPONSE") == "ALIVE":
                    logger.info(f"💓 [HEARTBEAT] OK - Master {current_master['ip']} respondeu.")
                else:
                    logger.warning(f"💤 [HEARTBEAT] Sem resposta do Master.")
        except Exception as e:
            logger.warning(f"💔 [HEARTBEAT] Erro ao enviar heartbeat: {e}")
        time.sleep(HEARTBEAT_INTERVAL)

# ---------------------------
# COMANDOS REMOTOS (REDIRECT / RETURN)
# ---------------------------
def tratar_comando(conn, addr):
    global current_master
    msg = receber_json(conn)
    if not msg:
        return
    task = msg.get("TASK")

    if task == "REDIRECT":
        info = msg["SERVER_REDIRECT"]
        new_ip, new_port = info["ip"], info["port"]
        logger.info(f"🔀 [REDIRECT] Mudando para {new_ip}:{new_port}")
        with lock:
            current_master["ip"], current_master["port"] = new_ip, new_port
            salvar_estado()
        time.sleep(1)
        threading.Thread(target=ciclo_worker, daemon=True).start()

    elif task == "RETURN":
        info = msg["SERVER_RETURN"]
        new_ip, new_port = info["ip"], info["port"]
        logger.info(f"🔁 [RETURN] Retornando a {new_ip}:{new_port}")
        with lock:
            current_master["ip"], current_master["port"] = new_ip, new_port
            salvar_estado()
        time.sleep(1)
        threading.Thread(target=ciclo_worker, daemon=True).start()

    elif task in ("HEARTBEAT", "WORKER_HEARTBEAT"):
        logger.debug("💤 [IGNORADO] Heartbeat recebido - ignorado.")
        return

    else:
        logger.warning(f"❓ [COMANDO] Payload desconhecido: {msg}")

# ---------------------------
# SERVIDOR LOCAL
# ---------------------------
def iniciar_servidor_local():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((HOST, PORT))
    s.listen(10)
    logger.info(f"🛰️ [LISTEN] Worker {WORKER_UUID} ouvindo comandos em {HOST}:{PORT}")
    try:
        while running:
            conn, addr = s.accept()
            threading.Thread(target=tratar_comando, args=(conn, addr), daemon=True).start()
    except Exception as e:
        logger.error(f"🔴 [SERVIDOR] Erro listener: {e}")
    finally:
        s.close()

# ---------------------------
# MÉTRICAS PERIÓDICAS
# ---------------------------
def metricas_loop():
    while running:
        with lock:
            logger.info(f"📈 [MÉTRICAS] Conectado a {current_master['ip']}:{current_master['port']} | Tarefas OK: {metrics['executadas']} | Falhas: {metrics['falhas']}")
        time.sleep(10)

# ---------------------------
# MAIN
# ---------------------------
def main():
    carregar_estado()
    logger.info(f"🚀 Iniciando Worker {WORKER_UUID} -> {current_master['ip']}:{current_master['port']}")
    threading.Thread(target=iniciar_servidor_local, daemon=True).start()
    threading.Thread(target=ciclo_worker, daemon=True).start()
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    threading.Thread(target=metricas_loop, daemon=True).start()
    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()
