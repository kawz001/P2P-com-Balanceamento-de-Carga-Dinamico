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

# Identificador do worker (mantive sua geração original)
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
# METRICS - Supervisor endpoint e psutil
# ---------------------------
# Este campo server_uuid identifica a "farm" do worker no dashboard
SERVER_UUID = "michel_2"

SUPERVISOR_HOST = "srv.webrelay.dev"
SUPERVISOR_PORT = 33905   # porta nova
METRICS_INTERVAL = 10

try:
    import psutil
    PSUTIL_AVAILABLE = True
    logger.info("psutil disponível: usando métricas reais.")
except Exception:
    psutil = None
    PSUTIL_AVAILABLE = False
    logger.warning("psutil NÃO disponível: usando métricas simulados (fallback).")

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
# MÉTRICAS PERIÓDICAS (NOVAS FUNÇÕES)
# ---------------------------
def coletar_metricas_worker():
    """Monta payload conforme template enviado (usa psutil se disponível)."""
    ts_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    mensage_id = str(uuid.uuid4())

    if PSUTIL_AVAILABLE:
        try:
            uptime_seconds = int(time.time() - psutil.boot_time())
        except Exception:
            uptime_seconds = 0
        try:
            la1, la5, la15 = psutil.getloadavg()
        except Exception:
            la1 = round(psutil.cpu_percent(interval=0.1), 1)
            la5 = la1
        cpu_usage = round(psutil.cpu_percent(interval=0.1), 1)
        cpu_count_logical = psutil.cpu_count(logical=True) or 1
        cpu_count_physical = psutil.cpu_count(logical=False) or cpu_count_logical
        vm = psutil.virtual_memory()
        mem_total_mb = int(vm.total // (1024 * 1024))
        mem_available_mb = int(vm.available // (1024 * 1024))
        mem_percent_used = round(vm.percent, 1)
        mem_used_gb = round((vm.total - vm.available) / (1024**3), 1)
        du = psutil.disk_usage("/")
        disk_total_gb = round(du.total / (1024**3), 1)
        disk_free_gb = round(du.free / (1024**3), 1)
        disk_percent_used = round(du.percent, 1)
    else:
        uptime_seconds = random.randint(10000, 200000)
        la1 = round(random.uniform(0, 4), 1)
        la5 = round(random.uniform(0, 4), 1)
        cpu_usage = round(random.uniform(0, 100), 1)
        cpu_count_logical = os.cpu_count() or 4
        cpu_count_physical = max(1, cpu_count_logical // 2)
        mem_total_mb = 8192
        mem_available_mb = random.randint(256, mem_total_mb)
        mem_percent_used = round(100 * (mem_total_mb - mem_available_mb) / mem_total_mb, 1)
        mem_used_gb = round((mem_total_mb - mem_available_mb) / 1024, 1)
        disk_total_gb = 256.0
        disk_free_gb = round(random.uniform(1, 240), 1)
        disk_percent_used = round(100 * (disk_total_gb - disk_free_gb) / disk_total_gb, 1)

    neighbors_list = [{
        "server_uuid": current_master.get("ip"),
        "status": "available",
        "last_heartbeat": ts_iso
    }]

    payload = {
        "server_uuid": SERVER_UUID,
        "task": "performance_report",
        "timestamp": ts_iso,
        "mensage_id": mensage_id,
        "performance": {
            "system": {
                "uptime_seconds": uptime_seconds,
                "load_average_1m": la1,
                "load_average_5m": la5,
                "cpu": {
                    "usage_percent": cpu_usage,
                    "count_logical": cpu_count_logical,
                    "count_physical": cpu_count_physical
                },
                "memory": {
                    "total_mb": mem_total_mb,
                    "available_mb": mem_available_mb,
                    "percent_used": mem_percent_used,
                    "memory_used": mem_used_gb
                },
                "disk": {
                    "total_gb": disk_total_gb,
                    "free_gb": disk_free_gb,
                    "percent_used": disk_percent_used
                }
            }
        },
        "farm_state": {
            "workers": {
                "total_registered": 0,
                "workers_utilization": 0,
                "workers_alive": 0,
                "workers_idle": 0,
                "workers_borrowed": 0,
                "workers_recieved": 0,
                "workers_failed": 0
            },
            "tasks": {
                "tasks_pending": 0,
                "tasks_running": 1
            }
        },
        "config_thresholds": {
            "max_task": 100
        },
        "neighbors": neighbors_list
    }
    return payload

def enviar_metricas_para_supervisor_worker(payload):
    """Enviar apenas SEND para o supervisor (sem aguardar resposta)."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(5)
            s.connect((SUPERVISOR_HOST, SUPERVISOR_PORT))
            s.sendall((json.dumps(payload) + "\n").encode())
        logger.info(f"📤 [METRICS-WORKER] Métricas enviadas ao supervisor: {payload['mensage_id']}")
    except Exception as e:
        logger.warning(f"⚠️ [METRICS-WORKER] Falha ao enviar métricas: {e}")

def metrics_sender_loop():
    """Thread que coleta e envia métricas a cada METRICS_INTERVAL segundos."""
    while True:
        try:
            payload = coletar_metricas_worker()
            # log para debug/local (mostra exatamente o que será enviado)
            logger.info("📦 PAYLOAD WORKER GERADO:\n" + json.dumps(payload, indent=4))
            enviar_metricas_para_supervisor_worker(payload)
        except Exception as e:
            logger.error(f"🔴 [METRICS-WORKER] Erro na coleta/envio: {e}")
        time.sleep(METRICS_INTERVAL)

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

    # NOVO: iniciar envio de métricas ao supervisor
    threading.Thread(target=metrics_sender_loop, daemon=True).start()

    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()
