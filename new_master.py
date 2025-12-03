import socket
import threading
import json
import time
import uuid
import logging
import random
import os

# ==========================================
# CONFIGURAÇÃO
# ==========================================
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")

# Nome fixo da farm/master
SERVER_UUID = "michel_1"

HOST = "0.0.0.0"
PORT = 5000

# Supervisor
SUPERVISOR_HOST = "srv.webrelay.dev"
SUPERVISOR_PORT = 33905      # <<< porta nova
METRICS_INTERVAL = 10        # enviar a cada 10s

# Lógica interna existente
NEIGHBORS = []
HEARTBEAT_TIMEOUT = 60

lock = threading.Lock()

workers_filhos = {}       # wid -> {host,port,status,last_seen}
workers_emprestados = {}
pending_tasks = []
known_masters = {}        # ip -> {uuid,last_seen}

logger = logging.getLogger("MASTER")

# ==========================================
# psutil (real metrics)
# ==========================================
try:
    import psutil
    PSUTIL_AVAILABLE = True
except:
    PSUTIL_AVAILABLE = False

# ==========================================
# JSON encoding util
# ==========================================
def enviar_json(sock, obj):
    sock.sendall((json.dumps(obj) + "\n").encode())

def receber_json(sock, timeout=5):
    sock.settimeout(timeout)
    data = b""
    try:
        while b"\n" not in data:
            chunk = sock.recv(4096)
            if not chunk:
                break
            data += chunk
    except:
        return None
    if not data:
        return None
    try:
        return json.loads(data.decode().split("\n")[0])
    except:
        return None

# ==========================================
# COLETAR MÉTRICAS MASTER
# ==========================================
def coletar_metricas_master():
    ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    msg_id = str(uuid.uuid4())

    # SYSTEM METRICS
    if PSUTIL_AVAILABLE:
        uptime_seconds = int(time.time() - psutil.boot_time())

        try:
            la1, la5, la15 = psutil.getloadavg()
        except:
            la1 = psutil.cpu_percent(interval=0.1)
            la5 = la1

        cpu_usage = psutil.cpu_percent(interval=0.1)
        cpu_logical = psutil.cpu_count(logical=True) or 1
        cpu_physical = psutil.cpu_count(logical=False) or cpu_logical

        vm = psutil.virtual_memory()
        mem_total_mb = vm.total // (1024 * 1024)
        mem_available_mb = vm.available // (1024 * 1024)
        mem_percent_used = vm.percent
        mem_used = round((vm.total - vm.available) / (1024**3), 1)

        du = psutil.disk_usage("/")
        disk_total_gb = round(du.total / (1024**3), 1)
        disk_free_gb = round(du.free / (1024**3), 1)
        disk_percent = du.percent
    else:
        uptime_seconds = random.randint(10000, 200000)
        la1 = la5 = 1.0
        cpu_usage = random.uniform(10, 90)
        cpu_logical = 4
        cpu_physical = 2
        mem_total_mb = 8192
        mem_available_mb = random.randint(1024, 8192)
        mem_percent_used = round((mem_total_mb - mem_available_mb)/mem_total_mb*100,1)
        mem_used = round((mem_total_mb - mem_available_mb)/1024, 1)
        disk_total_gb = 512
        disk_free_gb = random.uniform(10, 500)
        disk_percent = round((disk_total_gb - disk_free_gb)/disk_total_gb*100,1)

    # FARM STATE
    with lock:
        total_registered = len(workers_filhos)
        workers_utilization = sum(1 for w in workers_filhos.values() if w["status"] == "OCUPADO")
        workers_recieved = len(workers_emprestados)
        workers_alive = workers_utilization + workers_recieved    # NOVA REGRA
        workers_idle = sum(1 for w in workers_filhos.values() if w["status"] == "PARADO")
        workers_borrowed = len(workers_emprestados)

        # FAILED: worker sem contato > 60s
        now = time.time()
        workers_failed = sum(1 for w in workers_filhos.values()
                             if now - w.get("last_seen", now) > 60)

        tasks_pending = len(pending_tasks)
        tasks_running = workers_utilization

    # NEIGHBORS
    neighbors = []
    with lock:
        for ip, info in known_masters.items():
            hb_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                   time.gmtime(info["last_seen"]))
            neighbors.append({
                "server_uuid": info["uuid"],
                "status": "available"
                          if (time.time() - info["last_seen"]) < HEARTBEAT_TIMEOUT
                          else "unavailable",
                "last_heartbeat": hb_iso
            })

    return {
        "server_uuid": SERVER_UUID,
        "task": "performance_report",
        "timestamp": ts,
        "mensage_id": msg_id,   # ERRADO NO DOCUMENTO, MAS É ASSIM MESMO
        "performance": {
            "system": {
                "uptime_seconds": uptime_seconds,
                "load_average_1m": la1,
                "load_average_5m": la5,
                "cpu": {
                    "usage_percent": cpu_usage,
                    "count_logical": cpu_logical,
                    "count_physical": cpu_physical
                },
                "memory": {
                    "total_mb": mem_total_mb,
                    "available_mb": mem_available_mb,
                    "percent_used": mem_percent_used,
                    "memory_used": mem_used
                },
                "disk": {
                    "total_gb": disk_total_gb,
                    "free_gb": disk_free_gb,
                    "percent_used": disk_percent
                }
            }
        },
        "farm_state": {
            "workers": {
                "total_registered": total_registered,
                "workers_utilization": workers_utilization,
                "workers_alive": workers_alive,
                "workers_idle": workers_idle,
                "workers_borrowed": workers_borrowed,
                "workers_recieved": workers_recieved,
                "workers_failed": workers_failed
            },
            "tasks": {
                "tasks_pending": tasks_pending,
                "tasks_running": tasks_running
            }
        },
        "config_thresholds": {
            "max_task": 100
        },
        "neighbors": neighbors
    }

# ==========================================
# ENVIO SEM RECV
# ==========================================
def enviar_metricas_supervisor(payload):
    try:
        s = socket.socket()
        s.settimeout(4)
        s.connect((SUPERVISOR_HOST, SUPERVISOR_PORT))
        s.sendall((json.dumps(payload) + "\n").encode())
        s.close()
        logger.info("📤 MASTER METRIC SENT")
    except Exception as e:
        logger.warning(f"[SUPERVISOR] erro ao enviar: {e}")

def metrics_sender_loop():
    while True:
        payload = coletar_metricas_master()

        # DEBUG: mostrar no terminal
        logger.info("📦 PAYLOAD MASTER:\n" + json.dumps(payload, indent=4))

        enviar_metricas_supervisor(payload)
        time.sleep(METRICS_INTERVAL)

# ==========================================
# TRATAMENTO DE WORKERS (estrutura original simplificada)
# ==========================================
def tratar_cliente(conn, addr):
    msg = receber_json(conn)
    if not msg:
        return

    wid = msg.get("WORKER_UUID")
    if wid:
        with lock:
            if wid not in workers_filhos:
                workers_filhos[wid] = {
                    "host": addr[0],
                    "port": msg.get("port", 5000),
                    "status": "PARADO",
                    "last_seen": time.time()
                }
            else:
                workers_filhos[wid]["last_seen"] = time.time()

    if msg.get("WORKER") == "ALIVE":
        with lock:
            if pending_tasks:
                task = pending_tasks.pop(0)
                workers_filhos[wid]["status"] = "OCUPADO"
                enviar_json(conn, {"TASK": "QUERY", "USER": task["workload"]})
            else:
                enviar_json(conn, {"TASK": "NO_TASK"})
        return

    if msg.get("STATUS") in ["OK", "NOK"]:
        with lock:
            if wid in workers_filhos:
                workers_filhos[wid]["status"] = "PARADO"
        enviar_json(conn, {"STATUS": "ACK"})
        return

# ==========================================
# SERVIDOR TCP
# ==========================================
def servidor_master():
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((HOST, PORT))
    s.listen(30)
    logger.info(f"MASTER michel_1 rodando em {HOST}:{PORT}")

    while True:
        conn, addr = s.accept()
        threading.Thread(target=tratar_cliente, args=(conn, addr), daemon=True).start()

# ==========================================
# MAIN
# ==========================================
def main():
    # threads principais
    threading.Thread(target=servidor_master, daemon=True).start()
    threading.Thread(target=metrics_sender_loop, daemon=True).start()

    while True:
        with lock:
            logger.info(f"STATUS -> workers={len(workers_filhos)} tarefas={len(pending_tasks)}")
        time.sleep(10)

if __name__ == "__main__":
    main()
