import socket
import threading
import json
import time
import uuid
import random
import logging
import os

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")

logger = logging.getLogger("WORKER")

SERVER_UUID = "michel_2"
WORKER_UUID = f"WORKER-{str(uuid.uuid4())[:8]}"

HOST = "0.0.0.0"
PORT = random.randint(9000, 9999)

current_master = {"ip": "10.62.217.207", "port": 5000}

# Supervisor
SUPERVISOR_HOST = "srv.webrelay.dev"
SUPERVISOR_PORT = 33905
METRICS_INTERVAL = 10

try:
    import psutil
    PSUTIL_AVAILABLE = True
except:
    PSUTIL_AVAILABLE = False

lock = threading.Lock()
running = True
metrics = {"executadas": 0, "falhas": 0}

def enviar_json(sock, payload):
    sock.sendall((json.dumps(payload) + "\n").encode())

def receber_json(sock, timeout=5):
    sock.settimeout(timeout)
    data = b""
    try:
        while b"\n" not in data:
            part = sock.recv(4096)
            if not part:
                break
            data += part
    except:
        return None
    try:
        return json.loads(data.decode().split("\n")[0])
    except:
        return None

# ==========================================
# EXECUTAR TAREFA
# ==========================================
def executar_tarefa(task):
    time.sleep(random.uniform(1,3))
    ok = random.choice([True,True,False])

    if ok:
        metrics["executadas"] += 1
        return "OK"
    else:
        metrics["falhas"] += 1
        return "NOK"

# ==========================================
# CICLO WORKER
# ==========================================
def ciclo_worker():
    global current_master
    while running:
        try:
            s = socket.socket()
            s.settimeout(5)
            s.connect((current_master["ip"], current_master["port"]))
            enviar_json(s, {
                "WORKER": "ALIVE",
                "WORKER_UUID": WORKER_UUID,
                "port": PORT
            })
            resp = receber_json(s)
            s.close()

            if not resp:
                time.sleep(3)
                continue

            if resp.get("TASK") == "QUERY":
                status = executar_tarefa(resp["USER"])
                r2 = socket.socket()
                r2.connect((current_master["ip"], current_master["port"]))
                enviar_json(r2, {
                    "WORKER_UUID": WORKER_UUID,
                    "TASK": "QUERY",
                    "STATUS": status
                })
                r2.close()

            elif resp.get("TASK") == "NO_TASK":
                time.sleep(3)

        except:
            time.sleep(3)

# ==========================================
# MÉTRICAS DO WORKER
# ==========================================
def coletar_metricas_worker():

    ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    msg_id = str(uuid.uuid4())

    if PSUTIL_AVAILABLE:
        uptime = int(time.time() - psutil.boot_time())
        try:
            la1, la5, la15 = psutil.getloadavg()
        except:
            la1 = psutil.cpu_percent(interval=0.1)
            la5 = la1

        cpu = psutil.cpu_percent(interval=0.1)
        cpu_log = psutil.cpu_count(True)
        cpu_phy = psutil.cpu_count(False)

        vm = psutil.virtual_memory()
        mem_total = vm.total // (1024*1024)
        mem_avail = vm.available // (1024*1024)
        mem_used = round((vm.total - vm.available)/(1024**3),1)

        du = psutil.disk_usage("/")
        disk_total = round(du.total/(1024**3),1)
        disk_free = round(du.free/(1024**3),1)
        disk_used = du.percent
    else:
        uptime=10000
        la1=la5=1.0
        cpu=30
        cpu_log=4
        cpu_phy=2
        mem_total=8192
        mem_avail=4096
        mem_used=4
        disk_total=256
        disk_free=120
        disk_used=50

    neighbors = [{
        "server_uuid": current_master["ip"],
        "status": "available",
        "last_heartbeat": ts
    }]

    return {
        "server_uuid": SERVER_UUID,
        "task": "performance_report",
        "timestamp": ts,
        "mensage_id": msg_id,
        "performance": {
            "system": {
                "uptime_seconds": uptime,
                "load_average_1m": la1,
                "load_average_5m": la5,
                "cpu": {
                    "usage_percent": cpu,
                    "count_logical": cpu_log,
                    "count_physical": cpu_phy
                },
                "memory": {
                    "total_mb": mem_total,
                    "available_mb": mem_avail,
                    "percent_used": round((mem_total-mem_avail)/mem_total*100,1),
                    "memory_used": mem_used
                },
                "disk": {
                    "total_gb": disk_total,
                    "free_gb": disk_free,
                    "percent_used": disk_used
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
        "neighbors": neighbors
    }

def enviar_metricas_supervisor(payload):
    try:
        s = socket.socket()
        s.settimeout(4)
        s.connect((SUPERVISOR_HOST, SUPERVISOR_PORT))
        s.sendall((json.dumps(payload) + "\n").encode())
        s.close()
        logger.info("📤 WORKER METRIC SENT")
    except Exception as e:
        logger.warning(f"Erro ao enviar métricas worker: {e}")

def metrics_sender_loop():
    while True:
        p = coletar_metricas_worker()
        logger.info("📦 PAYLOAD WORKER:\n" + json.dumps(p, indent=4))
        enviar_metricas_supervisor(p)
        time.sleep(METRICS_INTERVAL)

# ==========================================
# SERVIDOR LOCAL DO WORKER
# ==========================================
def receber_comandos():
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((HOST, PORT))
    s.listen(10)
    logger.info(f"Worker ouvindo comandos em {HOST}:{PORT}")

    while True:
        conn, addr = s.accept()
        msg = receber_json(conn)
        conn.close()

        if not msg:
            continue

        if msg.get("TASK") == "REDIRECT":
            with lock:
                current_master["ip"] = msg["SERVER_REDIRECT"]["ip"]
                current_master["port"] = msg["SERVER_REDIRECT"]["port"]
            threading.Thread(target=ciclo_worker, daemon=True).start()

        if msg.get("TASK") == "RETURN":
            with lock:
                current_master["ip"] = msg["SERVER_RETURN"]["ip"]
                current_master["port"] = msg["SERVER_RETURN"]["port"]
            threading.Thread(target=ciclo_worker, daemon=True).start()

# ==========================================
# MAIN
# ==========================================
def main():
    logger.info(f"WORKER {WORKER_UUID} iniciado. Master inicial: {current_master}")
    threading.Thread(target=ciclo_worker, daemon=True).start()
    threading.Thread(target=metrics_sender_loop, daemon=True).start()
    threading.Thread(target=receber_comandos, daemon=True).start()

    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()
