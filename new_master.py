import socket
import threading
import json
import time
import uuid
import logging
import random
import os

# ---------------------------
# CONFIGURAÇÃO
# ---------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")

# ---------- IDENTIDADE ----------
MASTER_UUID = f"MASTER-{str(uuid.uuid4())[:8]}"  # identificador do master (pode setar um fixo)
# Mantive seu MASTER_UUID original (gerado), mas o "server_uuid" que será enviado ao supervisor é michel_1 (abaixo)
SERVER_UUID = "id_4"

HOST = "10.62.217.22"        # endereço onde o master escuta (0.0.0.0 para aceitar de qualquer interface)
PORT = 5000             # porta do master
NEIGHBORS = [("10.62.217.16", 5000), ("10.62.217.199", 5000), ("10.62.217.212", 5000)]  # lista de masters vizinhos (ip,port) - configure conforme sua rede
THRESHOLD = 10          # limite de saturação (tasks pendentes)
HEARTBEAT_INTERVAL = 5  # segundos entre heartbeats
HEARTBEAT_TIMEOUT = 15  # tempo para considerar master inativo
RELEASE_BATCH = 2       # quantos workers devolver por lote
TASK_GENERATION_INTERVAL = 6  # intervalo para gerar tarefas de teste

# ---------------------------
# ESTADO GLOBAL (protegido por lock)
# ---------------------------
lock = threading.Lock()
workers_filhos = {}        # wid -> {host, port, status, last_seen}
workers_emprestados = {}   # wid -> {host, port, status, original_master, last_seen}
pending_tasks = []         # fila de tarefas locais (cada tarefa é dict com task_id, workload)
known_masters = {}         # ip -> {"last_seen": timestamp, "uuid": uuid}
pending_releases = {}      # wid -> requester_master_ip (aguardando retorno)
logger = logging.getLogger("MASTER")

# ---------------------------
# METRICS - Supervisor endpoint e psutil
# ---------------------------
SUPERVISOR_HOST = "10.62.217.32"
SUPERVISOR_PORT = 8000   # uso a porta nova conforme última versão do documento
METRICS_INTERVAL = 10     # enviar a cada 10s

try:
    import psutil
    PSUTIL_AVAILABLE = True
    logger.info("psutil disponível: usando métricas reais.")
except Exception:
    psutil = None
    PSUTIL_AVAILABLE = False
    logger.warning("psutil NÃO disponível: usando métricas simuladas (fallback).")

# ---------------------------
# UTILITÁRIOS DE REDE/JSON
# ---------------------------
def enviar_json(sock, obj):
    """Envia JSON por socket com delimitador '\\n' (payloads oficiais exigem newline)."""
    data = json.dumps(obj) + "\n"
    sock.sendall(data.encode())

def receber_json(conn, timeout=5):
    """Recebe exatamente um JSON terminado em \\n. Retorna dict ou None."""
    conn.settimeout(timeout)
    buf = b""
    try:
        while b"\n" not in buf:
            chunk = conn.recv(4096)
            if not chunk:
                break
            buf += chunk
    except socket.timeout:
        pass
    except Exception as e:
        logger.debug(f"🟠 [receber_json] erro recv: {e}")
    if not buf:
        return None
    try:
        raw = buf.decode(errors="ignore")
        json_text = raw.split("\n", 1)[0]
        return json.loads(json_text)
    except Exception as e:
        logger.error(f"🔴 [receber_json] erro parse: {e}")
        return None

# ---------------------------
# TRATAMENTO DE CONEXÕES
# ---------------------------
def tratar_cliente(conn, addr):
    """
    Thread que trata cada conexão TCP.
    Entende todos os payloads oficiais do PDF e atualiza o estado interno.
    """
    try:
        msg = receber_json(conn)
        if not msg:
            return
        logger.info(f"📥 [RECV] {addr} -> {msg}")

        # 1.1 HEARTBEAT REQUEST (A -> B)
        if msg.get("TASK") == "HEARTBEAT" and "RESPONSE" not in msg:
            resp = {
                "SERVER_UUID": MASTER_UUID,
                "TASK": "HEARTBEAT",
                "RESPONSE": "ALIVE"
            }
            enviar_json(conn, resp)
            with lock:
                known_masters[addr[0]] = {"last_seen": time.time(), "uuid": msg.get("SERVER_UUID")}
            logger.info(f"💓 [HEARTBEAT] Req recebido de {addr[0]} - respondi ALIVE")
            return

        # 1.2 HEARTBEAT RESPONSE (B -> A)
        if msg.get("TASK") == "HEARTBEAT" and msg.get("RESPONSE") == "ALIVE":
            with lock:
                known_masters[addr[0]] = {"last_seen": time.time(), "uuid": msg.get("SERVER_UUID")}
            logger.info(f"💚 [HEARTBEAT] RESP ALIVE de {addr[0]}")
            return

        # 2.1 Worker pedindo tarefa (normal)
        if msg.get("WORKER") == "ALIVE" and "SERVER_UUID" not in msg:
            wid = msg.get("WORKER_UUID")
            port = msg.get("port", 6000)
            with lock:
                # registra novo worker se necessário
                if wid not in workers_filhos:
                    workers_filhos[wid] = {"host": addr[0], "port": port, "status": "PARADO", "last_seen": time.time()}
                    logger.info(f"🤖 [WORKER] Novo worker filho registrado: {wid} de {addr[0]}:{port}")
                else:
                    workers_filhos[wid].update({"host": addr[0], "port": port})
                    workers_filhos[wid]["status"] = "PARADO"
                    workers_filhos[wid]["last_seen"] = time.time()

                # entrega tarefa se houver
                if pending_tasks:
                    task = pending_tasks.pop(0)
                    workers_filhos[wid]["status"] = "OCUPADO"
                    resp = {"TASK": "QUERY", "USER": task.get("workload")}
                    enviar_json(conn, resp)
                    logger.info(f"📤 ✅ [TASK ENVIADA] Entregue {task['task_id']} para worker {wid} (workload={task.get('workload')})")
                else:
                    enviar_json(conn, {"TASK": "NO_TASK"})
                    logger.info(f"📭 [NO_TASK] Sem tarefa para worker {wid}")
            return

        # 2.1b Worker emprestado pedindo tarefa (informa SERVER_UUID original)
        if msg.get("WORKER") == "ALIVE" and "SERVER_UUID" in msg:
            wid = msg.get("WORKER_UUID")
            origin = msg.get("SERVER_UUID")
            port = msg.get("port", 6000)
            with lock:
                if wid not in workers_emprestados:
                    workers_emprestados[wid] = {"host": addr[0], "port": port, "status": "PARADO", "original_master": origin, "last_seen": time.time()}
                    logger.info(f"🤝 [WORKER EMPRESTADO] Registrado {wid} vindo de {origin}")
                    # notificar master de origem que worker chegou (opcional)
                    threading.Thread(target=notificar_worker_returned, args=(origin, wid), daemon=True).start()
                else:
                    workers_emprestados[wid].update({"host": addr[0], "port": port})
                    workers_emprestados[wid]["status"] = "PARADO"
                    workers_emprestados[wid]["last_seen"] = time.time()

                # entrega tarefa se houver
                if pending_tasks:
                    task = pending_tasks.pop(0)
                    workers_emprestados[wid]["status"] = "OCUPADO"
                    resp = {"TASK": "QUERY", "USER": task.get("workload")}
                    enviar_json(conn, resp)
                    logger.info(f"📤 ✅ [TASK ENVIADA] Entregue {task['task_id']} para worker emprestado {wid}")
                else:
                    enviar_json(conn, {"TASK": "NO_TASK"})
                    logger.info(f"📭 [NO_TASK] Sem tarefa para worker emprestado {wid}")
            return

        # 2.4 Worker reportando status de tarefa
        if msg.get("STATUS") in ["OK", "NOK"]:
            wid = msg.get("WORKER_UUID")
            task_type = msg.get("TASK")
            status = msg.get("STATUS")
            with lock:
                if wid in workers_filhos:
                    workers_filhos[wid]["status"] = "PARADO"
                    workers_filhos[wid]["last_seen"] = time.time()
                if wid in workers_emprestados:
                    workers_emprestados[wid]["status"] = "PARADO"
                    workers_emprestados[wid]["last_seen"] = time.time()
            # confirmar recebimento
            enviar_json(conn, {"STATUS": "ACK"})
            if status == "OK":
                logger.info(f"✅ [TASK COMPLETA] Worker {wid} finalizou {task_type} com STATUS=OK")
            else:
                logger.warning(f"⚠️ [TASK FALHOU] Worker {wid} finalizou {task_type} com STATUS=NOK")
            return

        # 3.1 Pedido de workers (WORKER_REQUEST) - outro Master solicitando suporte
        if msg.get("TASK") == "WORKER_REQUEST":
            req_info = msg.get("REQUESTOR_INFO", {})
            req_ip = req_info.get("ip", addr[0])
            req_port = req_info.get("port", 5000)
            logger.info(f"🤝 [WORKER_REQUEST] Pedido de {req_ip}:{req_port}")

            with lock:
                available = [(wid, w) for wid, w in workers_filhos.items() if w["status"] == "PARADO"]

            if available:
                # oferece até dois workers (ou quantos disponíveis)
                to_offer = available[:min(2, len(available))]
                offered_uuids = []
                for wid, w in to_offer:
                    offered_uuids.append(wid)
                    with lock:
                        workers_filhos[wid]["status"] = "TRANSFERIDO"
                    # envia ordem REDIRECT ao worker (2.6)
                    threading.Thread(target=enviar_comando_redirecionar, args=(wid, w, req_ip, req_port), daemon=True).start()

                resp = {"SERVER_UUID": MASTER_UUID, "RESPONSE": "AVAILABLE", "WORKERS_UUID": offered_uuids}
                enviar_json(conn, resp)
                logger.info(f"🤝 ✅ [WORKER_RESPONSE] AVAILABLE -> {offered_uuids}")
            else:
                resp = {"SERVER_UUID": MASTER_UUID, "RESPONSE": "UNAVAILABLE"}
                enviar_json(conn, resp)
                logger.info("🤝 ❌ [WORKER_RESPONSE] UNAVAILABLE")
            return

        # 4.1 Comando de liberação (COMMAND_RELEASE) - outro master avisando que vai devolver
        if msg.get("TASK") == "COMMAND_RELEASE":
            requester_uuid = msg.get("SERVER_UUID")
            worker_list = msg.get("WORKERS_UUID", [])
            requestor_info = msg.get("REQUESTOR_INFO", {})
            logger.info(f"🔄 [COMMAND_RELEASE] Recebido release de {requester_uuid} -> workers {worker_list}")

            # responder com RELEASE_ACK (4.2)
            ack = {"SERVER_UUID": MASTER_UUID, "RESPONSE": "RELEASE_ACK", "WORKERS_UUID": worker_list}
            enviar_json(conn, ack)
            logger.info(f"🔁 ✅ [RELEASE_ACK] Enviado para {requester_uuid}")

            # marcar pendentes e ordenar retorno localmente (enviar RETURN aos workers)
            threading.Thread(target=ordenar_retorno_workers, args=(worker_list, addr[0], requestor_info.get("port", PORT)), daemon=True).start()
            return

        # 4.3 Recebimento de RELEASE_COMPLETED (origin master foi notificado de retorno)
        if msg.get("RESPONSE") == "RELEASE_COMPLETED":
            origin_uuid = msg.get("SERVER_UUID")
            worker_list = msg.get("WORKERS_UUID", [])
            logger.info(f"🔔 [RELEASE_COMPLETED] {origin_uuid} informou retorno de {worker_list}")
            # atualiza estado local: workers que foram marcados como TRANSFERIDO que retornaram passam a PARADO
            with lock:
                for wid in worker_list:
                    if wid in workers_filhos and workers_filhos[wid]["status"] == "TRANSFERIDO":
                        workers_filhos[wid]["status"] = "PARADO"
                        workers_filhos[wid]["last_seen"] = time.time()
                        logger.info(f"🟢 [DISPONÍVEL] Worker {wid} agora PARADO")
            return

        # Caso mensagem não reconhecida
        logger.warning(f"❓ [TRATAR] Payload não reconhecido: {msg}")

    except Exception as e:
        logger.error(f"🔴 [ERRO tratar_cliente] {e}")
    finally:
        try:
            conn.close()
        except:
            pass

# ---------------------------
# AÇÕES DE EMPRESTIMO / REDIRECIONAMENTO / DEVOLUÇÃO
# ---------------------------
def enviar_comando_redirecionar(worker_id, worker_info, target_ip, target_port):
    payload = {"TASK": "REDIRECT", "SERVER_REDIRECT": {"ip": target_ip, "port": target_port}}
    try:
        with socket.socket() as s:
            s.settimeout(5)
            s.connect((worker_info["host"], worker_info["port"]))
            enviar_json(s, payload)
        logger.info(f"📤 🔀 [REDIRECT] Enviado REDIRECT para worker {worker_id} -> {target_ip}:{target_port}")
    except Exception as e:
        logger.error(f"🔴 [REDIRECT] erro ao enviar para worker {worker_id}: {e}")
        with lock:
            if worker_id in workers_filhos and workers_filhos[worker_id]["status"] == "TRANSFERIDO":
                workers_filhos[worker_id]["status"] = "PARADO"
                logger.info(f"↩️ [REDIRECT_FAIL] Revertido estado de {worker_id} para PARADO")

def ordenar_retorno_workers(worker_ids, return_ip, return_port):
    for wid in worker_ids:
        with lock:
            w = workers_emprestados.get(wid) or workers_filhos.get(wid)
            if not w:
                logger.warning(f"⚠️ [RETURN] Worker {wid} não encontrado para RETURN")
                continue
        payload = {"TASK": "RETURN", "SERVER_RETURN": {"ip": return_ip, "port": return_port}}
        try:
            with socket.socket() as s:
                s.settimeout(5)
                s.connect((w["host"], w["port"]))
                enviar_json(s, payload)
            logger.info(f"🔁 📤 [RETURN] Enviado RETURN para worker {wid} -> {return_ip}:{return_port}")
            with lock:
                if wid in workers_emprestados:
                    workers_emprestados.pop(wid, None)
                    logger.info(f"🗃️ [RETURN] Worker {wid} removido de emprestados")
        except Exception as e:
            logger.error(f"🔴 [RETURN] erro ao enviar RETURN para {wid}: {e}")

def notificar_worker_returned(origin_master_info, worker_id):
    target = None
    try:
        if isinstance(origin_master_info, str) and ":" in origin_master_info:
            h, p = origin_master_info.split(":")
            target = (h, int(p))
        elif isinstance(origin_master_info, str):
            target = (origin_master_info, PORT)
    except Exception:
        target = None
    if not target:
        if NEIGHBORS:
            target = NEIGHBORS[0]
        else:
            logger.warning("⚠️ [NOTIFICAR] Sem neighbor para notificar retorno")
            return

    payload = {"SERVER_UUID": MASTER_UUID, "RESPONSE": "RELEASE_COMPLETED", "WORKERS_UUID": [worker_id]}
    try:
        with socket.socket() as s:
            s.settimeout(5)
            s.connect(target)
            enviar_json(s, payload)
        logger.info(f"🔔 🤝 [NOTIFICAR] Enviado RELEASE_COMPLETED para {target} sobre {worker_id}")
    except Exception as e:
        logger.error(f"🔴 [NOTIFICAR] erro ao notificar {target}: {e}")

# ---------------------------
# FLUXOS ENTRE MASTERS (mantidos)
# ---------------------------
def solicitar_suporte():
    if not NEIGHBORS:
        logger.warning("⚠️ [SOLICITAR] Sem vizinhos configurados")
        return
    target = NEIGHBORS[0]
    payload = {"TASK": "WORKER_REQUEST", "REQUESTOR_INFO": {"ip": HOST, "port": PORT}}
    try:
        with socket.socket() as s:
            s.settimeout(5)
            s.connect(target)
            enviar_json(s, payload)
            resp = receber_json(s)
        if resp and resp.get("RESPONSE") == "AVAILABLE":
            offered = resp.get("WORKERS_UUID", [])
            logger.info(f"🤝 ✅ [SUPORTE] Vizinho forneceu workers: {offered}")
        elif resp and resp.get("RESPONSE") == "UNAVAILABLE":
            logger.warning("🤝 ❌ [SUPORTE] Vizinho respondeu UNAVAILABLE")
        else:
            logger.warning("🤝 ⚠️ [SUPORTE] Resposta inesperada ou ausência de resposta")
    except Exception as e:
        logger.error(f"🔴 [SOLICITAR] erro ao solicitar suporte: {e}")

def enviar_comando_release(origin_master_addr, worker_ids):
    target = origin_master_addr if origin_master_addr else (NEIGHBORS[0] if NEIGHBORS else None)
    if not target:
        logger.warning("⚠️ [RELEASE] Sem target para send_release_command")
        return
    payload = {"SERVER_UUID": MASTER_UUID, "TASK": "COMMAND_RELEASE", "WORKERS_UUID": worker_ids, "REQUESTOR_INFO": {"ip": HOST, "port": PORT}}
    try:
        with socket.socket() as s:
            s.settimeout(5)
            s.connect(target)
            enviar_json(s, payload)
            resp = receber_json(s)
        if resp and resp.get("RESPONSE") == "RELEASE_ACK":
            logger.info(f"🔁 ✅ [RELEASE] RELEASE_ACK recebido de {target}")
        else:
            logger.warning("🔁 ⚠️ [RELEASE] ACK não recebido ou resposta inesperada")
    except Exception as e:
        logger.error(f"🔴 [RELEASE] erro ao enviar COMMAND_RELEASE: {e}")

def liberar_workers_emprestados(borrowed_ids):
    by_origin = {}
    with lock:
        for wid in borrowed_ids:
            if wid in workers_emprestados:
                origin = workers_emprestados[wid].get("original_master")
                if origin:
                    by_origin.setdefault(origin, []).append(wid)
    for origin, wids in by_origin.items():
        for i in range(0, len(wids), RELEASE_BATCH):
            batch = wids[i:i+RELEASE_BATCH]
            target = None
            try:
                if ":" in origin:
                    h, p = origin.split(":")
                    target = (h, int(p))
                else:
                    target = (origin, PORT)
            except:
                target = None
            threading.Thread(target=enviar_comando_release, args=(target, batch), daemon=True).start()
            logger.info(f"🔁 📦 [LIBERAR] Enviando RELEASE para {origin} -> {batch}")
            time.sleep(0.3)

# ---------------------------
# MONITORAMENTO / DISTRIBUIÇÃO / GERAÇÃO DE TAREFAS (simulação)
# ---------------------------
def monitorar_carga():
    while True:
        with lock:
            pend = len(pending_tasks)
            borrowed = list(workers_emprestados.keys())
        if pend >= THRESHOLD:
            logger.warning(f"🟡 [LOAD] Saturação detectada: {pend} pendentes (>= {THRESHOLD}). Solicitando suporte.")
            solicitar_suporte()
        elif pend < THRESHOLD and borrowed:
            logger.info(f"🟢 [LOAD] Normalizado ({pend} pendentes). Iniciando liberação de {len(borrowed)} workers.")
            liberar_workers_emprestados(borrowed)
        time.sleep(3)

def distribuir_tarefas_loop():
    while True:
        time.sleep(1)

def gerar_tarefas_simulacao():
    i = 1
    while True:
        task = {"task_id": f"K{i}", "workload": [random.randint(1, 100)]}
        with lock:
            pending_tasks.append(task)
        logger.info(f"🆕 🧾 [GERADOR] Nova tarefa criada: {task['task_id']}")
        i += 1
        time.sleep(TASK_GENERATION_INTERVAL)

# ---------------------------
# HEARTBEAT ENTRE MASTERS
# ---------------------------
def heartbeat_loop():
    while True:
        for n in NEIGHBORS:
            try:
                with socket.socket() as s:
                    s.settimeout(5)
                    s.connect(n)
                    enviar_json(s, {"SERVER_UUID": MASTER_UUID, "TASK": "HEARTBEAT"})
                    resp = receber_json(s)
                if resp and resp.get("RESPONSE") == "ALIVE":
                    with lock:
                        known_masters[n[0]] = {"last_seen": time.time(), "uuid": resp.get("SERVER_UUID")}
                    logger.info(f"💓 [HEARTBEAT] {n} respondeu ALIVE")
                else:
                    logger.debug(f"💤 [HEARTBEAT] Sem resposta ALIVE de {n}")
            except Exception as e:
                logger.warning(f"⚠️ [HEARTBEAT] falha ao contactar {n}: {e}")
        time.sleep(HEARTBEAT_INTERVAL)

def monitorar_known_masters():
    while True:
        now = time.time()
        with lock:
            inativos = [ip for ip, info in list(known_masters.items()) if now - info.get("last_seen", 0) > HEARTBEAT_TIMEOUT]
            for ip in inativos:
                logger.warning(f"🔕 [MASTER] {ip} considerado inativo. Removendo.")
                known_masters.pop(ip, None)
        time.sleep(5)

# ---------------------------
# SERVIDOR TCP PRINCIPAL
# ---------------------------
# def iniciar_servidor_master():
#     s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#     s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
#     s.bind((HOST, PORT))
#     s.listen(20)
#     logger.info(f"🚀 [SERVER] Master {MASTER_UUID} escutando em {HOST}:{PORT}")
#     try:
#         while True:
#             conn, addr = s.accept()
#             threading.Thread(target=tratar_cliente, args=(conn, addr), daemon=True).start()
#     except Exception as e:
#         logger.error(f"🔴 [SERVER] erro no loop principal: {e}")
#     finally:
#         s.close()

# ---------------------------
# --- NOVAS FUNÇÕES ADICIONADAS: montagem/enfileiramento/envio das métricas (sem alterar estrutura)
# ---------------------------
def coletar_metricas_master():
    """Gera payload no formato requerido pelo dashboard (usa psutil quando disponível)."""
    ts_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    mensage_id = str(uuid.uuid4())
    # performance.system
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
        mem_total_mb = 16384
        mem_available_mb = random.randint(1024, mem_total_mb)
        mem_percent_used = round(100 * (mem_total_mb - mem_available_mb) / mem_total_mb, 1)
        mem_used_gb = round((mem_total_mb - mem_available_mb) / 1024, 1)
        disk_total_gb = 512.0
        disk_free_gb = round(random.uniform(10, 500), 1)
        disk_percent_used = round(100 * (disk_total_gb - disk_free_gb) / disk_total_gb, 1)

    # farm_state (coletar do estado interno do master)
    with lock:
        total_registered = len(workers_filhos)
        workers_utilization = sum(1 for w in workers_filhos.values() if w.get("status") == "OCUPADO")
        workers_recieved = len(workers_emprestados)
        # nova regra explicita: workers_alive = workers_utilization + workers_recieved
        workers_alive = workers_utilization + workers_recieved
        workers_idle = sum(1 for w in workers_filhos.values() if w.get("status") == "PARADO")
        workers_borrowed = len(workers_emprestados)
        # workers_failed: sem heartbeat > 60s
        now = time.time()
        workers_failed = sum(1 for w in workers_filhos.values() if now - w.get("last_seen", now) > 60)
        tasks_pending = len(pending_tasks)
        tasks_running = workers_utilization

    # neighbors: known_masters
    neighbors_list = []
    with lock:
        for ip, info in known_masters.items():
            last_hb = info.get("last_seen")
            if last_hb:
                last_hb_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(last_hb))
            else:
                last_hb_iso = None
            neighbors_list.append({
                "server_uuid": info.get("uuid") or ip,
                "status": "available" if (time.time() - info.get("last_seen", 0) < HEARTBEAT_TIMEOUT) else "unavailable",
                "last_heartbeat": last_hb_iso
            })

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
            "max_task": THRESHOLD
        },
        "neighbors": neighbors_list,
        },
       
    }

    return payload

def enviar_metricas_para_supervisor(payload):
    """Envia apenas SEND para o supervisor (sem aguardar resposta)."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(5)
            s.connect((SUPERVISOR_HOST, SUPERVISOR_PORT))
            s.sendall((json.dumps(payload) + "\n").encode())
        logger.info(f"📤 [METRICS] Métricas enviadas ao supervisor: {payload['mensage_id']}")
    except Exception as e:
        logger.warning(f"⚠️ [METRICS] Falha ao enviar métricas: {e}")

def metrics_sender_loop():
    """Thread para coletar e enviar métricas a cada METRICS_INTERVAL segundos; também loga o payload no terminal."""
    while True:
        try:
            payload = coletar_metricas_master()
            # log para debug/local (mostra exatamente o que será enviado)
            logger.info("📦 PAYLOAD MASTER GERADO:\n" + json.dumps(payload, indent=4))
            enviar_metricas_para_supervisor(payload)
        except Exception as e:
            logger.error(f"🔴 [METRICS] Erro na coleta/envio: {e}")
        time.sleep(METRICS_INTERVAL)

# ---------------------------
# SERVIDOR TCP PRINCIPAL (mantive como no seu código original)
# ---------------------------
def iniciar_servidor_master():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((HOST, PORT))
    s.listen(20)
    logger.info(f"🚀 [SERVER] Master {MASTER_UUID} escutando em {HOST}:{PORT}")
    try:
        while True:
            conn, addr = s.accept()
            threading.Thread(target=tratar_cliente, args=(conn, addr), daemon=True).start()
    except Exception as e:
        logger.error(f"🔴 [SERVER] erro no loop principal: {e}")
    finally:
        s.close()

# ---------------------------
# FUNÇÃO PRINCIPAL (mantive seu main e apenas adicionei a thread de metrics)
# ---------------------------
def main():
    # Threads principais originais
    threading.Thread(target=iniciar_servidor_master, daemon=True).start()
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    threading.Thread(target=monitorar_known_masters, daemon=True).start()
    threading.Thread(target=distribuir_tarefas_loop, daemon=True).start()
    threading.Thread(target=monitorar_carga, daemon=True).start()
    # Gerador de tarefas para simulação - comente se não quiser simular carga
    threading.Thread(target=gerar_tarefas_simulacao, daemon=True).start()

    # NOVO: iniciar thread de envio de métricas ao supervisor (não bloqueante)
    threading.Thread(target=metrics_sender_loop, daemon=True).start()

    # Log periódico de status (mantive)
    while True:
        with lock:
            logger.info(f"📊 [STATUS] Pendentes: {len(pending_tasks)} | Filhos: {len(workers_filhos)} | Emprestados: {len(workers_emprestados)} | Masters ativos: {len(known_masters)}")
        time.sleep(10)


if __name__ == "__main__":
    print("Iniciando Master (protocolo conforme PDF) — versão com emojis. Rode: python3 master_sd_emojis.py")
    main()
