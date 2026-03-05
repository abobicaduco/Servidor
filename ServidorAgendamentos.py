# -*- coding: utf-8 -*-
"""
C6 CRON SERVER v2.2.0 — Servidor Orquestrador de Automações Python
===================================================================
Backend Python puro com Flask + Flask-SocketIO.

Funcionalidades:
  - Localhost only (127.0.0.1) — sem exposição de IP.
  - Configuração portátil via .env (funciona em qualquer PC).
  - Path.home() como base para todos os caminhos.
  - SQLite (WAL) para rastreamento de execuções — SINGLE SOURCE OF TRUTH.
  - Scheduler com catch-up e slots por hora (timezone SP).
  - Máximo N processos concorrentes (padrão 10).
  - Timeout automático: scripts rodando >2h são encerrados.
  - Workflow engine (execução sequencial com bloqueio).
  - inactivated_at / NaT handling.
  - Logs estruturados (console + arquivo rotativo).
  - WebSocket real-time push (Flask-SocketIO).
  - LGPD compliance headers e avisos.
  - Thread/PID isolation total (RLock + DB lock separado).

Retorno: 0=OK | 1=ERRO
"""

from __future__ import annotations
import sys, os, subprocess, importlib.util

# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------
def bootstrap():
    pkgs = {
        "flask": "flask", "flask_socketio": "flask-socketio",
        "pandas": "pandas", "openpyxl": "openpyxl",
        "psutil": "psutil", "pytz": "pytz", "dotenv": "python-dotenv",
    }
    miss = [p for m, p in pkgs.items() if importlib.util.find_spec(m) is None]
    if miss:
        print(f"[BOOTSTRAP] Instalando: {', '.join(miss)}...")
        try:
            subprocess.check_call(
                [sys.executable, "-m", "pip", "install"] + miss +
                ["--no-input", "--quiet", "--upgrade"])
        except Exception as e:
            print(f"[BOOTSTRAP] FALHA: {e}", file=sys.stderr); sys.exit(1)

bootstrap()

# ---------------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------------
import json, time, copy, shutil, sqlite3, logging, secrets, threading, traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from logging.handlers import RotatingFileHandler

import pandas as pd
import psutil
import pytz
from dotenv import load_dotenv
from flask import Flask, jsonify, request, send_from_directory, Response
from flask_socketio import SocketIO

# ---------------------------------------------------------------------------
# .env
# ---------------------------------------------------------------------------
_env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=str(_env_path))

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
HOME = Path.home()
TZ_SP = pytz.timezone(os.getenv("TIMEZONE", "America/Sao_Paulo"))

_def_base = str(
    HOME / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A"
    / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano"
)
BASE_PATH       = Path(os.getenv("BASE_PATH", _def_base))
SCRIPTS_DIR     = Path(os.getenv("SCRIPTS_DIR", str(BASE_PATH / "automacoes")))
NEW_SERVER_DIR  = Path(os.getenv("NEW_SERVER_DIR", str(BASE_PATH / "novo_servidor")))
EXCEL_PATH      = Path(os.getenv("EXCEL_PATH", str(NEW_SERVER_DIR / "registro_automacoes.xlsx")))

DB_DIR          = NEW_SERVER_DIR / "db"
LOG_DIR         = NEW_SERVER_DIR / "logs"
STATIC_DIR      = Path(__file__).resolve().parent / "static"
WORKFLOWS_FILE  = NEW_SERVER_DIR / "workflows.json"

for d in (DB_DIR, LOG_DIR, STATIC_DIR):
    d.mkdir(parents=True, exist_ok=True)

DB_FILE         = str(DB_DIR / "orchestrator.db")
LOG_FILE        = LOG_DIR / "orchestrator.log"

MAX_CONCURRENT  = int(os.getenv("MAX_CONCURRENT", "10"))
SERVER_PORT     = int(os.getenv("SERVER_PORT", "3000"))
SERVER_HOST     = os.getenv("SERVER_HOST", "127.0.0.1")
TIMEOUT_SECONDS = int(os.getenv("SCRIPT_TIMEOUT_SECONDS", str(2 * 60 * 60)))  # 2h
APP_VERSION     = "2.2.0"

# ---------------------------------------------------------------------------
# Logger
# ---------------------------------------------------------------------------
def _build_logger() -> logging.Logger:
    lg = logging.getLogger("C6Cron")
    lg.setLevel(logging.DEBUG)
    lg.propagate = False
    for h in list(lg.handlers): lg.removeHandler(h)
    fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")
    ch = logging.StreamHandler(sys.stdout); ch.setLevel(logging.INFO); ch.setFormatter(fmt)
    lg.addHandler(ch)
    fh = RotatingFileHandler(str(LOG_FILE), maxBytes=10*1024*1024, backupCount=5, encoding="utf-8")
    fh.setLevel(logging.DEBUG); fh.setFormatter(fmt)
    lg.addHandler(fh)
    return lg

logger = _build_logger()

# ===========================================================================
# SQLite — SINGLE SOURCE OF TRUTH para execuções
# ===========================================================================
# O DB nunca é resetado durante o dia. Cada script+date+slot tem NO MÁXIMO
# uma row. O status final dessa row determina se o slot foi coberto.
# statuses que "cobrem" um slot: success, catchup_skipped, running, killed, error, timeout
# Ou seja, se QUALQUER status existe para script+date+slot, o scheduler
# NÃO reenfileira. Isso elimina o bug de re-execução.
# ===========================================================================

_db_lock = threading.Lock()

def _db() -> sqlite3.Connection:
    c = sqlite3.connect(DB_FILE, timeout=15)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=5000")
    return c

def init_db():
    with _db_lock, _db() as c:
        c.execute("""CREATE TABLE IF NOT EXISTS executions (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            script_name TEXT    NOT NULL,
            date        TEXT    NOT NULL,
            hour_slot   INTEGER NOT NULL,
            executed_at TEXT    NOT NULL,
            finished_at TEXT,
            status      TEXT    NOT NULL,
            requester   TEXT    NOT NULL DEFAULT 'AGENDADO',
            return_code INTEGER,
            duration_s  INTEGER
        )""")
        c.execute("CREATE INDEX IF NOT EXISTS ix_sd ON executions(script_name, date)")
        # Coluna finished_at pode não existir em DBs antigos
        try: c.execute("ALTER TABLE executions ADD COLUMN finished_at TEXT")
        except Exception: pass
        c.commit()
    logger.info(f"DB  | SQLite: {DB_FILE}")

def db_slot_exists(name: str, slot: int) -> bool:
    """Verifica se JÁ EXISTE qualquer registro para script+date+slot hoje."""
    today = datetime.now(TZ_SP).strftime("%Y-%m-%d")
    with _db_lock, _db() as c:
        cur = c.execute(
            "SELECT COUNT(*) FROM executions WHERE script_name=? AND date=? AND hour_slot=?",
            (name, today, slot))
        return cur.fetchone()[0] > 0

def db_mark(name: str, slot: int, status: str,
            requester: str = "AGENDADO", rc: Optional[int] = None,
            dur: Optional[int] = None):
    """Registra ou atualiza execução. UPSERT por script+date+slot."""
    today = datetime.now(TZ_SP).strftime("%Y-%m-%d")
    now_s = datetime.now(TZ_SP).strftime("%Y-%m-%d %H:%M:%S")
    fin = now_s if status in ("success", "error", "killed", "timeout") else None
    with _db_lock, _db() as c:
        existing = c.execute(
            "SELECT id, status FROM executions WHERE script_name=? AND date=? AND hour_slot=?",
            (name, today, slot)).fetchone()
        if existing:
            c.execute(
                "UPDATE executions SET status=?, finished_at=?, return_code=?, "
                "duration_s=?, requester=? WHERE id=?",
                (status, fin, rc, dur, requester, existing[0]))
        else:
            c.execute(
                "INSERT INTO executions "
                "(script_name,date,hour_slot,executed_at,finished_at,status,requester,return_code,duration_s) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (name, today, slot, now_s, fin, status, requester, rc, dur))
        c.commit()
    logger.debug(f"DB  | [{name}] slot={slot} status={status} req={requester}")

def db_covered_slots(name: str) -> Set[int]:
    """Retorna TODOS os slots que já tem registro hoje (qualquer status)."""
    today = datetime.now(TZ_SP).strftime("%Y-%m-%d")
    with _db_lock, _db() as c:
        cur = c.execute(
            "SELECT DISTINCT hour_slot FROM executions WHERE script_name=? AND date=?",
            (name, today))
        return {r[0] for r in cur.fetchall()}

def db_stats_today(name: str) -> dict:
    today = datetime.now(TZ_SP).strftime("%Y-%m-%d")
    with _db_lock, _db() as c:
        cur = c.execute(
            "SELECT requester, status FROM executions "
            "WHERE script_name=? AND date=? AND status NOT IN ('catchup_skipped')",
            (name, today))
        rows = cur.fetchall()
    ag = sum(1 for r in rows if r[0] == "AGENDADO")
    mn = sum(1 for r in rows if r[0] != "AGENDADO")
    ls = rows[-1][1] if rows else None
    return {"agendamento": ag, "manual": mn, "total": ag + mn, "last_status": ls}

def db_history(name: str, limit: int = 60) -> List[dict]:
    with _db_lock, _db() as c:
        c.row_factory = sqlite3.Row
        cur = c.execute(
            "SELECT * FROM executions WHERE script_name=? ORDER BY id DESC LIMIT ?",
            (name, limit))
        return [dict(r) for r in cur.fetchall()]

# ===========================================================================
# Models
# ===========================================================================
class ScriptModel:
    __slots__ = (
        "name", "path", "area_name", "schedule_slots", "is_active",
        "inactivated_at", "status", "active_pid", "start_time",
        "active_slot", "current_requester", "emails_principal",
        "emails_cc", "move_file", "created_at",
    )
    def __init__(self, name: str, path: Path):
        self.name = name; self.path = path
        self.area_name = "Sem Area"; self.schedule_slots: List[int] = []
        self.is_active = False; self.inactivated_at: Optional[str] = None
        self.status = "idle"; self.active_pid: Optional[int] = None
        self.start_time: Optional[float] = None; self.active_slot = 0
        self.current_requester: Optional[str] = None
        self.emails_principal = ""; self.emails_cc = ""
        self.move_file = False; self.created_at: Optional[str] = None

    def to_dict(self) -> dict:
        covered = db_covered_slots(self.name)
        stats = db_stats_today(self.name)
        now_h = datetime.now(TZ_SP).hour
        # next_run: próximo slot NÃO coberto
        next_run = "-"; is_delayed = False
        for sl in self.schedule_slots:
            if sl not in covered:
                if sl > now_h:
                    next_run = f"{sl}h"; break
                else:
                    next_run = f"{sl}h (Atrasado)"; is_delayed = True; break
        dur = int(time.time() - self.start_time) if self.start_time and self.status == "running" else 0
        # Contagem: quantos slots estão cobertos VERSUS total de slots
        total_slots = len(self.schedule_slots)
        covered_scheduled = len(covered & set(self.schedule_slots))
        return {
            "name": self.name, "path": str(self.path), "area": self.area_name,
            "is_active": self.is_active, "inactivated_at": self.inactivated_at,
            "status": self.status, "pid": self.active_pid,
            "raw_slots": self.schedule_slots, "next_run_hour": next_run,
            "is_delayed": is_delayed,
            "executions_today": f"{covered_scheduled}/{total_slots}",
            "exec_agendamento": stats["agendamento"], "exec_manual": stats["manual"],
            "last_status": stats["last_status"],
            "duration_seconds": dur, "current_requester": self.current_requester,
            "created_at": self.created_at,
        }

class ActiveFlow:
    def __init__(self):
        self.wf_id: Optional[str] = None
        self.name: Optional[str] = None
        self.scripts: List[str] = []
        self.current_step = 0; self.is_active = False
    def to_dict(self) -> Optional[dict]:
        if not self.is_active: return None
        return {"id": self.wf_id, "name": self.name, "scripts": self.scripts,
                "current_step": self.current_step, "total_steps": len(self.scripts)}

# ===========================================================================
# Orchestrator
# ===========================================================================
class Orchestrator:
    def __init__(self):
        self.scripts: Dict[str, ScriptModel] = {}
        self.queue: List[dict] = []
        self.running: Dict[str, subprocess.Popen] = {}
        self._log_fh: Dict[str, Any] = {}
        self.lock = threading.RLock()
        self._xls_mt: float = 0.0
        self.flow = ActiveFlow()
        self._sio: Optional[SocketIO] = None
        self._last_day: Optional[str] = None

    def bind_sio(self, s: SocketIO): self._sio = s

    def _emit(self):
        if self._sio:
            try: self._sio.emit("status_update", self.state(), namespace="/")
            except Exception: pass

    # ---- workflows ----
    def wf_list(self) -> List[dict]:
        if not WORKFLOWS_FILE.exists(): return []
        try:
            with open(str(WORKFLOWS_FILE), "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception: return []

    def wf_save(self, wfs: List[dict]):
        with open(str(WORKFLOWS_FILE), "w", encoding="utf-8") as f:
            json.dump(wfs, f, indent=2, ensure_ascii=False)

    def wf_trigger(self, wid: str):
        wfs = self.wf_list()
        wf = next((w for w in wfs if w.get("id") == wid), None)
        if not wf or not wf.get("scripts"):
            raise ValueError("Workflow inválido.")
        with self.lock:
            for sn in wf["scripts"]:
                self._kill_unlocked(sn)
            self.queue = [q for q in self.queue if q["name"] not in wf["scripts"]]
            self.flow.is_active = True; self.flow.wf_id = wf["id"]
            self.flow.name = wf["name"]; self.flow.scripts = list(wf["scripts"])
            self.flow.current_step = 0
            logger.info(f"WF  | START: '{wf['name']}' ({len(wf['scripts'])} scripts)")
        self._emit()

    def wf_stop(self):
        with self.lock:
            if self.flow.is_active:
                nm = self.flow.name; s = self.flow.current_step
                if s < len(self.flow.scripts):
                    self._kill_unlocked(self.flow.scripts[s])
                self.flow.is_active = False
                logger.info(f"WF  | STOP: '{nm}'")
        self._emit()

    # ---- scan ----
    def _scan(self) -> Dict[str, Path]:
        found: Dict[str, Path] = {}
        if not SCRIPTS_DIR.exists():
            logger.warning(f"SCAN | Not found: {SCRIPTS_DIR}")
            return found
        for p in SCRIPTS_DIR.rglob("*.py"):
            k = p.stem.lower()
            if k.startswith("__"): continue
            if k not in found or len(p.parts) < len(found[k].parts):
                found[k] = p
        return found

    # ---- NAT ----
    @staticmethod
    def _nat(v: Any) -> Optional[str]:
        if v is None: return None
        s = str(v).strip()
        if s.lower() in ("nat", "nan", "none", "null", "", "pd.nat"): return None
        try:
            dt = pd.to_datetime(s, errors="coerce")
            return None if pd.isna(dt) else dt.strftime("%Y-%m-%d")
        except Exception: return None

    # ---- sync ----
    def sync(self):
        if not EXCEL_PATH.exists(): return
        try:
            mt = os.path.getmtime(str(EXCEL_PATH))
            if mt == self._xls_mt and self.scripts: return
            self._xls_mt = mt
            local = self._scan()
            tmp = EXCEL_PATH.with_suffix(".tmp.xlsx")
            shutil.copy2(str(EXCEL_PATH), str(tmp))
            df = pd.read_excel(str(tmp), dtype=str, engine="openpyxl")
            try: os.remove(str(tmp))
            except Exception: pass
            df.columns = [str(c).lower().strip() for c in df.columns]
            logger.info(f"SYNC | Excel: {len(df)} rows, cols={list(df.columns)}")

            with self.lock:
                seen: Set[str] = set()
                for _, row in df.iterrows():
                    nm = str(row.get("script_name", "")).strip().lower().replace(".py", "")
                    if not nm or nm == "nan": continue
                    if nm not in local: continue
                    seen.add(nm)

                    # Preserva status em memória se já existe (idle, running, queued)
                    existing_status = self.scripts[nm].status if nm in self.scripts else "idle"
                    existing_pid    = self.scripts[nm].active_pid if nm in self.scripts else None
                    existing_start  = self.scripts[nm].start_time if nm in self.scripts else None
                    existing_slot   = self.scripts[nm].active_slot if nm in self.scripts else 0
                    existing_req    = self.scripts[nm].current_requester if nm in self.scripts else None

                    if nm not in self.scripts:
                        self.scripts[nm] = ScriptModel(nm, local[nm])
                    s = self.scripts[nm]
                    s.path = local[nm]

                    ar = str(row.get("area_name", "Sem Area")).strip()
                    s.area_name = ar if ar.lower() not in ("nan", "", "none") else "Sem Area"

                    va = str(row.get("is_active", "")).strip().lower()
                    s.is_active = va in ("true", "sim", "1", "v", "yes")

                    s.inactivated_at = self._nat(row.get("inactivated_at"))
                    if s.inactivated_at is not None: s.is_active = False

                    cr = str(row.get("cron_schedule", "")).strip()
                    if cr.lower() in ("nan", "none", ""): cr = ""
                    slots: List[int] = []
                    for p in cr.replace(";", ",").replace(".", ",").split(","):
                        p = p.strip()
                        if p.isdigit() and 0 <= int(p) <= 23: slots.append(int(p))
                    s.schedule_slots = sorted(set(slots))

                    ep = str(row.get("emails_principal", "")).strip()
                    s.emails_principal = ep if ep.lower() not in ("nan", "none", "") else ""
                    ec = str(row.get("emails_cc", "")).strip()
                    s.emails_cc = ec if ec.lower() not in ("nan", "none", "sem") else ""
                    mf = str(row.get("move_file", "")).strip().lower()
                    s.move_file = mf in ("true", "sim", "1")
                    s.created_at = self._nat(row.get("created_at"))

                    # RESTORE runtime state — never lose running/queued state during sync
                    s.status = existing_status
                    s.active_pid = existing_pid
                    s.start_time = existing_start
                    s.active_slot = existing_slot
                    s.current_requester = existing_req

                act = sum(1 for x in self.scripts.values() if x.is_active)
                logger.info(f"SYNC | {len(self.scripts)} mapped, {act} active")
        except Exception as e:
            logger.error(f"SYNC | {e}"); logger.debug(traceback.format_exc())

    # ---- state ----
    def state(self) -> dict:
        with self.lock:
            sd = [s.to_dict() for _, s in sorted(self.scripts.items(),
                  key=lambda x: (x[1].area_name, x[0]))]
            qd = [{"name": q["name"], "slot": q["slot"],
                   "delay": q.get("delay", 0), "requester": q.get("requester", "AGENDADO")}
                  for q in self.queue]
            return {
                "running_count": len(self.running), "queued_count": len(self.queue),
                "total_scripts": len(self.scripts),
                "active_scripts": sum(1 for x in self.scripts.values() if x.is_active),
                "server_time": datetime.now(TZ_SP).strftime("%H:%M:%S"),
                "server_date": datetime.now(TZ_SP).strftime("%d/%m/%Y"),
                "scripts": sd, "queue": qd, "active_flow": self.flow.to_dict(),
                "max_concurrent": MAX_CONCURRENT, "version": APP_VERSION,
            }

    # ---- scheduler thread ----
    def scheduler_loop(self):
        logger.info("SCHED | Loop started")
        while True:
            try:
                self.sync()
                self._check_day()
                h = datetime.now(TZ_SP).hour
                with self.lock:
                    if not self.flow.is_active:
                        self._schedule(h)
            except Exception as e:
                logger.error(f"SCHED | {e}")
            time.sleep(15)

    def _schedule(self, h: int):
        """
        Para cada script ativo com slots, verifica quais slots <=h
        NÃO estão cobertos no DB. Se houver missed slots:
          - marca todos exceto o mais recente como catchup_skipped
          - enfileira o mais recente (uma única vez)
        NUNCA reenfileira se o slot já tem qualquer registro no DB.
        """
        for nm, s in self.scripts.items():
            if not s.is_active or not s.schedule_slots: continue

            covered = db_covered_slots(nm)
            missed = [sl for sl in s.schedule_slots if sl <= h and sl not in covered]
            if not missed: continue

            # Catch-up: marca anteriores, executa o mais recente
            target = missed[-1]
            for sl in missed[:-1]:
                db_mark(nm, sl, "catchup_skipped")
                logger.info(f"SCHED | [{nm}] slot {sl}h -> catchup_skipped")

            # Não enfileira duplicata
            if any(q["name"] == nm for q in self.queue): continue
            if s.status == "running": continue

            # Marca no DB como "running" ANTES de enfileirar (previne race condition)
            db_mark(nm, target, "queued", "AGENDADO")

            dly = h - target
            self.queue.append({"name": nm, "slot": target, "delay": dly, "requester": "AGENDADO"})
            s.status = "queued"
            logger.info(f"SCHED | [{nm}] enqueued slot={target}h delay={dly}h")

    def _check_day(self):
        td = datetime.now(TZ_SP).strftime("%Y-%m-%d")
        if self._last_day and self._last_day != td:
            logger.info(f"SCHED | New day: {td}")
        self._last_day = td

    # ---- queue processor thread ----
    def proc_loop(self):
        logger.info("PROC | Loop started")
        while True:
            try:
                with self.lock:
                    self._cleanup()
                    self._timeout_check()
                    self._wf_step()
                    if not self.flow.is_active:
                        self._dequeue()
                self._emit()
            except Exception as e:
                logger.error(f"PROC | {e}")
            time.sleep(2)

    def _cleanup(self):
        done: List[str] = []
        for nm, proc in list(self.running.items()):
            rc = proc.poll()
            if rc is not None:
                done.append(nm)
                st = "success" if rc == 0 else "error"
                s = self.scripts.get(nm)
                dur = int(time.time() - (s.start_time or time.time())) if s else 0
                if s:
                    db_mark(nm, s.active_slot, st, s.current_requester or "AGENDADO", rc, dur)
                    s.status = "idle"; s.active_pid = None
                    s.current_requester = None; s.start_time = None
                    logger.info(f"PROC | [{nm}] DONE: {st} rc={rc} dur={dur}s")
                fh = self._log_fh.pop(nm, None)
                if fh:
                    try: fh.close()
                    except: pass
        for d in done:
            self.running.pop(d, None)

    def _timeout_check(self):
        """Encerra scripts rodando por mais de TIMEOUT_SECONDS."""
        now = time.time()
        for nm, proc in list(self.running.items()):
            s = self.scripts.get(nm)
            if not s or not s.start_time: continue
            elapsed = now - s.start_time
            if elapsed > TIMEOUT_SECONDS:
                logger.warning(
                    f"TIMEOUT | [{nm}] Rodando há {int(elapsed)}s (>{TIMEOUT_SECONDS}s). "
                    f"Encerrando automaticamente."
                )
                dur = int(elapsed)
                self._kill_process(nm, proc)
                db_mark(nm, s.active_slot, "timeout", s.current_requester or "AGENDADO", None, dur)
                s.status = "idle"; s.active_pid = None
                s.current_requester = None; s.start_time = None
                self.running.pop(nm, None)
                fh = self._log_fh.pop(nm, None)
                if fh:
                    try:
                        fh.write(f"\n[TIMEOUT] Encerrado automaticamente após {dur}s\n")
                        fh.close()
                    except: pass

    def _kill_process(self, nm: str, proc: subprocess.Popen):
        """Kill a process and all its children."""
        try:
            parent = psutil.Process(proc.pid)
            children = parent.children(recursive=True)
            for c in children:
                try: c.terminate()
                except: pass
            parent.terminate()
            gone, alive = psutil.wait_procs([parent] + children, timeout=3)
            for p in alive:
                try: p.kill()
                except: pass
        except psutil.NoSuchProcess:
            pass
        except Exception as e:
            logger.warning(f"PROC | [{nm}] Kill error: {e}")

    def _wf_step(self):
        if not self.flow.is_active: return
        st = self.flow.current_step
        scs = self.flow.scripts
        if st >= len(scs):
            logger.info(f"WF  | DONE: '{self.flow.name}'")
            self.flow.is_active = False; return
        csn = scs[st]
        s = self.scripts.get(csn)
        if not s: 
            logger.warning(f"WF  | Script '{csn}' not found, skipping")
            self.flow.current_step += 1; return
        if csn in self.running:
            return  # still running, wait
        if s.status == "idle":
            # check if it already ran this step (DB)
            stats = db_stats_today(csn)
            if stats["total"] > 0 and st > 0:
                # already ran, advance
                self.flow.current_step += 1
                return
            # start it
            self._start(csn, 99, f"WORKFLOW: {self.flow.name}")
        elif s.status == "error":
            # error, advance anyway
            logger.warning(f"WF  | [{csn}] ended with error, advancing")
            self.flow.current_step += 1

    def _dequeue(self):
        self.queue.sort(key=lambda x: x.get("delay", 0), reverse=True)
        while len(self.running) < MAX_CONCURRENT and self.queue:
            task = self.queue.pop(0)
            nm = task["name"]
            if nm in self.running: continue
            self._start(nm, task["slot"], task.get("requester", "AGENDADO"))

    # ---- process start ----
    def _start(self, nm: str, slot: int, req: str):
        if nm not in self.scripts: return
        s = self.scripts[nm]
        try:
            sld = LOG_DIR / "scripts"; sld.mkdir(parents=True, exist_ok=True)
            lp = sld / f"{nm}.log"
            fh = open(str(lp), "a", encoding="utf-8", buffering=1)
            fh.write(
                f"\n{'='*60}\n"
                f"INICIO : {datetime.now(TZ_SP).strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"SLOT   : {slot}h | REQ: {req}\nPATH   : {s.path}\n{'='*60}\n"
            ); fh.flush()

            env = os.environ.copy()
            env["ENV_EXEC_MODE"] = "AGENDAMENTO" if req == "AGENDADO" else "SOLICITACAO"
            env["ENV_EXEC_USER"] = req if "@" in req else f"{req}@c6bank.com"

            flags = subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0
            proc = subprocess.Popen(
                [sys.executable, str(s.path)],
                stdout=fh, stderr=subprocess.STDOUT,
                cwd=str(s.path.parent), env=env, creationflags=flags,
            )
            self.running[nm] = proc; self._log_fh[nm] = fh
            s.status = "running"; s.active_pid = proc.pid
            s.active_slot = slot; s.start_time = time.time(); s.current_requester = req

            # Atualiza DB: marca como running
            db_mark(nm, slot, "running", req)

            logger.info(f"PROC | [{nm}] STARTED PID={proc.pid} slot={slot} req={req}")
        except Exception as e:
            logger.error(f"PROC | [{nm}] Start error: {e}"); s.status = "error"
            db_mark(nm, slot, "error", req)

    # ---- kill ----
    def kill(self, nm: str) -> bool:
        with self.lock: return self._kill_unlocked(nm)

    def _kill_unlocked(self, nm: str) -> bool:
        old = len(self.queue)
        self.queue = [q for q in self.queue if q["name"] != nm]
        removed_from_queue = len(self.queue) < old
        if removed_from_queue and nm in self.scripts:
            self.scripts[nm].status = "idle"

        if nm not in self.running:
            return removed_from_queue

        proc = self.running[nm]
        self._kill_process(nm, proc)
        self.running.pop(nm, None)
        fh = self._log_fh.pop(nm, None)
        if fh:
            try: fh.write("\n[KILLED] Encerrado manualmente\n"); fh.close()
            except: pass

        s = self.scripts.get(nm)
        if s:
            dur = int(time.time() - (s.start_time or time.time()))
            db_mark(nm, s.active_slot, "killed", s.current_requester or "MANUAL", None, dur)
            s.status = "idle"; s.active_pid = None
            s.current_requester = None; s.start_time = None
        logger.info(f"PROC | [{nm}] KILLED")
        return True

    def force_run(self, nm: str, req: str = "MANUAL") -> str:
        with self.lock:
            if nm not in self.scripts: return "Script não encontrado."
            if nm in self.running: return "Já está rodando."
            if any(q["name"] == nm for q in self.queue): return "Já está na fila."
            self.queue.insert(0, {"name": nm, "slot": 99, "delay": 999, "requester": req})
            self.scripts[nm].status = "queued"
            logger.info(f"PROC | [{nm}] Manual run by {req}")
        return "OK"

    def reload(self):
        self._xls_mt = 0.0; self.sync(); self._emit()
        logger.info("SYNC | Forced reload done")


# ===========================================================================
# Flask App
# ===========================================================================
init_db()
orch = Orchestrator()

app = Flask(__name__, static_folder=str(STATIC_DIR), static_url_path="/static")
app.config["SECRET_KEY"] = secrets.token_hex(32)

sio = SocketIO(app, cors_allowed_origins="*", async_mode="threading",
               logger=False, engineio_logger=False)
orch.bind_sio(sio)

# ---- Security headers (LGPD) ----
@app.after_request
def _sec(r: Response) -> Response:
    r.headers["X-Content-Type-Options"] = "nosniff"
    r.headers["X-Frame-Options"] = "DENY"
    r.headers["X-XSS-Protection"] = "1; mode=block"
    r.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    r.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline' https://cdn.socket.io https://cdn.jsdelivr.net https://unpkg.com https://cdn.tailwindcss.com; "
        "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
        "font-src 'self' https://fonts.gstatic.com; "
        "connect-src 'self' ws://127.0.0.1:* ws://localhost:*; "
        "img-src 'self' data:;")
    r.headers["Cache-Control"] = "no-store"
    return r

# ---- Routes ----
@app.route("/")
def idx(): return send_from_directory(str(STATIC_DIR), "index.html")

@app.route("/api/status")
def api_st(): return jsonify(orch.state())

@app.route("/api/reload", methods=["POST"])
def api_rl(): orch.reload(); return jsonify({"success": True, "message": "Recarregado."})

@app.route("/api/run", methods=["POST"])
def api_run():
    d = request.get_json(force=True, silent=True) or {}
    nm = str(d.get("name", "")).strip().lower()
    req = str(d.get("requester", "MANUAL")).strip()
    if not nm: return jsonify({"success": False, "message": "Nome vazio."}), 400
    r = orch.force_run(nm, req); orch._emit()
    if r == "OK": return jsonify({"success": True, "message": f"{nm} enfileirado."})
    return jsonify({"success": False, "message": r}), 400

@app.route("/api/kill", methods=["POST"])
def api_kill():
    d = request.get_json(force=True, silent=True) or {}
    nm = str(d.get("name", "")).strip().lower()
    if not nm: return jsonify({"success": False, "message": "Nome vazio."}), 400
    ok = orch.kill(nm); orch._emit()
    if ok: return jsonify({"success": True, "message": f"{nm} encerrado."})
    return jsonify({"success": False, "message": "Não está rodando."}), 404

@app.route("/api/kill-all", methods=["POST"])
def api_ka():
    with orch.lock:
        ns = list(orch.running.keys())
        for n in ns: orch._kill_unlocked(n)
    orch._emit()
    return jsonify({"success": True, "message": f"{len(ns)} encerrados."})

@app.route("/api/history/<name>")
def api_hist(name: str): return jsonify(db_history(name.lower()))

@app.route("/api/script-log/<name>")
def api_slog(name: str):
    lp = LOG_DIR / "scripts" / f"{name.lower()}.log"
    if not lp.exists(): return jsonify({"log": "(Sem log)"})
    try:
        with open(str(lp), "r", encoding="utf-8", errors="replace") as f:
            return jsonify({"log": "".join(f.readlines()[-150:])})
    except: return jsonify({"log": "(Erro)"})

@app.route("/api/workflows", methods=["GET"])
def api_wfl(): return jsonify(orch.wf_list())

@app.route("/api/workflows", methods=["POST"])
def api_wfc():
    d = request.get_json(force=True, silent=True) or {}
    d["id"] = secrets.token_hex(4); wfs = orch.wf_list(); wfs.append(d)
    orch.wf_save(wfs); return jsonify(d)

@app.route("/api/workflows/<wid>", methods=["PUT"])
def api_wfu(wid):
    d = request.get_json(force=True, silent=True) or {}
    wfs = orch.wf_list()
    for i, w in enumerate(wfs):
        if w.get("id") == wid: d["id"] = wid; wfs[i] = d; orch.wf_save(wfs); return jsonify(d)
    return jsonify({"error": "Not found"}), 404

@app.route("/api/workflows/<wid>", methods=["DELETE"])
def api_wfd(wid):
    orch.wf_save([w for w in orch.wf_list() if w.get("id") != wid])
    return jsonify({"success": True})

@app.route("/api/workflows/<wid>/trigger", methods=["POST"])
def api_wft(wid):
    try: orch.wf_trigger(wid); return jsonify({"success": True})
    except Exception as e: return jsonify({"success": False, "message": str(e)}), 400

@app.route("/api/workflows/stop", methods=["POST"])
def api_wfs(): orch.wf_stop(); return jsonify({"success": True})

@app.route("/api/about")
def api_about():
    return jsonify({
        "name": "C6 Cron Server", "version": APP_VERSION,
        "description": (
            "Servidor orquestrador de automações Python da Célula Python Monitoração. "
            "Gerencia agendamento tipo cron (por hora), fila de execução com limite de "
            "concorrência, timeout automático (2h), workflows sequenciais e monitoramento "
            "em tempo real via WebSocket."
        ),
        "lgpd": (
            "Este sistema NÃO coleta, armazena ou transmite dados pessoais de clientes. "
            "Opera exclusivamente com metadados de execução de scripts (nomes de scripts, "
            "horários, status) em localhost. Nenhum dado é enviado para servidores externos. "
            "Todos os logs são locais e contêm apenas informações operacionais. "
            "Em conformidade com a Lei Geral de Proteção de Dados (Lei nº 13.709/2018)."
        ),
        "security": {
            "network": "Somente localhost (127.0.0.1), sem exposição de rede.",
            "headers": "CSP, X-Frame-Options DENY, X-XSS-Protection ativados.",
            "auth": "Acesso restrito à máquina local.",
            "data": "SQLite local (WAL mode), sem transmissão externa.",
        },
        "tech_stack": ["Python 3.10+", "Flask", "Flask-SocketIO", "SQLite (WAL)",
                       "pandas", "psutil", "pytz", "Vanilla HTML/CSS/JS"],
        "author": "Célula Python Monitoração — C6 Bank",
        "timeout": f"{TIMEOUT_SECONDS}s ({TIMEOUT_SECONDS//3600}h)",
    })

@sio.on("connect")
def _c(): sio.emit("status_update", orch.state())
@sio.on("request_status")
def _r(): sio.emit("status_update", orch.state())

# ===========================================================================
# Main
# ===========================================================================
def main():
    logger.info("=" * 60)
    logger.info(f"C6 CRON SERVER v{APP_VERSION}")
    logger.info(f"  HOME          : {HOME}")
    logger.info(f"  BASE_PATH     : {BASE_PATH}")
    logger.info(f"  SCRIPTS_DIR   : {SCRIPTS_DIR}")
    logger.info(f"  EXCEL_PATH    : {EXCEL_PATH}")
    logger.info(f"  DB_FILE       : {DB_FILE}")
    logger.info(f"  TIMEOUT       : {TIMEOUT_SECONDS}s ({TIMEOUT_SECONDS//3600}h)")
    logger.info(f"  MAX_CONCURRENT: {MAX_CONCURRENT}")
    logger.info(f"  HOST:PORT     : {SERVER_HOST}:{SERVER_PORT}")
    logger.info("=" * 60)
    orch.sync()
    threading.Thread(target=orch.scheduler_loop, daemon=True, name="Sched").start()
    threading.Thread(target=orch.proc_loop, daemon=True, name="Proc").start()
    logger.info(f"Server at http://{SERVER_HOST}:{SERVER_PORT}")
    sio.run(app, host=SERVER_HOST, port=SERVER_PORT,
            debug=False, use_reloader=False, allow_unsafe_werkzeug=True)

if __name__ == "__main__":
    main()
