
import sys
import os
import time
import json
import threading
import subprocess
import ctypes
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from typing import List, Dict, Set, Optional

# Third-party imports
try:
    from fastapi import FastAPI, HTTPException
    from fastapi.middleware.cors import CORSMiddleware
    from pydantic import BaseModel
    import uvicorn
    LIBS_OK = True
except ImportError:
    LIBS_OK = False
    print("AVISO: Bibliotecas faltando. O servidor tentará instalar automaticamente.")
    
    # Dummy classes to prevent crash during parsing of routes
    class BaseModel: pass
    class DummyApp:
        def add_middleware(self, *args, **kwargs): pass
        def get(self, *args, **kwargs): return lambda func: func
        def post(self, *args, **kwargs): return lambda func: func
    
    # Mock FastAPI class
    def FastAPI(): return DummyApp()
    class CORSMiddleware: pass # Dummy

# Pandas Logic (same as Servidor.py)
try:
    import pandas as pd
    PANDAS_AVAIL = True
except ImportError:
    PANDAS_AVAIL = False
    print("WARNING: Pandas not found. Excel filtering will fail.")

# ==============================================================================
# CONFIGS (Loaded from .env)
# ==============================================================================
from dotenv import load_dotenv
load_dotenv()

HOME = Path.home()
# Configurable Roots from ENV
POSSIBLE_ROOTS = [
    HOME / os.getenv("PATH_ROOT_1", "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A"),
    HOME / os.getenv("PATH_ROOT_2", "Meu Drive/C6 CTVM"),
    HOME / os.getenv("PATH_ROOT_3", "C6 CTVM"),
]
ROOT_DRIVE = next((p for p in POSSIBLE_ROOTS if p.exists()), HOME / "C6 CTVM")

ATOM_BASE = os.getenv("PATH_AUTOMATION_BASE", "Mensageria e Cargas Operacionais - 11.CelulaPython/graciliano/automacoes")
ATOM_FALLBACK = os.getenv("PATH_AUTOMATION_BASE_FALLBACK", "graciliano/automacoes")

BASE_PATH = ROOT_DRIVE / ATOM_BASE
if not BASE_PATH.exists():
    BASE_PATH = ROOT_DRIVE / ATOM_FALLBACK

CONFIG_ROOT = Path(os.getenv("TEMP")) / "C6_RPA_EXEC"
CONFIG_ROOT.mkdir(parents=True, exist_ok=True)
HISTORY_FILE = CONFIG_ROOT / "scheduler_history.json"
EXCEL_PATH = Path(os.getenv("EXCEL_FILENAME", "registro_automacoes.xlsx")).absolute() 

if not EXCEL_PATH.exists():
    # Try looking in the folder of Servidor.py
    POSSIBLE_EXCEL = Path(os.getcwd()) / os.getenv("EXCEL_FILENAME", "registro_automacoes.xlsx")
    if POSSIBLE_EXCEL.exists():
        EXCEL_PATH = POSSIBLE_EXCEL

# ==============================================================================
# ENGINE LOGIC (Headless adaptation of EngineWorker)
# ==============================================================================
def format_duration(seconds):
    if not seconds: return "0s"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    parts = []
    if h > 0: parts.append(f"{h}h")
    if m > 0: parts.append(f"{m}m")
    parts.append(f"{s}s")
    return " ".join(parts)

class HeadlessEngine:
    def __init__(self):
        self.running = True
        self.scripts_map = {}
        self.cron_map = {} 
        self.running_tasks = {} 
        self.last_discovery = 0
        self.last_bq_sync = 0
        self.bq_sync_interval = 600 
        
        self.execution_queue = []
        self.max_concurrent = 5
        self.daily_execution_cache = defaultdict(int)
        self.history_df = pd.DataFrame()
        
        # BQ Configs
        # BQ Configs
        self.PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "datalab-pagamentos")
        self.DATASET = os.getenv("BIGQUERY_DATASET", "ADMINISTRACAO_CELULA_PYTHON")
        self.TABLE_EXEC = "automacoes_exec"
        
        self.bq_verified = False # Safety flag
        self.mock_mode = False # Mock Mode flag
        
        # Mock Data (for personal PC testing)
        self.MOCK_SCRIPTS = [
            {"name": "Conciliacao_Bancaria_Diaria", "area": "FINANCE", "path": "mock/finance/daily_recon.py", "status": "SUCCESS", "last_run": "10:30", "next_run": "Tomorrow", "duration": "45s"},
            {"name": "Relatorio_Risco_Mercado", "area": "RISK", "path": "mock/risk/market_risk_report.py", "status": "ERROR", "last_run": "09:15", "next_run": "Manual", "duration": "12s"},
            {"name": "Extracao_Dados_B3", "area": "DATA_ENG", "path": "mock/data/b3_extractor.py", "status": "RUNNING", "last_run": "Now", "next_run": "Hourly", "duration": "Running..."},
            {"name": "Validacao_KYC_Clientes", "area": "COMPLIANCE", "path": "mock/compliance/kyc_validator.py", "status": "IDLE", "last_run": "Yesterday", "next_run": "12:00", "duration": "0s"},
            {"name": "Processamento_Boletas", "area": "OPERATIONS", "path": "mock/ops/trade_processing.py", "status": "SUCCESS", "last_run": "11:00", "next_run": "11:15", "duration": "1m 20s"},
        ]
        
        # State for API
        self.script_states = {} # path -> {status, last_run, next_run, duration}

        self.setup_credentials()
        self.load_history()
        
        # Start background thread
        self.thread = threading.Thread(target=self.run_loop, daemon=True)
        self.thread.start()

    def setup_credentials(self):
        try:
            cred_dir = Path.home() / "AppData" / "Roaming" / "CELPY"
            if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS") and cred_dir.exists():
                jsons = list(cred_dir.glob("*.json"))
                if jsons:
                    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(jsons[0])
        except: pass

    def load_history(self):
        local_exec = CONFIG_ROOT / "automacoes_exec.xlsx"
        if local_exec.exists() and PANDAS_AVAIL:
            try:
                self.history_df = pd.read_excel(local_exec)
                self.update_local_cache_from_df()
            except: pass

    def update_local_cache_from_df(self):
        if self.history_df.empty: return
        try:
            today_str = datetime.now().strftime("%Y-%m-%d")
            # Convert date column safely - DERIVE FROM start_time FOR ROBUSTNESS
            self.history_df['temp_date'] = pd.to_datetime(self.history_df['start_time'], errors='coerce')
            self.history_df['date_str'] = self.history_df['temp_date'].dt.strftime("%Y-%m-%d")

            # Fallback if start_time parse fails (use existing date col)
            mask = self.history_df['date_str'].isna()
            if mask.any():
                 self.history_df.loc[mask, 'date_str'] = pd.to_datetime(self.history_df.loc[mask, 'date'], errors='coerce').dt.strftime("%Y-%m-%d")

            today_df = self.history_df[self.history_df['date_str'] == today_str]
            counts = today_df.groupby('script_name').size().to_dict()
            
            # Infinite Loop Fix: Use MAX of current cache vs new BQ data
            for name, bq_count in counts.items():
                self.daily_execution_cache[name] = max(self.daily_execution_cache[name], bq_count)
            
            for name in counts:
                 if name not in self.daily_execution_cache:
                     self.daily_execution_cache[name] = counts[name]
        except Exception as e:
            print(f"Error updating cache: {e}")

    def sync_bq(self):
        if not PANDAS_AVAIL: return
        print("Syncing BQ History...")
        try:
            # Note: Assuming pandas-gbq and google-auth are installed
            query = f"""
                SELECT script_name, status, start_time, duration_seconds, date
                FROM `{self.PROJECT_ID}.{self.DATASET}.{self.TABLE_EXEC}`
                WHERE DATE(start_time) = CURRENT_DATE('America/Sao_Paulo')
            """
            # Using pandas_gbq
            if PANDAS_AVAIL:
                import pandas_gbq
                new_df = pandas_gbq.read_gbq(query, project_id=self.PROJECT_ID, use_bqstorage_api=False)
            else:
                 new_df = pd.DataFrame() # Should not happen due to check above
            
            if not new_df.empty:
                self.history_df = new_df
                local_exec = CONFIG_ROOT / "automacoes_exec.xlsx"
                self.history_df.to_excel(local_exec, index=False)
                self.update_local_cache_from_df()
                self.bq_verified = True
                print(f"BQ Sync Success. Loaded {len(new_df)} rows. (Verified: {self.bq_verified})")
            else:
                # Today is empty. perform SAFETY CHECK (Health Verification)
                print("Today is empty. Verifying table health...")
                check_query = f"SELECT script_name FROM `{self.PROJECT_ID}.{self.DATASET}.{self.TABLE_EXEC}` LIMIT 1"
                check_df = pandas_gbq.read_gbq(check_query, project_id=self.PROJECT_ID, use_bqstorage_api=False)
                
                if not check_df.empty:
                    self.bq_verified = True
                    print("BQ Verification: Table is readable.")
                else:
                    self.bq_verified = False
                    print("CRITICAL: BQ Verification FAILED. No data found or Read Error.")
                    self.bq_verified = False
                    print("CRITICAL: BQ Verification FAILED. No data found or Read Error.")
        except Exception as e:
            print(f"BQ Sync Failed: {e}")
            if "Access Denied" in str(e) or "403" in str(e):
                print(">>> DETECTED PERSONAL PC ENVIRONMENT (NO BQ ACCESS) <<<")
                self.mock_mode = True
                self.bq_verified = True # Allow 'mock' execution
                print(">>> SWITCHING TO MOCK MODE <<<")
            elif str(e): 
                self.bq_verified = False

    def parse_cron(self, cron_str):
        cron_str = str(cron_str).upper().strip()
        if cron_str in ["SEM", "MANUAL", "NONE", "NAN", "NAT", ""]:
            return set()
        if cron_str == "ALL":
            return "ALL"
            
        hours = set()
        try:
            parts = cron_str.split(",")
            for p in parts:
                if p.strip().isdigit():
                    hours.add(int(p.strip()))
        except: pass
        return hours

    def discover(self):
        # 1. Scan Files
        all_found_scripts = {}
        try:
            for root, dirs, files in os.walk(str(BASE_PATH)):
                if os.path.basename(root).lower() == "metodos":
                    for f in files:
                        if f.endswith(".py") and not f.startswith("__"):
                            full_path = str(Path(root) / f)
                            stem = Path(f).stem
                            all_found_scripts[stem] = full_path
        except: pass
        
        # MOCK MODE CHECK
        if self.mock_mode or (not all_found_scripts and not EXCEL_PATH.exists()):
            if not self.scripts_map: # Only populate if empty
                print("Populating Mock Scripts...")
                self.mock_mode = True
                self.bq_verified = True
                for m in self.MOCK_SCRIPTS:
                    self.script_states[m['path']] = {
                        "id": m['path'],
                        "name": m['name'],
                        "area": m['area'],
                        "path": m['path'],
                        "status": m['status'],
                        "lastRun": m.get('last_run', 'Never'),
                        "nextRun": m.get('next_run', 'N/A'),
                        "duration": m.get('duration', '0s'),
                        "description": "Mock Script for Testing"
                    }
                return

        # 2. Excel logic
        final_map = defaultdict(list)
        new_cron_map = {}
        
        if PANDAS_AVAIL and EXCEL_PATH.exists():
            try:
                try: df = pd.read_excel(EXCEL_PATH)
                except: 
                    time.sleep(1)
                    df = pd.read_excel(EXCEL_PATH)
                    
                df.columns = [c.lower().strip() for c in df.columns]
                col_name = next((c for c in df.columns if 'script' in c or 'process' in c or 'nome' in c), None)
                col_area = next((c for c in df.columns if 'area' in c or 'depto' in c), None)
                col_cron = next((c for c in df.columns if 'cron' in c or 'schedule' in c), None)
                col_active = next((c for c in df.columns if 'active' in c or 'ativo' in c), None)
                
                if col_name and col_area:
                    for _, row in df.iterrows():
                        if col_active:
                            val = str(row[col_active]).lower().strip()
                            if val not in ['true', '1', 'verdadeiro', 'sim', 's', 'on']:
                                continue
                        
                        s_name = str(row[col_name]).strip()
                        if s_name.lower().endswith(".py"): s_name = s_name[:-3]
                        
                        if s_name in all_found_scripts:
                            path = all_found_scripts[s_name]
                            area = str(row[col_area]).strip().upper()
                            if not area or area == "NAN": area = "GENERAL"
                            final_map[area].append(path)
                            
                            if col_cron:
                                new_cron_map[s_name] = self.parse_cron(str(row[col_cron]).strip())
                else:
                     final_map = self._fallback_discovery(all_found_scripts)
            except Exception as e:
                print(f"Excel Discover Error: {e}")
                final_map = self._fallback_discovery(all_found_scripts)
        else:
            final_map = self._fallback_discovery(all_found_scripts)

        self.cron_map = new_cron_map
        self.scripts_map = dict(sorted(final_map.items()))
        
        # Initialize missing states
        for area, paths in self.scripts_map.items():
            for p in paths:
                if p not in self.script_states:
                    self.script_states[p] = {
                        "status": "IDLE",
                        "last_run": "Never",
                        "next_run": "N/A",
                        "duration": "0s",
                        "description": "Script Python Automation" # Placeholder
                    }

    def _fallback_discovery(self, all_scripts):
        mapped = defaultdict(list)
        for name, path in all_scripts.items():
            try:
                parts = Path(path).parts
                if "metodos" in parts:
                    mapped[parts[parts.index("metodos")-1].upper()].append(path)
                else:
                    mapped["UNKNOWN"].append(path)
            except:
                mapped["UNKNOWN"].append(path)
        return mapped

    def check_schedule_logic(self):
        current_hour = datetime.now().hour
        for area, paths in self.scripts_map.items():
            for path in paths:
                script_name = Path(path).stem
                cron = self.cron_map.get(script_name, set())
                
                target_runs = 0
                if cron == "ALL": target_runs = current_hour + 1
                elif isinstance(cron, set): target_runs = sum(1 for h in cron if h <= current_hour)
                else: continue
                    
                actual_runs = self.daily_execution_cache[script_name]
                if target_runs - actual_runs > 0:
                     if not self.bq_verified: # Safety Check
                         print(f"Skipping {script_name} - BQ Not Verified")
                         continue
                         
                     if script_name not in self.running_tasks and script_name not in [x[1] for x in self.execution_queue]:
                        print(f"Queueing {script_name}")
                        self.execution_queue.append((0, script_name, path))

    def process_queue(self):
        # Cleanup
        finished = [k for k, v in self.running_tasks.items() if v.poll() is not None]
        for k in finished:
            print(f"Task finished: {k}")
            self.daily_execution_cache[k] += 1
            del self.running_tasks[k]
            
        # Start new
        while len(self.running_tasks) < self.max_concurrent and self.execution_queue:
            priority, s_name, s_path = self.execution_queue.pop(0)
            if s_name in self.running_tasks: continue
            self.run_script(s_path, s_name)

    def run_script(self, path, name=None):
        if not name: name = Path(path).stem
        if name in self.running_tasks: return
        
        # MOCK EXECUTION
        if self.mock_mode:
            print(f"MOCK RUN: {name}")
            if path in self.script_states:
                self.script_states[path]['status'] = 'RUNNING'
            
            # Helper to allow cancellation compatible with Popen
            class MockTask:
                def __init__(self): 
                    self.cancelled = False
                    self.returncode = None
                def poll(self): return self.returncode
                def wait(self, timeout=None): return 0
                def terminate(self): self.cancelled = True
                def kill(self): self.cancelled = True
            
            task = MockTask()
            self.running_tasks[name] = task
            
            def mock_finish():
                # Loop for duration (60s) checking cancellation
                for _ in range(120): # 120 * 0.5s = 60s
                    if task.cancelled: 
                        print(f"MOCK STOPPED: {name}")
                        task.returncode = -1 # Signal completion
                        if path in self.script_states:
                            self.script_states[path]['status'] = 'IDLE'
                        return
                    time.sleep(0.5)
                
                # Success Logic
                if path in self.script_states:
                    self.script_states[path]['status'] = 'SUCCESS'
                    self.script_states[path]['lastRun'] = datetime.now().strftime('%H:%M')
                    self.script_states[path]['duration'] = '1m 0s'
                
                print(f"MOCK FINISH: {name}")
                task.returncode = 0 # Signal completion

            threading.Thread(target=mock_finish, daemon=True).start()
            return

        try:
            cwd = os.path.dirname(path)
            proc = subprocess.Popen([sys.executable, path], cwd=cwd,
                                    creationflags=subprocess.CREATE_NO_WINDOW if sys.platform=='win32' else 0)
            self.running_tasks[name] = proc
        except Exception as e:
            print(f"Error running {name}: {e}")

    def kill_script(self, path):
        name = Path(path).stem
        
        # MOCK KILL
        if self.mock_mode:
             if name in self.running_tasks:
                 task = self.running_tasks[name]
                 if hasattr(task, 'cancelled'): task.cancelled = True
                 del self.running_tasks[name]
                 return True
             return False

        if name in self.running_tasks:
            try:
                proc = self.running_tasks[name]
                proc.terminate()
                time.sleep(0.5)
                if proc.poll() is None: proc.kill()
                del self.running_tasks[name]
                return True
            except: pass
        return False

    def prevent_sleep(self):
        try:
             ctypes.windll.kernel32.SetThreadExecutionState(0x80000003)
        except: pass

    def update_states(self):
        today_str = datetime.now().strftime("%Y-%m-%d")
        
        for area, paths in self.scripts_map.items():
            for path in paths:
                s_name = Path(path).stem
                status = "IDLE"
                last_run_txt = "Never"
                next_run_txt = "N/A"
                duration_txt = "0s"

                # Check History
                if not self.history_df.empty:
                    date_col = 'date_str' if 'date_str' in self.history_df.columns else 'date'
                    try:
                        matches = self.history_df[
                             (self.history_df['script_name'] == s_name) & 
                             (self.history_df[date_col].astype(str).str.startswith(today_str))
                        ]
                        if not matches.empty:
                            row = matches.iloc[-1]
                            bq_stat = str(row['status']).upper()
                            if 'SUCCESS' in bq_stat: status = "SUCCESS"
                            elif 'ERROR' in bq_stat: status = "ERROR"
                            elif 'NO_DATA' in bq_stat: status = "NO_DATA"
                            else: status = bq_stat
                            
                            # Format last run
                            st = row['start_time']
                            t = ""
                            if hasattr(st, 'strftime'):
                                t = st.strftime('%H:%M')
                            elif isinstance(st, str):
                                try:
                                    if 'T' in st: t = st.split('T')[1][:5]
                                    elif ' ' in st: t = st.split(' ')[1][:5]
                                    else: t = st[11:16]
                                except: t = "??:??"
                            else:
                                t = str(st)[11:16]
                            
                            dur = row.get('duration_seconds', 0)
                            d_txt = format_duration(float(dur)) if dur else "?"
                            last_run_txt = f"{t}" # Simplified for UI
                            duration_txt = d_txt
                    except: pass
                
                # Check Running
                if s_name in self.running_tasks:
                    status = "RUNNING"
                    last_run_txt = "Now"
                    duration_txt = "Running..."
                
                # Check Queue
                if s_name in [x[1] for x in self.execution_queue] and status == "IDLE":
                     status = "SCHEDULED"

                # Next run
                cron = self.cron_map.get(s_name, set())
                cur_h = datetime.now().hour
                if cron == "ALL": next_run_txt = "Hourly"
                elif isinstance(cron, set) and cron:
                    upcoming = sorted([h for h in cron if h > cur_h])
                    next_run_txt = f"{upcoming[0]}:00" if upcoming else "Tomorrow"
                
                self.script_states[path] = {
                    "id": path,
                    "name": s_name.replace("_", " "),
                    "area": area,
                    "path": path,
                    "status": status,
                    "lastRun": last_run_txt,
                    "nextRun": next_run_txt,
                    "duration": duration_txt,
                    "description": "Automated Python Script" 
                }

    def run_loop(self):
        self.prevent_sleep()
        self.sync_bq()
        self.last_bq_sync = time.time()
        
        while self.running:
            now = time.time()
            if now - self.last_discovery > 60:
                self.discover()
                self.last_discovery = now
            
            if now - self.last_bq_sync > self.bq_sync_interval:
                self.sync_bq()
                self.last_bq_sync = now
                
            self.check_schedule_logic()
            self.process_queue()
            self.update_states()
            
            time.sleep(1)

# ==============================================================================
# ==============================================================================
# FASTAPI APP
# ==============================================================================
if LIBS_OK:
    app = FastAPI()
    
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
else:
    app = None

engine = HeadlessEngine()

class RunRequest(BaseModel):
    path: str

@app.get("/scripts")
def get_scripts():
    # Return flat list of scripts
    return list(engine.script_states.values())

@app.get("/stats")
def get_stats():
    total = len(engine.script_states)
    running = sum(1 for s in engine.script_states.values() if s['status'] == 'RUNNING')
    success = sum(1 for s in engine.script_states.values() if s['status'] == 'SUCCESS')
    error = sum(1 for s in engine.script_states.values() if s['status'] == 'ERROR')
    
    # Calc time to next sync
    now = time.time()
    elapsed = now - engine.last_bq_sync
    next_seconds = max(0, int(engine.bq_sync_interval - elapsed))
    
    return {
        "total": total, 
        "running": running, 
        "success": success, 
        "error": error,
        "next_refresh_seconds": next_seconds
    }

@app.post("/run")
def trigger_run(req: RunRequest):
    engine.run_script(req.path)
    return {"status": "triggered", "path": req.path}

@app.post("/stop")
def trigger_stop(req: RunRequest):
    success = engine.kill_script(req.path)
    return {"status": "stopped" if success else "failed", "path": req.path}

# ==============================================================================
# BOOTSTRAP & AUTO-SETUP
# ==============================================================================
def install_py_libs():
    print("Installing missing Python libraries...")
    required = ["fastapi", "uvicorn", "pandas", "pandas-gbq", "google-auth"]
    subprocess.check_call([sys.executable, "-m", "pip", "install", *required])

import shutil
import webbrowser

def setup_frontend():
    # Check portable first
    portable_node = Path("binaries/node")
    if not shutil.which("npm") and not portable_node.exists():
        print("WARNING: Node.js (npm) not found. Frontend cannot be installed/started.")
        print("To fix: Install Node.js or download the portable version and add to PATH.")
        return False

    frontend_dir = Path.cwd() / "web_frontend"
    node_modules = frontend_dir / "node_modules"
    
    if not node_modules.exists():
        print("Installing Frontend Dependencies (this happens only once)...")
        # Shell=True needed for npm on windows
        subprocess.check_call("npm install", shell=True, cwd=frontend_dir)
    else:
        print("Frontend dependencies already installed.")
    return True

def start_frontend_process():
    if not shutil.which("npm"):
         return None

    frontend_dir = Path.cwd() / "web_frontend"
    print("Starting Web Frontend (npm run dev)...")
    # Popen to run in background
    return subprocess.Popen("npm run dev", shell=True, cwd=frontend_dir)

if __name__ == "__main__":
    # 1. Check Python Libs
    if not LIBS_OK:
        print("Bibliotecas criticas (FastAPI/Uvicorn) nao encontradas.")
        print("Tentando instalar via install_requirements_proxy.bat...")
        try:
             # Tenta rodar o bat de install
             subprocess.check_call("install_requirements_proxy.bat", shell=True)
             print("Instalacao concluida. Reinicie o servidor.")
             sys.exit(0)
        except Exception as e:
             print(f"Falha na auto-instalacao: {e}")
             sys.exit(1)

    # 2. Setup Frontend (npm install)
    frontend_ok = False
    try:
        frontend_ok = setup_frontend()
    except Exception as e:
        print(f"Failed to setup frontend: {e}")
        print("Please ensure Node.js is installed.")

    # 3. Start Frontend Background Process
    frontend_proc = None
    if frontend_ok:
        try:
            frontend_proc = start_frontend_process()
        except Exception as e:
            print(f"Failed to start frontend: {e}")
            
    # Auto-Open Browser Logic
    def open_browser():
        time.sleep(3) # Wait for servers to start
        if frontend_proc:
            print("Opening Web Interface...")
            webbrowser.open("http://localhost:5173")
        else:
            print("NPM missing or Frontend failed. Opening API Docs instead...")
            webbrowser.open("http://localhost:8000/docs")

    threading.Thread(target=open_browser, daemon=True).start()

    # 4. Start Backend API
    print("Starting Backend API...")
    try:
        # Prevent auto-reload to avoid restarting both when files change (optional)
        uvicorn.run(app, host="0.0.0.0", port=8000)
    finally:
        # Cleanup
        if frontend_proc:
            print("Stopping Frontend...")
            frontend_proc.terminate()
