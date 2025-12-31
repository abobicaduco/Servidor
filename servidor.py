import os
import sys
import time
import json
import shutil
import ctypes
import threading
import subprocess
import pandas as pd
import pandas_gbq
import webbrowser
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# PySide6 Imports
from PySide6.QtWidgets import QApplication
from frontend.styles import GLOBAL_STYLESHEET
from frontend.ui import MainWindow

# ==============================================================================
# CONFIGURATION & CONSTANTS
# ==============================================================================
load_dotenv()

def get_root_path():
    # User's logic: Path.home() + Specific Cloud Folder
    home = Path.home()
    
    # Priority 1: Exact User Request
    p1 = home / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A"
    
    # Priority 2: Alternative (for diff environments)
    p2 = home / "Meu Drive (carlosfrenesi01@gmail.com)/C6 CTVM"
    p3 = home / "Meu Drive/C6 CTVM"
    
    if p1.exists(): return p1
    if p2.exists(): return p2
    if p3.exists(): return p3
    
    # Fallback to current dir's parent chain if needed or just return raw
    return home

ROOT_DRIVE = get_root_path()
# The sub-path requested: 
BASE_PATH = ROOT_DRIVE / "Mensageria e Cargas Operacionais - 11.CelulaPython/graciliano/automacoes"

# Debug Path
print(f"[INIT] Resolved BASE_PATH: {BASE_PATH}")
if not BASE_PATH.exists():
    print(f"[WARNING] BASE_PATH does not exist on disk!")

TEMP_DIR = Path(os.environ.get("TEMP")) / "C6_RPA_EXEC"
TEMP_DIR.mkdir(exist_ok=True)
CACHE_FILE = TEMP_DIR / "automacoes_exec.xlsx"
CONFIG_FILE = Path(__file__).parent / (os.environ.get("EXCEL_FILENAME", "registro_automacoes.xlsx"))

# Ensure Google Credentials
if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
    creds_search = list((Path(os.getenv('APPDATA')) / "Roamin/CELPY").glob("*.json"))
    if creds_search:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(creds_search[0])

# ==============================================================================
# BACKEND WORKER
# ==============================================================================
class EngineWorker(threading.Thread):
    def __init__(self):
        super().__init__()
        self.daemon = True
        self.running = True
        self.paused = False
        
        # State
        self.execution_queue = [] # (priority, script_name, path)
        self.running_tasks = {}   # script_name: subprocess
        self.daily_execution_cache = {} # script_name: count
        self.last_finish_times = {} # script_name: timestamp
        self.scripts_map = {}     # script_name: path
        self.scripts_config = {}  # script_name: {area, cron, active}
        
        # Timing
        self.last_discovery = 0
        self.last_bq_sync = 0
        self.bq_sync_interval = 300
        self.current_date_track = datetime.now().date()
        
        # Data
        self.history_df = pd.DataFrame()
        self.bq_verified = False

    def run(self):
        # Initial Discovery MUST happen before BQ Sync
        # otherwise we have no config to match against history.
        self.discover()
        self.last_discovery = time.time()
        
        self.sync_bq()
        self.last_bq_sync = time.time()
        
        while self.running:
            try:
                now = time.time()
                today = datetime.now().date()
                
                # Midnight Reset
                if today != self.current_date_track:
                    print(f"--- RESET DAILY COUNTERS ({today}) ---")
                    self.daily_execution_cache.clear()
                    self.last_finish_times.clear()
                    self.history_df = pd.DataFrame()
                    self.current_date_track = today
                    self.last_bq_sync = 0
                
                # Discovery
                if now - self.last_discovery > 60:
                    self.discover()
                    self.last_discovery = now
                
                # BQ Sync
                if not self.paused and (now - self.last_bq_sync > self.bq_sync_interval):
                    self.sync_bq()
                    self.last_bq_sync = now
                    
                # Core Logic
                self.check_schedule()
                self.process_queue()
                
                time.sleep(1)
            except Exception as e:
                print(f"Engine Loop Error: {e}")
                time.sleep(5)

    def clean_key(self, s):
        # Remove whitespace, non-printable, underscores, extensions
        s = str(s).lower()
        if s.endswith('.py'): s = s[:-3]
        # Keep only alphanumeric
        return "".join(c for c in s if c.isalnum())

    def discover(self):
        found_files = {}
        print(f"[DISCOVERY] Scanning {BASE_PATH} for .py files...")
        for root, dirs, files in os.walk(str(BASE_PATH)):
            # Normalize path check just in case
            lower_root = str(root).lower()
            if "metodos" in lower_root:
                for f in files:
                    if f.endswith(".py"):
                        # USE CLEAN KEY
                        key = self.clean_key(f) 
                        found_files[key] = Path(root) / f
        
        print(f"[DISCOVERY] Found {len(found_files)} Python files in 'metodos'.")
        
        if not CONFIG_FILE.exists(): return

        try:
            df = pd.read_excel(CONFIG_FILE)
            df = df.fillna('')
            df.columns = [c.lower() for c in df.columns]
            
            # Temporary dicts for strict sync
            new_map = {}
            new_config = {}
            
            for _, row in df.iterrows():
                raw_name = str(row.get('script_name', '')).strip()
                if not raw_name: continue
                
                # USE CLEAN KEY
                clean_name = self.clean_key(raw_name)
                
                if clean_name not in found_files:
                     # Skip if not found on disk
                     continue
                
                active_val = str(row.get('is_active', row.get('active', ''))).lower()
                is_active = active_val in ['true', '1', 'sim', 's', 'on', 'verdadeiro']
                
                if is_active:
                    new_map[clean_name] = found_files[clean_name]
                    
                    cron_raw = row.get('cron_schedule', row.get('cron', ''))
                    cron_sched = "MANUAL"
                    if str(cron_raw).upper() == 'ALL': cron_sched = "ALL"
                    elif ',' in str(cron_raw) or isinstance(cron_raw, (int, float)):
                        try:
                            parts = str(cron_raw).replace(',', ' ').split()
                            cron_sched = {int(float(p)) for p in parts}
                        except: pass
                    
                    new_config[clean_name] = {
                        'area': str(row.get('area_name', row.get('area', 'GERAL'))).upper(),
                        'cron': cron_sched,
                        'target_runs': row.get('target_runs', 0),
                        'display_name': raw_name
                    }
                    
                    # Initialize cache for new items if needed
                    if clean_name not in self.daily_execution_cache:
                        self.daily_execution_cache[clean_name] = 0
            
            # Atomic Match Update
            self.scripts_map = new_map
            self.scripts_config = new_config
            print(f"[DISCOVERY] Active & Matched Scripts: {len(self.scripts_config)}")

        except Exception as e:
            print(f"Discovery Error: {e}")

    def sync_bq(self):
        try:
            q = f"""
            SELECT script_name, status, start_time, duration_seconds
            FROM `datalab-pagamentos.ADMINISTRACAO_CELULA_PYTHON.automacoes_exec`
            WHERE DATE(start_time) = CURRENT_DATE()
            """
            self.history_df = pandas_gbq.read_gbq(q, project_id=os.environ.get("GOOGLE_CLOUD_PROJECT", "datalab-pagamentos"))
            
            print(f"\n[BQ] Rows Found: {len(self.history_df)}")
            
            # Reset Cache
            new_cache = {name: 0 for name in self.scripts_config.keys()}
            
            if not self.history_df.empty:
                bq_counts = self.history_df.groupby('script_name').size().to_dict()
                
                matches = 0
                for bq_name, count in bq_counts.items():
                    # USE CLEAN KEY
                    clean_bq_name = self.clean_key(bq_name)
                    
                    if clean_bq_name in new_cache:
                        new_cache[clean_bq_name] = count
                        matches += 1
                    else:
                        print(f"[BQ MISMATCH] BQ: '{bq_name}' -> Clean: '{clean_bq_name}' NOT IN CONFIG")
                        # Debug keys
                        # print(f"  Config Keys Sample: {list(new_cache.keys())[:5]}")
                
                print(f"[BQ] Matched {matches} scripts with Config.")
                
                # CRITICAL SAFETY: If we have BQ rows but 0 matches, SOMETHING IS WRONG.
                if len(bq_counts) > 0 and matches == 0:
                    print("CRITICAL ERROR: BigQuery has data but NO matches found in Config. ABORTING SCHEDULE.")
                    self.bq_verified = False
                    return
            
            self.daily_execution_cache = new_cache
            
            # Verify
            non_zero = {k:v for k,v in self.daily_execution_cache.items() if v > 0}
            print(f"[CACHE VERIFY] Non-Zero Items: {non_zero}")
                
            self.bq_verified = True
            print("BQ Sync Success.\n")
        except Exception as e:
            self.bq_verified = False
            print(f"BQ Sync Failed: {e}")

    def check_schedule(self):
        now_h = datetime.now().hour
        now_ts = time.time()
        
        for name, config in self.scripts_config.items():
            cron = config['cron']
            
            total_daily_target = 0
            current_target = 0
            
            if cron == 'ALL':
                total_daily_target = 24
                current_target = now_h + 1
            elif isinstance(cron, set):
                total_daily_target = len(cron)
                current_target = len([h for h in cron if h <= now_h])
            
            config['target_runs'] = total_daily_target
            
            actual = self.daily_execution_cache.get(name, 0)
            
            # Logic: Need more runs?
            if actual < current_target:
                if self.bq_verified and name not in self.running_tasks:
                    # Queue Check
                    if not any(item[1] == name for item in self.execution_queue):
                        # COOLDOWN CHECK: Wait 60s after last finish
                        last_finish = self.last_finish_times.get(name, 0)
                        if (now_ts - last_finish) > 60:
                            # DEBUG: Show WHY it is scheduling
                            print(f"Scheduling {name} (Cron: {cron}, Actual: {actual} / Target Now: {current_target})")
                            self.execution_queue.append((0, name, self.scripts_map[name]))

    def process_queue(self):
        finished = []
        for name, proc in self.running_tasks.items():
            if proc.poll() is not None:
                finished.append(name)
        
        for name in finished:
            print(f"Task Finished: {name}")
            self.daily_execution_cache[name] += 1
            self.last_finish_times[name] = time.time() # Record finish time for cooldown
            del self.running_tasks[name]
            
        MAX_CONCURRENT = 5
        while (len(self.running_tasks) < MAX_CONCURRENT) and self.execution_queue:
            prio, name, path = self.execution_queue.pop(0)
            self.run_script(path, name)

    def run_script(self, path, name=None):
        if not name: name = Path(path).stem
        if name in self.running_tasks: return
        
        try:
            env = os.environ.copy()
            env["ENV_EXEC_MODE"] = "AGENDAMENTO"
            
            proc = subprocess.Popen(
                [sys.executable, str(path)],
                cwd=str(Path(path).parent),
                env=env,
                creationflags=subprocess.CREATE_NO_WINDOW if sys.platform=='win32' else 0
            )
            self.running_tasks[name] = proc
        except Exception as e:
            print(f"Run Error {name}: {e}")

    def kill_script(self, name):
        if name in self.running_tasks:
            try:
                self.running_tasks[name].terminate()
            except: pass


if __name__ == "__main__":
    # Prevent Sleep
    try: ctypes.windll.kernel32.SetThreadExecutionState(0x80000003)
    except: pass
    
    # Start Backend
    SERV_WORKER = EngineWorker()
    SERV_WORKER.start()
    
    # Start Frontend
    app = QApplication(sys.argv)
    app.setStyleSheet(GLOBAL_STYLESHEET)
    
    window = MainWindow(SERV_WORKER)
    window.show()
    
    sys.exit(app.exec())
