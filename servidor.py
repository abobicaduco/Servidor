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
    home = Path.home()
    possible_roots = [
        home / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A",
        home / "Meu Drive/C6 CTVM",
        home / "C6 CTVM",
    ]
    for p in possible_roots:
        if p.exists(): return p
    return home / "C6 CTVM"

ROOT_DRIVE = get_root_path()
BASE_PATH = ROOT_DRIVE / "Mensageria e Cargas Operacionais - 11.CelulaPython/graciliano/automacoes"
if not BASE_PATH.exists():
    BASE_PATH = ROOT_DRIVE / "graciliano/automacoes"

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

    def discover(self):
        # 1. Scan Files
        found_files = {}
        for root, dirs, files in os.walk(str(BASE_PATH)):
            if "metodos" in Path(root).parts or "metodos" in str(Path(root)).lower():
                for f in files:
                    if f.endswith(".py"):
                        key = f[:-3].lower()
                        found_files[key] = Path(root) / f
        
        # 2. Read Config
        if not CONFIG_FILE.exists():
            return

        try:
            df = pd.read_excel(CONFIG_FILE)
            df = df.fillna('')
            df.columns = [c.lower() for c in df.columns]
            
            match_count = 0
            for _, row in df.iterrows():
                name = str(row.get('script_name', '')).strip().lower()
                if not name: continue
                
                if name not in found_files:
                     continue
                
                active_val = str(row.get('is_active', row.get('active', ''))).lower()
                is_active = active_val in ['true', '1', 'sim', 's', 'on', 'verdadeiro']
                
                if is_active:
                    self.scripts_map[name] = found_files[name]
                    match_count += 1
                    
                    # Parse Cron
                    cron_raw = row.get('cron_schedule', row.get('cron', ''))
                    cron_sched = "MANUAL"
                    
                    if str(cron_raw).upper() == 'ALL':
                        cron_sched = "ALL"
                    elif ',' in str(cron_raw) or isinstance(cron_raw, (int, float)):
                        try:
                            parts = str(cron_raw).replace(',', ' ').split()
                            cron_sched = {int(float(p)) for p in parts}
                        except:
                            pass
                    
                    self.scripts_config[name] = {
                        'area': str(row.get('area_name', row.get('area', 'GERAL'))).upper(),
                        'cron': cron_sched,
                        'target_runs': row.get('target_runs', 0)
                    }
                    
                    if name not in self.daily_execution_cache:
                        self.daily_execution_cache[name] = 0

        except Exception as e:
            print(f"Discovery Error: {e}")

    def sync_bq(self):
        try:
            # Using CURRENT_DATE() strictly as requested to match BigQuery Server Time
            # or implicitly Session Time zone. 
            q = f"""
            SELECT script_name, status, start_time, duration_seconds
            FROM `datalab-pagamentos.ADMINISTRACAO_CELULA_PYTHON.automacoes_exec`
            WHERE DATE(start_time) = CURRENT_DATE()
            """
            self.history_df = pandas_gbq.read_gbq(q, project_id=os.environ.get("GOOGLE_CLOUD_PROJECT", "datalab-pagamentos"))
            
            # DEBUG: Print exact findings
            print(f"\n[BQ SYNC] Found {len(self.history_df)} execution rows for TODAY.")
            if not self.history_df.empty:
                 print(f"[BQ SYNC] Sample Data:\n{self.history_df.head(2).to_string()}")
            
            # "Delete cache local... recomece"
            new_cache = {name: 0 for name in self.scripts_config.keys()}
            
            if not self.history_df.empty:
                bq_counts = self.history_df.groupby('script_name').size().to_dict()
                
                print(f"[BQ SYNC] BQ Counts Summary: {bq_counts}")
                
                # DEBUG: Print Config Keys to check against BQ Keys
                # print(f"[BQ SYNC] Config Keys: {list(new_cache.keys())}")
                
                for name, count in bq_counts.items():
                    norm_name = str(name).strip().lower()
                    
                    # Direct assignment attempt with debug
                    if norm_name in new_cache:
                        new_cache[norm_name] = count
                        # print(f"[BQ SYNC] Matched {norm_name}: {count}")
                    else:
                        # Try to find a partial match or notify?
                        # Maybe the config name has underscores? "envio_arquivo_conciliacao"?
                        # But user provided table has "envioarquivoconciliacao"
                        pass
                        
            self.daily_execution_cache = new_cache
            
            # CRITICAL DEBUG: Print final cache for verified items
            print(f"[BQ SYNC] Final Cache Verification:")
            for k, v in self.daily_execution_cache.items():
                if v > 0:
                    print(f"  -> {k}: {v}")
                
            self.bq_verified = True
            print("BQ Sync Success: Cache Overwritten with BQ Data\n")
        except Exception as e:
            self.bq_verified = False # Explicitly fail if query fails
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
                            print(f"Scheduling {name} (Actual: {actual} / Target Now: {current_target})")
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
