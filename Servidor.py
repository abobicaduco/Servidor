import sys
import os
import ctypes
import json
import time
import subprocess
import threading
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# Pandas for Excel Logic
try:
    import pandas as pd
    PANDAS_AVAIL = True
except ImportError:
    PANDAS_AVAIL = False
    print("WARNING: Pandas not found. Excel filtering will fail.")

# Try importing psutil for system stats (Optional enhancement)
try:
    import psutil
    PSUTIL_AVAIL = True
except ImportError:
    PSUTIL_AVAIL = False

# PySide6 Imports
from PySide6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout,
                               QHBoxLayout, QLabel, QPushButton, QScrollArea,
                               QLineEdit, QStackedWidget, QFrame, QGridLayout,
                               QLayout, QSizePolicy, QMessageBox, QListWidget, 
                               QListWidgetItem, QGraphicsDropShadowEffect, QButtonGroup)
from PySide6.QtCore import Qt, QTimer, Signal, QThread, QSize, QPoint, QRect
from PySide6.QtGui import QColor, QFont, QIcon, QPainter, QAction

# Stylesheet (Specific C6 RPA Dashboard Design)
from dotenv import load_dotenv
load_dotenv()

# CONFIGS & CONSTANTS
HOME = Path.home()
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
CONFIG_FILE = CONFIG_ROOT / "scheduler_config.json"
HISTORY_FILE = CONFIG_ROOT / "scheduler_history.json"
# EXCEL PATH
EXCEL_PATH = Path(__file__).parent / os.getenv("EXCEL_FILENAME", "registro_automacoes.xlsx") 

CONFIG_ROOT.mkdir(parents=True, exist_ok=True)

# Stylesheet (Specific C6 RPA Dashboard Design)
STYLESHEET = """
QMainWindow { background-color: #0B0E14; } /* Deep Dark BG */
QWidget { font-family: 'Segoe UI', 'Roboto', sans-serif; color: #E5E7EB; }

/* Scrollbars */
QScrollBar:vertical { border: none; background: #0B0E14; width: 8px; margin: 0; }
QScrollBar::handle:vertical { background: #374151; min-height: 20px; border-radius: 4px; }
QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { height: 0px; }

/* Sidebar */
QWidget#Sidebar { background-color: #0F1219; border-right: 1px solid #1F2937; }
QLabel#Logo { color: #3B82F6; font-size: 16px; font-weight: 800; }
QPushButton#NavButton {
    text-align: left;
    padding: 12px 20px;
    background-color: transparent;
    border: none;
    color: #9CA3AF;
    font-size: 13px;
    font-weight: 600;
}
QPushButton#NavButton:hover { background-color: #1F2937; color: #F3F4F6; }
QPushButton#NavButton:checked { 
    background-color: #1E3A8A; 
    color: #60A5FA; 
    border-left: 3px solid #3B82F6; 
}

/* Header */
QLabel#PageTitle { font-size: 24px; font-weight: 700; color: #F3F4F6; }
QLabel#PageSubTitle { font-size: 13px; font-weight: 500; color: #6B7280; }
QLabel#SystemStatus { font-size: 10px; font-weight: 700; color: #10B981; }

/* Inputs */
QLineEdit {
    background-color: #161B22;
    border: 1px solid #374151;
    border-radius: 8px;
    padding: 10px 16px;
    font-size: 13px;
    color: #D1D5DB;
}
QLineEdit:focus { border: 1px solid #3B82F6; background-color: #1F2937; }

/* Filter Pills */
QPushButton#PillButton {
    background-color: #1F2937;
    border: 1px solid #374151;
    border-radius: 16px;
    padding: 6px 14px;
    color: #9CA3AF;
    font-size: 11px;
    font-weight: 700;
}
QPushButton#PillButton:checked {
    background-color: #2563EB;
    color: white;
    border: 1px solid #2563EB;
}
QPushButton#PillButton:hover:!checked {
    border: 1px solid #6B7280;
    color: #F3F4F6;
}

/* Buttons */
QPushButton#PrimaryButton {
    background-color: #2563EB;
    color: white;
    border: none;
    border-radius: 6px;
    padding: 6px 16px;
    font-weight: 600;
    font-size: 12px;
}
QPushButton#PrimaryButton:hover { background-color: #1D4ED8; }

QPushButton#DangerButton {
    background-color: #EF4444; 
    color: white;
    border: 1px solid #DC2626;
    border-radius: 6px;
    padding: 6px 16px;
    font-weight: 600;
    font-size: 12px;
}
QPushButton#DangerButton:hover { background-color: #DC2626; }

/* Cards */
QFrame#Card {
    background-color: #161B22;
    border: 2px solid #2D3748;
    border-radius: 10px;
}
QFrame#Card:hover {
    border: 1px solid #4B5563; 
    background-color: #181E27;
}

/* Badges */
QLabel#StatusBadge { 
    border-radius: 4px; padding: 2px 8px; 
    font-size: 9px; font-weight: 800; 
}
"""

# ==============================================================================
# HELPER FUNCTIONS
def format_duration(seconds):
    if not seconds: return "0s"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    parts = []
    if h > 0: parts.append(f"{h}h")
    if m > 0: parts.append(f"{m}m")
    parts.append(f"{s}s")
    return " ".join(parts)

# ==============================================================================
# BACKEND WORKER (Merged Logic)
class EngineWorker(QThread):
    scripts_discovered = Signal(dict)
    status_update = Signal(str, str, str, str, bool) # path, status, last_run_txt, next_run_txt, is_running
    monitor_update = Signal(list, list) # active, upcoming
    discovery_timing = Signal(float, float) # last_ts, next_ts

    def __init__(self):
        super().__init__()
        self.running = True
        self.scripts_map = {}
        self.schedules = {}
        self.cron_map = {} # script_name -> set(hours) or 'ALL'
        self.running_tasks = {} 
        self.last_discovery = 0
        self.last_bq_sync = 0
        self.bq_sync_interval = 600 # 10 minutes
        
        # Scheduling & Queue
        self.execution_queue = [] # List of (priority_hour, script_name, path)
        self.max_concurrent = 5
        self.daily_execution_cache = defaultdict(int) # script_name -> count_today
        self.history_df = pd.DataFrame()
        
        self.PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "datalab-pagamentos")
        self.DATASET = os.getenv("BIGQUERY_DATASET", "ADMINISTRACAO_CELULA_PYTHON")
        self.TABLE_EXEC = "automacoes_exec"
        
        self.bq_verified = False # Safety flag
        
        self.setup_credentials()
        self.load_history()

    def setup_credentials(self):
        """Auto-configure Google Credentials from user AppData"""
        try:
            cred_dir = Path.home() / "AppData" / "Roaming" / "CELPY"
            if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS") and cred_dir.exists():
                jsons = list(cred_dir.glob("*.json"))
                if jsons:
                    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(jsons[0])
        except: pass

    def load_history(self):
        """Initial load from local Excel cache if available"""
        local_exec = CONFIG_ROOT / "automacoes_exec.xlsx"
        if local_exec.exists() and PANDAS_AVAIL:
            try:
                self.history_df = pd.read_excel(local_exec)
                self.update_local_cache_from_df()
            except: pass

    def update_local_cache_from_df(self):
        """Update daily counts based on history DF"""
        if self.history_df.empty: return
        
        try:
            # Ensure filtering for TODAY only
            today_str = datetime.now().strftime("%Y-%m-%d")
            
            # Convert date column safely - DERIVE FROM start_time FOR ROBUSTNESS
            # User reported format issues. start_time is likely '2025-12-12T12:02:07'
            self.history_df['temp_date'] = pd.to_datetime(self.history_df['start_time'], errors='coerce')
            self.history_df['date_str'] = self.history_df['temp_date'].dt.strftime("%Y-%m-%d")
            
            # Fallback if start_time parse fails (use existing date col)
            mask = self.history_df['date_str'].isna()
            if mask.any():
                 self.history_df.loc[mask, 'date_str'] = pd.to_datetime(self.history_df.loc[mask, 'date'], errors='coerce').dt.strftime("%Y-%m-%d")

            today_df = self.history_df[self.history_df['date_str'] == today_str]
            
            counts = today_df.groupby('script_name').size().to_dict()
            # Infinite Loop Fix: Use MAX of current cache vs new BQ data
            # This prevents a laggy BQ sync from overwriting local progress (resetting count to 0)
            for name, bq_count in counts.items():
                self.daily_execution_cache[name] = max(self.daily_execution_cache[name], bq_count)
            
            # Also ensure keys that exist in BQ but not in cache are added
            for name in counts:
                 if name not in self.daily_execution_cache:
                     self.daily_execution_cache[name] = counts[name]

        except Exception as e:
            print(f"Error updating cache: {e}")

    def sync_bq(self):
        """Pull latest execution history from BigQuery"""
        if not PANDAS_AVAIL: return
        
        print("Syncing BQ History...")
        try:
            # Query for TODAY's executions (limited columns for efficiency)
            # FIX: Hardcode timezone to avoid 'Hora oficial do Brasil' error
            # FIX: Use DATE(start_time) because 'date' column might be null
            query = f"""
                SELECT script_name, status, start_time, duration_seconds, date
                FROM `{self.PROJECT_ID}.{self.DATASET}.{self.TABLE_EXEC}`
                WHERE DATE(start_time) = CURRENT_DATE('America/Sao_Paulo')
            """
            
            # Using pandas_gbq
            import pandas_gbq
            new_df = pandas_gbq.read_gbq(query, project_id=self.PROJECT_ID, use_bqstorage_api=False)
            
            if not new_df.empty:
                self.history_df = new_df
                # Save to local cache
                local_exec = CONFIG_ROOT / "automacoes_exec.xlsx"
                self.history_df.to_excel(local_exec, index=False)
                
                # Update memory cache
                self.update_local_cache_from_df()
                self.bq_verified = True
                print(f"BQ Sync Success. Loaded {len(new_df)} rows. (Verified: {self.bq_verified})")
            else:
                # Today is empty. perform SAFETY CHECK (Health Verification)
                # Verify if we can read ANY data from the table (to distinguish 'Empty Day' vs 'Read Error')
                print("Today is empty. Verifying table health...")
                check_query = f"SELECT script_name FROM `{self.PROJECT_ID}.{self.DATASET}.{self.TABLE_EXEC}` LIMIT 1"
                check_df = pd.read_gbq(check_query, project_id=self.PROJECT_ID, use_bqstorage_api=False)
                
                if not check_df.empty:
                    self.bq_verified = True
                    print("BQ Verification: Table is readable (Historical data found).")
                else:
                    self.bq_verified = False
                    print("CRITICAL: BQ Verification FAILED. No data found in table (Empty?) or Read Error.")
                
        except Exception as e:
            print(f"BQ Sync Failed: {e}")
            # Do NOT set bq_verified = False here immediately if it was previously True? 
            # Ideally strict mode: connectivity lost = pause execution.
            if str(e): self.bq_verified = False

    def parse_cron(self, cron_str):
        """Parse cron schedule string into a set of hours or 'ALL'"""
        cron_str = str(cron_str).upper().strip()
        if cron_str in ["SEM", "MANUAL", "NONE", "NAN", "NAT"]:
            return set()
        if cron_str == "ALL":
            return "ALL"
        if not cron_str:
            return set()
            
        hours = set()
        try:
            parts = cron_str.split(",")
            for p in parts:
                if p.strip().isdigit():
                    hours.add(int(p.strip()))
        except: pass
        return hours

    def check_schedule_logic(self):
        """Check if scripts need to run based on Schedule vs Cache"""
        current_hour = datetime.now().hour
        
        # FIX: Iterate correctly over nested map {Area: [paths]}
        for area, paths in self.scripts_map.items():
            for path in paths:
                script_name = Path(path).stem
                cron = self.cron_map.get(script_name, set())
                
                target_runs = 0
                if cron == "ALL":
                    target_runs = current_hour + 1 # Should have run once for each hour so far
                elif isinstance(cron, set):
                    target_runs = sum(1 for h in cron if h <= current_hour)
                else:
                    continue
                    
                actual_runs = self.daily_execution_cache[script_name]
                
                # Calculate backlog
                needed = target_runs - actual_runs
                
                if needed > 0:
                    # Safety: Only run if BQ verified
                    if not self.bq_verified:
                        print(f"Skipping {script_name} - BQ Not Verified")
                        continue

                    # Add to queue if not already running or queued
                    if script_name not in self.running_tasks and script_name not in [x[1] for x in self.execution_queue]:
                        print(f"Queueing {script_name} (Target: {target_runs}, Actual: {actual_runs})")
                        self.execution_queue.append((0, script_name, path))

    def process_queue(self):
        """Process the backlog queue respecting concurrency"""
        # Cleanup finished tasks
        finished = [k for k, v in self.running_tasks.items() if v.poll() is not None]
        for k in finished:
            # Update cache locally immediately to prevent re-queueing (until next BQ sync confirms)
            print(f"Task finished: {k}")
            self.daily_execution_cache[k] += 1 
            del self.running_tasks[k]
            
        # Start new tasks if slots available
        while len(self.running_tasks) < self.max_concurrent and self.execution_queue:
            # Get next task
            priority, s_name, s_path = self.execution_queue.pop(0)
            
            if s_name in self.running_tasks: continue # Skip if already running
            
            print(f"Starting Queued Task: {s_name}")
            self.run_script(s_path, s_name)

    def run_script(self, path, name):
        try:
            # Use 'run_command' style via subprocess
            # Ensure we use the proper python env (sys.executable)
            proc = subprocess.Popen(
                [sys.executable, path],
                cwd=os.path.dirname(path),
                creationflags=subprocess.CREATE_NO_WINDOW if sys.platform=='win32' else 0
            )
            self.running_tasks[name] = proc
        except Exception as e:
            print(f"Failed to start {name}: {e}")

    def stop(self):
        self.running = False
        self.wait()

    def run(self):
        # Initial BQ Sync
        self.sync_bq()
        self.last_bq_sync = time.time()
        
        while self.running:
            now = time.time()
            
            # Discovery (every 1m)
            if now - self.last_discovery > 60:
                self.discover()
                self.last_discovery = now
                self.discovery_timing.emit(self.last_discovery, self.last_discovery + 60)
                
            # BQ Sync (every 10m)
            if now - self.last_bq_sync > self.bq_sync_interval:
                self.sync_bq()
                self.last_bq_sync = now
                
            # Scheduler logic
            self.check_schedule_logic()
            self.process_queue()
            
            # Updates
            self.emit_all_statuses()
            self.emit_monitor_data()
            
            time.sleep(1) # Interval for checks
    def discover(self):
        # 1. Scan All Py Files First
        all_found_scripts = {} # stem -> full_path
        try:
            for root, dirs, files in os.walk(str(BASE_PATH)):
                if os.path.basename(root).lower() == "metodos":
                    for f in files:
                        if f.endswith(".py") and not f.startswith("__"):
                            full_path = str(Path(root) / f)
                            stem = Path(f).stem
                            all_found_scripts[stem] = full_path
        except: pass
        
        # 2. Read Excel & Filter/Group
        final_map = defaultdict(list)
        new_cron_map = {}
        
        if PANDAS_AVAIL and EXCEL_PATH.exists():
            try:
                # Read with retry in case of lock
                try: 
                    df = pd.read_excel(EXCEL_PATH)
                except: 
                    time.sleep(1)
                    df = pd.read_excel(EXCEL_PATH)
                    
                # Ensure columns exist, forgiving case
                df.columns = [c.lower().strip() for c in df.columns]
                
                # Identify columns
                col_name = next((c for c in df.columns if 'script' in c or 'process' in c or 'nome' in c), None)
                col_area = next((c for c in df.columns if 'area' in c or 'depto' in c), None)
                col_cron = next((c for c in df.columns if 'cron' in c or 'schedule' in c), None)
                col_active = next((c for c in df.columns if 'active' in c or 'ativo' in c), None)
                
                if col_name and col_area:
                    for _, row in df.iterrows():
                        # Check Active
                        if col_active:
                            val = str(row[col_active]).lower().strip()
                            if val not in ['true', '1', 'verdadeiro', 'sim', 's', 'on']:
                                continue # Skip inactive

                        s_name = str(row[col_name]).strip()
                        # Clean extension if present in excel
                        if s_name.lower().endswith(".py"): 
                            s_name = s_name[:-3]
                            
                        # Lookup
                        if s_name in all_found_scripts:
                            path = all_found_scripts[s_name]
                            area = str(row[col_area]).strip().upper()
                            if not area or area == "NAN": area = "GENERAL"
                            
                            final_map[area].append(path)
                            
                            # Cron Logic
                            if col_cron:
                                cron_val = str(row[col_cron]).strip()
                                new_cron_map[s_name] = self.parse_cron(cron_val)
                            
                else:
                    print("Excel columns not identified. Using fallback discovery.")
                    final_map = self._fallback_discovery(all_found_scripts)
            except Exception as e:
                print(f"Excel Discover Error: {e}")
                final_map = self._fallback_discovery(all_found_scripts)
        else:
            final_map = self._fallback_discovery(all_found_scripts)

        self.cron_map = new_cron_map
        self.scripts_map = dict(sorted(final_map.items()))
        self.scripts_discovered.emit(self.scripts_map)

    def _fallback_discovery(self, all_scripts):
        mapped = defaultdict(list)
        for name, path in all_scripts.items():
            try:
                parts = Path(path).parts
                if "metodos" in parts:
                    idx = parts.index("metodos")
                    area = parts[idx-1].upper()
                    mapped[area].append(path)
                else:
                    mapped["UNKNOWN"].append(path)
            except:
                mapped["UNKNOWN"].append(path)
        return mapped

    def emit_all_statuses(self):
        today_str = datetime.now().strftime("%Y-%m-%d")
        
        for area, paths in self.scripts_map.items():
            for path in paths:
                s_name = Path(path).stem
                
                # Default Status
                status = "IDLE"
                
                # Check history first
                # Optimization: Filter mask once outside loop? 
                # Doing it inside for safety/simplicity first, can optimize later.
                if not self.history_df.empty:
                    date_col = 'date_str' if 'date_str' in self.history_df.columns else 'date'
                    # Safe filter
                    try:
                        matches = self.history_df[
                            (self.history_df['script_name'] == s_name) & 
                            (self.history_df[date_col].astype(str).str.startswith(today_str))
                        ]
                        
                        if not matches.empty:
                            last_run_row = matches.iloc[-1]
                            bq_status = str(last_run_row['status']).upper()
                            
                            if 'SUCCESS' in bq_status or 'SUCESSO' in bq_status:
                                status = "SUCCESS"
                            elif 'ERROR' in bq_status or 'ERRO' in bq_status:
                                status = "ERROR"
                            elif 'NO_DATA' in bq_status:
                                status = "NO_DATA"
                            else:
                                status = bq_status # Show raw status if unknown
                    except: pass
                
                # Check if running (override history)
                is_running = s_name in self.running_tasks
                if is_running:
                    status = "RUNNING"
                
                # Queue check
                in_queue = s_name in [x[1] for x in self.execution_queue]
                if in_queue and status == "IDLE": 
                    status = "SCHEDULED"

                # Text Details
                last_run_txt = "Last: Never"
                next_run_txt = "Next: N/A"
                
                # Calculate Next Run text from cron
                cron = self.cron_map.get(s_name, set())
                cur_h = datetime.now().hour
                if cron == "ALL":
                    next_run_txt = "Next: Hourly"
                elif isinstance(cron, set) and cron:
                    upcoming = sorted([h for h in cron if h > cur_h])
                    if upcoming:
                        next_run_txt = f"Next: {upcoming[0]}:00"
                    else:
                        next_run_txt = "Next: Tomorrow"
                elif not cron:
                     next_run_txt = "Next: Manually"

                # Calculate Last Run text from matches
                if 'matches' in locals() and not matches.empty:
                     try:
                         # Assume last_run_row is valid
                         last_run_row = matches.iloc[-1]
                         st = last_run_row['start_time']
                         dur = last_run_row['duration_seconds']
                         if hasattr(st, 'strftime'):
                             t_str = st.strftime('%H:%M')
                         elif isinstance(st, str):
                             # Try parsing ISO format with T
                             try:
                                 if 'T' in st:
                                     # "2025-12-12T12:02:07" -> split T -> 12:02:07 -> [:5] -> 12:02
                                     t_str = st.split('T')[1][:5]
                                 elif ' ' in st:
                                     # "2025-12-12 12:02:07"
                                     t_str = st.split(' ')[1][:5]
                                 else:
                                     # Unlikely but fallback
                                     t_str = st[11:16]
                             except:
                                 t_str = "??:??"
                         else:
                             t_str = str(st)[11:16] # Fallback string slice
                         
                         dur_txt = format_duration(float(dur)) if dur else "?"
                         last_run_txt = f"Today {t_str} > {dur_txt}"
                     except: pass
                
                self.status_update.emit(path, status, last_run_txt, next_run_txt, is_running)

    def emit_monitor_data(self):
        # Running
        running_data = []
        for name, proc in self.running_tasks.items():
            running_data.append({
                'name': name.upper(),
                'duration': "Running..."
            })

        # Upcoming - Restore User Logic
        upcoming_data = []
        now_dt = datetime.now()
        next_h = (now_dt.hour + 1) % 24

        for s_name, cron in self.cron_map.items():
            if isinstance(cron, set) and next_h in cron:
                 upcoming_data.append({
                    'name': s_name.upper(),
                    'hour': next_h
                })

        self.monitor_update.emit(running_data, upcoming_data)

    def run_script(self, path, name=None):
        if not name: name = Path(path).stem
        if name in self.running_tasks: return
        try:
            cwd = os.path.dirname(path)
            # Use same python env
            proc = subprocess.Popen([sys.executable, path], cwd=cwd,
                                    creationflags=subprocess.CREATE_NO_WINDOW if sys.platform=='win32' else 0)
            self.running_tasks[name] = proc
        except Exception as e:
            print(f"Error running {name}: {e}")

    def kill_script(self, path):
        name = Path(path).stem
        if name in self.running_tasks:
            try:
                proc = self.running_tasks[name]
                proc.terminate()
                time.sleep(0.5)
                if proc.poll() is None: proc.kill()
                del self.running_tasks[name]
            except: pass


# ==============================================================================
# PREMIUM UI COMPONENT (ScriptCard)
class PremiumScriptCard(QFrame):
    def __init__(self, name, area, path, on_run, on_kill):
        super().__init__()
        self.setObjectName("Card")
        self.path = path
        self.on_run = on_run
        self.on_kill = on_kill
        self.setFixedSize(270, 160) # Compact to fit grid
        
        # Shadow
        shadow = QGraphicsDropShadowEffect()
        shadow.setBlurRadius(16)
        shadow.setColor(QColor(0,0,0, 80))
        shadow.setOffset(0, 4)
        self.setGraphicsEffect(shadow)

        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(15, 15, 15, 15)
        main_layout.setSpacing(6)

        # --- ROW 1: Icon | Title/Subtitle | Badge ---
        row1 = QHBoxLayout()
        self.icon_lbl = QLabel("📄")
        self.icon_lbl.setFixedSize(28, 28)
        self.icon_lbl.setAlignment(Qt.AlignCenter)
        self.icon_lbl.setStyleSheet("background: #1F2937; border-radius: 6px; color: #9CA3AF; font-size: 14px;")
        row1.addWidget(self.icon_lbl)
        
        v_titles = QVBoxLayout()
        v_titles.setSpacing(0)
        self.lbl_name = QLabel(name)
        self.lbl_name.setStyleSheet("font-size: 13px; font-weight: 700; color: #F3F4F6;")
        self.lbl_area = QLabel(area)
        self.lbl_area.setStyleSheet("font-size: 9px; font-weight: 700; color: #6B7280; text-transform: uppercase;")
        v_titles.addWidget(self.lbl_name)
        v_titles.addWidget(self.lbl_area)
        row1.addLayout(v_titles)
        
        row1.addStretch()
        
        self.status_badge = QLabel("IDLE")
        self.status_badge.setObjectName("StatusBadge")
        self.status_badge.setAlignment(Qt.AlignCenter)
        self.status_badge.setStyleSheet("background-color: #1F2937; color: #9CA3AF;") 
        row1.addWidget(self.status_badge)
        
        main_layout.addLayout(row1)
        main_layout.addSpacing(6)

        # --- ROW 2: Last Run ---
        row2 = QHBoxLayout()
        self.icon_last = QLabel("🕒") 
        self.icon_last.setStyleSheet("color: #6B7280; font-size: 11px; margin-right: 4px;")
        self.lbl_last_run = QLabel("Never")
        self.lbl_last_run.setStyleSheet("font-size: 10px; color: #9CA3AF;")
        
        row2.addWidget(self.icon_last)
        row2.addWidget(self.lbl_last_run)
        row2.addStretch()
        main_layout.addLayout(row2)

        # --- ROW 3: Next Run ---
        row3 = QHBoxLayout()
        self.icon_next = QLabel("📅")
        self.icon_next.setStyleSheet("color: #6B7280; font-size: 11px; margin-right: 4px;")
        self.lbl_next_run = QLabel("Manual Trigger Only")
        self.lbl_next_run.setStyleSheet("font-size: 10px; color: #9CA3AF;")
        
        row3.addWidget(self.icon_next)
        row3.addWidget(self.lbl_next_run)
        row3.addStretch()
        main_layout.addLayout(row3)
        
        main_layout.addStretch()

        # --- SEPARATOR ---
        sep = QLabel()
        sep.setFixedHeight(1)
        sep.setStyleSheet("background-color: #2D3748;")
        main_layout.addWidget(sep)
        main_layout.addSpacing(6)

        # --- ROW 4: Actions ---
        row4 = QHBoxLayout()
        
        self.btn_menu = QPushButton("⋮")
        self.btn_menu.setFixedSize(20, 26)
        self.btn_menu.setStyleSheet("background: transparent; color: #6B7280; font-size: 16px; font-weight: 900;")
        self.btn_menu.setCursor(Qt.PointingHandCursor)
        
        row4.addWidget(self.btn_menu)
        
        self.btn_run = QPushButton("Run")
        self.btn_run.setObjectName("PrimaryButton")
        self.btn_run.setCursor(Qt.PointingHandCursor)
        self.btn_run.clicked.connect(self._on_click)
        row4.addWidget(self.btn_run)
        
        main_layout.addLayout(row4)
        
        self.is_running = False

    def _on_click(self):
        if self.is_running:
            # Single click on running = Confirm Stop (or just trigger logic handled by double click if preferred, 
            # but user specifically asked for double click. Keeping single click as STOP for now, usually buttons are single click)
            # User request: "quando eu der duplo clique ... apareça um popup". 
            # Implies the CARD double click? Or the Button? 
            # "double click over some script... popup... confirm... merely that thread"
            # I will assume double click on the CARD body.
            # But the Button is explicit "Run/Stop". I should probably add confirmation to the Stop button too or leave it?
            # User said "double click about some script that is executing".
            # I will leave the Button as is (immediate action? or maybe add check there too?).
            # Given the safety concern, I'll add confirmation to the Stop Button too.
            self.confirm_kill()
        else:
            self.on_run(self.path)

    def mouseDoubleClickEvent(self, event):
        if self.is_running:
            self.confirm_kill()
        event.accept()

    def confirm_kill(self):
        reply = QMessageBox.question(
            self, 'Confirmar Interrupção', 
            f"Deseja mesmo interromper a execução de:\n\n{self.lbl_name.text()}\n\nIsso encerrará apenas este processo isoladamente.",
            QMessageBox.Yes | QMessageBox.No, QMessageBox.No
        )
        if reply == QMessageBox.Yes:
            self.on_kill(self.path)

    def update_state(self, status, last_run, next_run, running):
        self.is_running = running
        self.lbl_last_run.setText(last_run)
        self.lbl_next_run.setText(next_run)
        self.status_badge.setText(status)
        
        if status == "RUNNING":
            self.status_badge.setStyleSheet("background-color: #1E3A8A; color: #60A5FA; border: 1px solid #1E40AF;") # Blue
            self.btn_run.setText("Stop")
            self.btn_run.setObjectName("DangerButton") 
            self.icon_lbl.setText("⚡")
            self.status_badge.setStyleSheet("background-color: #1E3A8A; color: #60A5FA; border: 1px solid #1E40AF;") # Blue
            self.btn_run.setText("Stop")
            self.btn_run.setObjectName("DangerButton") 
            self.icon_lbl.setText("⚡")
            self.setStyleSheet("QFrame#Card { border: 2px solid #3B82F6; }") 
        elif status == "SUCCESS":
            self.status_badge.setStyleSheet("background-color: #064E3B; color: #34D399; border: 1px solid #065F46;") # Green
            self.btn_run.setText("Run")
            self.btn_run.setObjectName("PrimaryButton")
            self.icon_lbl.setText("✅")
            self.setStyleSheet("")
        elif status == "ERROR":
            self.status_badge.setStyleSheet("background-color: #7F1D1D; color: #F87171; border: 1px solid #991B1B;") # Red
            self.btn_run.setText("Run")
            self.btn_run.setObjectName("PrimaryButton")
            self.icon_lbl.setText("❌")
            self.setStyleSheet("")
        elif status == "NO_DATA":
            self.status_badge.setStyleSheet("background-color: #78350F; color: #FBBF24; border: 1px solid #92400E;") # Yellow
            self.btn_run.setText("Run")
            self.btn_run.setObjectName("PrimaryButton")
            self.icon_lbl.setText("⚠️")
            self.setStyleSheet("")
        elif status == "SCHEDULED":
            self.status_badge.setStyleSheet("background-color: #374151; color: #D1D5DB; border: 1px solid #4B5563;") # Gray
            self.btn_run.setText("Run")
            self.btn_run.setObjectName("PrimaryButton")
            self.icon_lbl.setText("⏰")
            self.setStyleSheet("") 
        elif status == "DISABLED":
             self.status_badge.setStyleSheet("background-color: #111827; color: #6B7280; border: 1px solid #374151;") # Dark
             self.btn_run.setText("Run")
             self.btn_run.setObjectName("PrimaryButton")
             self.icon_lbl.setText("🚫")
             self.setStyleSheet("QFrame#Card { opacity: 0.6; }")
        else:
            self.status_badge.setStyleSheet("background-color: #1F2937; color: #9CA3AF;")
            self.btn_run.setText("Run")
            self.btn_run.setObjectName("PrimaryButton")
            self.icon_lbl.setText("📄")
            self.setStyleSheet("")
            
        self.btn_run.style().unpolish(self.btn_run)
        self.btn_run.style().polish(self.btn_run)


# ==============================================================================
# MAIN WINDOW (C6 RPA DASHBOARD)
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("C6 RPA Dashboard")
        self.resize(1380, 850)
        
        
        # Worker
        self.worker = EngineWorker()
        self.worker.scripts_discovered.connect(self.on_discovery)
        self.worker.status_update.connect(self.on_status_update)
        self.worker.monitor_update.connect(self.on_monitor_update)
        self.worker.discovery_timing.connect(self.on_discovery_timing) # NEW
        self.worker.start()
        
        # Countdown Timer (Independent of Backend)
        self.next_discovery_ts = 0
        self.timer_countdown = QTimer(self)
        self.timer_countdown.timeout.connect(self.update_countdown)
        self.timer_countdown.start(1000) # 1s tick

        # UI
        central = QWidget()
        central.setObjectName("Central")
        self.setCentralWidget(central)
        
        # Main Layout: HBox (Sidebar + Content)
        main_layout = QHBoxLayout(central)
        main_layout.setContentsMargins(0,0,0,0)
        main_layout.setSpacing(0)

        # ---------------------------------------------------------
        # 1. SIDEBAR (Fixed Width)
        sidebar = QWidget()
        sidebar.setObjectName("Sidebar")
        sidebar.setFixedWidth(240)
        side_vbox = QVBoxLayout(sidebar)
        side_vbox.setContentsMargins(20, 30, 20, 20)
        side_vbox.setSpacing(10)

        # Logo
        logo_row = QHBoxLayout()
        icon = QLabel("📚") 
        icon.setStyleSheet("font-size: 20px;")
        lbl_logo = QLabel("C6 RPA")
        lbl_logo.setObjectName("Logo")
        logo_row.addWidget(icon)
        logo_row.addWidget(lbl_logo)
        logo_row.addStretch()
        side_vbox.addLayout(logo_row)
        
        side_vbox.addSpacing(30)
        
        # Menu Label
        lbl_menu = QLabel("MAIN MENU")
        lbl_menu.setStyleSheet("color: #6B7280; font-size: 11px; font-weight: 700; letter-spacing: 0.5px;")
        side_vbox.addWidget(lbl_menu)
        side_vbox.addSpacing(5)

        # Nav Buttons (Exclusive Group)
        self.nav_group = QButtonGroup(self)
        self.nav_group.setExclusive(True)

        self.btn_monitor = self.create_nav_btn("    Monitor", "88", True)
        self.btn_autos   = self.create_nav_btn("    Automations", "Suggestion", False) 
        
        # Add to layout
        side_vbox.addWidget(self.btn_monitor)
        side_vbox.addWidget(self.btn_autos)

        side_vbox.addWidget(self.btn_autos)

        side_vbox.addStretch()

        # Update Countdown Label
        self.lbl_countdown = QLabel("Next Update: --")
        self.lbl_countdown.setStyleSheet("color: #4B5563; font-size: 10px; font-weight: 700;")
        self.lbl_countdown.setAlignment(Qt.AlignCenter)
        side_vbox.addWidget(self.lbl_countdown)
        
        side_vbox.addSpacing(10)

        # Disconnect
        btn_disc = QPushButton("    Disconnect")
        btn_disc.setObjectName("NavButton")
        side_vbox.addWidget(btn_disc)
        
        main_layout.addWidget(sidebar)

        # ---------------------------------------------------------
        # 2. MAIN CONTENT AREA
        content = QWidget()
        content_vbox = QVBoxLayout(content)
        content_vbox.setContentsMargins(40, 40, 40, 40)
        content_vbox.setSpacing(20)

        # HEADER ROW
        header_row = QHBoxLayout()
        
        # Text Column
        v_head_text = QVBoxLayout()
        self.lbl_page_title = QLabel("Automation Scripts")
        self.lbl_page_title.setObjectName("PageTitle")
        self.lbl_sub_title = QLabel("Welcome back, Administrator.")
        self.lbl_sub_title.setObjectName("PageSubTitle")
        v_head_text.addWidget(self.lbl_page_title)
        v_head_text.addWidget(self.lbl_sub_title)
        header_row.addLayout(v_head_text)
        
        header_row.addStretch()
        
        # System Status
        v_status = QVBoxLayout()
        v_status.setAlignment(Qt.AlignRight | Qt.AlignTop)
        lbl_stat_title = QLabel("SYSTEM STATUS")
        lbl_stat_title.setStyleSheet("color: #6B7280; font-size: 9px; font-weight: 700;")
        lbl_stat_val = QLabel("● Online")
        lbl_stat_val.setObjectName("SystemStatus")
        v_status.addWidget(lbl_stat_title)
        v_status.addWidget(lbl_stat_val)
        header_row.addLayout(v_status)
        
        # Reload Button (Visual)
        btn_reload = QPushButton("↻")
        btn_reload.setFixedSize(30, 30)
        btn_reload.setStyleSheet("background: #1F2937; border-radius: 15px; color: #9CA3AF;")
        header_row.addWidget(btn_reload)

        content_vbox.addLayout(header_row)
        content_vbox.addSpacing(10)

        # FILTER BAR ROW
        filter_row = QHBoxLayout()
        
        # Search
        self.txt_search = QLineEdit()
        self.txt_search.setPlaceholderText("Q Search scripts by name...")
        self.txt_search.setFixedWidth(300)
        self.txt_search.textChanged.connect(self.on_search)
        filter_row.addWidget(self.txt_search)
        
        filter_row.addStretch()

        # Filter Pills
        icon_filter = QLabel("Y") # Filter Icon placeholder
        icon_filter.setStyleSheet("color: #6B7280; font-weight: 800; margin-right: 5px;")
        filter_row.addWidget(icon_filter)

        self.pill_scroll = QScrollArea()
        self.pill_scroll.setWidgetResizable(True)
        self.pill_scroll.setFixedHeight(50) # Fixed height for pill row
        self.pill_scroll.setStyleSheet("background: transparent; border: none;")
        self.pill_scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.pill_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)

        self.pill_area = QWidget()
        self.pill_area.setStyleSheet("background: transparent;")
        self.pill_layout = QHBoxLayout(self.pill_area)
        self.pill_layout.setContentsMargins(0,0,0,0)
        self.pill_layout.setSpacing(8)
        self.pill_layout.setAlignment(Qt.AlignLeft) # Important for horizontal flow
        
        self.pill_group = QButtonGroup(self)
        self.pill_group.setExclusive(True)
        self.pill_group.buttonClicked.connect(self.on_pill_clicked)
        
        # Default All
        self.add_pill("ALL", True)
        
        self.pill_scroll.setWidget(self.pill_area)
        filter_row.addWidget(self.pill_scroll)
        
        content_vbox.addLayout(filter_row)
        
        # STACKED PAGES
        self.stack = QStackedWidget()
        
        # Page 0: Automations (PILL FILTERABLE CARDS)
        self.page_automations = QWidget()
        grid_layout_wrap = QVBoxLayout(self.page_automations)
        grid_layout_wrap.setContentsMargins(0,0,0,0)

        self.scroll = QScrollArea()
        self.scroll.setWidgetResizable(True)
        self.scroll.setStyleSheet("background: transparent; border: none;")
        
        self.cards_container = QWidget()
        self.cards_layout = QGridLayout(self.cards_container) 
        self.cards_layout.setContentsMargins(0, 5, 0, 50)
        self.cards_layout.setSpacing(20)
        self.cards_layout.setAlignment(Qt.AlignTop | Qt.AlignLeft)
        
        self.cards_container.setLayout(self.cards_layout)
        self.scroll.setWidget(self.cards_container)
        
        grid_layout_wrap.addWidget(self.scroll)
        self.stack.addWidget(self.page_automations)
        
        # Page 1: Active Monitor (List of running)
        self.page_monitor = QWidget()
        pm_layout = QVBoxLayout(self.page_monitor)
        
        lbl_list_title = QLabel("Active Processes")
        lbl_list_title.setObjectName("PageTitle")
        pm_layout.addWidget(lbl_list_title)
        
        self.monitor_list = QListWidget()
        self.monitor_list.setStyleSheet("background: transparent; border: none;")
        pm_layout.addWidget(self.monitor_list)
        
        self.stack.addWidget(self.page_monitor)
        
        content_vbox.addWidget(self.stack)

        main_layout.addWidget(content)

        # Initialization
        self.all_cards = {}
        self.current_area_filter = "ALL"
        self.known_areas = set()
        
        # Set Default Page -> Automations
        self.stack.setCurrentIndex(0)
        self.btn_autos.setChecked(True)

        # Prevent Sleep (Now Safe)
        self.prevent_sleep()

    def create_nav_btn(self, text, icon_name, checked):
        btn = QPushButton(text)
        btn.setObjectName("NavButton")
        btn.setCursor(Qt.PointingHandCursor)
        btn.setCheckable(True)
        btn.setChecked(checked)
        btn.clicked.connect(lambda: self.on_nav_click(text))
        self.nav_group.addButton(btn)
        return btn

    def add_pill(self, name, checked=False):
        btn = QPushButton(name)
        btn.setObjectName("PillButton")
        btn.setCursor(Qt.PointingHandCursor)
        btn.setCheckable(True)
        if checked: btn.setChecked(True)
        self.pill_layout.addWidget(btn)
        self.pill_group.addButton(btn)

    def on_nav_click(self, text):
        clean = text.strip()
        if "Monitor" in clean:
            self.lbl_page_title.setText("System Monitor")
            self.stack.setCurrentIndex(1)
        elif "Automations" in clean:
            self.lbl_page_title.setText("Automation Scripts")
            self.stack.setCurrentIndex(0)
        else:
            self.lbl_page_title.setText(clean)
            # Placeholder pages
    
    def on_discovery(self, scripts_map):
        # Update Pills
        new_areas = set(scripts_map.keys()) - self.known_areas
        for area in sorted(new_areas):
            self.add_pill(area)
            self.known_areas.add(area)

        # Create/Update Cards
        for area, paths in scripts_map.items():
            for p in paths:
                if p not in self.all_cards:
                    name = Path(p).stem.replace("_", " ").title()
                    # Updated Premium Card
                    card = PremiumScriptCard(name, area, p, self.worker.run_script, self.worker.kill_script)
                    self.all_cards[p] = card
                    card.setVisible(False)
                    self.cards_layout.addWidget(card)

    def prevent_sleep(self):
        """Prevent Windows from sleeping or turning off the display"""
        try:
            # ES_CONTINUOUS | ES_SYSTEM_REQUIRED | ES_DISPLAY_REQUIRED
            # 0x80000000 | 0x00000001 | 0x00000002
            ctypes.windll.kernel32.SetThreadExecutionState(0x80000003) 
            print("Power Save blocked: Display & System required.")
        except Exception as e:
            print(f"Failed to set execution state: {e}")
        
        self.filter_view()

    def on_pill_clicked(self, btn):
        self.current_area_filter = btn.text()
        self.filter_view()

    def filter_view(self):
        query = self.txt_search.text().lower()
        
        while self.cards_layout.count():
            item = self.cards_layout.takeAt(0)
            if item.widget(): item.widget().setVisible(False)
            
        visible = []
        for p, card in self.all_cards.items():
            # Pill Logic
            in_area = False
            card_area = card.lbl_area.text().upper()
            
            if self.current_area_filter == "ALL":
                in_area = True
            elif self.current_area_filter == card_area:
                in_area = True
                
            if in_area and query in Path(p).stem.lower():
                visible.append(card)
        
        visible.sort(key=lambda c: c.path)
        
        cols = 4 # Fixed columns
        for idx, card in enumerate(visible):
            row = idx // cols
            col = idx % cols
            self.cards_layout.addWidget(card, row, col)
            card.setVisible(True)

    def on_status_update(self, path, status, last_run, next_run, is_running):
        if path in self.all_cards:
            self.all_cards[path].update_state(status, last_run, next_run, is_running)

    def on_monitor_update(self, running_list, upcoming):
        self.monitor_list.clear() # Primitive refresh
        if not running_list:
            self.monitor_list.addItem(QListWidgetItem("No active processes."))
        else:
                w_item = QListWidgetItem(f"⚡ {item['name']}   —   Running for: {item['duration']}")
                w_item.setForeground(QColor("#F59E0B"))
                self.monitor_list.addItem(w_item)

    def on_discovery_timing(self, last, next_ts):
        self.next_discovery_ts = next_ts
        self.update_countdown()

    def update_countdown(self):
        if self.next_discovery_ts == 0:
            self.lbl_countdown.setText("Initializing...")
            return
            
        remaining = int(self.next_discovery_ts - time.time())
        if remaining < 0: remaining = 0
        
        self.lbl_countdown.setText(f"UPDATE IN: {remaining}s")
        
        if remaining < 10:
             self.lbl_countdown.setStyleSheet("color: #F59E0B; font-size: 10px; font-weight: 800;")
        else:
             self.lbl_countdown.setStyleSheet("color: #4B5563; font-size: 10px; font-weight: 700;")
             


    def on_search(self):
        self.filter_view()
        
    def closeEvent(self, event):
        self.worker.stop()
        super().closeEvent(event)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyleSheet(STYLESHEET)
    
    window = MainWindow()
    window.show()
    sys.exit(app.exec())
