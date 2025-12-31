import os
import sys
import time
import json
import ctypes
import threading
import subprocess
import pandas as pd
import pandas_gbq
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

from PySide6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
                               QFrame, QLabel, QPushButton, QScrollArea, QGridLayout, 
                               QLineEdit, QProgressBar)
from PySide6.QtCore import Qt, QTimer

# ==============================================================================
# STYLES
# ==============================================================================
GLOBAL_STYLESHEET = """
QMainWindow {
    background-color: #020617;
}

QScrollArea {
    border: none;
    background: transparent;
}

QScrollBar:vertical {
    border: none;
    background: #0f172a;
    width: 8px;
    margin: 0px;
}
QScrollBar::handle:vertical {
    background: #334155;
    min-height: 20px;
    border-radius: 4px;
}
QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
    height: 0px;
}

QLabel { color: #e2e8f0; font-family: 'Segoe UI', sans-serif; }

/* Script Card */
QFrame#ScriptCard {
    background-color: #1e293b;
    border: 1px solid #334155;
    border-radius: 12px;
}
QFrame#ScriptCard:hover {
    border: 1px solid #3b82f6;
    background-color: #253349;
}

QLabel#CardTitle {
    font-size: 16px;
    font-weight: bold;
    color: white;
}
QLabel#CardArea {
    font-size: 10px;
    font-weight: bold;
    color: #94a3b8;
}
QLabel#CardStatus {
    font-size: 11px;
    font-weight: bold;
    padding: 4px 8px;
    border-radius: 4px;
}

QPushButton#RunBtn {
    background-color: #2563eb;
    color: white;
    border: none;
    padding: 8px;
    border-radius: 6px;
    font-weight: bold;
}
QPushButton#RunBtn:hover { background-color: #1d4ed8; }
QPushButton#RunBtn:disabled { background-color: #1e293b; color: #475569; }

QPushButton#StopBtn {
    background-color: #dc2626;
    color: white;
    border: none;
    padding: 8px;
    border-radius: 6px;
    font-weight: bold;
}
QPushButton#StopBtn:hover { background-color: #b91c1c; }
QPushButton#StopBtn:disabled { background-color: #1e293b; color: #475569; }

/* Sidebar */
QFrame#SideListItem {
    background-color: #1e293b;
    border-radius: 6px;
    border: 1px solid #334155;
}
"""

# ==============================================================================
# UI COMPONENTS
# ==============================================================================
class ScriptCard(QFrame):
    def __init__(self, worker, parent=None):
        super().__init__(parent)
        self.worker = worker # Reference to EngineWorker
        self.setObjectName("ScriptCard")
        self.setFixedSize(320, 320) # Increased Size
        
        layout = QVBoxLayout(self)
        layout.setSpacing(8)
        layout.setContentsMargins(15, 15, 15, 15)
        
        # Header (Icon + Area + Status)
        header = QHBoxLayout()
        
        # Icon
        icon_lbl = QLabel("🐍")
        icon_lbl.setStyleSheet("font-size: 24px;")
        header.addWidget(icon_lbl)
        
        meta_layout = QVBoxLayout()
        self.lbl_area = QLabel("AREA")
        self.lbl_area.setObjectName("CardArea")
        meta_layout.addWidget(self.lbl_area)
        
        self.lbl_status = QLabel("IDLE")
        self.lbl_status.setObjectName("CardStatus")
        self.lbl_status.setAlignment(Qt.AlignCenter)
        self.lbl_status.setStyleSheet("background-color: #1e293b; color: #94a3b8;")
        meta_layout.addWidget(self.lbl_status)
        
        header.addLayout(meta_layout)
        header.addStretch()
        layout.addLayout(header)
        
        # Title
        self.lbl_name = QLabel("Script Name")
        self.lbl_name.setObjectName("CardTitle")
        self.lbl_name.setWordWrap(True)
        self.lbl_name.setFixedHeight(45)
        self.lbl_name.setAlignment(Qt.AlignTop | Qt.AlignLeft)
        layout.addWidget(self.lbl_name)
        
        # Last Exec
        self.lbl_last_title = QLabel("ÚLTIMA EXECUÇÃO:")
        self.lbl_last_title.setObjectName("CardArea")
        layout.addWidget(self.lbl_last_title)
        
        self.lbl_last_val = QLabel("--:--:--")
        self.lbl_last_val.setStyleSheet("font-size: 13px; font-weight: bold; color: white;")
        layout.addWidget(self.lbl_last_val)
        
        # Next Run
        self.lbl_next = QLabel("PRÓXIMA: ---")
        self.lbl_next.setStyleSheet("font-size: 11px; font-weight: bold; color: #3b82f6;")
        layout.addWidget(self.lbl_next)
        
        layout.addStretch()
        
        # Progress (Runs Hoje)
        prog_info = QHBoxLayout()
        prog_info.addWidget(QLabel("RUNS HOJE", objectName="CardArea"))
        self.lbl_runs = QLabel("0 / 0", objectName="CardArea", alignment=Qt.AlignRight)
        prog_info.addWidget(self.lbl_runs)
        layout.addLayout(prog_info)
        
        self.progress = QProgressBar()
        self.progress.setTextVisible(False)
        layout.addWidget(self.progress)
        
        # Buttons
        btn_layout = QHBoxLayout()
        self.btn_run = QPushButton("RUN")
        self.btn_run.setObjectName("RunBtn")
        self.btn_stop = QPushButton("STOP")
        self.btn_stop.setObjectName("StopBtn")
        
        btn_layout.addWidget(self.btn_run)
        btn_layout.addWidget(self.btn_stop)
        layout.addLayout(btn_layout)
        
        # Callbacks
        self.script_id = None
        self.btn_run.clicked.connect(self.request_run)
        self.btn_stop.clicked.connect(self.request_stop)
        
    def request_run(self):
        if self.script_id and self.worker:
             path = self.worker.scripts_map.get(self.script_id)
             if path: self.worker.run_script(path, self.script_id)
            
    def request_stop(self):
        if self.script_id and self.worker:
             self.worker.kill_script(self.script_id)

    def update_data(self, script_id, data):
        self.script_id = script_id
        
        self.lbl_name.setText(data['name'].replace('_', ' ').upper())
        self.lbl_area.setText(data['area'])
        
        # Status Color
        st = data['status']
        
        display_status = st
        if st == 'RUNNING' and data.get('run_duration') is not None:
            display_status = f"RUNNING ({int(data['run_duration'])}s)"
            
        self.lbl_status.setText(display_status)
        
        if st == 'RUNNING':
            self.lbl_status.setStyleSheet("background-color: #1e3a8a; color: #60a5fa;")
            self.btn_run.setDisabled(True)
            self.btn_stop.setDisabled(False)
        elif st == 'SUCCESS':
            self.lbl_status.setStyleSheet("background-color: #14532d; color: #4ade80;")
            self.btn_run.setDisabled(False)
            self.btn_stop.setDisabled(True)
        elif st == 'ERROR':
            self.lbl_status.setStyleSheet("background-color: #450a0a; color: #f87171;")
            self.btn_run.setDisabled(False)
            self.btn_stop.setDisabled(True)
        else:
            self.lbl_status.setStyleSheet("background-color: #1e293b; color: #94a3b8;")
            self.btn_run.setDisabled(False)
            self.btn_stop.setDisabled(True)
            
        # Last exec
        if data['last_exec']:
            ts = data['last_exec']['timestamp']
            status = data['last_exec']['status']
            
            try:
                dt = datetime.fromisoformat(str(ts))
                t_str = dt.strftime("%H:%M:%S")
            except:
                t_str = str(ts)[:8]
            
            # Use color for status in the text
            color = "#f8fafc"
            if 'ERROR' in status.upper(): color = "#f87171"
            elif 'SUCCESS' in status.upper(): color = "#4ade80"
            
            self.lbl_last_val.setText(f"{t_str} - {status}")
            self.lbl_last_val.setStyleSheet(f"font-size: 13px; font-weight: bold; color: {color};")
        else:
             self.lbl_last_val.setText("--:--:--")
             self.lbl_last_val.setStyleSheet("font-size: 13px; font-weight: bold; color: #64748b;")
        
        # Next Run
        self.lbl_next.setText(f"PRÓXIMA: {data['next_run']}")
        
        # Progress
        runs = data['daily_runs']
        target = data['target_runs']
        self.lbl_runs.setText(f"{runs} / {target}")
        
        if target > 0:
            val = int((runs / target) * 100)
            self.progress.setValue(min(100, val))
        else:
            self.progress.setValue(0)

class SideListItem(QFrame):
    def __init__(self, title, subtitle, time_text, status_icon="✔", status_color="#4ade80"):
        super().__init__()
        self.setObjectName("SideListItem")
        
        layout = QHBoxLayout(self)
        layout.setContentsMargins(5, 5, 5, 5)
        
        # Status Icon
        lbl_icon = QLabel(status_icon)
        lbl_icon.setStyleSheet(f"color: {status_color}; font-size: 16px; font-weight: bold;")
        layout.addWidget(lbl_icon)
        
        # Text
        text_layout = QVBoxLayout()
        lbl_title = QLabel(title)
        lbl_title.setStyleSheet("color: white; font-weight: bold; font-size: 11px;")
        lbl_sub = QLabel(subtitle)
        lbl_sub.setStyleSheet("color: #64748b; font-size: 10px;")
        text_layout.addWidget(lbl_title)
        text_layout.addWidget(lbl_sub)
        layout.addLayout(text_layout)
        
        layout.addStretch()
        
        # Time
        lbl_time = QLabel(time_text)
        lbl_time.setStyleSheet("color: #3b82f6; font-weight: bold; font-size: 11px;")
        layout.addWidget(lbl_time)

class TimerWidget(QFrame):
    def __init__(self, label, max_val=60):
        super().__init__()
        layout = QVBoxLayout(self)
        layout.setSpacing(2)
        layout.setContentsMargins(0,0,0,0)
        
        top = QHBoxLayout()
        self.lbl_name = QLabel(label)
        self.lbl_name.setStyleSheet("color: #94a3b8; font-size: 10px; font-weight: bold;")
        self.lbl_val = QLabel("0s")
        self.lbl_val.setStyleSheet("color: #3b82f6; font-size: 10px; font-weight: bold;")
        
        top.addWidget(self.lbl_name)
        top.addStretch()
        top.addWidget(self.lbl_val)
        layout.addLayout(top)
        
        self.bar = QProgressBar()
        self.bar.setFixedHeight(4)
        self.bar.setTextVisible(False)
        self.bar.setRange(0, max_val)
        layout.addWidget(self.bar)
        
    def update_timer(self, current, total):
        self.lbl_val.setText(f"{int(current)}s")
        self.bar.setValue(int(current))
        self.bar.setRange(0, int(total))

# ==============================================================================
# MAIN WINDOW
# ==============================================================================
class MainWindow(QMainWindow):
    def __init__(self, worker):
        super().__init__()
        self.worker = worker
        self.setWindowTitle("C6 RPA Orchestrator")
        self.resize(1600, 900) 
        
        # Central Widget
        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QHBoxLayout(central)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)
        
        # 1. LEFT SIDEBAR
        sidebar = QFrame()
        sidebar.setObjectName("Sidebar")
        sidebar.setFixedWidth(260)
        self.side_layout = QVBoxLayout(sidebar)
        self.side_layout.setContentsMargins(20, 30, 20, 30)
        self.side_layout.setSpacing(15)
        
        # Logo
        lbl_logo = QLabel("C6 | CELULA PYTHON")
        lbl_logo.setStyleSheet("font-size: 18px; font-weight: 900; color: white; margin-bottom: 20px;")
        self.side_layout.addWidget(lbl_logo)
        
        # Filters Container
        self.filter_container = QVBoxLayout()
        self.filter_container.setSpacing(5)
        self.filter_btns = {}
        self.dynamic_filters_initialized = False
        
        # Initial Monitor Button
        self.add_filter_btn("MONITOR GERAL", "MONITOR")
        self.filter_btns["MONITOR"].setChecked(True)
        self.active_filter = "MONITOR"
        
        self.side_layout.addLayout(self.filter_container)
        self.side_layout.addStretch()
        
        # Timers
        self.timer_bq = TimerWidget("BIGQUERY SYNC", 300)
        self.timer_disc = TimerWidget("EXCEL DISCOVERY", 60)
        
        self.side_layout.addWidget(self.timer_bq)
        self.side_layout.addWidget(self.timer_disc)
        
        # System status
        lbl_sys = QLabel("🟢 SISTEMA OPERACIONAL")
        lbl_sys.setStyleSheet("color: #4ade80; font-size: 10px; font-weight: bold; margin-top: 10px;")
        self.side_layout.addWidget(lbl_sys)
        
        main_layout.addWidget(sidebar)
        
        # 2. MAIN CONTENT (Center)
        content_area = QWidget()
        content_area.setObjectName("ContentWidget")
        content_area.setStyleSheet("#ContentWidget { background-color: #020617; }")
        content_layout = QVBoxLayout(content_area)
        content_layout.setContentsMargins(40, 40, 40, 40)
        content_layout.setSpacing(20)
        
        # Top Header Area
        top_header = QHBoxLayout()
        
        # Title & Subtitle
        title_box = QVBoxLayout()
        lbl_title = QLabel("PAINEL DE CONTROLE")
        lbl_title.setStyleSheet("font-size: 24px; font-weight: 900; color: white;")
        lbl_sub = QLabel("⚡ ATUALIZAÇÃO EM TEMPO REAL ATIVA")
        lbl_sub.setStyleSheet("color: #3b82f6; font-weight: bold; font-size: 11px;")
        title_box.addWidget(lbl_title)
        title_box.addWidget(lbl_sub)
        top_header.addLayout(title_box)
        
        top_header.addStretch()
        
        # Search
        self.search_inp = QLineEdit()
        self.search_inp.setPlaceholderText("Buscar script...")
        self.search_inp.setStyleSheet("""
            QLineEdit { 
                background-color: #0f172a; 
                border: 1px solid #1e293b; 
                border-radius: 6px; 
                color: white; 
                padding: 10px; 
                min-width: 250px;
            }
            QLineEdit:focus { border: 1px solid #3b82f6; }
        """)
        self.search_inp.textChanged.connect(self.refresh_grid_visibility)
        top_header.addWidget(self.search_inp)
        
        # Stats Boxes
        self.stat_fila = self.create_stat_box("FILA", "0")
        self.stat_threads = self.create_stat_box("THREADS", "0/5")
        
        top_header.addWidget(self.stat_fila)
        top_header.addWidget(self.stat_threads)
        
        content_layout.addLayout(top_header)
        
        # Scroll Area for Grid
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setStyleSheet("background: transparent; border: none;")
        
        self.grid_widget = QWidget()
        self.grid_widget.setStyleSheet("background: transparent;")
        self.grid = QGridLayout(self.grid_widget)
        self.grid.setAlignment(Qt.AlignTop | Qt.AlignLeft)
        self.grid.setSpacing(20)
        
        scroll.setWidget(self.grid_widget)
        content_layout.addWidget(scroll)
        
        main_layout.addWidget(content_area, stretch=1)
        
        # 3. RIGHT SIDEBAR (Queue & History)
        right_sidebar = QFrame()
        right_sidebar.setStyleSheet("background-color: #0f172a; border-left: 1px solid #1e293b;")
        right_sidebar.setFixedWidth(320)
        
        right_layout = QVBoxLayout(right_sidebar)
        right_layout.setContentsMargins(20, 30, 20, 30)
        right_layout.setSpacing(20)
        
        # Queue Section
        right_layout.addWidget(QLabel("PRÓXIMOS", styleSheet="color: white; font-weight: 900; font-size: 14px;"))
        lbl_q_sub = QLabel("FILA CRONOLÓGICA (HOJE)", styleSheet="color: #64748b; font-size: 10px; font-weight: bold;")
        right_layout.addWidget(lbl_q_sub)
        
        self.queue_container = QVBoxLayout()
        right_layout.addLayout(self.queue_container)
        
        right_layout.addSpacing(20)
        
        # History Section
        right_layout.addWidget(QLabel("HISTÓRICO", styleSheet="color: white; font-weight: 900; font-size: 14px;"))
        lbl_h_sub = QLabel("EXECUÇÕES DE HOJE", styleSheet="color: #64748b; font-size: 10px; font-weight: bold;")
        right_layout.addWidget(lbl_h_sub)
        
        self.history_scroll = QScrollArea()
        self.history_scroll.setWidgetResizable(True)
        self.history_widget = QWidget()
        self.history_layout = QVBoxLayout(self.history_widget)
        self.history_layout.setAlignment(Qt.AlignTop)
        self.history_scroll.setWidget(self.history_widget)
        right_layout.addWidget(self.history_scroll)
        
        main_layout.addWidget(right_sidebar)

        # DATA & LOGIC
        self.cards = {}
        self.timer = QTimer()
        self.timer.timeout.connect(self.refresh_ui)
        self.timer.start(1000)

    def create_stat_box(self, title, value):
        box = QFrame()
        box.setStyleSheet("background-color: #0f172a; border: 1px solid #1e293b; border-radius: 8px;")
        box.setFixedSize(100, 60)
        l = QVBoxLayout(box)
        l.setContentsMargins(0,10,0,10)
        l.setSpacing(2)
        
        t = QLabel(title)
        t.setAlignment(Qt.AlignCenter)
        t.setStyleSheet("color: #64748b; font-size: 10px; font-weight: bold;")
        
        v = QLabel(value)
        v.setObjectName("StatValue") 
        v.setAlignment(Qt.AlignCenter)
        v.setStyleSheet("color: white; font-size: 18px; font-weight: 900;")
        
        l.addWidget(t)
        l.addWidget(v)
        return box
        
    def add_filter_btn(self, label, code):
        if code in self.filter_btns: return
        btn = QPushButton(label)
        btn.setCheckable(True)
        btn.setObjectName("SidebarBtn")
        btn.clicked.connect(lambda checked, x=code: self.set_filter(x))
        
        if code == "MONITOR":
             btn.setStyleSheet("""
                QPushButton#SidebarBtn {
                    background-color: #2563eb;
                    color: white;
                    border: none;
                }
                QPushButton#SidebarBtn:checked { background-color: #2563eb; }
             """)
             self.filter_container.insertWidget(0, btn)
        else:
             self.filter_container.addWidget(btn)
             
        self.filter_btns[code] = btn

    def update_sidebar_filters(self):
        if self.dynamic_filters_initialized: return
        
        areas = set()
        for name, config in self.worker.scripts_config.items():
            area = config.get('area', 'GERAL')
            areas.add(area)
            
        if not areas: return
        
        for area in sorted(list(areas)):
             if area != "MONITOR": 
                 self.add_filter_btn(area, area)
                 
        self.dynamic_filters_initialized = True

    def set_filter(self, area):
        self.active_filter = area
        for k, btn in self.filter_btns.items():
            btn.setChecked(k == area)
        self.refresh_grid_visibility()

    def refresh_grid_visibility(self):
        # Hide all first
        for name, card in self.cards.items():
            card.setVisible(False)
            
        visible_cards = []
        search_txt = self.search_inp.text().lower()
        
        for name, card in self.cards.items():
            config = self.worker.scripts_config.get(name, {})
            c_area = config.get('area', 'GERAL')
            
            show = False
            
            # Global Search Override
            if search_txt:
                if search_txt in name:
                    show = True
            else:
                # Normal Filter Logic
                if self.active_filter == "MONITOR":
                    # Monitor logic: Running or Scheduled
                    is_running = name in self.worker.running_tasks
                    is_scheduled = any(i[1] == name for i in self.worker.execution_queue)
                    if is_running or is_scheduled:
                        show = True
                elif self.active_filter == "ALL":
                     show = True
                elif c_area == self.active_filter:
                    show = True
            
            if show:
                visible_cards.append(card)
        
        # Visual Clear
        for i in reversed(range(self.grid.count())): 
            self.grid.itemAt(i).widget().setParent(None)
            
        # Re-add
        col_count = 0
        row_count = 0
        MAX_COLS = 3
        
        sorted_cards = sorted(visible_cards, key=lambda c: c.lbl_name.text())
        for card in sorted_cards:
            self.grid.addWidget(card, row_count, col_count)
            card.setVisible(True)
            card.setParent(self.grid_widget)
            col_count += 1
            if col_count >= MAX_COLS:
                col_count = 0
                row_count += 1

    def refresh_ui(self):
        if not self.worker: return
        
        # 1. Update Timers
        now = datetime.now()
        # BQ (Countdown)
        elapsed_bq = (now.timestamp() - self.worker.last_bq_sync) if self.worker.last_bq_sync else 0
        rem_bq = max(0, self.worker.bq_sync_interval - elapsed_bq)
        self.timer_bq.update_timer(rem_bq, self.worker.bq_sync_interval)
        
        # Discovery (Countdown)
        elapsed_disc = (now.timestamp() - self.worker.last_discovery) if self.worker.last_discovery else 0
        rem_disc = max(0, 60 - elapsed_disc)
        self.timer_disc.update_timer(rem_disc, 60)
        
        # 2. Update Stats Boxes
        q_len = len(self.worker.execution_queue)
        t_len = len(self.worker.running_tasks)
        
        val_fila = self.stat_fila.findChild(QLabel, "StatValue")
        if val_fila: val_fila.setText(str(q_len))
        
        val_threads = self.stat_threads.findChild(QLabel, "StatValue")
        if val_threads: val_threads.setText(f"{t_len}/5")
        
        # 3. Dynamic Filters
        if not self.dynamic_filters_initialized and self.worker.scripts_config:
            self.update_sidebar_filters()
        
        # 4. Sync Cards
        current_script_names = set(self.worker.scripts_map.keys())
        needs_layout_update = False
        
        # Add New Cards
        for name in current_script_names:
            if name not in self.cards:
                card = ScriptCard(self.worker)
                self.cards[name] = card
                needs_layout_update = True
        
        # Remove Stale Cards
        for name in list(self.cards.keys()):
             if name not in current_script_names:
                 self.cards[name].deleteLater()
                 del self.cards[name]
                 needs_layout_update = True
        
        if needs_layout_update:
            self.refresh_grid_visibility()
        elif self.active_filter == "MONITOR":
            # Force refresh to handle state changes
            self.refresh_grid_visibility()

        # Update Card Data
        for name, card in self.cards.items():
            config = self.worker.scripts_config.get(name, {})
            
            status = "IDLE"
            if name in self.worker.running_tasks: status = "RUNNING"
            elif any(item[1] == name for item in self.worker.execution_queue): status = "SCHEDULED"
            else:
                if not self.worker.history_df.empty:
                    runs = self.worker.history_df[self.worker.history_df['script_name'] == name]
                    if not runs.empty:
                        lr = runs.iloc[-1]['status'].upper()
                        if 'SUCCESS' in lr: status = "SUCCESS"
                        elif 'ERROR' in lr: status = "ERROR"

            cron = config.get('cron')
            next_run = "Manual"
            if cron == 'ALL':
                h = datetime.now().hour
                next_run = f"{(h+1)%24:02d}:00"
            elif isinstance(cron, set) and cron:
                h = datetime.now().hour
                upcoming = sorted([x for x in cron if x > h])
                if upcoming: next_run = f"{upcoming[0]:02d}:00"
                else: next_run = "Amanhã"
             
            last_obj = None
            if not self.worker.history_df.empty:
                runs = self.worker.history_df[self.worker.history_df['script_name'] == name]
                if not runs.empty:
                    r = runs.iloc[-1]
                    last_obj = {'timestamp': r['start_time'], 'status': r['status']}
            
            # Run Duration
            run_duration = None
            if status == "RUNNING" and hasattr(self.worker, 'task_start_times'):
                 start_ts = self.worker.task_start_times.get(name)
                 if start_ts:
                     run_duration = time.time() - start_ts

            data = {
                "name": name,
                "area": config.get('area', 'GERAL'),
                "status": status,
                "daily_runs": self.worker.daily_execution_cache.get(name, 0),
                "target_runs": config.get('target_runs', 0),
                "next_run": next_run,
                "last_exec": last_obj,
                "run_duration": run_duration
            }
            card.update_data(name, data)

        # 5. Right Sidebar (Queue & History)
        self.update_right_sidebar()
        
    def update_right_sidebar(self):
        # Update Queue
        for i in reversed(range(self.queue_container.count())): 
            self.queue_container.itemAt(i).widget().setParent(None)
            
        for item in self.worker.execution_queue:
            name = item[1]
            config = self.worker.scripts_config.get(name, {})
            area = config.get('area', 'GERAL')
            cron = config.get('cron')
            next_t = "00:00" 
            if cron == 'ALL': next_t = f"{(datetime.now().hour+1)%24:02d}:00"
            
            w = SideListItem(name.upper(), area, next_t, "⏳", "#3b82f6")
            self.queue_container.addWidget(w)
            
        # Update History
        for i in reversed(range(self.history_layout.count())): 
            self.history_layout.itemAt(i).widget().setParent(None)
            
        if not self.worker.history_df.empty:
            df = self.worker.history_df.sort_values(by='start_time', ascending=False).head(20)
            for _, row in df.iterrows():
                name = row['script_name']
                status = row['status']
                start = row['start_time']
                
                t_str = start.strftime("%H:%M") if hasattr(start, 'strftime') else str(start)[11:16]
                
                icon = "✔"
                col = "#4ade80"
                if "ERRO" in status.upper():
                    icon = "❌"
                    col = "#f87171"
                    
                w = SideListItem(name.upper(), status, t_str, icon, col)
                self.history_layout.addWidget(w)


# ==============================================================================
# CONFIGURATION
# ==============================================================================
load_dotenv()

def get_root_path():
    home = Path.home()
    p1 = home / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A"
    p2 = home / "Meu Drive (carlosfrenesi01@gmail.com)/C6 CTVM"
    p3 = home / "Meu Drive/C6 CTVM"
    
    if p1.exists(): return p1
    if p2.exists(): return p2
    if p3.exists(): return p3
    return home

ROOT_DRIVE = get_root_path()
BASE_PATH = ROOT_DRIVE / "Mensageria e Cargas Operacionais - 11.CelulaPython/graciliano/automacoes"

print(f"[INIT] Resolved BASE_PATH: {BASE_PATH}")
if not BASE_PATH.exists():
    print(f"[WARNING] BASE_PATH does not exist on disk!")

TEMP_DIR = Path(os.environ.get("TEMP")) / "C6_RPA_EXEC"
TEMP_DIR.mkdir(exist_ok=True)
CONFIG_FILE = Path(__file__).parent / (os.environ.get("EXCEL_FILENAME", "registro_automacoes.xlsx"))

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
        
        self.execution_queue = [] 
        self.running_tasks = {}   
        self.daily_execution_cache = {} 
        self.last_finish_times = {} 
        self.scripts_map = {}     
        self.scripts_config = {}  
        
        self.last_discovery = 0
        self.last_bq_sync = 0
        self.bq_sync_interval = 300
        self.current_date_track = datetime.now().date()
        
        self.history_df = pd.DataFrame()
        self.bq_verified = False
        self.last_finish_times = {}
        self.task_start_times = {} 

    def run(self):
        self.discover()
        self.last_discovery = time.time()
        
        self.sync_bq()
        self.last_bq_sync = time.time()
        
        while self.running:
            try:
                now = time.time()
                today = datetime.now().date()
                
                if today != self.current_date_track:
                    print(f"--- RESET DAILY COUNTERS ({today}) ---")
                    self.daily_execution_cache.clear()
                    self.last_finish_times.clear()
                    self.history_df = pd.DataFrame()
                    self.current_date_track = today
                    self.last_bq_sync = 0
                
                if now - self.last_discovery > 60:
                    self.discover()
                    self.last_discovery = now
                
                if not self.paused and (now - self.last_bq_sync > self.bq_sync_interval):
                    self.sync_bq()
                    self.last_bq_sync = now
                    
                self.check_schedule()
                self.process_queue()
                
                time.sleep(1)
            except Exception as e:
                print(f"Engine Loop Error: {e}")
                time.sleep(5)

    def clean_key(self, s):
        s = str(s).lower()
        if s.endswith('.py'): s = s[:-3]
        return "".join(c for c in s if c.isalnum())

    def discover(self):
        found_files = {}
        print(f"[DISCOVERY] Scanning {BASE_PATH} for .py files...")
        for root, dirs, files in os.walk(str(BASE_PATH)):
            lower_root = str(root).lower()
            if "metodos" in lower_root:
                for f in files:
                    if f.endswith(".py"):
                        key = self.clean_key(f) 
                        found_files[key] = Path(root) / f
        
        print(f"[DISCOVERY] Found {len(found_files)} Python files in 'metodos'.")
        
        if not CONFIG_FILE.exists(): return

        try:
            df = pd.read_excel(CONFIG_FILE)
            df = df.fillna('')
            df.columns = [c.lower() for c in df.columns]
            
            new_map = {}
            new_config = {}
            
            for _, row in df.iterrows():
                raw_name = str(row.get('script_name', '')).strip()
                if not raw_name: continue
                
                clean_name = self.clean_key(raw_name)
                
                if clean_name not in found_files:
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
                    
                    if clean_name not in self.daily_execution_cache:
                        self.daily_execution_cache[clean_name] = 0
            
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
            
            print(f"\\n[BQ] Rows Found: {len(self.history_df)}")
            
            new_cache = {name: 0 for name in self.scripts_config.keys()}
            
            if not self.history_df.empty:
                bq_counts = self.history_df.groupby('script_name').size().to_dict()
                
                matches = 0
                for bq_name, count in bq_counts.items():
                    clean_bq_name = self.clean_key(bq_name)
                    
                    if clean_bq_name in new_cache:
                        new_cache[clean_bq_name] = count
                        matches += 1
                    else:
                        print(f"[BQ MISMATCH] BQ: '{bq_name}' -> Clean: '{clean_bq_name}' NOT IN CONFIG")
                
                print(f"[BQ] Matched {matches} scripts with Config.")
                
                if len(bq_counts) > 0 and matches == 0:
                    print("CRITICAL ERROR: BigQuery has data but NO matches found in Config. ABORTING SCHEDULE.")
                    self.bq_verified = False
                    return
            
            self.daily_execution_cache = new_cache
            
            non_zero = {k:v for k,v in self.daily_execution_cache.items() if v > 0}
            print(f"[CACHE VERIFY] Non-Zero Items: {non_zero}")
                
            self.bq_verified = True
            print("BQ Sync Success.\\n")
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
            
            if actual < current_target:
                if self.bq_verified and name not in self.running_tasks:
                    if not any(item[1] == name for item in self.execution_queue):
                        last_finish = self.last_finish_times.get(name, 0)
                        if (now_ts - last_finish) > 60:
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
            self.last_finish_times[name] = time.time() 
            del self.running_tasks[name]
            if name in self.task_start_times:
                del self.task_start_times[name]
            
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
            self.task_start_times[name] = time.time()
        except Exception as e:
            print(f"Run Error {name}: {e}")

    def kill_script(self, name):
        if name in self.running_tasks:
            try:
                self.running_tasks[name].terminate()
            except: pass


if __name__ == "__main__":
    try: ctypes.windll.kernel32.SetThreadExecutionState(0x80000003)
    except: pass
    
    SERV_WORKER = EngineWorker()
    SERV_WORKER.start()
    
    app = QApplication(sys.argv)
    app.setStyleSheet(GLOBAL_STYLESHEET)
    
    window = MainWindow(SERV_WORKER)
    window.show()
    
    sys.exit(app.exec())
