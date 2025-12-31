from datetime import datetime
from PySide6.QtWidgets import (QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
                               QFrame, QLabel, QPushButton, QScrollArea, QGridLayout, 
                               QLineEdit, QSizePolicy)
from PySide6.QtCore import Qt, QTimer
from PySide6.QtGui import QIcon

from frontend.components import ScriptCard, SideListItem, TimerWidget

class MainWindow(QMainWindow):
    def __init__(self, worker):
        super().__init__()
        self.worker = worker
        self.setWindowTitle("C6 RPA Orchestrator")
        self.resize(1600, 900) # Wider for 3 columns
        
        # Central Widget
        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QHBoxLayout(central)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)
        
        # ==============================================================================
        # 1. LEFT SIDEBAR
        # ==============================================================================
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
        
        # ==============================================================================
        # 2. MAIN CONTENT (Center)
        # ==============================================================================
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
        
        # Stats Boxes (Fila / Threads)
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
        
        # ==============================================================================
        # 3. RIGHT SIDEBAR (Queue & History)
        # ==============================================================================
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

        # ==============================================================================
        # DATA & LOGIC
        # ==============================================================================
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
        
        # Use simple append logic now since we have a dedicated layout
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
            if search_txt and search_txt not in name:
                continue
                
            config = self.worker.scripts_config.get(name, {})
            area = config.get('area', 'GERAL')
            
            # MONITOR LOGIC: active or scheduled only?
            # User request: "Monitor apenas as informacoes dos scripts que estão em execucao naquele momento"
            # And also "Schedule"? Usually Monitor implies "Active things".
            
            show = False
            if self.active_filter == "MONITOR":
                # Show only if Running OR Scheduled/Queued
                is_running = name in self.worker.running_tasks
                is_queued = any(item[1] == name for item in self.worker.execution_queue)
                if is_running or is_queued:
                    show = True
            elif area == self.active_filter:
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
        # BQ
        elapsed_bq = (now.timestamp() - self.worker.last_bq_sync) if self.worker.last_bq_sync else 0
        self.timer_bq.update_timer(elapsed_bq, self.worker.bq_sync_interval)
        
        # Discovery
        elapsed_disc = (now.timestamp() - self.worker.last_discovery) if self.worker.last_discovery else 0
        self.timer_disc.update_timer(elapsed_disc, 60)
        
        # 2. Update Stats Boxes
        q_len = len(self.worker.execution_queue)
        t_len = len(self.worker.running_tasks)
        self.stat_fila.findChild(QLabel, "").setText(str(q_len)) # Assuming 2nd label is value
        # Actually finding child by type might be risky if order changes. 
        # Better to keep reference? doing naive update for now.
        self.stat_fila.layout().itemAt(1).widget().setText(str(q_len))
        self.stat_threads.layout().itemAt(1).widget().setText(f"{t_len}/5")
        
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
        
        # Remove Stale Cards (Strict Sync as requested)
        # Iterate over a copy of keys since we modify the dict
        for name in list(self.cards.keys()):
             if name not in current_script_names:
                 self.cards[name].deleteLater()
                 del self.cards[name]
                 needs_layout_update = True
        
        if needs_layout_update:
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

             # If Monitor View and scripts changed state (e.g. stopped running), we might need to refresh layou
             # But complete refresh every second is expensive. 
             # For now, just update data. 
             # TODO: Optimize Monitor Refresh.

             data = {
                 "name": name,
                 "area": config.get('area', 'GERAL'),
                 "status": status,
                 "daily_runs": self.worker.daily_execution_cache.get(name, 0),
                 "target_runs": config.get('target_runs', 0),
                 "next_run": next_run,
                 "last_exec": last_obj
             }
             card.update_data(name, data)

        # 5. Right Sidebar (Queue & History)
        self.update_right_sidebar()
        
    def update_right_sidebar(self):
        # Update Queue
        # Clear
        for i in reversed(range(self.queue_container.count())): 
            self.queue_container.itemAt(i).widget().setParent(None)
            
        for item in self.worker.execution_queue:
            # item = (priority, name, path)
            name = item[1]
            config = self.worker.scripts_config.get(name, {})
            area = config.get('area', 'GERAL')
            
            # Determine time (next hour?)
            cron = config.get('cron')
            next_t = "00:00" # Placeholder
            if cron == 'ALL': next_t = f"{(datetime.now().hour+1)%24:02d}:00"
            
            w = SideListItem(name.upper(), area, next_t, "⏳", "#3b82f6")
            self.queue_container.addWidget(w)
            
        # Update History
        # Clear
        for i in reversed(range(self.history_layout.count())): 
            self.history_layout.itemAt(i).widget().setParent(None)
            
        if not self.worker.history_df.empty:
            # Sort by time desc
            df = self.worker.history_df.sort_values(by='start_time', ascending=False).head(20)
            for _, row in df.iterrows():
                name = row['script_name']
                status = row['status']
                start = row['start_time']
                
                # Format
                t_str = start.strftime("%H:%M") if hasattr(start, 'strftime') else str(start)[11:16]
                
                icon = "✔"
                col = "#4ade80"
                if "ERRO" in status.upper():
                    icon = "❌"
                    col = "#f87171"
                    
                w = SideListItem(name.upper(), status, t_str, icon, col)
                self.history_layout.addWidget(w)
