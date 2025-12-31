from datetime import datetime
from PySide6.QtWidgets import (QFrame, QVBoxLayout, QHBoxLayout, QLabel, 
                               QPushButton, QProgressBar)
from PySide6.QtCore import Qt

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
        
        # Divider
        # layout.addWidget(QLabel("<hr style='border-top: 1px solid #1e293b;'>"))
        
        # Last Exec (Simplified as requested)
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
        self.lbl_status.setText(st)
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
        # remove inline style for background since we have it in stylesheet now, 
        # but keep it if QSS isn't loading perfectly for custom widgets. 
        # Actually, let's trust QSS + explicit border if needed.
        # self.setStyleSheet("background-color: #0f172a; border-radius: 6px; padding: 8px;") 
        
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
