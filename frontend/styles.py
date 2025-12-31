# ==============================================================================
# DARK THEME STYLESHEET
# ==============================================================================

GLOBAL_STYLESHEET = """
QMainWindow {
    background-color: #020617;
    color: #f8fafc;
}
QWidget {
    font-family: 'Segoe UI', sans-serif;
    color: #f8fafc;
}
QScrollArea {
    border: none;
    background-color: transparent;
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

/* Sidebar */
QFrame#Sidebar {
    background-color: #0f172a;
    border-right: 1px solid #1e293b;
}
QPushButton#SidebarBtn {
    background-color: transparent;
    color: #94a3b8;
    text-align: left;
    padding: 12px 20px;
    border: none;
    border-radius: 8px;
    font-weight: bold;
    font-size: 13px;
}
QPushButton#SidebarBtn:hover {
    background-color: #1e293b;
    color: #f8fafc;
}
QPushButton#SidebarBtn:checked {
    background-color: #1e293b;
    color: #3b82f6;
    border-left: 3px solid #3b82f6;
}

/* Card */
QFrame#ScriptCard {
    background-color: #0f172a;
    border-radius: 12px;
    border: 1px solid #1e293b;
}
QFrame#ScriptCard:hover {
    border: 1px solid #3b82f6;
}
QLabel#CardTitle {
    font-size: 14px;
    font-weight: bold;
    color: #f8fafc;
}
QLabel#CardArea {
    font-size: 10px;
    font-weight: bold;
    color: #64748b;
}
QLabel#CardStatus {
    font-size: 10px;
    font-weight: bold;
    padding: 2px 6px;
    border-radius: 4px;
}

/* Progress Bar */
QProgressBar {
    border: none;
    background-color: #1e293b;
    border-radius: 3px;
    height: 6px;
    text-align: center;
}
QProgressBar::chunk {
    background-color: #3b82f6;
    border-radius: 3px;
}

/* Side List Item */
QFrame#SideListItem {
    background-color: #0f172a;
    border-radius: 6px;
    padding: 8px;
    margin-bottom: 8px; /* separation */
}

/* Buttons */
QPushButton#RunBtn {
    background-color: #2563eb;
    color: white;
    border-radius: 6px;
    font-weight: bold;
    padding: 6px;
    border: none;
}
QPushButton#RunBtn:hover {
    background-color: #1d4ed8;
}
QPushButton#RunBtn:disabled {
    background-color: #1e293b;
    color: #475569;
}
QPushButton#StopBtn {
    background-color: #1e293b;
    color: #94a3b8;
    border-radius: 6px;
    font-weight: bold;
    padding: 6px;
    border: 1px solid #334155;
}
QPushButton#StopBtn:hover {
    background-color: #ef4444; 
    color: white;
    border: none;
}
"""
