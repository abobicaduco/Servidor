from __future__ import annotations

import sys
import os
import subprocess
import threading
import time
import re
import warnings
import psutil
import shutil
import gc
import unicodedata
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, Callable, Optional
from functools import partial
from google.oauth2.credentials import Credentials
import logging
import traceback
import getpass

import pandas as pd
from zoneinfo import ZoneInfo
from google.cloud import bigquery

_env_headless = os.getenv("SERVIDOR_HEADLESS", "").strip().lower()
if _env_headless in {"1", "true", "yes", "sim"}:
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

QT_AVAILABLE = True
QT_IMPORT_ERROR: Optional[Exception] = None
try:
    from PySide6.QtCore import Qt, QTimer, QSize, Signal, Slot, QThread
    from PySide6.QtGui import QFont, QColor, QTextCursor, QIcon, QAction
    from PySide6.QtWidgets import (
        QApplication,
        QMainWindow,
        QWidget,
        QVBoxLayout,
        QHBoxLayout,
        QScrollArea,
        QGridLayout,
        QLabel,
        QPushButton,
        QFrame,
        QSizePolicy,
        QDialog,
        QTextEdit,
        QCheckBox,
        QMessageBox,
        QListWidget,
        QListWidgetItem,
        QSplitter,
        QProgressBar,
        QSystemTrayIcon,
        QMenu,
        QLineEdit,
        QStackedWidget,
    )
except Exception as qt_import_error:
    QT_AVAILABLE = False
    QT_IMPORT_ERROR = qt_import_error

    def Slot(*_args, **_kwargs):
        def _decorator(fn):
            return fn

        return _decorator

    class Signal:
        def __init__(self, *_args, **_kwargs):
            pass

        def connect(self, *_args, **_kwargs):
            pass

        def emit(self, *_args, **_kwargs):
            pass

    class QThread:
        def __init__(self, *_, **__):
            pass

    class Qt:
        UserRole = 0
        AlignLeft = 0
        AlignVCenter = 0
        KeepAspectRatio = 0

    class _Dummy:
        def __init__(self, *_, **__):
            pass

    class QTimer(_Dummy):
        pass

    class QSize(_Dummy):
        def __init__(self, *_args, **_kwargs):
            super().__init__()

    class QFont(_Dummy):
        Bold = 0

    class QColor(_Dummy):
        pass

    class QTextCursor(_Dummy):
        End = 0

    class QIcon(_Dummy):
        pass

    class QAction(_Dummy):
        def __init__(self, *_args, **_kwargs):
            super().__init__()

    class QApplication(_Dummy):
        @staticmethod
        def instance():
            return None

        def exec(self):
            return 0

    class QMainWindow(_Dummy):
        pass

    class QWidget(_Dummy):
        pass

    class QVBoxLayout(_Dummy):
        def __init__(self, *_args, **_kwargs):
            super().__init__()

    class QHBoxLayout(_Dummy):
        def __init__(self, *_args, **_kwargs):
            super().__init__()

    class QScrollArea(_Dummy):
        def setWidgetResizable(self, *_args, **_kwargs):
            pass

        def setWidget(self, *_args, **_kwargs):
            pass

    class QGridLayout(_Dummy):
        def __init__(self, *_args, **_kwargs):
            super().__init__()

        def setContentsMargins(self, *_args, **_kwargs):
            pass

        def setSpacing(self, *_args, **_kwargs):
            pass

        def addWidget(self, *_args, **_kwargs):
            pass

    class QLabel(_Dummy):
        def __init__(self, *_args, **_kwargs):
            super().__init__()

        def setPixmap(self, *_args, **_kwargs):
            pass

    class QPushButton(_Dummy):
        def __init__(self, *_args, **_kwargs):
            super().__init__()

    class QFrame(_Dummy):
        StyledPanel = 0
        Sunken = 0

    class QSizePolicy(_Dummy):
        Expanding = 0
        Preferred = 0

    class QDialog(_Dummy):
        pass

    class QTextEdit(_Dummy):
        def __init__(self, *_args, **_kwargs):
            super().__init__()

    class QCheckBox(_Dummy):
        def __init__(self, *_args, **_kwargs):
            super().__init__()

    class QMessageBox(_Dummy):
        Yes = 1
        No = 0

        @staticmethod
        def question(*_args, **_kwargs):
            return QMessageBox.No

    class QListWidget(_Dummy):
        def __init__(self, *_args, **_kwargs):
            super().__init__()

        def count(self):
            return 0

        def item(self, *_args, **_kwargs):
            return None

        def selectedItems(self):
            return []

        def row(self, *_args, **_kwargs):
            return 0

        def takeItem(self, *_args, **_kwargs):
            pass

        def insertItem(self, *_args, **_kwargs):
            pass

        def addItem(self, *_args, **_kwargs):
            pass

        def clear(self):
            pass

    class QListWidgetItem(_Dummy):
        def __init__(self, *_args, **_kwargs):
            super().__init__()

        def text(self):
            return ""

        def setData(self, *_args, **_kwargs):
            pass

        def setSizeHint(self, *_args, **_kwargs):
            pass

        def setSelected(self, *_args, **_kwargs):
            pass

    class QSplitter(_Dummy):
        def __init__(self, *_args, **_kwargs):
            super().__init__()

        def addWidget(self, *_args, **_kwargs):
            pass

        def setSizes(self, *_args, **_kwargs):
            pass

    class QProgressBar(_Dummy):
        def setValue(self, *_args, **_kwargs):
            pass

        def setFormat(self, *_args, **_kwargs):
            pass

        def setStyleSheet(self, *_args, **_kwargs):
            pass

    class QSystemTrayIcon(_Dummy):
        Trigger = 0
        Information = 0

        def __init__(self, *_args, **_kwargs):
            super().__init__()

        @staticmethod
        def isSystemTrayAvailable():
            return False

        def setIcon(self, *_args, **_kwargs):
            pass

        def setToolTip(self, *_args, **_kwargs):
            pass

        def setContextMenu(self, *_args, **_kwargs):
            pass

        def show(self):
            pass

        def showMessage(self, *_args, **_kwargs):
            pass

        def hide(self):
            pass

        def activated(self, *_args, **_kwargs):
            pass

        def isVisible(self):
            return False

    class QMenu(_Dummy):
        def __init__(self, *_args, **_kwargs):
            super().__init__()

        def addAction(self, *_args, **_kwargs):
            pass

        def addSeparator(self, *_args, **_kwargs):
            pass

    class QLineEdit(_Dummy):
        def __init__(self, *_args, **_kwargs):
            super().__init__()

        def text(self):
            return ""

        def setPlaceholderText(self, *_args, **_kwargs):
            pass

        def setStyleSheet(self, *_args, **_kwargs):
            pass

        def textChanged(self, *_args, **_kwargs):
            pass

    class QStackedWidget(_Dummy):
        def __init__(self, *_args, **_kwargs):
            super().__init__()

        def count(self):
            return 0

        def widget(self, *_args, **_kwargs):
            return None

        def removeWidget(self, *_args, **_kwargs):
            pass

        def addWidget(self, *_args, **_kwargs):
            pass

        def currentIndexChanged(self, *_args, **_kwargs):
            pass

NOME_SERVIDOR = "Servidor.py"
NOME_AUTOMACAO = "novo_servidor"
HEADLESS = False
ENVIAR_EMAIL_FALHA = ["carlos.lsilva@c6bank.com", "sofia.fernandes@c6bank.com"]
REGRAVAREXCEL = False
NOME_SCRIPT = Path(__file__).stem.lower()
TZ = ZoneInfo("America/Sao_Paulo")

def _path_from_env(var_name: str, default: Path) -> Path:
    valor = os.getenv(var_name)
    if valor:
        try:
            return Path(valor).expanduser().resolve()
        except Exception:
            return default
    return default


BASE_SERVIDOR_DIR = Path(__file__).resolve().parent
BASE_DIR = _path_from_env(
    "SERVIDOR_BASE_DIR",
    Path.home()
    / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A"
    / "Mensageria e Cargas Operacionais - 11.CelulaPython"
    / "graciliano"
    / "automacoes",
)
DIR_LOGS_BASE = _path_from_env(
    "SERVIDOR_LOG_DIR",
    Path.home() / "graciliano" / "automacoes" / "cacatua" / "logs" / Path(__file__).stem.lower(),
)
DIR_CRED_CELPY = _path_from_env(
    "SERVIDOR_CRED_DIR",
    Path.home() / "AppData" / "Roaming" / "CELPY" / "tokens",
)

MAX_CONCURRENCY = int(os.getenv("SERVIDOR_MAX_CONCURRENCY", "3"))

DOWNLOADS_DIR = _path_from_env("SERVIDOR_DOWNLOAD_DIR", Path.home() / "Downloads")
DIR_XLSX_AUTEXEC = DOWNLOADS_DIR / "automacoes_exec"
DIR_XLSX_REG = DOWNLOADS_DIR / "registro_automacoes"
ARQ_XLSX_AUTEXEC = DIR_XLSX_AUTEXEC / "automacoes_exec.xlsx"
ARQ_XLSX_REG = DIR_XLSX_REG / "registro_automacoes.xlsx"

PROJECT_ID = "datalab-pagamentos"
DATASET_ADMIN = "ADMINISTRACAO_CELULA_PYTHON"
TBL_AUTOMACOES_EXEC = f"{PROJECT_ID}.{DATASET_ADMIN}.automacoes_exec"
TBL_REGISTRO_AUTOMACOES = f"{PROJECT_ID}.{DATASET_ADMIN}.Registro_automacoes"

MAPA_DIAS_SEMANA = {
    "segunda": 0, "terca": 1, "terça": 1, "quarta": 2,
    "quinta": 3, "sexta": 4, "sabado": 5, "sábado": 5, "domingo": 6,
}


class ConfiguradorLogger:
    @staticmethod
    def criar_logger():
        logger = logging.getLogger(NOME_SCRIPT)
        logger.setLevel(logging.INFO)
        logger.propagate = False
        for h in list(logger.handlers):
            logger.removeHandler(h)
        fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
        dia_dir = DIR_LOGS_BASE / datetime.now(TZ).strftime("%d.%m.%Y")
        dia_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(TZ).strftime("%Y%m%d_%H%M%S")
        log_path = dia_dir / f"{Path(__file__).stem.lower()}_{ts}.log"
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(fmt)
        fh = logging.FileHandler(log_path, encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(sh)
        logger.addHandler(fh)
        return logger, log_path, fmt


class QtLogHandler(logging.Handler):
    """
    Handler de logging que empurra as mensagens para um sinal Qt.
    Usado para preencher o painel de log em tempo real.
    """
    def __init__(self, emit_fn):
        super().__init__()
        self.emit_fn = emit_fn

    def emit(self, record):
        try:
            msg = self.format(record)
            self.emit_fn(msg)
        except Exception:
            pass


class StdoutRedirector:
    """Redireciona prints para o logger, que por sua vez cai no painel de log via QtLogHandler."""
    def __init__(self, logger, level=logging.INFO):
        self.logger = logger
        self.level = level

    def write(self, msg):
        msg = str(msg)
        if msg.strip():
            self.logger.log(self.level, msg.rstrip())

    def flush(self):
        pass


class ClienteBigQuery:
    """
    Cliente único de BigQuery com dois modos:
    - modo="servidor": usa credencial de usuário se existir, depois ADC
    - modo="planilhas": procura json em DIR_CRED_CELPY e seta GOOGLE_APPLICATION_CREDENTIALS, depois ADC
    """
    def __init__(self, logger, modo: str = "servidor", location: Optional[str] = None, timeout: Optional[int] = None):
        self.logger = logger
        self.client: Optional[bigquery.Client] = None
        self.location = location or os.getenv("BQ_LOCATION", "US")
        self.timeout = int(timeout or os.getenv("BQ_QUERY_TIMEOUT", "180"))
        self.offline = False
        self.modo = (modo or "servidor").lower().strip()
        self.inicializar()

    def inicializar(self):
        try:
            if self.modo == "planilhas":
                self._inicializar_planilhas()
            else:
                self._inicializar_servidor()
        except Exception as e:
            self.logger.error(
                "bq_inicializar_erro_fatal modo=%s tipo=%s erro=%s - modo OFFLINE",
                self.modo,
                type(e).__name__,
                e,
            )
            self.client = None
            self.offline = True

    def _inicializar_servidor(self):
        """
        1) Tenta credencial específica CELPY (arquivo de usuário).
        2) Se não, tenta ADC (bigquery.Client()).
        3) Se tudo falhar, modo offline.
        """
        cred_path = DIR_CRED_CELPY / "pydata_google_credentials.json"
        try:
            if cred_path.exists():
                try:
                    creds = Credentials.from_authorized_user_file(str(cred_path))
                    self.client = bigquery.Client(
                        project=PROJECT_ID,
                        credentials=creds,
                    )
                    self.logger.info(
                        "bq_inicializar_ok modo=servidor_credencial_arquivo caminho=%s",
                        cred_path,
                    )
                    return
                except FileNotFoundError:
                    self.logger.warning(
                        "bq_servidor_cred_arquivo_nao_encontrado caminho=%s - tentando ADC",
                        cred_path,
                    )
                except Exception as e:
                    self.logger.error(
                        "bq_servidor_cred_arquivo_erro caminho=%s tipo=%s erro=%s - tentando ADC",
                        cred_path,
                        type(e).__name__,
                        e,
                    )

            try:
                self.client = bigquery.Client(project=PROJECT_ID)
                self.logger.info("bq_inicializar_ok modo=servidor_adc_sem_arquivo")
                return
            except Exception as e_adc:
                self.logger.error(
                    "bq_servidor_adc_erro tipo=%s erro=%s - modo OFFLINE",
                    type(e_adc).__name__,
                    e_adc,
                )

        except Exception as e:
            self.logger.error(
                "bq_servidor_inicializar_erro tipo=%s erro=%s - modo OFFLINE",
                type(e).__name__,
                e,
            )

        self.client = None
        self.offline = True
        self.logger.warning("bq_servidor_modo_offline_ativado")

    def _inicializar_planilhas(self):
        """
        1) Procura json em DIR_CRED_CELPY.
        2) Seta GOOGLE_APPLICATION_CREDENTIALS.
        3) Fallback ADC.
        """
        try:
            cred: Optional[str] = None
            cred_especifico = DIR_CRED_CELPY / "pydata_google_credentials.json"

            if cred_especifico.exists():
                cred = str(cred_especifico)
            elif DIR_CRED_CELPY.exists():
                cand = list(DIR_CRED_CELPY.glob("*.json"))
                if cand:
                    cred = str(cand[0])

            if cred:
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred
                self.logger.info("bq_planilhas_inicializar_ok modo=arquivo caminho=%s", cred)
                self.client = bigquery.Client(project=PROJECT_ID)
                return

            self.client = bigquery.Client(project=PROJECT_ID)
            self.logger.info("bq_planilhas_inicializar_ok modo=adc_sem_arquivo")
        except Exception as e:
            self.logger.error(
                "bq_planilhas_inicializar_erro tipo=%s erro=%s - modo OFFLINE",
                type(e).__name__,
                e,
            )
            self.client = None
            self.offline = True

    def query_df(self, sql, params=None) -> pd.DataFrame:
        if self.offline or self.client is None:
            self.logger.warning("bq_query_df_offline sql_ignorado=%s", str(sql)[:200])
            return pd.DataFrame()

        try:
            job_config = bigquery.QueryJobConfig()
            if params:
                job_config.query_parameters = params
            job = self.client.query(sql, job_config=job_config, location=self.location)
            df = job.result(timeout=self.timeout).to_dataframe(create_bqstorage_client=False)
            return df
        except Exception as e:
            self.logger.error("bq_query_df_erro tipo=%s erro=%s sql=%s", type(e).__name__, e, sql)
            return pd.DataFrame()


class NormalizadorDF:
    @staticmethod
    def norm_key(valor):
        if valor is None:
            return ""
        texto = str(valor).strip()
        texto = texto.replace(".py", "")
        texto = "".join(c for c in unicodedata.normalize("NFD", texto) if unicodedata.category(c) != "Mn")
        texto = texto.lower()
        texto = re.sub(r"[^a-z0-9]", "", texto)
        return texto


class DescobridorMetodos:
    def __init__(self, logger):
        self.logger = logger

    def _scan_metodos(self) -> Dict[str, Dict[str, Any]]:
        resultado = {}
        try:
            if not BASE_DIR.exists():
                return resultado
            for automacao_dir in BASE_DIR.iterdir():
                if not automacao_dir.is_dir():
                    continue
                nome_dir = automacao_dir.name.lower()
                if "gaveta" in nome_dir:
                    continue
                pasta_metodos = automacao_dir / "metodos"
                if not pasta_metodos.exists() or not pasta_metodos.is_dir():
                    continue
                for py in pasta_metodos.glob("*.py"):
                    if py.name.startswith("__"):
                        continue
                    stem = py.stem
                    chave = NormalizadorDF.norm_key(stem)
                    resultado[chave] = {"stem": stem, "path": py, "norm_key": chave}
            self.logger.info(f"descobrir_metodos_scan total_metodos_fs={len(resultado)}")
        except Exception as e:
            self.logger.error(f"descobrir_metodos_erro tipo={type(e).__name__} erro={e}")
        return resultado

    def mapear_por_registro(self, df_reg):
        origem_registro = "arg"
        try:
            if df_reg is None or df_reg.empty:
                try:
                    if ARQ_XLSX_REG.exists():
                        df_reg = pd.read_excel(ARQ_XLSX_REG, sheet_name=0, dtype=str)
                        origem_registro = "arquivo_xlsx"
                    else:
                        origem_registro = "vazio_sem_arquivo"
                except Exception as e:
                    origem_registro = "erro_leitura_arquivo"
                    self.logger.error(f"descobrir_metodos_leitura_registro_erro tipo={type(e).__name__} erro={e}")
        except Exception as e:
            self.logger.error(f"descobrir_metodos_pre_registro_erro tipo={type(e).__name__} erro={e}")
        metodos_fs = self._scan_metodos()
        mapeamento = {}
        registro_por_norm = {}
        total_linhas_registro = 0
        try:
            if df_reg is not None and not df_reg.empty:
                total_linhas_registro = len(df_reg)
                cols = {c.lower(): c for c in df_reg.columns}
                col_id_automacao = cols.get("id_automacao")
                col_nome_automacao = cols.get("nome_automacao")
                col_status = cols.get("status_automacao")
                col_metodo = cols.get("metodo_automacao")
                col_descricao = cols.get("descricao")
                col_origem = cols.get("origem_processo")
                col_emails_principais = cols.get("emails_principais")
                col_emails_cc = cols.get("emails_cc")
                col_horario = cols.get("horario")
                col_dia_semana = cols.get("dia_semana")
                col_mov_fin = cols.get("movimentacao_financeira")
                col_interacao = cols.get("interacao_cliente")
                col_contingencia = cols.get("contingencia")
                col_area = cols.get("area_solicitante")
                col_data_lanc = cols.get("data_lancamento")
                col_data_inat = cols.get("data_inativacao")
                col_motivo_inat = cols.get("motivo_inativacao")
                col_tempo_manual = cols.get("tempo_manual")

                def campo(linha, col):
                    if not col:
                        return ""
                    try:
                        val = linha[col]
                    except Exception:
                        return ""
                    if val is None:
                        return ""
                    return str(val).strip()

                for _, linha in df_reg.iterrows():
                    if not col_metodo:
                        continue
                    metodo_raw = campo(linha, col_metodo)
                    if not metodo_raw:
                        continue
                    norm = NormalizadorDF.norm_key(metodo_raw)
                    nome_aba_raw = campo(linha, col_nome_automacao) or "GERAL"
                    status_raw = campo(linha, col_status)
                    status_up = status_raw.upper()
                    nome_aba = nome_aba_raw
                    if status_up in {"ISOLADO", "ISOLADOS", "ISOLADA", "ISOLADAS"}:
                        nome_aba = "ISOLADOS"
                    registro_por_norm[norm] = {
                        "id_automacao": campo(linha, col_id_automacao),
                        "nome_automacao": nome_aba_raw,
                        "status_automacao": status_raw,
                        "metodo_automacao": metodo_raw,
                        "descricao": campo(linha, col_descricao),
                        "origem_processo": campo(linha, col_origem),
                        "emails_principais": campo(linha, col_emails_principais),
                        "emails_cc": campo(linha, col_emails_cc),
                        "horario": campo(linha, col_horario),
                        "dia_semana": campo(linha, col_dia_semana),
                        "movimentacao_financeira": campo(linha, col_mov_fin),
                        "interacao_cliente": campo(linha, col_interacao),
                        "contingencia": campo(linha, col_contingencia),
                        "area_solicitante": campo(linha, col_area),
                        "data_lancamento": campo(linha, col_data_lanc),
                        "data_inativacao": campo(linha, col_data_inat),
                        "motivo_inativacao": campo(linha, col_motivo_inat),
                        "tempo_manual": campo(linha, col_tempo_manual),
                        "norm_key": norm,
                    }
            self.logger.info(
                "descobrir_metodos_registro origem=%s total_linhas_registro=%s metodos_registro_unicos=%s",
                origem_registro,
                total_linhas_registro,
                len(registro_por_norm),
            )
        except Exception as e:
            self.logger.error(f"descobrir_metodos_registro_erro tipo={type(e).__name__} erro={e}")
        total_match = 0
        total_sem_atr = 0
        try:
            for norm, dados in metodos_fs.items():
                stem = dados["stem"]
                caminho = dados["path"]
                info_reg = registro_por_norm.get(norm)
                if info_reg:
                    nome_aba = info_reg.get("nome_automacao") or "GERAL"
                    nome_aba = nome_aba.strip()
                    if not nome_aba:
                        nome_aba = "GERAL"
                    status_up = (info_reg.get("status_automacao") or "").strip().upper()
                    if status_up in {"ISOLADO", "ISOLADOS", "ISOLADA", "ISOLADAS"}:
                        nome_aba = "ISOLADOS"
                    nome_aba = nome_aba.upper()
                    mapeamento.setdefault(nome_aba, {})[stem] = {
                        "path": caminho,
                        "registro": info_reg,
                        "norm_key": norm,
                    }
                    total_match += 1
                else:
                    mapeamento.setdefault("SEM_ATRIBUICAO", {})[stem] = {
                        "path": caminho,
                        "registro": None,
                        "norm_key": norm,
                    }
                    total_sem_atr += 1
            if not mapeamento:
                mapeamento["SEM_ATRIBUICAO"] = {}
            resumo_abas = {aba: len(metodos) for aba, metodos in mapeamento.items()}
            self.logger.info(
                "descobrir_metodos_mapeamento total_fs=%s casados_registro=%s sem_atribuicao=%s abas=%s",
                len(metodos_fs),
                total_match,
                total_sem_atr,
                resumo_abas,
            )
        except Exception as e:
            self.logger.error(f"descobrir_metodos_mapeamento_erro tipo={type(e).__name__} erro={e}")
        return mapeamento


class EstilosGUI:
    @staticmethod
    def obter_paleta():
        return {
            "bg_fundo": "#0c1220",
            "bg_card": "rgba(255,255,255,0.08)",
            "bg_card_hover": "rgba(255,255,255,0.16)",
            "destaque": "#0A84FF",
            "verde": "#34C759",
            "sucesso": "#34C759",
            "amarelo": "#FFD60A",
            "aviso": "#FF9F0A",
            "azul": "#64D2FF",
            "branco": "#F7F9FC",
            "texto_sec": "#A8B7CE",
            "borda_suave": "rgba(255,255,255,0.12)",
            "borda_suave_clara": "rgba(255,255,255,0.2)",
            "gradient_top": "linear-gradient(120deg, rgba(10,132,255,0.75), rgba(100,210,255,0.55), rgba(255,255,255,0.2))",
        }

    @staticmethod
    def estilo_janela():
        p = EstilosGUI.obter_paleta()
        return f"""
        QMainWindow, QWidget {{
            background-color: {p['bg_fundo']};
            font-family: "Montserrat", "Segoe UI", sans-serif;
            color: {p['branco']};
        }}
        QMainWindow {{
            background: radial-gradient(circle at 18% 18%, rgba(255,255,255,0.08), transparent 30%),
                        radial-gradient(circle at 78% 6%, rgba(10,132,255,0.18), transparent 36%),
                        radial-gradient(circle at 50% 100%, rgba(52,199,89,0.12), transparent 32%),
                        {p['bg_fundo']};
        }}
        QSplitter::handle {{
            background-color: {p['borda_suave']};
            width: 2px;
        }}
        QScrollArea {{
            border: none;
            background-color: {p['bg_fundo']};
        }}
        QWidget#conteudoMonitor {{
            background-color: {p['bg_fundo']};
        }}
        QLabel {{
            color: {p['branco']};
        }}
        QLineEdit {{
            background-color: rgba(255,255,255,0.12);
            border-radius: 12px;
            border: 1px solid {p['borda_suave_clara']};
            padding: 9px 14px;
            color: {p['branco']};
            font-size: 12px;
            selection-background-color: {p['destaque']};
            selection-color: #0B1220;
        }}
        QLineEdit::placeholder {{
            color: rgba(255,255,255,0.45);
        }}
        QLineEdit:focus {{
            border: 1px solid {p['destaque']};
            background-color: rgba(255,255,255,0.18);
        }}
        QCheckBox {{
            spacing: 5px;
            color: {p['texto_sec']};
        }}
        QCheckBox::indicator {{
            width: 40px;
            height: 20px;
            border-radius: 10px;
            background-color: {p['borda_suave']};
        }}
        QCheckBox::indicator:checked {{
            background-color: {p['destaque']};
        }}
        QScrollBar:vertical {{
            border: none;
            background: {p['bg_fundo']};
            width: 8px;
            margin: 0px;
            border-radius: 4px;
        }}
        QScrollBar::handle:vertical {{
            background: #3A3A3A;
            min-height: 20px;
            border-radius: 4px;
        }}
        QScrollBar::handle:vertical:hover {{
            background: #4A4A4A;
        }}
        QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{
            background: none;
        }}
        QScrollBar:horizontal {{
            border: none;
            background: {p['bg_fundo']};
            height: 8px;
            margin: 0px;
            border-radius: 4px;
        }}
        QScrollBar::handle:horizontal {{
            background: #3A3A3A;
            min-width: 20px;
            border-radius: 4px;
        }}
        QScrollBar::handle:horizontal:hover {{
            background: #4A4A4A;
        }}
        QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {{
            background: none;
        }}
        QListWidget {{
            background-color: {p['bg_card']};
            border: 1px solid {p['borda_suave_clara']};
            border-radius: 12px;
            padding: 8px;
        }}
        QListWidget#listaNavegacao {{
            background-color: {p['bg_card']};
            border-radius: 14px;
            border: 1px solid {p['borda_suave_clara']};
        }}
        QListWidget#listaNavegacao::item {{
            padding: 12px 12px;
            border-radius: 10px;
            margin: 4px 4px;
            color: {p['texto_sec']};
            font-weight: 700;
        }}
        QListWidget#listaNavegacao::item:selected {{
            background: {p['gradient_top']};
            color: #0B1220;
        }}
        QListWidget#listaNavegacao::item:hover {{
            background-color: {p['bg_card_hover']};
            color: {p['branco']};
        }}
        QListWidget::item {{
            padding: 5px;
        }}
        """

    @staticmethod
    def estilo_card_kanban(cor_borda=None):
        p = EstilosGUI.obter_paleta()
        cor = cor_borda if cor_borda else p["borda_suave"]
        return f"""
        QFrame#cardKanban {{
            background: linear-gradient(155deg, rgba(255,255,255,0.16), rgba(255,255,255,0.04));
            border-radius: 18px;
            border: 1px solid {cor};
        }}
        QFrame#cardKanban:hover {{
            background-color: {p['bg_card_hover']};
            border: 1px solid {p['destaque']};
        }}
        QPushButton {{
            border-radius: 10px;
            padding: 9px 12px;
            font-weight: 800;
            font-size: 10px;
            text-transform: uppercase;
        }}
        QPushButton#botaoExecutar {{
            background-color: {p['destaque']};
            color: #FFFFFF;
            border: none;
        }}
        QPushButton#botaoExecutar:hover {{
            background-color: #2a93ff;
        }}
        QPushButton#botaoExecutar:disabled {{
            background-color: #4A4A4A;
            color: #888;
        }}
        QPushButton#botaoParar {{
            background-color: rgba(255,255,255,0.12);
            color: {p['destaque']};
            border: 1px solid {p['destaque']};
        }}
        QPushButton#botaoParar:hover {{
            background-color: rgba(10,132,255,0.15);
            color: {p['branco']};
        }}
        QPushButton#botaoLog {{
            background-color: transparent;
            color: {p['texto_sec']};
            border: 1px solid {p['texto_sec']};
        }}
        QPushButton#botaoLog:hover {{
            border-color: {p['branco']};
            color: {p['branco']};
        }}
        QLabel#tituloCard {{
            color: {p['branco']};
            font-size: 16px;
            font-weight: 900;
            letter-spacing: 0.5px;
        }}
        QLabel#linhaInfo {{
            color: {p['texto_sec']};
            font-size: 12px;
        }}
        QLabel#linhaDestaque {{
            color: {p['verde']};
            font-size: 11px;
            font-weight: 600;
        }}
        """

    @staticmethod
    def estilo_dashboard_box(cor_titulo):
        p = EstilosGUI.obter_paleta()
        return f"""
        QFrame#DashboardBox {{
            background-color: {p['bg_card']};
            border-radius: 12px;
            border: 1px solid {p['borda_suave']};
        }}
        QLabel#tituloBox {{
            font-size: 14px;
            font-weight: 800;
            color: {cor_titulo};
            padding: 6px 8px;
        }}
        """

    @staticmethod
    def estilo_botao_topo():
        p = EstilosGUI.obter_paleta()
        return f"""
        QPushButton {{
            background-color: {p['bg_card']};
            color: {p['branco']};
            border: 1px solid {p['borda_suave']};
            padding: 8px 15px;
            border-radius: 4px;
        }}
        QPushButton:hover {{
            background-color: {p['bg_card_hover']};
            border: 1px solid {p['destaque']};
        }}
        """


class CardKanban(QFrame):
    def __init__(self, titulo):
        super().__init__()
        self.setObjectName("cardKanban")
        self.setMinimumSize(QSize(250, 280))
        self.setMaximumSize(QSize(280, 320))

        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(6)

        self.lbl_titulo = QLabel(str(titulo).upper())
        self.lbl_titulo.setObjectName("tituloCard")
        self.lbl_titulo.setAlignment(Qt.AlignLeft | Qt.AlignTop)
        self.lbl_titulo.setWordWrap(True)
        self.lbl_titulo.setMinimumHeight(46)
        self.lbl_titulo.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.MinimumExpanding)

        self.lbl_ultima_exec = QLabel("ÚLTIMA EXEC: -")
        self.lbl_ultima_exec.setObjectName("linhaInfo")
        self.lbl_ultima_exec.setWordWrap(True)

        self.lbl_proxima_exec = QLabel("PRÓXIMA EXEC: -")
        self.lbl_proxima_exec.setObjectName("linhaInfo")
        self.lbl_proxima_exec.setWordWrap(True)

        self.lbl_modo_exec = QLabel("MODO EXEC: -")
        self.lbl_modo_exec.setObjectName("linhaInfo")
        self.lbl_modo_exec.setWordWrap(True)

        self.lbl_status_exec = QLabel("STATUS: -")
        self.lbl_status_exec.setObjectName("linhaInfo")
        self.lbl_status_exec.setWordWrap(True)

        self.lbl_tempo_exec = QLabel("")
        self.lbl_tempo_exec.setObjectName("linhaDestaque")
        self.lbl_tempo_exec.setWordWrap(True)

        self.lbl_area = QLabel("ÁREA: -")
        self.lbl_area.setObjectName("linhaInfo")
        self.lbl_area.setAlignment(Qt.AlignLeft | Qt.AlignTop)
        self.lbl_area.setWordWrap(True)

        self.btn_executar = QPushButton("EXECUTAR")
        self.btn_executar.setObjectName("botaoExecutar")
        self.btn_executar.setCursor(Qt.PointingHandCursor)
        self.btn_executar.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)

        self.btn_parar = QPushButton("PARAR")
        self.btn_parar.setObjectName("botaoParar")
        self.btn_parar.setCursor(Qt.PointingHandCursor)
        self.btn_parar.setVisible(False)

        self.btn_log = QPushButton("VER LOG")
        self.btn_log.setObjectName("botaoLog")
        self.btn_log.setCursor(Qt.PointingHandCursor)

        layout.addWidget(self.lbl_titulo)
        layout.addSpacing(2)
        layout.addWidget(self.lbl_ultima_exec)
        layout.addWidget(self.lbl_proxima_exec)
        layout.addWidget(self.lbl_modo_exec)
        layout.addWidget(self.lbl_status_exec)
        layout.addWidget(self.lbl_tempo_exec)
        layout.addSpacing(4)
        layout.addWidget(self.lbl_area)
        layout.addStretch(1)

        row_btns = QHBoxLayout()
        row_btns.addWidget(self.btn_log)
        row_btns.addWidget(self.btn_parar)
        layout.addLayout(row_btns)
        layout.addWidget(self.btn_executar)

        self.definir_status_visual("AGUARDANDO")

    def definir_status_visual(self, status_texto: str):
        p = EstilosGUI.obter_paleta()
        cor_borda = p["borda_suave"]
        st = (str(status_texto) or "").upper()

        if st == "RODANDO":
            cor_borda = p["azul"]
        elif st in ["FALHA", "ERRO"]:
            cor_borda = p["destaque"]
        elif st == "SUCESSO":
            cor_borda = p["sucesso"]
        elif st == "AVISO":
            cor_borda = p["aviso"]
        elif st == "AGENDADO":
            cor_borda = p["amarelo"]

        self.setStyleSheet(EstilosGUI.estilo_card_kanban(cor_borda))

        badge_cor = {
            "RODANDO": p["azul"],
            "SUCESSO": p["sucesso"],
            "FALHA": p["destaque"],
            "ERRO": p["destaque"],
            "AVISO": p["aviso"],
            "AGENDADO": p["amarelo"],
        }.get(st, "#333333")

        self.lbl_status_exec.setStyleSheet(
            f"""
            background-color: {badge_cor};
            color: #000000;
            border-radius: 10px;
            padding: 2px 8px;
            font-weight: 800;
            font-size: 11px;
            """
        )


def smart_update_listwidget(widget: QListWidget, itens_texto):
    """
    Atualiza QListWidget sem destruir tudo: mantém seleção e ordem,
    só adiciona/remove/reordena o necessário.
    """
    novos = list(itens_texto or [])
    atuais = [widget.item(i).text() for i in range(widget.count())]
    if atuais == novos:
        return

    selecionados = {i.text() for i in widget.selectedItems()}

    itens_existentes = {
        widget.item(i).text(): widget.item(i) for i in range(widget.count())
    }
    novos_set = set(novos)

    # remove o que não existe mais
    for texto, item in list(itens_existentes.items()):
        if texto not in novos_set:
            row = widget.row(item)
            widget.takeItem(row)
            itens_existentes.pop(texto, None)

    # garante ordem e cria novos
    for idx, texto in enumerate(novos):
        item = itens_existentes.get(texto)
        if item is None:
            item = QListWidgetItem(texto)
            widget.insertItem(idx, item)
        else:
            row_atual = widget.row(item)
            if row_atual != idx:
                widget.takeItem(row_atual)
                widget.insertItem(idx, item)
        item.setSelected(texto in selecionados)


class DashboardBox(QFrame):
    def __init__(self, titulo, cor_titulo):
        super().__init__()
        self.setObjectName("DashboardBox")
        self.setStyleSheet(EstilosGUI.estilo_dashboard_box(cor_titulo))
        self.setMinimumSize(400, 300)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)
        lbl = QLabel(titulo)
        lbl.setObjectName("tituloBox")
        lbl.setAlignment(Qt.AlignCenter)
        layout.addWidget(lbl)
        self.lista = QListWidget()
        self.lista.setAlternatingRowColors(True)
        layout.addWidget(self.lista)

    def atualizar_lista(self, itens_texto):
        smart_update_listwidget(self.lista, itens_texto)

    def add_widget(self, widget):
        self.layout().addWidget(widget)


class LogDialog(QDialog):
    def __init__(self, titulo, conteudo, parent=None):
        super().__init__(parent)
        self.setWindowTitle(f"Log: {titulo}")
        self.resize(800, 600)
        self.setStyleSheet("background-color: #141414; color: white;")
        layout = QVBoxLayout(self)
        lbl = QLabel(f"Último Log Disponível - {titulo}")
        lbl.setStyleSheet("font-weight: bold; font-size: 14px; margin-bottom: 10px;")
        layout.addWidget(lbl)
        self.text_edit = QTextEdit()
        self.text_edit.setReadOnly(True)
        self.text_edit.setStyleSheet(
            "background-color: #1F1F1F; border: 1px solid #404040; "
            "padding: 10px; font-family: Consolas, monospace;"
        )
        self.text_edit.setText(conteudo)
        layout.addWidget(self.text_edit)
        btn_fechar = QPushButton("FECHAR")
        btn_fechar.setStyleSheet(EstilosGUI.estilo_botao_topo())
        btn_fechar.clicked.connect(self.close)
        layout.addWidget(btn_fechar, alignment=Qt.AlignRight)


class MonitorSolicitacoes:
    def __init__(
        self,
        logger,
        diretorio_solicitacoes: Path,
        callback_resolver_metodo,
        callback_checar_permissao,
        callback_enfileirar,
        intervalo_segundos: int = 10,
    ):
        self.logger = logger
        self.dir = diretorio_solicitacoes
        self.dir.mkdir(parents=True, exist_ok=True)
        self.callback_resolver_metodo = callback_resolver_metodo
        self.callback_checar_permissao = callback_checar_permissao
        self.callback_enfileirar = callback_enfileirar
        self.intervalo_segundos = intervalo_segundos
        self._parar = False
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def _extrair_metodo_login(self, stem: str):
        if "." in stem:
            partes = stem.split(".")
            metodo = ".".join(partes[:-1]).strip()
            login = partes[-1].strip()
            return metodo, login
        if "_" in stem:
            partes = stem.split("_")
            metodo = partes[0].strip()
            login = "_".join(partes[1:]).strip()
            return metodo, login
        return stem.strip(), ""

    def _loop(self):
        while not self._parar:
            try:
                arquivos = list(self.dir.glob("*.txt"))
                for f in arquivos:
                    try:
                        nome = f.name

                        if nome.startswith("~") or nome.startswith("."):
                            f.unlink(missing_ok=True)
                            continue

                        try:
                            if f.stat().st_size == 0:
                                f.unlink(missing_ok=True)
                                continue
                        except Exception:
                            try:
                                f.unlink(missing_ok=True)
                            except Exception:
                                pass
                            continue

                        try:
                            conteudo = f.read_text(encoding="utf-8", errors="ignore").strip()
                        except Exception:
                            conteudo = ""

                        stem = f.stem
                        metodo_raw, login_raw = self._extrair_metodo_login(stem)
                        if not metodo_raw:
                            try:
                                f.unlink(missing_ok=True)
                            except Exception:
                                pass
                            continue

                        metodo_norm, caminho = self.callback_resolver_metodo(metodo_raw)
                        if not caminho:
                            try:
                                f.unlink(missing_ok=True)
                            except Exception:
                                pass
                            self.logger.info(
                                "solicitacao_metodo_nao_encontrado metodo=%s arquivo=%s",
                                metodo_raw,
                                nome,
                            )
                            continue

                        alvo_login = str(login_raw or "").strip().lower()
                        if alvo_login and "@" not in alvo_login:
                            alvo_login = f"{alvo_login}@c6bank.com"

                        pode = self.callback_checar_permissao(
                            metodo_norm or metodo_raw,
                            alvo_login or "*",
                        )
                        if not pode:
                            try:
                                f.unlink(missing_ok=True)
                            except Exception:
                                pass
                            self.logger.info(
                                "solicitacao_sem_permissao metodo=%s login=%s",
                                metodo_raw,
                                alvo_login,
                            )
                            continue

                        ctx = {
                            "origem": "solicitacao",
                            "usuario": alvo_login,
                            "observacao": conteudo,
                            "justificativa": "Solicitação da área",
                        }
                        self.callback_enfileirar(
                            metodo_norm or metodo_raw,
                            caminho,
                            ctx,
                            datetime.now(TZ),
                        )
                        try:
                            f.unlink(missing_ok=True)
                        except Exception:
                            pass
                        self.logger.info(
                            "solicitacao_enfileirada metodo=%s login=%s",
                            metodo_norm or metodo_raw,
                            alvo_login,
                        )
                    except Exception as e_arquivo:
                        self.logger.error(
                            "monitor_solicitacoes_arquivo_erro arquivo=%s tipo=%s erro=%s",
                            getattr(f, "name", "?"),
                            type(e_arquivo).__name__,
                            e_arquivo,
                        )
                        try:
                            f.unlink(missing_ok=True)
                        except Exception:
                            pass
                        continue
            except Exception as e:
                self.logger.error("monitor_solicitacoes_erro tipo=%s erro=%s", type(e).__name__, e)
            finally:
                gc.collect()
            time.sleep(self.intervalo_segundos)

    def parar(self):
        self._parar = True


class SincronizadorPlanilhas:
    def __init__(self, logger, cliente_bq, intervalo_segundos=600, callback_atualizacao=None):
        self.logger = logger
        self.cliente_bq = cliente_bq
        self.intervalo_segundos = intervalo_segundos
        self.callback_atualizacao = callback_atualizacao
        self.ultima_execucao: Optional[datetime] = None
        self.proxima_execucao: Optional[datetime] = None
        self._parar = False
        self._pausado = False

        DIR_XLSX_AUTEXEC.mkdir(parents=True, exist_ok=True)
        DIR_XLSX_REG.mkdir(parents=True, exist_ok=True)

        try:
            self.logger.info("sincronizador_init_primeira_execucao")
            self.sincronizar_de_arquivos()
        except Exception as e:
            self.logger.error(
                "sincronizador_init_primeira_execucao_erro tipo=%s erro=%s",
                type(e).__name__,
                e,
            )

        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def pausar(self, status: bool):
        self._pausado = status
        self.logger.info("sincronizador_pausado status=%s", status)

    def forcar_atualizacao(self):
        t = threading.Thread(target=self.sincronizar_de_arquivos, daemon=True)
        t.start()

    def _converter_tudo_para_texto(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return df
        df2 = df.copy()
        for col in df2.columns:
            # preserva dt_full como datetime; converte o resto para texto
            if col == "dt_full":
                continue
            df2[col] = df2[col].map(lambda x: "" if pd.isna(x) else str(x))
        return df2

    def _preparar_exec_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Converte data_exec/hora_exec em dt_full e ordena por dt_full DESC.
        Isso roda na thread do sincronizador, não na GUI.
        """
        if df is None or df.empty:
            return df
        try:
            df2 = df.copy()
            cols = {c.lower(): c for c in df2.columns}
            c_data = cols.get("data_exec")
            c_hora = cols.get("hora_exec")
            if not c_data:
                return df2

            d_str = df2[c_data].astype(str).str.strip()
            if c_hora:
                h_str = df2[c_hora].astype(str).str.strip()
                combined = (d_str + " " + h_str).str.strip()
            else:
                combined = d_str

            formatos = [
                "%d/%m/%Y %H:%M:%S",
                "%d/%m/%Y %H:%M",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M",
                "%d/%m/%Y",
                "%Y-%m-%d",
            ]

            dt = None
            for fmt in formatos:
                try:
                    dt = pd.to_datetime(combined, format=fmt, errors="raise")
                    break
                except Exception:
                    continue

            if dt is None:
                with warnings.catch_warnings():
                    warnings.filterwarnings(
                        "ignore",
                        message=(
                            "Could not infer format, so each element will "
                            "be parsed individually, falling back to `dateutil`."
                        ),
                        category=UserWarning,
                    )
                    dt = pd.to_datetime(combined, dayfirst=True, errors="coerce")

            df2["dt_full"] = dt
            try:
                df2 = df2.sort_values("dt_full", ascending=False)
            except Exception:
                pass
            return df2
        except Exception as e:
            self.logger.error("sincronizador_preparar_exec_df_erro tipo=%s erro=%s", type(e).__name__, e)
            return df

    def ler_exec_df(self) -> pd.DataFrame:
        if not ARQ_XLSX_AUTEXEC.exists():
            self.logger.info("ler_exec_df_sem_arquivo caminho=%s", str(ARQ_XLSX_AUTEXEC))
            return pd.DataFrame()
        try:
            df = pd.read_excel(ARQ_XLSX_AUTEXEC, sheet_name=0, dtype=str)
            self.logger.info(
                "ler_exec_df_ok caminho=%s shape=%s cols=%s",
                str(ARQ_XLSX_AUTEXEC),
                df.shape,
                list(df.columns),
            )
            return df
        except Exception as e:
            self.logger.error(
                "ler_exec_df_erro caminho=%s tipo=%s erro=%s",
                str(ARQ_XLSX_AUTEXEC),
                type(e).__name__,
                e,
            )
            return pd.DataFrame()

    def ler_reg_df(self) -> pd.DataFrame:
        if not ARQ_XLSX_REG.exists():
            self.logger.info("ler_reg_df_sem_arquivo caminho=%s", str(ARQ_XLSX_REG))
            return pd.DataFrame()
        try:
            df = pd.read_excel(ARQ_XLSX_REG, sheet_name=0, dtype=str)
            self.logger.info(
                "ler_reg_df_ok caminho=%s shape=%s cols=%s",
                str(ARQ_XLSX_REG),
                df.shape,
                list(df.columns),
            )
            return df
        except Exception as e:
            self.logger.error(
                "ler_reg_df_erro caminho=%s tipo=%s erro=%s",
                str(ARQ_XLSX_REG),
                type(e).__name__,
                e,
            )
            return pd.DataFrame()

    def sincronizar_de_arquivos(self):
        df_exec = pd.DataFrame()
        df_reg = pd.DataFrame()

        # reset pastas XLSX
        for pasta in (DIR_XLSX_AUTEXEC, DIR_XLSX_REG):
            try:
                if pasta.exists():
                    shutil.rmtree(pasta)
                pasta.mkdir(parents=True, exist_ok=True)
                self.logger.info("sincronizador_reset_pasta_ok pasta=%s", str(pasta))
            except Exception as e:
                self.logger.error(
                    "sincronizador_reset_pasta_erro pasta=%s tipo=%s erro=%s",
                    str(pasta),
                    type(e).__name__,
                    e,
                )

        try:
            if not getattr(self.cliente_bq, "offline", False) and self.cliente_bq.client is not None:
                self.logger.info("sincronizador_download_bq_inicio")

                sql_exec = f"SELECT * FROM `{TBL_AUTOMACOES_EXEC}`"
                df_exec_bq = self.cliente_bq.query_df(sql_exec)
                self.logger.info(
                    "sincronizador_download_exec_ok linhas=%s cols=%s",
                    len(df_exec_bq),
                    list(df_exec_bq.columns),
                )
                df_exec_bq.to_excel(ARQ_XLSX_AUTEXEC, index=False)

                sql_reg = f"SELECT * FROM `{TBL_REGISTRO_AUTOMACOES}`"
                df_reg_bq = self.cliente_bq.query_df(sql_reg)
                self.logger.info(
                    "sincronizador_download_reg_ok linhas=%s cols=%s",
                    len(df_reg_bq),
                    list(df_reg_bq.columns),
                )
                df_reg_bq.to_excel(ARQ_XLSX_REG, index=False)
            else:
                self.logger.warning("sincronizador_modo_offline - usando apenas planilhas locais (se existirem)")
        except Exception as e:
            self.logger.error("sincronizador_download_bq_erro tipo=%s erro=%s", type(e).__name__, e)

        df_exec = self.ler_exec_df()
        df_reg = self.ler_reg_df()

        # prepara df_exec com dt_full já calculado e ordenado
        df_exec = self._preparar_exec_df(df_exec)

        df_exec = self._converter_tudo_para_texto(df_exec)
        df_reg = self._converter_tudo_para_texto(df_reg)

        self.ultima_execucao = datetime.now(TZ)
        self.proxima_execucao = self.ultima_execucao + timedelta(seconds=self.intervalo_segundos)

        self.logger.info(
            "sincronizador_planilhas_arquivos ultima_execucao=%s df_exec_shape=%s df_reg_shape=%s",
            self.ultima_execucao.isoformat(),
            getattr(df_exec, "shape", None),
            getattr(df_reg, "shape", None),
        )

        if self.callback_atualizacao:
            try:
                self.callback_atualizacao(df_exec, df_reg)
            except Exception as e:
                self.logger.error("callback_atualizacao_erro tipo=%s erro=%s", type(e).__name__, e)

    def _loop(self):
        while not self._parar:
            if not self._pausado:
                try:
                    self.sincronizar_de_arquivos()
                except Exception as e:
                    self.logger.error(
                        "loop_sincronizar_planilhas_erro tipo=%s erro=%s",
                        type(e).__name__,
                        e,
                    )
            for _ in range(self.intervalo_segundos):
                if self._parar:
                    break
                time.sleep(1)
            self.proxima_execucao = datetime.now(TZ) + timedelta(seconds=self.intervalo_segundos)
            gc.collect()

    def parar(self):
        self._parar = True


class AgendadorMetodos:
    def __init__(
        self,
        logger,
        obter_mapeamento: Callable[[], Dict[str, Dict[str, Any]]],
        obter_exec_df: Callable[[], Optional[Any]],
        enfileirar_callback: Callable[[str, Path, Dict[str, Any], datetime], None],
        intervalo_segundos: int = 60,
    ):
        self.logger = logger
        self.obter_mapeamento = obter_mapeamento
        self.obter_exec_df = obter_exec_df
        self.enfileirar_callback = enfileirar_callback
        self.intervalo_segundos = max(5, int(intervalo_segundos))
        self.tz = TZ

        self.lock = threading.Lock()
        self.proximas_execucoes: Dict[str, Optional[datetime]] = {}
        self.status_agendamento: Dict[str, str] = {}

        self._stop = False

        agora = datetime.now(self.tz)
        self.data_ref = agora.date()
        self._catchup_executado = False

        self.thread_scheduler = threading.Thread(target=self._loop_scheduler, daemon=True)
        self.thread_recalc = threading.Thread(target=self._loop_recalc, daemon=True)
        self.thread_scheduler.start()
        self.thread_recalc.start()

    def parar(self):
        self._stop = True

    def atualizar_planilhas(self):
        try:
            self._recalcular_agenda()
            self._catchup_executado = False
            self.logger.info("agendador_atualizar_planilhas_catchup_reset")
        except Exception as e:
            self.logger.error("agendador_atualizar_planilhas_erro tipo=%s erro=%s", type(e).__name__, e)

    def _normalizar_horarios(self, texto: str):
        if not texto:
            return []
        t = str(texto).strip().lower()
        if t in {"sem", "sob demanda", "sob_demanda", "sob-demanda"}:
            return []
        partes = re.split(r"[,\s;/]+", t)
        horarios = []
        for p in partes:
            p = p.strip()
            if not p:
                continue
            if re.fullmatch(r"\d{1,2}:\d{2}", p):
                hora, minuto = p.split(":")
                horarios.append(f"{int(hora):02d}:{int(minuto):02d}")
        horarios_ordenados = sorted(
            horarios,
            key=lambda x: int(x.split(":")[0]) * 60 + int(x.split(":")[1]),
        )
        return horarios_ordenados

    def _normalizar_dias_semana(self, texto: str):
        if not texto:
            return set(range(0, 7))
        t = str(texto).strip().lower()
        if t in {"sem", "sob demanda", "sob_demanda", "sob-demanda"}:
            return set()
        partes = re.split(r"[,\s;/]+", t)
        dias = set()
        for p in partes:
            p = p.strip()
            if not p:
                continue
            if p in MAPA_DIAS_SEMANA:
                dias.add(MAPA_DIAS_SEMANA[p])
        if not dias:
            return set(range(0, 7))
        return dias

    def _calcular_proxima_execucao(self, horarios, dias_validos, base: datetime):
        if not horarios or not dias_validos:
            return None
        hoje = base.date()
        for soma_dia in range(0, 8):
            dia = hoje + timedelta(days=soma_dia)
            if dia.weekday() not in dias_validos:
                continue
            for hhmm in horarios:
                try:
                    h, m = [int(x) for x in hhmm.split(":")]
                    dt_candidato = datetime(dia.year, dia.month, dia.day, h, m, 0, tzinfo=self.tz)
                except Exception:
                    continue
                if dt_candidato > base:
                    return dt_candidato
        return None

    def _recalcular_agenda(self):
        try:
            agora = datetime.now(self.tz)
            mapeamento = self.obter_mapeamento() or {}
            proximas: Dict[str, Optional[datetime]] = {}
            status_agendamento: Dict[str, str] = {}
            total_com_registro = 0
            total_com_agenda = 0

            for _, metodos in mapeamento.items():
                for metodo, info in metodos.items():
                    registro = info.get("registro") or {}
                    if not registro:
                        proximas[metodo] = None
                        status_agendamento[metodo] = "SEM_REGISTRO"
                        continue

                    total_com_registro += 1
                    status = str(registro.get("status_automacao") or "").strip().upper()
                    horario_txt = str(registro.get("horario") or "").strip()
                    dia_semana_txt = str(registro.get("dia_semana") or "").strip()
                    horarios = self._normalizar_horarios(horario_txt)
                    dias_validos = self._normalizar_dias_semana(dia_semana_txt)

                    if status != "ATIVA":
                        proximas[metodo] = None
                        status_agendamento[metodo] = "INATIVO"
                        continue

                    if not horarios or not dias_validos:
                        proximas[metodo] = None
                        status_agendamento[metodo] = "SEM_AGENDA"
                        continue

                    dt_prox = self._calcular_proxima_execucao(horarios, dias_validos, agora)
                    proximas[metodo] = dt_prox
                    if dt_prox is not None:
                        total_com_agenda += 1
                        status_agendamento[metodo] = "AGENDADO"
                    else:
                        status_agendamento[metodo] = "SEM_PROXIMA"

            with self.lock:
                self.proximas_execucoes = proximas
                self.status_agendamento = status_agendamento

            self.logger.info(
                "agendador_recalculo total_metodos=%s com_registro=%s com_agenda=%s",
                len(self.proximas_execucoes),
                total_com_registro,
                total_com_agenda,
            )
        except Exception as e:
            self.logger.error("agendador_recalculo_erro tipo=%s erro=%s", type(e).__name__, e)

    def _safe_date_convert_local(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            if df is None or df.empty:
                return df
            cols = {c.lower(): c for c in df.columns}
            c_data = cols.get("data_exec")
            c_hora = cols.get("hora_exec")
            if not c_data:
                return df

            d_str = df[c_data].astype(str).str.strip()
            if c_hora:
                h_str = df[c_hora].astype(str).str.strip()
                combined = (d_str + " " + h_str).str.strip()
            else:
                combined = d_str

            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=UserWarning)
                dt = pd.to_datetime(combined, dayfirst=True, errors="coerce")

            df2 = df.copy()
            df2["dt_full"] = dt
            return df2
        except Exception:
            return df

    def _reset_diario(self, nova_data):
        self.logger.info(
            "agendador_reset_diario data_anterior=%s nova_data=%s",
            self.data_ref,
            nova_data,
        )
        self.data_ref = nova_data
        self._catchup_executado = False
        self._recalcular_agenda()

    def _catchup_pendencias(self, agora: datetime):
        try:
            agora_naive = agora.replace(tzinfo=None)

            df_exec = self.obter_exec_df()
            if df_exec is None:
                df_exec = pd.DataFrame()
        except Exception as e:
            self.logger.error(
                "agendador_catchup_obter_exec_df_erro tipo=%s erro=%s",
                type(e).__name__,
                e,
            )
            df_exec = pd.DataFrame()

        mapeamento = self.obter_mapeamento() or {}
        if not mapeamento:
            return

        ult_exec_por_norm: Dict[str, datetime] = {}

        if not df_exec.empty:
            df_exec = self._safe_date_convert_local(df_exec)
            if "dt_full" in df_exec.columns:
                cols = {c.lower(): c for c in df_exec.columns}
                c_met = cols.get("metodo_automacao")
                if c_met:
                    df_dia = df_exec[df_exec["dt_full"].dt.date == agora.date()].copy()
                    if not df_dia.empty:
                        df_dia["_norm_key"] = df_dia[c_met].apply(NormalizadorDF.norm_key)
                        df_dia = df_dia.sort_values("dt_full")
                        for norm, grupo in df_dia.groupby("_norm_key"):
                            ult_exec_por_norm[norm] = grupo["dt_full"].iloc[-1]

        pendencias = []

        for _, metodos in mapeamento.items():
            for metodo, info in metodos.items():
                registro = info.get("registro") or {}
                if not registro:
                    continue

                status = str(registro.get("status_automacao") or "").strip().upper()
                if status != "ATIVA":
                    continue

                horarios = self._normalizar_horarios(registro.get("horario") or "")
                dias_validos = self._normalizar_dias_semana(registro.get("dia_semana") or "")

                if not horarios or agora.weekday() not in dias_validos:
                    continue

                norm = NormalizadorDF.norm_key(metodo)
                ult_exec = ult_exec_por_norm.get(norm)

                for hhmm in horarios:
                    try:
                        h, m = [int(x) for x in hhmm.split(":")]
                    except Exception:
                        continue

                    dt_slot = datetime(
                        agora_naive.year,
                        agora_naive.month,
                        agora_naive.day,
                        h,
                        m,
                        0,
                    )

                    if dt_slot >= agora_naive:
                        continue

                    if ult_exec is None:
                        pendente = True
                    else:
                        pendente = ult_exec < dt_slot

                    if pendente:
                        pendencias.append((dt_slot, metodo, info["path"]))

        if not pendencias:
            self.logger.info("agendador_catchup_sem_pendencias data=%s", agora.date())
            return

        pendencias.sort(key=lambda x: x[0])

        for dt_slot, metodo, caminho in pendencias:
            ctx = {
                "origem": "agendado_catchup",
                "usuario": "",
                "observacao": "",
                "justificativa": "Execução automática (catch-up)",
            }
            try:
                self.enfileirar_callback(metodo, caminho, ctx, dt_slot)
                self.logger.info(
                    "agendador_catchup_enfileirado metodo=%s slot=%s",
                    metodo,
                    dt_slot.isoformat(),
                )
            except Exception as e:
                self.logger.error(
                    "agendador_catchup_enfileirar_erro metodo=%s tipo=%s erro=%s",
                    metodo,
                    type(e).__name__,
                    e,
                )

    def _disparar_vencidos(self):
        try:
            agora = datetime.now(self.tz)
            with self.lock:
                snapshot = dict(self.proximas_execucoes)
            if not snapshot:
                self._recalcular_agenda()
                with self.lock:
                    snapshot = dict(self.proximas_execucoes)

            mapeamento = self.obter_mapeamento() or {}

            for metodo, dt_prox in snapshot.items():
                if not dt_prox:
                    continue
                if dt_prox <= agora and dt_prox.date() == agora.date():
                    caminho = None
                    registro = None
                    for _, metodos in mapeamento.items():
                        if metodo in metodos:
                            caminho = metodos[metodo]["path"]
                            registro = metodos[metodo].get("registro") or {}
                            break
                    if not caminho:
                        continue
                    ctx = {
                        "origem": "agendado",
                        "usuario": "",
                        "observacao": "",
                        "justificativa": "Execução agendada",
                    }
                    try:
                        self.enfileirar_callback(metodo, caminho, ctx, dt_prox)
                        self.logger.info(
                            "agendador_enfileirou metodo=%s horario=%s dt=%s",
                            metodo,
                            dt_prox.strftime("%H:%M"),
                            dt_prox.isoformat(),
                        )
                    except Exception as e:
                        self.logger.error(
                            "agendador_enfileirar_erro metodo=%s tipo=%s erro=%s",
                            metodo,
                            type(e).__name__,
                            e,
                        )

                    nova_base = agora + timedelta(seconds=1)
                    if registro:
                        status = str(registro.get("status_automacao") or "").strip().upper()
                        horario_txt = str(registro.get("horario") or "").strip()
                        dia_semana_txt = str(registro.get("dia_semana") or "").strip()
                        horarios = self._normalizar_horarios(horario_txt)
                        dias_validos = self._normalizar_dias_semana(dia_semana_txt)
                        if status == "ATIVA" and horarios and dias_validos:
                            dt_nova = self._calcular_proxima_execucao(
                                horarios, dias_validos, nova_base
                            )
                        else:
                            dt_nova = None
                    else:
                        dt_nova = None

                    with self.lock:
                        self.proximas_execucoes[metodo] = dt_nova
        except Exception as e:
            self.logger.error("agendador_disparar_erro tipo=%s erro=%s", type(e).__name__, e)

    def _loop_scheduler(self):
        while not self._stop:
            try:
                agora = datetime.now(self.tz)

                if agora.date() != self.data_ref:
                    self._reset_diario(agora.date())

                if not self._catchup_executado:
                    self._catchup_pendencias(agora)
                    self._catchup_executado = True

                self._disparar_vencidos()
            except Exception as e:
                self.logger.error("agendador_loop_scheduler_erro tipo=%s erro=%s", type(e).__name__, e)

            for _ in range(self.intervalo_segundos):
                if self._stop:
                    break
                time.sleep(1)
            gc.collect()

    def _loop_recalc(self):
        while not self._stop:
            try:
                self._recalcular_agenda()
            except Exception as e:
                self.logger.error("agendador_loop_recalc_erro tipo=%s erro=%s", type(e).__name__, e)
            for _ in range(60):
                if self._stop:
                    break
                time.sleep(1)
            gc.collect()

    def get_proxima_exec_dt(self, metodo: str) -> Optional[datetime]:
        with self.lock:
            return self.proximas_execucoes.get(metodo)

    def get_proxima_exec_str(self, metodo: str) -> str:
        with self.lock:
            dt = self.proximas_execucoes.get(metodo)
        if not dt:
            return "-"
        try:
            return dt.strftime("%d/%m/%Y %H:%M:%S")
        except Exception:
            return "-"

    def get_status_agendamento(self, metodo: str) -> str:
        with self.lock:
            return self.status_agendamento.get(metodo, "")

    def snapshot_agendamentos(self):
        with self.lock:
            return dict(self.proximas_execucoes)


class ExecutorMetodos:
    def __init__(self, logger, max_concorrencia, callback_exec_inicio=None, callback_exec_fim=None):
        self.logger = logger
        self.max_concorrencia = max_concorrencia
        self.callback_exec_inicio = callback_exec_inicio
        self.callback_exec_fim = callback_exec_fim

        self.cv = threading.Condition()
        self.fila = []
        self.em_execucao: Dict[str, Dict[str, Any]] = {}
        self.metodos_ocupados = set()
        self.threads_trabalho = []

        for _ in range(self.max_concorrencia):
            t = threading.Thread(target=self._worker, daemon=True)
            t.start()
            self.threads_trabalho.append(t)

    def _worker(self):
        while True:
            with self.cv:
                while not self.fila:
                    self.cv.wait()
                metodo, caminho, contexto, quando = self.fila.pop(0)
                self.em_execucao[metodo] = {
                    "inicio": datetime.now(TZ),
                    "contexto": contexto,
                    "pid": None,
                }
            try:
                self._executar_subprocesso(metodo, caminho, contexto)
            finally:
                gc.collect()

    def _executar_subprocesso(self, metodo, caminho, contexto):
        rc = 1
        log_filho = None
        proc = None
        try:
            env = os.environ.copy()
            env["SERVIDOR_ORIGEM"] = NOME_SERVIDOR
            env["MODO_EXECUCAO"] = (contexto.get("origem", "") or "").upper()
            env["OBSERVACAO"] = contexto.get("observacao", "") or ""
            env["USUARIO_EXEC"] = contexto.get("usuario", "") or ""

            dia_dir = DIR_LOGS_BASE / metodo.lower() / datetime.now(TZ).strftime("%d.%m.%Y")
            dia_dir.mkdir(parents=True, exist_ok=True)
            ts = datetime.now(TZ).strftime("%Y%m%d_%H%M%S")
            log_filho = dia_dir / f"{metodo.upper()}_{ts}.child.log"

            cmd = [sys.executable, str(caminho), "--executado-por-servidor"]

            with open(log_filho, "w", encoding="utf-8", errors="replace") as fh:
                proc = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    env=env,
                )

                with self.cv:
                    if metodo in self.em_execucao:
                        self.em_execucao[metodo]["pid"] = proc.pid

                if callable(self.callback_exec_inicio):
                    try:
                        self.callback_exec_inicio(metodo, contexto, datetime.now(TZ))
                    except Exception:
                        pass

                for linha in iter(proc.stdout.readline, ""):
                    try:
                        fh.write(linha)
                        fh.flush()
                    except Exception:
                        pass

                rc = proc.wait()

            if rc is None:
                rc = 1

            if rc == 0:
                rc_final = 0
            elif rc == 2:
                rc_final = 2
            else:
                rc_final = 1

            if callable(self.callback_exec_fim):
                try:
                    self.callback_exec_fim(metodo, contexto, rc_final, str(log_filho))
                except Exception:
                    pass
        except Exception as e:
            self.logger.error("executor_erro tipo=%s erro=%s", type(e).__name__, e)
            if callable(self.callback_exec_fim):
                try:
                    self.callback_exec_fim(metodo, contexto, 1, str(log_filho) if log_filho else "")
                except Exception:
                    pass
        finally:
            with self.cv:
                self.em_execucao.pop(metodo, None)
                self.metodos_ocupados.discard(metodo)
                self.cv.notify_all()

    def enfileirar(self, metodo, caminho, contexto, quando=None):
        with self.cv:
            if metodo in self.metodos_ocupados:
                self.logger.info("executor_metodo_ocupado metodo=%s ignorando_nova_execucao", metodo)
                return False
            self.metodos_ocupados.add(metodo)
            self.fila.append((metodo, caminho, contexto, quando or datetime.now(TZ)))
            self.cv.notify_all()
            return True

    def snapshot_execucao(self):
        with self.cv:
            return dict(self.em_execucao)

    def parar_processo(self, metodo) -> bool:
        with self.cv:
            info = self.em_execucao.get(metodo)
            pid = (info or {}).get("pid")

        if not pid:
            self.logger.warning("kill_switch_sem_pid metodo=%s", metodo)
            return False

        try:
            try:
                parent = psutil.Process(pid)
            except psutil.NoSuchProcess:
                self.logger.warning("kill_switch_processo_inexistente metodo=%s pid=%s", metodo, pid)
                return False
            except psutil.AccessDenied as e:
                self.logger.error(
                    "kill_switch_access_denied metodo=%s pid=%s erro=%s",
                    metodo,
                    pid,
                    e,
                )
                return False

            children = parent.children(recursive=True)
            for p in children:
                try:
                    p.terminate()
                except Exception:
                    pass
            try:
                parent.terminate()
            except Exception:
                pass

            gone, alive = psutil.wait_procs([parent] + children, timeout=10)

            for p in alive:
                try:
                    p.kill()
                except Exception:
                    pass

            self.logger.warning(
                "kill_switch_ok metodo=%s pid=%s filhos_mortos=%s ainda_vivos=%s",
                metodo,
                pid,
                len(gone),
                len(alive),
            )
            return True
        except Exception as e:
            self.logger.error(
                "kill_switch_erro metodo=%s tipo=%s erro=%s",
                metodo,
                type(e).__name__,
                e,
            )
            return False


class MonitorRecursos(QThread):
    """
    Monitora CPU, RAM, Swap e tamanho da pasta TEMP.
    Emite sinal a cada 1s. Recalcula tamanho da TEMP a cada 30s para evitar gargalo.
    Possui método de limpeza segura da pasta TEMP.
    """
    sinal_recursos = Signal(float, float, float, int)  # cpu, ram%, swap%, temp_MB
    sinal_msg = Signal(str)

    def __init__(self, logger, parent=None):
        super().__init__(parent)
        self.logger = logger
        self._stop = False
        self._temp_dir = self._resolver_temp_dir()
        self._temp_size_mb_cache = 0
        self._lock = threading.Lock()

    def _resolver_temp_dir(self) -> Path:
        base_env = os.environ.get("SERVIDOR_TEMP_DIR")
        if base_env:
            base = Path(base_env).expanduser()
        else:
            base = Path.home() / ".servidor_temp"
        base.mkdir(parents=True, exist_ok=True)
        return base

    def _calcular_tamanho_temp_mb(self) -> int:
        try:
            if not self._temp_dir.exists():
                return 0
            total = 0
            for root, dirs, files in os.walk(self._temp_dir):
                for f in files:
                    fp = Path(root) / f
                    try:
                        total += fp.stat().st_size
                    except Exception:
                        pass
            return int((total + (1024 * 1024 - 1)) / (1024 * 1024))
        except Exception as e:
            self.logger.error(
                "monitor_recursos_temp_size_erro tipo=%s erro=%s",
                type(e).__name__,
                e,
            )
            return self._temp_size_mb_cache

    def limpar_temp(self):
        """Dispara limpeza da pasta TEMP em thread separada."""
        t = threading.Thread(target=self._limpar_temp_bg, daemon=True)
        t.start()

    def _limpar_temp_bg(self):
        try:
            if not self._temp_dir.exists():
                self.sinal_msg.emit("Pasta TEMP não existe, nada a limpar.")
                return
            for item in self._temp_dir.iterdir():
                try:
                    if item.is_file() or item.is_symlink():
                        item.unlink(missing_ok=True)
                    elif item.is_dir():
                        shutil.rmtree(item, ignore_errors=True)
                except Exception:
                    # ignora arquivo travado
                    pass
            novo_tam = self._calcular_tamanho_temp_mb()
            with self._lock:
                self._temp_size_mb_cache = novo_tam
            self.sinal_msg.emit(f"TEMP limpa. Tamanho atual: {novo_tam} MB")
        except Exception as e:
            self.logger.error(
                "monitor_recursos_limpar_temp_erro tipo=%s erro=%s",
                type(e).__name__,
                e,
            )
            self.sinal_msg.emit(f"Erro ao limpar TEMP: {e}")

    def run(self):
        # pequeno delay para não competir com renderização inicial
        time.sleep(0.5)

        contador_temp = 0
        # calcula uma vez no início
        with self._lock:
            self._temp_size_mb_cache = self._calcular_tamanho_temp_mb()

        while not self._stop:
            try:
                cpu = psutil.cpu_percent(interval=None)
                mem = psutil.virtual_memory()
                swap = psutil.swap_memory()

                if contador_temp == 0:
                    novo = self._calcular_tamanho_temp_mb()
                    with self._lock:
                        self._temp_size_mb_cache = novo

                with self._lock:
                    temp_mb = self._temp_size_mb_cache

                self.sinal_recursos.emit(
                    float(cpu),
                    float(mem.percent),
                    float(swap.percent),
                    int(temp_mb),
                )
            except Exception as e:
                self.logger.error("monitor_recursos_loop_erro tipo=%s erro=%s", type(e).__name__, e)
            finally:
                gc.collect()

            time.sleep(1)
            contador_temp = (contador_temp + 1) % 30

    def parar(self):
        self._stop = True


class JanelaServidor(QMainWindow):
    sig_atualizar_dados = Signal(object, object)
    sig_marcar_ocupado = Signal(str, bool)
    sig_log = Signal(str)

    def __init__(self, logger, executor, descobridor, sincronizador, monitor_recursos, get_proxima_exec_str_callback=None, get_status_agendamento_callback=None):
        super().__init__()
        self.logger = logger
        self.executor = executor
        self.descobridor = descobridor
        self.sincronizador = sincronizador
        self.monitor_recursos = monitor_recursos
        self.get_prox_exec = get_proxima_exec_str_callback
        self.get_status_agendamento = get_status_agendamento_callback

        self.mapeamento = {}
        self.df_exec = pd.DataFrame()
        self.df_reg = pd.DataFrame()
        self.cards = {}
        self.infos = {}
        self.dashboard_boxes = {}
        self.agendador = None

        self.log_painel: Optional[QTextEdit] = None
        self.btn_parar_rodando: Optional[QPushButton] = None
        self.chk_auto_sync: Optional[QCheckBox] = None
        self.input_busca: Optional[QLineEdit] = None
        self.nav_list: Optional[QListWidget] = None
        self.stack: Optional[QStackedWidget] = None
        self.navegacao_indices: Dict[str, int] = {}
        self.card_secao: Dict[str, str] = {}
        self._status_pre_busca = ""

        # health bar
        self.cpu_bar: Optional[QProgressBar] = None
        self.ram_bar: Optional[QProgressBar] = None
        self.swap_bar: Optional[QProgressBar] = None
        self.lbl_temp: Optional[QLabel] = None
        self._ultima_atualizacao_planilhas: Optional[datetime] = None
        self._proxima_atualizacao_planilhas: Optional[datetime] = None

        # aba de recursos (swap/memória por método)
        self.lista_recursos_metodos: Optional[QListWidget] = None

        # resumos já pré-calculados (para não rodar Pandas a cada tick)
        self._resumo_sucesso = []
        self._resumo_falhas = []
        self._resumo_outros = []

        # system tray
        self.tray_icon: Optional[QSystemTrayIcon] = None

        self.setWindowTitle("SERVIDOR DE AUTOMAÇÕES - C6")
        self.resize(1400, 900)

        self._setup_ui()
        self._setup_tray_icon()

        self.sig_atualizar_dados.connect(self.atualizar_dados)
        self.sig_marcar_ocupado.connect(self._slot_marcar_ocupado)
        self.sig_log.connect(self._append_log)

        self.monitor_recursos.sinal_recursos.connect(self._on_recursos_atualizados)
        self.monitor_recursos.sinal_msg.connect(self._append_log)
        # inicia o MonitorRecursos com pequeno delay
        QTimer.singleShot(500, self._start_monitor_recursos)

        self.timer_gui = QTimer(self)
        self.timer_gui.timeout.connect(self._tick_gui)
        self.timer_gui.start(1000)

    def _setup_tray_icon(self):
        if not QSystemTrayIcon.isSystemTrayAvailable():
            self.logger.warning("system_tray_nao_disponivel")
            return
        self.tray_icon = QSystemTrayIcon(self)
        icon = self.windowIcon()
        if icon.isNull():
            icon = QIcon()
        self.tray_icon.setIcon(icon)
        self.tray_icon.setToolTip("Servidor de Automações - C6")
        menu = QMenu()
        act_show = QAction("Restaurar", self)
        act_quit = QAction("Sair Definitivamente", self)
        act_show.triggered.connect(self._from_tray_show)
        act_quit.triggered.connect(self._sair_definitivo)
        menu.addAction(act_show)
        menu.addSeparator()
        menu.addAction(act_quit)
        self.tray_icon.setContextMenu(menu)
        self.tray_icon.activated.connect(self._on_tray_activated)
        self.tray_icon.show()

    def _from_tray_show(self):
        self.showNormal()
        self.raise_()
        self.activateWindow()

    def _on_tray_activated(self, reason):
        if reason == QSystemTrayIcon.Trigger:
            self._from_tray_show()

    def _sair_definitivo(self):
        res = QMessageBox.question(
            self,
            "Sair",
            "Deseja encerrar definitivamente o servidor?",
            QMessageBox.Yes | QMessageBox.No,
        )
        if res == QMessageBox.Yes:
            QApplication.instance().quit()

    def closeEvent(self, event):
        if self.tray_icon is not None and self.tray_icon.isVisible():
            self.hide()
            self.tray_icon.showMessage(
                "Servidor de Automações",
                "Servidor rodando em segundo plano.",
                QSystemTrayIcon.Information,
                3000,
            )
            event.ignore()
        else:
            super().closeEvent(event)

    def _start_monitor_recursos(self):
        if not self.monitor_recursos.isRunning():
            self.monitor_recursos.start()

    def _setup_ui(self):
        self.setStyleSheet(EstilosGUI.estilo_janela())
        self.central_widget = QWidget()
        self.setCentralWidget(self.central_widget)

        self.main_layout = QVBoxLayout(self.central_widget)
        self.main_layout.setContentsMargins(20, 20, 20, 20)
        self.main_layout.setSpacing(8)

        topo = self._criar_topo()
        p_topo = EstilosGUI.obter_paleta()
        topo_frame = QFrame()
        topo_frame.setObjectName("topoFrame")
        topo_frame.setStyleSheet(
            f"""
            QFrame#topoFrame {{
                background: {p_topo['gradient_top']};
                border-radius: 14px;
                padding: 14px 18px;
                border: 1px solid {p_topo['borda_suave_clara']};
            }}
            QLabel#statusLabel {{
                color: #0B1220;
                font-weight: 800;
                letter-spacing: 0.5px;
                font-size: 13px;
            }}
            """
        )
        topo_frame.setLayout(topo)
        self.nav_list = QListWidget()
        self.nav_list.setObjectName("listaNavegacao")
        self.nav_list.setFixedWidth(260)
        self.nav_list.setSpacing(4)
        self.nav_list.setAlternatingRowColors(False)
        self.nav_list.currentRowChanged.connect(self._on_secao_alterada)

        self.stack = QStackedWidget()
        self.stack.setObjectName("pilhaSecoes")

        splitter = QSplitter()
        splitter.setHandleWidth(2)
        splitter.addWidget(self.nav_list)
        splitter.addWidget(self.stack)
        splitter.setStretchFactor(0, 0)
        splitter.setStretchFactor(1, 1)

        self.main_layout.addWidget(topo_frame)
        self.main_layout.addWidget(splitter, stretch=1)

        # Painel de LOG fixo
        p_log = EstilosGUI.obter_paleta()
        self.log_painel = QTextEdit()
        self.log_painel.setReadOnly(True)
        self.log_painel.setMaximumHeight(220)
        self.log_painel.setStyleSheet(
            f"background-color: {p_log['bg_card']}; color: {p_log['branco']}; "
            f"font-family: 'JetBrains Mono', 'Consolas', monospace; font-size: 11px;"
            f"border: 1px solid {p_log['borda_suave_clara']}; border-radius: 10px; padding: 8px;"
        )
        self.main_layout.addWidget(self.log_painel)

        # Health bar no rodapé
        self._setup_healthbar()

    def _criar_topo(self):
        topo = QHBoxLayout()
        topo.setContentsMargins(0, 0, 0, 0)
        topo.setSpacing(12)
        self.lbl_status = QLabel("Aguardando dados...")
        self.lbl_status.setObjectName("statusLabel")
        self.lbl_status.setStyleSheet("font-size: 13px;")

        self.chk_auto_sync = QCheckBox("ATUALIZAÇÃO AUTOMÁTICA: ATIVA")
        self.chk_auto_sync.setCursor(Qt.PointingHandCursor)
        self.chk_auto_sync.setChecked(True)
        self.chk_auto_sync.toggled.connect(self._on_toggle_auto_sync)

        btn_force = QPushButton("FORÇAR ATUALIZAÇÃO")
        btn_force.setStyleSheet(EstilosGUI.estilo_botao_topo())
        btn_force.setCursor(Qt.PointingHandCursor)
        btn_force.clicked.connect(self._forcar_update)

        self.input_busca = self._criar_input_busca()

        topo.addWidget(self.lbl_status)
        topo.addStretch(1)
        topo.addWidget(self.input_busca)
        topo.addWidget(self.chk_auto_sync)
        topo.addWidget(btn_force)

        return topo

    def _criar_input_busca(self):
        p = EstilosGUI.obter_paleta()
        barra = QLineEdit()
        barra.setPlaceholderText("Buscar automação...")
        barra.setClearButtonEnabled(True)
        barra.setFixedWidth(280)
        barra.setStyleSheet(
            f"""
            QLineEdit {{
                background-color: rgba(255,255,255,0.14);
                border: 1px solid {p['borda_suave_clara']};
                border-radius: 12px;
                padding: 10px 14px;
                color: {p['branco']};
                font-size: 13px;
            }}
            QLineEdit:focus {{
                border: 1px solid {p['destaque']};
            }}
            QLineEdit::placeholder {{
                color: {p['texto_sec']};
            }}
            """
        )
        barra.textChanged.connect(self._on_busca_text_changed)
        return barra

    def _setup_healthbar(self):
        p = EstilosGUI.obter_paleta()
        hb = QHBoxLayout()
        hb.setContentsMargins(0, 0, 0, 0)
        hb.setSpacing(10)

        def criar_bar(label_text):
            lbl = QLabel(label_text)
            lbl.setStyleSheet(f"color: {p['texto_sec']}; font-size: 11px;")
            bar = QProgressBar()
            bar.setRange(0, 100)
            bar.setValue(0)
            bar.setTextVisible(True)
            bar.setFormat(label_text + " 0%")
            bar.setFixedHeight(18)
            return lbl, bar

        lbl_cpu, self.cpu_bar = criar_bar("CPU")
        lbl_ram, self.ram_bar = criar_bar("RAM")
        lbl_swap, self.swap_bar = criar_bar("SWAP")

        self.lbl_temp = QLabel("TEMP: -")
        self.lbl_temp.setStyleSheet(f"color: {p['texto_sec']}; font-size: 11px;")

        btn_limpar_temp = QPushButton("LIMPAR TEMP")
        btn_limpar_temp.setStyleSheet(EstilosGUI.estilo_botao_topo())
        btn_limpar_temp.setCursor(Qt.PointingHandCursor)
        btn_limpar_temp.clicked.connect(self._on_limpar_temp)

        for lbl, bar in [(lbl_cpu, self.cpu_bar), (lbl_ram, self.ram_bar), (lbl_swap, self.swap_bar)]:
            box = QVBoxLayout()
            box.setSpacing(2)
            box.addWidget(lbl)
            box.addWidget(bar)
            hb.addLayout(box)

        hb.addStretch(1)
        hb.addWidget(self.lbl_temp)
        hb.addWidget(btn_limpar_temp)

        rodape = QFrame()
        rodape.setFrameShape(QFrame.NoFrame)
        rodape.setLayout(hb)
        self.main_layout.addWidget(rodape)

    def _on_secao_alterada(self, row: int):
        if self.stack is None:
            return
        if 0 <= row < self.stack.count():
            self.stack.setCurrentIndex(row)

    def _ir_para_secao(self, secao: str):
        if self.nav_list is None or self.stack is None:
            return
        alvo = (secao or "").lower().strip()
        for i in range(self.nav_list.count()):
            item = self.nav_list.item(i)
            if item and item.data(Qt.UserRole) == alvo:
                self.nav_list.setCurrentRow(i)
                self.stack.setCurrentIndex(i)
                return

    def _on_toggle_auto_sync(self, checked: bool):
        if checked:
            self.chk_auto_sync.setText("ATUALIZAÇÃO AUTOMÁTICA: ATIVA")
            self.sincronizador.pausar(False)
            self.logger.info("ui_auto_sync_ativado")
        else:
            self.chk_auto_sync.setText("ATUALIZAÇÃO AUTOMÁTICA: PAUSADA")
            self.sincronizador.pausar(True)
            self.logger.info("ui_auto_sync_pausado")

    def _forcar_update(self):
        self.lbl_status.setText("Forçando atualização...")
        self.sincronizador.forcar_atualizacao()

    def _on_limpar_temp(self):
        res = QMessageBox.question(
            self,
            "Limpar TEMP",
            "Deseja realmente limpar a pasta TEMP do sistema?",
            QMessageBox.Yes | QMessageBox.No,
        )
        if res == QMessageBox.Yes:
            self.monitor_recursos.limpar_temp()

    @Slot(float, float, float, int)
    def _on_recursos_atualizados(self, cpu, ram, swap, temp_mb):
        def atualizar_bar(bar: QProgressBar, valor: float, label_prefix: str):
            bar.setValue(int(valor))
            bar.setFormat(f"{label_prefix}: {valor:.0f}%")
            if valor < 60:
                cor = "#46D369"
            elif valor < 85:
                cor = "#FFCC00"
            else:
                cor = "#E50914"
            bar.setStyleSheet(
                f"""
                QProgressBar {{
                    background-color: #333333;
                    border-radius: 5px;
                    text-align: center;
                    color: #FFFFFF;
                    font-size: 10px;
                }}
                QProgressBar::chunk {{
                    border-radius: 5px;
                    background-color: {cor};
                }}
                """
            )

        if self.cpu_bar:
            atualizar_bar(self.cpu_bar, cpu, "CPU")
        if self.ram_bar:
            atualizar_bar(self.ram_bar, ram, "RAM")
        if self.swap_bar:
            atualizar_bar(self.swap_bar, swap, "SWAP")
        if self.lbl_temp is not None:
            if temp_mb >= 1024:
                self.lbl_temp.setText(f"TEMP: {temp_mb/1024:.1f} GB")
            else:
                self.lbl_temp.setText(f"TEMP: {temp_mb} MB")

    @Slot(object, object)
    def atualizar_dados(self, df_exec, df_reg):
        self.df_exec = df_exec.copy() if df_exec is not None else pd.DataFrame()
        self.df_reg = df_reg.copy() if df_reg is not None else pd.DataFrame()

        novo = self.descobridor.mapear_por_registro(self.df_reg)
        chaves_mudaram = set(novo.keys()) != set(self.mapeamento.keys())
        abas_vazias = self.stack is None or self.stack.count() == 0
        self.mapeamento = novo

        # recalcula resumos (sucesso/falha/outros) só quando planilhas mudam
        self._recalcular_resumos_execucao()

        if chaves_mudaram or abas_vazias:
            QTimer.singleShot(0, self._reconstruir_abas)
        else:
            QTimer.singleShot(0, self._preencher_cards)
            QTimer.singleShot(0, self._atualizar_monitor)

        ultima = self._ultima_atualizacao_planilhas or datetime.now(TZ)
        proxima = self._proxima_atualizacao_planilhas
        self.atualizar_status_planilhas(ultima, proxima)

    def atualizar_mapeamento_threadsafe(self, df_exec, df_reg):
        self.sig_atualizar_dados.emit(df_exec, df_reg)

    def atualizar_mapeamento(self, df_exec, df_reg):
        self.atualizar_dados(df_exec, df_reg)

    def _recalcular_resumos_execucao(self):
        """Calcula listas de SUCESSO/FALHA/OUTROS para o dia, uma vez por atualização de planilha."""
        self._resumo_sucesso = []
        self._resumo_falhas = []
        self._resumo_outros = []

        df = self.df_exec.copy()
        if df.empty:
            return

        if "dt_full" not in df.columns:
            return

        cols = {c.lower(): c for c in df.columns}
        c_met = cols.get("metodo_automacao")
        c_stat = cols.get("status")
        if not c_met or not c_stat:
            return

        hoje = datetime.now(TZ).date()
        try:
            df_hj = df[df["dt_full"].dt.date == hoje].sort_values("dt_full", ascending=False)
        except Exception:
            return

        for _, r in df_hj.iterrows():
            m = str(r[c_met])
            s = str(r[c_stat]).upper()
            h = r["dt_full"].strftime("%H:%M") if pd.notna(r["dt_full"]) else "-"
            item_txt = f"{h} - {m} ({s})"
            if s == "SUCESSO":
                self._resumo_sucesso.append(item_txt)
            elif s == "FALHA":
                self._resumo_falhas.append(item_txt)
            else:
                self._resumo_outros.append(item_txt)

    def _reconstruir_abas(self):
        if self.nav_list is None or self.stack is None:
            return

        self.navegacao_indices = {}
        self.card_secao = {}
        self.cards.clear()
        self.infos.clear()
        self.dashboard_boxes.clear()
        self.btn_parar_rodando = None
        self.lista_recursos_metodos = None

        self.nav_list.clear()
        while self.stack.count():
            widget = self.stack.widget(0)
            self.stack.removeWidget(widget)
            widget.deleteLater()

        def adicionar_secao(nome_secao: str, widget: QWidget):
            chave_secao = nome_secao.lower().strip()
            item = QListWidgetItem(nome_secao)
            item.setData(Qt.UserRole, chave_secao)
            item.setSizeHint(QSize(220, 44))
            self.nav_list.addItem(item)
            self.stack.addWidget(widget)
            self.navegacao_indices[chave_secao] = self.stack.count() - 1

        p = EstilosGUI.obter_paleta()

        pm = QWidget()
        lm = QVBoxLayout(pm)

        sm = QScrollArea()
        sm.setWidgetResizable(True)
        container_monitor = QWidget()
        grid_monitor = QGridLayout(container_monitor)
        grid_monitor.setContentsMargins(30, 30, 30, 30)
        grid_monitor.setSpacing(20)

        box_pendentes = DashboardBox("A RODAR HOJE", p["amarelo"])
        box_rodando = DashboardBox("RODANDO AGORA", p["azul"])
        box_sucesso = DashboardBox("SUCESSO HOJE", p["sucesso"])
        box_falhas = DashboardBox("FALHAS / ATENÇÃO HOJE", p["destaque"])
        box_outros = DashboardBox("OUTROS STATUS HOJE", p["texto_sec"])

        self.dashboard_boxes["pendentes"] = box_pendentes
        self.dashboard_boxes["rodando"] = box_rodando
        self.dashboard_boxes["sucesso"] = box_sucesso
        self.dashboard_boxes["falhas"] = box_falhas
        self.dashboard_boxes["outros"] = box_outros

        self.btn_parar_rodando = QPushButton("PARAR SELECIONADOS")
        self.btn_parar_rodando.setStyleSheet(EstilosGUI.estilo_botao_topo())
        self.btn_parar_rodando.setCursor(Qt.PointingHandCursor)
        self.btn_parar_rodando.clicked.connect(self._parar_selecionados_monitor)
        box_rodando.add_widget(self.btn_parar_rodando)

        grid_monitor.addWidget(box_pendentes, 0, 0)
        grid_monitor.addWidget(box_rodando, 0, 1)
        grid_monitor.addWidget(box_sucesso, 1, 0)
        grid_monitor.addWidget(box_falhas, 1, 1)
        grid_monitor.addWidget(box_outros, 2, 0, 1, 2)

        sm.setWidget(container_monitor)
        lm.addWidget(sm)
        adicionar_secao("MONITOR", pm)

        tab_rec = QWidget()
        lay_rec = QVBoxLayout(tab_rec)
        lbl_rec = QLabel("Consumo de memória/paginação por método em execução")
        lbl_rec.setStyleSheet("font-size: 13px; font-weight: bold;")
        lay_rec.addWidget(lbl_rec)
        self.lista_recursos_metodos = QListWidget()
        lay_rec.addWidget(self.lista_recursos_metodos)
        adicionar_secao("RECURSOS", tab_rec)

        for aba, itens in self.mapeamento.items():
            if not itens:
                continue

            if aba == "SEM_ATRIBUICAO":
                nome_tab = "EXPLORAR / MANUAIS"
            else:
                nome_tab = aba

            pg = QWidget()
            vb = QVBoxLayout(pg)

            lbl_secao = QLabel(nome_tab)
            lbl_secao.setStyleSheet("font-size: 16px; font-weight: 900; letter-spacing: 0.5px;")
            vb.addWidget(lbl_secao)

            sc = QScrollArea()
            sc.setWidgetResizable(True)
            ct = QWidget()
            gd = QGridLayout(ct)
            gd.setContentsMargins(20, 20, 20, 20)
            gd.setSpacing(16)
            gd.setAlignment(Qt.AlignTop | Qt.AlignLeft)

            r = 0
            c = 0
            max_c = 5

            for met in sorted(itens.keys()):
                try:
                    info = itens[met].get("registro") or {}
                    self.infos[met] = info
                    card = CardKanban(met)
                    card.btn_executar.clicked.connect(partial(self._acao_executar, met))
                    card.btn_parar.clicked.connect(partial(self._acao_parar, met))
                    card.btn_log.clicked.connect(partial(self._acao_ver_log, met))
                    gd.addWidget(card, r, c)
                    self.cards[met] = card
                    self.card_secao[met] = nome_tab.lower().strip()
                    c += 1
                    if c >= max_c:
                        c = 0
                        r += 1
                except Exception as e:
                    self.logger.error("erro_criar_card metodo=%s erro=%s", met, e)

            sc.setWidget(ct)
            vb.addWidget(sc)
            adicionar_secao(nome_tab, pg)

        self._preencher_cards()
        self._atualizar_monitor()
        self._atualizar_tab_recursos()

        if self.nav_list.count() > 0:
            self.nav_list.setCurrentRow(0)

        if self.input_busca is not None:
            self._on_busca_text_changed(self.input_busca.text())

    def _preencher_cards(self):
        df = self.df_exec.copy()
        if df.empty:
            execs_vazios = self.executor.snapshot_execucao()
            for met, card in self.cards.items():
                inf = self.infos.get(met, {})
                card.lbl_area.setText(f"ÁREA: {inf.get('area_solicitante', '-')}")
                prox = "-"
                if self.get_prox_exec:
                    try:
                        raw = self.get_prox_exec(met)
                        if raw and raw != "-":
                            prox = raw
                    except Exception:
                        pass
                card.lbl_proxima_exec.setText(f"PRÓXIMA EXEC: {prox}")
                card.lbl_ultima_exec.setText("ÚLTIMA EXEC: -")
                status_txt = "-"
                if self.get_status_agendamento:
                    try:
                        st_ag = self.get_status_agendamento(met)
                        if st_ag == "AGENDADO":
                            status_txt = "AGENDADO"
                    except Exception:
                        pass
                card.lbl_status_exec.setText(f"STATUS: {status_txt}")
                card.lbl_modo_exec.setText("MODO EXEC: -")
                if met in execs_vazios:
                    card.definir_status_visual("RODANDO")
                else:
                    card.definir_status_visual(status_txt)
            return

        if "dt_full" not in df.columns:
            return

        try:
            hoje = datetime.now(TZ).date()
            df = df[df["dt_full"].dt.date == hoje]
        except Exception:
            return

        cols = {c.lower(): c for c in df.columns}
        c_met = cols.get("metodo_automacao")
        c_stat = cols.get("status")
        c_modo = cols.get("modo_execucao")
        if not c_met or not c_stat or not c_modo:
            return

        try:
            df["_norm_key"] = df[c_met].apply(NormalizadorDF.norm_key)
            grouped = df.sort_values("dt_full", ascending=False).groupby("_norm_key")
        except Exception:
            return

        execs = self.executor.snapshot_execucao()

        for met, card in self.cards.items():
            norm = NormalizadorDF.norm_key(met)
            inf = self.infos.get(met, {})
            card.lbl_area.setText(f"ÁREA: {inf.get('area_solicitante', '-')}")

            prox = "-"
            if self.get_prox_exec:
                try:
                    raw = self.get_prox_exec(met)
                    if raw and raw != "-":
                        prox = raw
                except Exception:
                    pass
            card.lbl_proxima_exec.setText(f"PRÓXIMA EXEC: {prox}")

            status_para_cor = "-"
            if norm in grouped.groups:
                grp = grouped.get_group(norm)
                if not grp.empty:
                    last = grp.iloc[0]
                    dt_val = last["dt_full"]
                    st = str(last[c_stat]).strip().upper()
                    md = str(last[c_modo]).strip().upper()
                    status_para_cor = st
                    if pd.notna(dt_val):
                        card.lbl_ultima_exec.setText(f"ÚLTIMA EXEC: {dt_val.strftime('%d/%m %H:%M')}")
                    else:
                        card.lbl_ultima_exec.setText("ÚLTIMA EXEC: -")
                    card.lbl_status_exec.setText(f"STATUS: {st}")
                    card.lbl_modo_exec.setText(f"MODO EXEC: {md}")
            else:
                card.lbl_ultima_exec.setText("ÚLTIMA EXEC: -")
                status_ag = "-"
                if self.get_status_agendamento:
                    try:
                        st_ag = self.get_status_agendamento(met)
                        if st_ag == "AGENDADO":
                            status_ag = "AGENDADO"
                    except Exception:
                        pass
                card.lbl_status_exec.setText(f"STATUS: {status_ag}")
                status_para_cor = status_ag
                card.lbl_modo_exec.setText("MODO EXEC: -")

            if met in execs:
                card.definir_status_visual("RODANDO")
            else:
                card.definir_status_visual(status_para_cor)

    def _atualizar_monitor(self):
        if not self.dashboard_boxes:
            return

        execs = self.executor.snapshot_execucao()

        # RODANDO AGORA
        lista_rodando = []
        if execs:
            for m, i in execs.items():
                ini = i.get("inicio")
                dur = str(datetime.now(TZ) - ini).split(".")[0] if ini else "-"
                lista_rodando.append(f"{m} (Duração: {dur})")
        else:
            lista_rodando.append("Nenhuma automação rodando.")
        self.dashboard_boxes["rodando"].atualizar_lista(lista_rodando)

        # SUCESSO / FALHA / OUTROS (já pré-calculado)
        self.dashboard_boxes["sucesso"].atualizar_lista(
            self._resumo_sucesso if self._resumo_sucesso else ["Nenhum sucesso hoje."]
        )
        self.dashboard_boxes["falhas"].atualizar_lista(
            self._resumo_falhas if self._resumo_falhas else ["Nenhuma falha hoje."]
        )
        self.dashboard_boxes["outros"].atualizar_lista(
            self._resumo_outros if self._resumo_outros else ["Nenhum registro em outros status hoje."]
        )

        # PENDENTES (usar agendador)
        lista_pendentes = []
        if hasattr(self, "agendador") and self.agendador:
            snapshot = self.agendador.snapshot_agendamentos()
            agora = datetime.now(TZ)
            for met, dt in snapshot.items():
                if dt and dt.date() == agora.date() and dt > agora:
                    lista_pendentes.append(f"{dt.strftime('%H:%M')} - {met}")
        lista_pendentes.sort()
        self.dashboard_boxes["pendentes"].atualizar_lista(
            lista_pendentes if lista_pendentes else ["Nada pendente para hoje."]
        )

    def _atualizar_tab_recursos(self):
        """Atualiza aba RECURSOS com consumo de memória/paginação por método."""
        if self.lista_recursos_metodos is None:
            return

        execs = self.executor.snapshot_execucao()
        linhas = []
        for met, info in execs.items():
            pid = (info or {}).get("pid")
            if not pid:
                continue
            try:
                p = psutil.Process(pid)
                mem = p.memory_info()
                rss_mb = mem.rss / (1024 * 1024)
                vms_mb = mem.vms / (1024 * 1024)
                swap_mb = 0.0
                try:
                    full = p.memory_full_info()
                    if hasattr(full, "swap"):
                        swap_mb = full.swap / (1024 * 1024)
                except Exception:
                    pass
                linhas.append(
                    f"{met} | PID {pid} | RSS {rss_mb:.1f} MB | VMS {vms_mb:.1f} MB | SWAP {swap_mb:.1f} MB"
                )
            except psutil.NoSuchProcess:
                continue
            except Exception as e:
                self.logger.error(
                    "recursos_per_metodo_erro metodo=%s pid=%s tipo=%s erro=%s",
                    met,
                    pid,
                    type(e).__name__,
                    e,
                )
        if not linhas:
            linhas = ["Nenhum método em execução."]
        smart_update_listwidget(self.lista_recursos_metodos, linhas)

    def _tick_gui(self):
        running = self.executor.snapshot_execucao()
        for met, card in self.cards.items():
            is_running = met in running
            card.btn_executar.setEnabled(not is_running)
            card.btn_executar.setText("RODANDO..." if is_running else "EXECUTAR")
            card.btn_parar.setVisible(is_running)
            if not is_running:
                card.btn_parar.setEnabled(True)
                card.btn_parar.setText("PARAR")

            if is_running:
                info = running[met]
                ini = info["inicio"]
                s = int((datetime.now(TZ) - ini).total_seconds())
                card.lbl_tempo_exec.setText(f"⏱ {s//60:02d}:{s%60:02d}")
                card.definir_status_visual("RODANDO")
            else:
                st = card.lbl_status_exec.text().replace("STATUS:", "").strip()
                if st == "RODANDO":
                    card.definir_status_visual("AGUARDANDO")
                else:
                    card.definir_status_visual(st)
                card.lbl_tempo_exec.setText("")

        self._atualizar_monitor()
        self._atualizar_tab_recursos()

    def _on_busca_text_changed(self, texto: str):
        termo = (texto or "").strip().lower()
        secoes_com_resultado = set()
        total_encontrado = 0
        if termo and not self._status_pre_busca:
            self._status_pre_busca = self.lbl_status.text()
        for met, card in self.cards.items():
            if not termo:
                card.setVisible(True)
                continue
            base = met.lower()
            info = self.infos.get(met, {})
            nome_auto = str(info.get("nome_automacao", "")).lower()
            area = str(info.get("area_solicitante", "")).lower()
            titulo_card = card.lbl_titulo.text().lower()
            visivel = any(
                termo in campo for campo in (base, nome_auto, titulo_card, area)
            )
            card.setVisible(visivel)
            if visivel:
                total_encontrado += 1
                secao = self.card_secao.get(met)
                if secao:
                    secoes_com_resultado.add(secao)

        if termo:
            if secoes_com_resultado:
                alvo = sorted(secoes_com_resultado)[0]
                self._ir_para_secao(alvo)
                self.lbl_status.setText(
                    f"Buscando '{texto.strip()}' • {total_encontrado} resultado(s)"
                )
            else:
                self.lbl_status.setText(f"Nada encontrado para '{texto.strip()}'")
        else:
            if self._status_pre_busca:
                self.lbl_status.setText(self._status_pre_busca)
                self._status_pre_busca = ""
            if self.nav_list is not None and self.nav_list.count() > 0 and self.nav_list.currentRow() < 0:
                self.nav_list.setCurrentRow(0)

    def _acao_executar(self, metodo):
        path = None
        if "ISOLADOS" in self.mapeamento and metodo in self.mapeamento["ISOLADOS"]:
            path = self.mapeamento["ISOLADOS"][metodo]["path"]
        else:
            for _, its in self.mapeamento.items():
                if metodo in its:
                    path = its[metodo]["path"]
                    break

        if path:
            if self.executor.enfileirar(
                metodo,
                path,
                {"origem": "manual", "usuario": getpass.getuser()},
            ):
                if metodo in self.cards:
                    self.cards[metodo].btn_executar.setEnabled(False)
                    self.cards[metodo].btn_executar.setText("INICIANDO...")

    def _acao_parar(self, metodo):
        res = QMessageBox.question(
            self,
            "Parar",
            f"Interromper {metodo}?",
            QMessageBox.Yes | QMessageBox.No,
        )
        if res == QMessageBox.Yes:
            card = self.cards.get(metodo)
            if card is not None:
                card.btn_parar.setEnabled(False)
                card.btn_parar.setText("PARANDO...")
            self.executor.parar_processo(metodo)

    def _parar_selecionados_monitor(self):
        if "rodando" not in self.dashboard_boxes:
            return
        lista = self.dashboard_boxes["rodando"].lista
        itens = lista.selectedItems()
        if not itens:
            QMessageBox.information(
                self,
                "Parar automações",
                "Selecione pelo menos uma automação em 'RODANDO AGORA'.",
            )
            return

        nomes = []
        for it in itens:
            texto = it.text()
            nome = texto.split(" (", 1)[0].strip()
            if nome and nome != "Nenhuma automação rodando.":
                nomes.append(nome)

        if not nomes:
            return

        if len(nomes) == 1:
            msg = f"Deseja interromper {nomes[0]}?"
        else:
            msg = "Deseja interromper as seguintes automações?\n- " + "\n- ".join(nomes)

        res = QMessageBox.question(
            self,
            "Parar automações",
            msg,
            QMessageBox.Yes | QMessageBox.No,
        )
        if res != QMessageBox.Yes:
            return

        for nome in nomes:
            card = self.cards.get(nome)
            if card is not None:
                card.btn_parar.setEnabled(False)
                card.btn_parar.setText("PARANDO...")
            self.executor.parar_processo(nome)

    def _acao_ver_log(self, metodo):
        df = self.df_exec.copy()
        log_content = "Nenhum log recente encontrado."
        if not df.empty and "dt_full" in df.columns:
            try:
                df = df.sort_values("dt_full", ascending=False)
            except Exception:
                pass
            cols = {c.lower(): c for c in df.columns}
            c_met = cols.get("metodo_automacao")
            c_log = cols.get("log_completo")
            if c_met and c_log:
                target = NormalizadorDF.norm_key(metodo)
                for _, row in df.iterrows():
                    if NormalizadorDF.norm_key(str(row[c_met])) == target:
                        val = str(row[c_log])
                        if val and val.lower() != "nan":
                            log_content = val
                        break

        if self.log_painel is not None:
            self.log_painel.append("\n" + "=" * 80)
            self.log_painel.append(f"LOG: {metodo}")
            self.log_painel.append(log_content)
            self.log_painel.append("=" * 80)
        else:
            dlg = LogDialog(metodo, log_content, self)
            dlg.exec()

    def marcar_metodo_ocupado(self, metodo, ocupado):
        self.sig_marcar_ocupado.emit(metodo, ocupado)

    @Slot(str, bool)
    def _slot_marcar_ocupado(self, metodo: str, ocupado: bool):
        try:
            card = self.cards.get(metodo)
            if card is None:
                print(f"[WARN] Card '{metodo}' não encontrado em self.cards.")
                return

            if not hasattr(card, "btn_executar") or not hasattr(card, "btn_parar"):
                print(f"[ERROR] Card '{metodo}' não possui os atributos esperados.")
                return

            card.btn_executar.setEnabled(not ocupado)
            card.btn_parar.setVisible(ocupado)
            if ocupado:
                card.btn_parar.setEnabled(True)
                card.btn_parar.setText("PARAR")
        except Exception as e:
            print(f"[ERROR] Falha ao marcar card '{metodo}' como ocupado: {e}")

    @Slot(str)
    def _append_log(self, msg: str):
        if self.log_painel is not None:
            self.log_painel.append(msg)
            # ring buffer de log – manter no máx. 1000 linhas
            MAX_LINHAS = 1000
            doc = self.log_painel.document()
            linhas = doc.blockCount()
            if linhas > MAX_LINHAS:
                cursor = self.log_painel.textCursor()
                cursor.movePosition(QTextCursor.Start)
                cursor.movePosition(
                    QTextCursor.Down,
                    QTextCursor.KeepAnchor,
                    linhas - MAX_LINHAS,
                )
                cursor.removeSelectedText()
                cursor.deleteChar()

    def atualizar_status_planilhas(self, ultima, proxima):
        self._ultima_atualizacao_planilhas = ultima
        self._proxima_atualizacao_planilhas = proxima
        partes = []
        if ultima:
            partes.append(f"Última atualização: {ultima.strftime('%d/%m/%Y %H:%M:%S')}")
        if proxima:
            partes.append(f"Próxima atualização: {proxima.strftime('%d/%m/%Y %H:%M:%S')}")
        if partes:
            self.lbl_status.setText(" | ".join(partes))
            self._status_pre_busca = self.lbl_status.text()


def main():
    warnings.filterwarnings(
        "ignore",
        message=r"Could not infer format, so each element will be parsed individually.*",
        category=UserWarning,
    )

    logger, log_path, fmt_logger = ConfiguradorLogger.criar_logger()

    # Redireciona stdout/stderr para o logger (vai cair no painel via QtLogHandler)
    sys.stdout = StdoutRedirector(logger, level=logging.INFO)
    sys.stderr = StdoutRedirector(logger, level=logging.ERROR)

    try:
        logger.info(f"inicio_servidor nome_script={NOME_SCRIPT} log_file={str(log_path)}")

        extra_args = sys.argv[1:]
        env_headless = os.getenv("SERVIDOR_HEADLESS", "").strip().lower()
        headless = bool(extra_args) or env_headless in {"1", "true", "yes", "sim"}
        global HEADLESS
        HEADLESS = headless
        logger.info("modo_execucao headless=%s args=%s env_headless=%s", HEADLESS, extra_args, env_headless)

        _cliente_bq_servidor = ClienteBigQuery(logger, modo="servidor")
        cliente_bq_planilhas = ClienteBigQuery(logger, modo="planilhas")

        df_exec_inicial = pd.DataFrame()
        df_reg_inicial = pd.DataFrame()

        descobridor = DescobridorMetodos(logger)
        executor = ExecutorMetodos(logger, MAX_CONCURRENCY)

        sync_holder = {"df_exec": df_exec_inicial, "df_reg": df_reg_inicial}
        janela_holder = {"janela": None}
        agendador_holder = {"ag": None}
        sincronizador_holder = {"obj": None}

        def callback_planilhas(df_exec, df_reg):
            sync_holder["df_exec"] = df_exec
            sync_holder["df_reg"] = df_reg
            if janela_holder["janela"] is not None:
                janela_holder["janela"].atualizar_mapeamento_threadsafe(df_exec, df_reg)
                sin = sincronizador_holder.get("obj")
                ultima = getattr(sin, "ultima_execucao", None)
                proxima = getattr(sin, "proxima_execucao", None)
                janela_holder["janela"].atualizar_status_planilhas(ultima, proxima)
            ag = agendador_holder.get("ag")
            if ag is not None:
                ag.atualizar_planilhas()

        sincronizador = SincronizadorPlanilhas(
            logger,
            cliente_bq_planilhas,
            intervalo_segundos=600,
            callback_atualizacao=callback_planilhas,
        )
        sincronizador_holder["obj"] = sincronizador

        def obter_mapeamento_global():
            try:
                df_reg = sync_holder["df_reg"]
            except Exception:
                df_reg = pd.DataFrame()
            return descobridor.mapear_por_registro(df_reg)

        def resolver_metodo(metodo_raw):
            texto_bruto = str(metodo_raw or "").strip()
            norm_alvo = NormalizadorDF.norm_key(texto_bruto)
            mapa = obter_mapeamento_global()

            for _, itens in mapa.items():
                for metodo, info in itens.items():
                    if NormalizadorDF.norm_key(metodo) == norm_alvo:
                        return metodo, info["path"]

            for _, itens in mapa.items():
                for metodo, info in itens.items():
                    if metodo.lower() == texto_bruto.lower():
                        return metodo, info["path"]

            for _, itens in mapa.items():
                for metodo, info in itens.items():
                    if metodo.lower().startswith(texto_bruto.lower()):
                        return metodo, info["path"]

            return texto_bruto, None

        def checar_permissao(metodo_norm, login):
            return True

        def enfileirar_solicitacao(metodo, caminho, ctx, quando):
            executor.enfileirar(metodo, caminho, ctx, quando)

        dir_solic = (
            Path.home()
            / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A"
            / "Mensageria e Cargas Operacionais - 11.CelulaPython"
            / "graciliano"
            / "novo_servidor"
            / "solicitacoes_das_areas"
        )

        _monitor_solic = MonitorSolicitacoes(
            logger,
            dir_solic,
            resolver_metodo,
            checar_permissao,
            enfileirar_solicitacao,
            intervalo_segundos=10,
        )

        def obter_mapeamento_ag():
            return obter_mapeamento_global()

        def obter_exec_df_ag():
            return sync_holder["df_exec"]

        def enfileirar_agendado(metodo, caminho, ctx, quando):
            executor.enfileirar(metodo, caminho, ctx, quando)

        agendador = AgendadorMetodos(
            logger,
            obter_mapeamento_ag,
            obter_exec_df_ag,
            enfileirar_agendado,
            intervalo_segundos=20,
        )
        agendador_holder["ag"] = agendador
        agendador.atualizar_planilhas()

        def on_exec_inicio(metodo, contexto, inicio):
            logger.info(f"exec_inicio metodo={metodo} origem={contexto.get('origem')}")
            try:
                if janela_holder["janela"] is not None:
                    janela_holder["janela"].marcar_metodo_ocupado(metodo, True)
            except Exception as e:
                logger.error(f"exec_inicio_marcar_ocupado_erro metodo={metodo} erro={e}")

        def on_exec_fim(metodo, contexto, rc, log_filho):
            status_txt = "SUCESSO" if rc == 0 else ("SEM DADOS PARA PROCESSAR" if rc == 2 else "FALHA")
            logger.info(f"exec_fim metodo={metodo} rc={rc} status={status_txt}")
            try:
                if janela_holder["janela"] is not None:
                    janela_holder["janela"].marcar_metodo_ocupado(metodo, False)
            except Exception as e:
                logger.error(f"exec_fim_marcar_livre_erro metodo={metodo} erro={e}")

        executor.callback_exec_inicio = on_exec_inicio
        executor.callback_exec_fim = on_exec_fim

        if HEADLESS:
            logger.info("servidor_iniciado_modo_headless (sem GUI)")
            try:
                while True:
                    time.sleep(5)
                    gc.collect()
            except KeyboardInterrupt:
                logger.info("headless_interrompido_por_keyboardinterrupt")
            return 0

        # Modo com GUI
        app = QApplication.instance() or QApplication(sys.argv)

        monitor_recursos = MonitorRecursos(logger)

        janela = JanelaServidor(
            logger,
            executor,
            descobridor,
            sincronizador,
            monitor_recursos,
            get_proxima_exec_str_callback=lambda m: agendador_holder["ag"].get_proxima_exec_str(m)
            if agendador_holder["ag"] is not None
            else "-",
            get_status_agendamento_callback=lambda m: agendador_holder["ag"].get_status_agendamento(m)
            if agendador_holder["ag"] is not None
            else "",
        )

        janela.mapeamento = obter_mapeamento_global()
        janela.atualizar_mapeamento(df_exec_inicial, df_reg_inicial)
        janela_holder["janela"] = janela
        janela.agendador = agendador

        qt_handler = QtLogHandler(lambda msg: janela.sig_log.emit(msg))
        qt_handler.setFormatter(fmt_logger)
        logger.addHandler(qt_handler)

        janela.show()
        rc_app = app.exec()
        return rc_app

    except Exception as e:
        tb = traceback.format_exc()
        logger.error(f"servidor_falhou tipo={type(e).__name__} erro={e} traceback={tb}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
