# ==============================================================================
# AUTO-INSTALL DEPENDENCIES & CONFIG
# ==============================================================================
import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
import time
import shutil
import traceback
import logging
import zipfile
import re
import hashlib
import io
import requests
import ssl
import certifi
import lxml.etree
from PySide6.QtWidgets import QApplication, QDialog, QLabel, QLineEdit, QVBoxLayout, QPushButton, QHBoxLayout
from PySide6.QtCore import QSettings

# Define Root Path (approximated)
HOME = Path.home()
POSSIBLE_ROOTS = [
    HOME / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A",
    HOME / "Meu Drive/C6 CTVM",
    HOME / "C6 CTVM",
]
ROOT_DRIVE = next((p for p in POSSIBLE_ROOTS if p.exists()), HOME / "C6 CTVM")

try:
    import bootstrap_deps
    SCRIPT_DEPS = [
        "pandas",
        "pandas-gbq",
        "pywin32",
        "google-cloud-bigquery",
        "pydata-google-auth",
        "requests",
        "lxml",
        "pyside6"
    ]
    bootstrap_deps.install_requirements(extra_libs=SCRIPT_DEPS)
except: pass

import pandas as pd
import pandas_gbq
import win32com.client as win32
from google.cloud import bigquery
from pydata_google_auth import cache, get_user_credentials

# ==============================================================================
# CONSTANTES DO SCRIPT
# ==============================================================================
SCRIPT_NAME = Path(__file__).stem.upper()
SCRIPT_NAME_LOWER = SCRIPT_NAME.lower()
START_TIME = datetime.now().replace(microsecond=0)
AREA_NAME = "BO OFICIOS"

PROJECT_ID = "datalab-pagamentos"
TABLE_CONFIG = f"{PROJECT_ID}.ADMINISTRACAO_CELULA_PYTHON.registro_automacoes"
TABLE_EXEC = f"{PROJECT_ID}.ADMINISTRACAO_CELULA_PYTHON.automacoes_exec"

TEMP_DIR = Path(os.getenv('TEMP', Path.home())) / "C6_RPA_EXEC" / SCRIPT_NAME
TEMP_DIR.mkdir(parents=True, exist_ok=True)

TARGET_XML_DIR = ROOT_DRIVE / "Mensageria e Cargas Operacionais - 11.CelulaPython/graciliano/automacoes/BO OFICIOS/arquivos_xml"
if not TARGET_XML_DIR.exists(): TARGET_XML_DIR = ROOT_DRIVE / "graciliano/automacoes/BO OFICIOS/arquivos_xml"
TARGET_XML_DIR.mkdir(parents=True, exist_ok=True)

GLOBAL_CONFIG = {'area_name': AREA_NAME, 'emails_principal': [], 'emails_cc': [], 'move_file': False}

# ==============================================================================
# SETUP LOGGING
# ==============================================================================
def setup_logger():
    logger = logging.getLogger(SCRIPT_NAME)
    logger.setLevel(logging.INFO)
    logger.handlers = []
    
    fmt = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
    
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    
    log_file = TEMP_DIR / f"{SCRIPT_NAME}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    fh = logging.FileHandler(log_file, encoding='utf-8')
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    
    return logger, log_file

LOGGER, LOG_FILE = setup_logger()

# ==============================================================================
# CREDENCIAIS & BIGQUERY
# ==============================================================================
SCOPES = ["https://www.googleapis.com/auth/bigquery"]
CREDENTIALS = None

if not CREDENTIALS:
    try:
        TOKENS_DIR = Path.home() / "AppData" / "Roaming" / "CELPY" / "tokens"
        CREDENTIALS = get_user_credentials(SCOPES, credentials_cache=cache.ReadWriteCredentialsCache(str(TOKENS_DIR)), auth_local_webserver=True)
        pandas_gbq.context.credentials = CREDENTIALS
        pandas_gbq.context.project = PROJECT_ID
    except: pass

# ==============================================================================
# CLASSES AUXILIARES (STA)
# ==============================================================================
from requests.adapters import HTTPAdapter

class SSLContextAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        context = ssl.create_default_context(cafile=certifi.where())
        kwargs["ssl_context"] = context
        return super(SSLContextAdapter, self).init_poolmanager(*args, **kwargs)

class STA:
    def __init__(self, tipo_autenticacao="simples", arquivo=None, id_usuario=None, usuario=None, senha=None):
        import base64
        self.sessao = requests.Session()
        adapter = SSLContextAdapter()
        self.sessao.mount("https://", adapter)
        self.sessao.headers.update({
            "User-Agent": "python-requests",
            "Accept": "application/xml",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Content-Type": "application/xml",
        })
        self.sessao.verify = os.getenv("REQUESTS_CA_BUNDLE", certifi.where())
        
        self.base_url = "https://sta.bcb.gov.br/staws"
        if usuario and senha:
            self.sessao.auth = requests.auth.HTTPBasicAuth(usuario, senha)
            token = base64.b64encode(f"{usuario}:{senha}".encode()).decode()
            self.sessao.headers["Authorization"] = f"Basic {token}"

    def download_arquivo(self, protocolo, hash_str):
        url = f"{self.base_url}/arquivos/{protocolo}/conteudo"
        ret = self.sessao.get(url, headers={"Content-Type": None}, timeout=300)
        
        if ret.status_code != 200:
            raise Exception(f"ERRO DOWNLOAD: Status={ret.status_code}")
            
        xhash = ret.headers.get("X-Content-Hash", "")
        content = ret.content
        
        # Verify Hash
        hasher = hashlib.sha256()
        hasher.update(content)
        calc = hasher.hexdigest()
        esperado = xhash.replace("SHA-256 ", "")
        
        if calc != esperado and esperado != hash_str:
            LOGGER.warning(f"HASH mismatch: calc={calc} header={esperado} param={hash_str}")
            
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            xml = zf.read(zf.namelist()[0])
        return xml

    def listar_disponiveis(self, data_ini, data_fim, arquivo, sistema=None, qtdMax=100):
        url = f"{self.base_url}/arquivos"
        dummy_xml = '<?xml version="1.0" encoding="UTF-8"?><Parametros/>'
        
        params = {
            "tipoConsulta": "AVANC",
            "nivelDetalhe": "COMPL",
            "dataHoraInicio": data_ini.isoformat(timespec="minutes"),
            "dataHoraFim": data_fim.isoformat(timespec="minutes"),
            "identificadorDocumento": arquivo,
            "qtdMaxResultados": qtdMax,
        }
        if sistema: params["sistemas"] = sistema
        
        ret = self.sessao.get(url, params=params, data=dummy_xml, timeout=300)
        
        if ret.status_code != 200:
            raise Exception(f"Status <> 200 | {ret.status_code}")
            
        # Parse XML
        tree = lxml.etree.fromstring(ret.content)
        rows = []
        for el in tree.iter():
            if "Arquivo" in el.tag:
                row = {}
                for child in el:
                    tag_clean = child.tag.split("}")[-1]
                    row[tag_clean] = child.text
                rows.append(row)
        return rows

def extrair_numctrl(xml_bytes):
    root = lxml.etree.fromstring(xml_bytes)
    for elem in root.iter():
        if "NumCtrlCCS" in elem.tag:
            return elem.text
    raise ValueError("NumCtrlCCS NÃO ENCONTRADO NO XML")

def obter_dados_interface():
    # Only run in manual mode or if forced
    app = QApplication.instance() or QApplication(sys.argv)
    dlg = QDialog()
    dlg.setWindowTitle("LOGIN + PERÍODO - COLETA BACEN")
    lay = QVBoxLayout(dlg)
    settings = QSettings("C6Automacao", "ColetaBacen")

    lay.addWidget(QLabel("USUÁRIO BACEN:"))
    le_user = QLineEdit(settings.value("usuario", ""))
    lay.addWidget(le_user)
    lay.addWidget(QLabel("SENHA BACEN:"))
    le_pass = QLineEdit(settings.value("senha", ""))
    le_pass.setEchoMode(QLineEdit.Password)
    lay.addWidget(le_pass)
    lay.addWidget(QLabel("DATA INICIAL (dd/mm/aaaa):"))
    le_data_ini = QLineEdit(settings.value("data_ini", ""))
    lay.addWidget(le_data_ini)
    lay.addWidget(QLabel("DATA FINAL (dd/mm/aaaa):"))
    le_data_fim = QLineEdit(settings.value("data_fim", ""))
    lay.addWidget(le_data_fim)
    
    btn = QPushButton("INICIAR")
    lay.addWidget(btn)
    btn.clicked.connect(dlg.accept)
    
    if not dlg.exec(): sys.exit()
    
    settings.setValue("usuario", le_user.text())
    settings.setValue("senha", le_pass.text())
    settings.setValue("data_ini", le_data_ini.text())
    settings.setValue("data_fim", le_data_fim.text())
    
    return le_user.text(), le_pass.text(), f"{le_data_ini.text()} 00:00", f"{le_data_fim.text()} 23:59"

# ==============================================================================
# CLASSE PRINCIPAL
# ==============================================================================
class AutomationTask:
    def __init__(self):
        self.output_files = []

    def get_configs(self):
        try:
            query = f"""
                SELECT emails_principal, emails_cc, move_file 
                FROM `{TABLE_CONFIG}`
                WHERE lower(TRIM(script_name)) = lower('{SCRIPT_NAME_LOWER}')
                ORDER BY created_at DESC LIMIT 1
            """
            try:
                df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID)
            except:
                query = query.replace(f"lower('{SCRIPT_NAME_LOWER}')", f"lower('{AREA_NAME.lower()}')")
                df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID)

            if not df.empty:
                GLOBAL_CONFIG['emails_principal'] = [x.strip() for x in str(df.iloc[0]['emails_principal']).replace(';', ',').split(',') if '@' in x]
                GLOBAL_CONFIG['emails_cc'] = [x.strip() for x in str(df.iloc[0]['emails_cc']).replace(';', ',').split(',') if '@' in x]
            else:
                GLOBAL_CONFIG['emails_principal'] = ["carlos.lsilva@c6bank.com"]
        except Exception as e:
            LOGGER.error(f"Erro configs: {e}")

    def run(self):
        self.get_configs()
        modo_exec = os.environ.get("ENV_EXEC_MODE", "AGENDAMENTO")
        usuario_exec = os.environ.get("ENV_EXEC_USER", f"{os.getlogin().lower()}@c6bank.com")
        status = "ERROR"
        
        try:
            LOGGER.info(">>> INICIO <<<")
            
            # Credentials
            sta_user = os.getenv("STA_USER")
            sta_pass = os.getenv("STA_PASS")
            dt_ini = os.getenv("STA_DT_INI")
            dt_fim = os.getenv("STA_DT_FIM")

            if not (sta_user and sta_pass and dt_ini and dt_fim):
                 if modo_exec == "MANUAL":
                     sta_user, sta_pass, dt_ini, dt_fim = obter_dados_interface()
                 else:
                     raise ValueError("Credenciais/Periodo não encontrados em ENV para execução automática.")

            data_inicio = datetime.strptime(dt_ini, "%d/%m/%Y %H:%M")
            data = datetime.strptime(dt_fim, "%d/%m/%Y %H:%M")
            
            sta = STA(usuario=sta_user, senha=sta_pass)
            
            coletados = []
            while data > data_inicio:
                interval = timedelta(minutes=300)
                while True:
                    try:
                        inicio_int = data - interval
                        LOGGER.info(f"Consultando {inicio_int} a {data}")
                        disponiveis = sta.listar_disponiveis(inicio_int, data, "AMES102", "CCS", 1000)
                        
                        novos = [r for r in disponiveis if "CCS0011" in r.get("Observacao","") and int(r.get("EstadoAtual_Codigo",0)) != 45]
                        
                        for r in novos:
                            prot = r["Protocolo"]
                            try:
                                xml = sta.download_arquivo(prot, r["Hash"])
                                num_ctrl = extrair_numctrl(xml)
                                target = TARGET_XML_DIR / f"{num_ctrl}.xml"
                                with open(target, "wb") as f: f.write(xml)
                                LOGGER.info(f"XML salvo: {target}")
                                self.output_files.append(target)
                            except Exception as e:
                                LOGGER.error(f"Erro baixar {prot}: {e}")
                                
                        data = inicio_int
                        break
                    except Exception as e:
                        if "401" in str(e): raise
                        LOGGER.error(f"Erro intervalo: {e}")
                        time.sleep(15)

            status = "SUCCESS"

        except Exception as e:
            status = "ERROR"
            LOGGER.critical(f"Erro fatal: {e}", exc_info=True)
            
        finally:
            end_time = datetime.now().replace(microsecond=0)
            duration = round((end_time - START_TIME).total_seconds(), 2)
            zip_path = self._create_smart_zip()
            self._upload_metrics(status, usuario_exec, modo_exec, end_time, duration)
            self._send_email(status, zip_path)

    def _create_smart_zip(self):
        zip_path = TEMP_DIR / f"{SCRIPT_NAME}_{datetime.now().strftime('%H%M%S')}.zip"
        try:
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
                if LOG_FILE.exists(): zf.write(LOG_FILE, LOG_FILE.name)
        except: pass
        return zip_path

    def _upload_metrics(self, status, user, mode, end, duration):
        try:
            df = pd.DataFrame([{
                "script_name": SCRIPT_NAME,
                "area_name": GLOBAL_CONFIG['area_name'],
                "start_time": START_TIME,
                "end_time": end,
                "duration_seconds": duration,
                "status": status,
                "usuario": user,
                "modo_exec": mode
            }])
            pandas_gbq.to_gbq(df, TABLE_EXEC, project_id=PROJECT_ID, if_exists='append')
        except: pass

    def _send_email(self, status, zip_path):
        try:
            to = GLOBAL_CONFIG['emails_principal']
            if status=="SUCCESS": to += GLOBAL_CONFIG['emails_cc']
            if not to: return

            import pythoncom
            pythoncom.CoInitialize()
            outlook = win32.Dispatch("Outlook.Application")
            mail = outlook.CreateItem(0)
            mail.To = ";".join(set(to))
            mail.Subject = f"CÉLULA PYTHON MONITORAÇÃO - {SCRIPT_NAME} - {status}"
            mail.Body = f"Status: {status}"
            if zip_path.exists(): mail.Attachments.Add(str(zip_path))
            mail.Send()
        except: pass

if __name__ == "__main__":
    AutomationTask().run()