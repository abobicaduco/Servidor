# ==============================================================================
# AUTO-INSTALL DEPENDENCIES & CONFIG
# ==============================================================================
import sys
import os
from pathlib import Path
from datetime import datetime
import time
import shutil
import traceback
import logging
import zipfile
import re
import unicodedata
import msvcrt

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
        "openpyxl",
        "playwright",
        "xlsxwriter"
    ]
    bootstrap_deps.install_requirements(extra_libs=SCRIPT_DEPS)
except: pass

import pandas as pd
import pandas_gbq
import win32com.client as win32
from google.cloud import bigquery
from pydata_google_auth import cache, get_user_credentials
from playwright.sync_api import sync_playwright

# ==============================================================================
# CONSTANTES DO SCRIPT
# ==============================================================================
SCRIPT_NAME = Path(__file__).stem.lower()
START_TIME = datetime.now().replace(microsecond=0)
AREA_NAME = "BO INVESTIMENTOS"

PROJECT_ID = "datalab-pagamentos"
DATASET_ID = "tarifas"
TABLE_TARGET = "SUSPENSOES_ATENDIMENTO"
FULL_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_TARGET}"

TABLE_CONFIG = f"{PROJECT_ID}.ADMINISTRACAO_CELULA_PYTHON.registro_automacoes"
TABLE_EXEC = f"{PROJECT_ID}.ADMINISTRACAO_CELULA_PYTHON.automacoes_exec"

TEMP_DIR = Path(os.getenv('TEMP', Path.home())) / "C6_RPA_EXEC" / SCRIPT_NAME
TEMP_DIR.mkdir(parents=True, exist_ok=True)

# URL e Seletores
URL_INICIAL = "https://forms.office.com/Pages/DesignPageV2.aspx?subpage=design&id=j2eUUhQf-ky3Ez07Xbm0xspb9IfO1RtApFWZwT2GEQ5UMjRTTEFIMFJMTVdSVTFPS0NTTFpRVEw3Wi4u&analysis=true"
SEL_MENU_EXCEL = "#ExcelDropdownMenu"
SEL_MENU_OPEN = "[id^='DropdownId'][aria-hidden='false']"

# Mapeamento
COLUNAS_ORIGINAIS = ["ID","Start time","Completion time","Email","Name","Last modified time","CPF/CNPJ do Cliente: (Apenas números sem \".-/\")","Categoria da Tarifa:","Qual a ação que devemos tomar:","Meses de suspensão, caso seja temporária: (Coloque apenas número)","Reativação a partir de:"]
MAP_COLS = {"ID":"id","Start time":"start_time","Completion time":"completion_time","Email":"email","Name":"name","Last modified time":"last_modified_time","CPF/CNPJ do Cliente: (Apenas números sem \".-/\")":"cpf_cnpj","Categoria da Tarifa:":"categoria_tarifa","Qual a ação que devemos tomar:":"acao","Meses de suspensão, caso seja temporária: (Coloque apenas número)":"meses_suspensao","Reativação a partir de:":"reativacao_a_partir_de"}
COLUNAS_BQ = [MAP_COLS[c] for c in COLUNAS_ORIGINAIS] + ["dt_coleta_utc","arquivo_nome"]

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
# CLASSES AUXILIARES
# ==============================================================================
class FileLock:
    def __init__(self, path: Path, timeout_s: int = 60, poll_ms: int = 200):
        self.path = path
        self.timeout_s = timeout_s
        self.poll_ms = poll_ms
        self._fh = None
    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(self.path, "a+b")
        start = time.time()
        acquired = False
        while time.time() - start < self.timeout_s:
            try:
                msvcrt.locking(self._fh.fileno(), msvcrt.LK_NBLCK, 1)
                acquired = True
                break
            except OSError:
                time.sleep(self.poll_ms / 1000.0)
        if not acquired:
            raise TimeoutError("LOCK_TIMEOUT")
        return self
    def __exit__(self, exc_type, exc, tb):
        try: msvcrt.locking(self._fh.fileno(), msvcrt.LK_UNLCK, 1)
        except: pass
        try: self._fh.close()
        except: pass

class BrowserHelper:
    def __init__(self, headless=False):
        self.headless = headless
        base_autom = ROOT_DRIVE / "graciliano" / "automacoes" / "cacatua" 
        if not base_autom.exists(): base_autom = ROOT_DRIVE / "graciliano" / "automacoes" / AREA_NAME # Fallback

        self.session_root = base_autom / ".playwright" / SCRIPT_NAME
        self.profile = self.session_root / "profile"
        self.downloads = self.session_root / "downloads"
        self.profile.mkdir(parents=True, exist_ok=True)
        self.downloads.mkdir(parents=True, exist_ok=True)

    def download_report(self):
        with sync_playwright() as p:
            context = p.chromium.launch_persistent_context(
                user_data_dir=str(self.profile),
                headless=self.headless,
                accept_downloads=True,
                viewport={"width": 1920, "height": 1080},
                args=["--start-maximized"]
            )
            page = context.pages[0] if context.pages else context.new_page()
            
            try:
                LOGGER.info(f"Navegando: {URL_INICIAL}")
                page.goto(URL_INICIAL, timeout=60000, wait_until="domcontentloaded")
                
                # Wait for Excel button
                LOGGER.info("Aguardando botão Excel...")
                page.wait_for_selector(SEL_MENU_EXCEL, timeout=30000)
                
                # Click logic (simplified from original but robust)
                btn = page.locator(SEL_MENU_EXCEL).first
                btn.click()
                
                # Wait for menu item
                try:
                    item = page.get_by_role("menuitem", name="Baixar uma cópia")
                    item.wait_for(timeout=5000)
                except:
                    item = page.get_by_role("menuitem", name="Download a copy")
                
                LOGGER.info("Iniciando download...")
                with page.expect_download(timeout=60000) as d:
                    item.click()
                
                download = d.value
                fpath = TEMP_DIR / "download.xlsx"
                download.save_as(str(fpath))
                LOGGER.info(f"Download concluído: {fpath}")
                return fpath

            finally:
                context.close()

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
                WHERE lower(TRIM(script_name)) = lower('{SCRIPT_NAME}')
                ORDER BY created_at DESC LIMIT 1
            """
            try:
                df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID)
            except:
                query = query.replace(f"lower('{SCRIPT_NAME}')", f"lower('{AREA_NAME.lower()}')")
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
        
        lock_path = TEMP_DIR / f"{SCRIPT_NAME}.lock"
        
        try:
            with FileLock(lock_path):
                LOGGER.info(">>> INICIO <<<")
                
                # 1. Download
                helper = BrowserHelper(headless=False) # Changed to False as per original recommendation for Office forms often needing auth
                excel_path = helper.download_report()
                
                if not excel_path or not excel_path.exists():
                    raise Exception("Download falhou ou arquivo não encontrado.")
                
                # 2. Process
                df = pd.read_excel(excel_path)
                
                # Canoniza colunas
                def canon(s):
                    t = str(s).strip().lower()
                    t = unicodedata.normalize("NFD", t)
                    t = "".join(ch for ch in t if unicodedata.category(ch) != "Mn")
                    return re.sub(r"\s+", " ", t) 
                
                target_map = {canon(c): c for c in COLUNAS_ORIGINAIS}
                rename_map = {}
                for c in df.columns:
                    cc = canon(c)
                    if cc in target_map:
                        rename_map[c] = MAP_COLS[target_map[cc]]
                
                df = df.rename(columns=rename_map)
                
                # Add missing
                for c_orig in COLUNAS_ORIGINAIS:
                    c_bq = MAP_COLS[c_orig]
                    if c_bq not in df.columns:
                        df[c_bq] = None
                        
                df = df[[MAP_COLS[c] for c in COLUNAS_ORIGINAIS]].copy()
                df["dt_coleta_utc"] = datetime.utcnow().isoformat()
                df["arquivo_nome"] = excel_path.name
                
                # Convert content to string/safe types
                df = df.astype(str).replace({'nan': None, 'NaT': None})
                
                # 3. Upload
                if not df.empty:
                    LOGGER.info(f"Subindo {len(df)} linhas para {FULL_TABLE_ID}")
                    pandas_gbq.to_gbq(df, FULL_TABLE_ID, project_id=PROJECT_ID, if_exists='replace')
                    status = "SUCCESS"
                    self.output_files.append(excel_path)
                else:
                    status = "NO_DATA"

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
                for f in self.output_files:
                    if f.exists(): zf.write(f, f.name)
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