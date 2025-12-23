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

# Define Root Path (approximated)
HOME = Path.home()
POSSIBLE_ROOTS = [
    HOME / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A",
    HOME / "Meu Drive/C6 CTVM",
    HOME / "C6 CTVM",
]
ROOT_DRIVE = next((p for p in POSSIBLE_ROOTS if p.exists()), HOME / "C6 CTVM")

# Add modules to path for 'dollynho' if needed
MODULES_DIR = ROOT_DRIVE / "Mensageria e Cargas Operacionais - 11.CelulaPython/graciliano/novo_servidor/modules"
if not MODULES_DIR.exists():
    MODULES_DIR = ROOT_DRIVE / "graciliano/novo_servidor/modules"
if str(MODULES_DIR) not in sys.path:
    sys.path.append(str(MODULES_DIR))

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
        "xlsxwriter",
        "unidecode"
    ]
    bootstrap_deps.install_requirements(extra_libs=SCRIPT_DEPS)
except: pass

import pandas as pd
import pandas_gbq
import win32com.client as win32
from google.cloud import bigquery
from pydata_google_auth import cache, get_user_credentials
from playwright.sync_api import sync_playwright
from unidecode import unidecode

# ==============================================================================
# CONSTANTES DO SCRIPT
# ==============================================================================
SCRIPT_NAME = Path(__file__).stem.lower()
START_TIME = datetime.now().replace(microsecond=0)
AREA_NAME = "BO INVESTIMENTOS"

# Project Configs
PROJECT_ID = "datalab-pagamentos"
DATASET_ID = "bo_investimentos"
TABLE_TARGET = f"{PROJECT_ID}.{DATASET_ID}.{SCRIPT_NAME}"

TABLE_CONFIG = f"{PROJECT_ID}.ADMINISTRACAO_CELULA_PYTHON.registro_automacoes"
TABLE_EXEC = f"{PROJECT_ID}.ADMINISTRACAO_CELULA_PYTHON.automacoes_exec"

# Diretórios
TEMP_DIR = Path(os.getenv('TEMP', Path.home())) / "C6_RPA_EXEC" / SCRIPT_NAME
TEMP_DIR.mkdir(parents=True, exist_ok=True)

INPUT_DIR = ROOT_DRIVE / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "automacoes" / AREA_NAME / "arquivos input" / SCRIPT_NAME
if not INPUT_DIR.exists():
     INPUT_DIR = ROOT_DRIVE / "graciliano" / "automacoes" / AREA_NAME / "arquivos input" / SCRIPT_NAME
INPUT_DIR.mkdir(parents=True, exist_ok=True)

GLOBAL_CONFIG = {'area_name': AREA_NAME, 'emails_principal': [], 'emails_cc': [], 'move_file': False}

# URLs e Seletores
URL_LOGIN = "https://maps-funds-backoffice.prod.broker.gondor.infra/distribuicao/app/emissoresprocure"
URL_ATIVOS = "https://maps-funds-backoffice.prod.broker.gondor.infra/distribuicao/app/ativos"

X_USER = '//*[@id="username"]'
X_PASS = '//*[@id="password"]'
X_BTN_ENTRAR = '//*[@id="login"]'
X_GERENCIAR_REGISTRO = '//*[@id="mat-tab-label-0-1"]/div'
X_PESQUISAR = '//*[@id="mat-tab-content-0-1"]/div/div/ativo-gerencial/mat-card/mat-card-actions/async-button/button/span[1]'
X_TBODY_ROW1 = '//*[@id="mat-tab-content-0-1"]/div/div/ativo-gerencial/div/table/tbody/tr[1]'
X_EXPORTAR = '//*[@id="mat-tab-content-0-1"]/div/div/ativo-gerencial/mat-card/mat-card-actions/export-button/button/span[1]'
X_CSV_MENU_ITEM = '//*[@id="mat-menu-panel-1"]/div/div/button/div'

# Sessão
PLAYWRIGHT_SESSION_DIR = Path.home() / "AppData" / "Local" / "PLAYWRIGHT_SESSIONS"
PLAYWRIGHT_SESSION_FILE = PLAYWRIGHT_SESSION_DIR / f"{SCRIPT_NAME}.json"

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
# CLASSE PRINCIPAL
# ==============================================================================
class AutomationTask:
    def __init__(self):
        self.output_files = []
        self.HEADLESS = True

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
                GLOBAL_CONFIG['move_file'] = bool(df.iloc[0].get('move_file', False))
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
            
            # 1. Obter Credenciais
            user_site = None
            pass_site = None
            try:
                import dollynho
                cred = dollynho.get_credencial(SCRIPT_NAME)
                user_site, pass_site = cred
            except: pass
            
            if user_site and pass_site:
                # 2. Executar Playwright
                worker = PlaywrightWorker()
                csv_path = worker.run(user_site, pass_site, self.HEADLESS)
                
                if csv_path and Path(csv_path).exists():
                    self.output_files.append(Path(csv_path))
                    
                    # 3. Processar
                    df = self._process_csv(Path(csv_path))
                    
                    if not df.empty:
                        # 4. Upload BQ
                        self._upload_bq(df)
                        status = "SUCCESS"
                        
                        # 5. Gerar Excel Tratado
                        xlsx = self._save_excel(df, Path(csv_path))
                        self.output_files.append(xlsx)
                    else:
                        status = "NO_DATA"
                else:
                    status = "ERROR" # download fail
            else:
                LOGGER.error("Sem credenciais Dollynho.")
                status = "ERROR"

        except Exception as e:
            status = "ERROR"
            LOGGER.critical(f"Erro fatal: {e}", exc_info=True)
            
        finally:
            end_time = datetime.now().replace(microsecond=0)
            duration = round((end_time - START_TIME).total_seconds(), 2)
            
            zip_path = self._create_smart_zip()
            self._upload_metrics(status, usuario_exec, modo_exec, end_time, duration)
            self._send_email(status, zip_path)
            
            if GLOBAL_CONFIG['move_file'] and status == "SUCCESS":
                 self._move_files_to_network(zip_path)

    def _process_csv(self, path):
        try:
            df = pd.read_csv(path, sep=';', encoding='utf-8-sig', dtype=str, keep_default_na=False)
            
            # Normalize Columns
            df.columns = [
                re.sub(r"[^0-9a-zA-Z]+", "_", 
                       "".join(c for c in unicodedata.normalize("NFKD", str(col)) if not unicodedata.combining(c))
                ).strip("_").lower() 
                for col in df.columns
            ]
            
            # Clean CNPJ Logic (Scientific Notation)
            for col in df.columns:
                if 'cnpj' in col:
                     df[col] = df[col].apply(lambda x: self._tratar_cnpj(x))
            
            df["dt_coleta_utc"] = datetime.now().isoformat()
            df["arquivo_nome"] = path.name
            return df
        except Exception as e:
            LOGGER.error(f"Erro processamento CSV: {e}")
            return pd.DataFrame()

    def _tratar_cnpj(self, val):
        if pd.isna(val) or str(val).strip() == "": return ""
        s_val = str(val).strip()
        if "E+" in s_val or "," in s_val:
            try:
                f_val = float(s_val.replace(",", "."))
                return str(int(f_val)).zfill(14)
            except: pass
        return re.sub(r'\D', '', s_val).zfill(14)

    def _save_excel(self, df, src_path):
        out = TEMP_DIR / f"{src_path.stem}_TRATADO.xlsx"
        
        writer = pd.ExcelWriter(out, engine='xlsxwriter')
        df.to_excel(writer, index=False, sheet_name='Base')
        workbook = writer.book
        worksheet = writer.sheets['Base']
        fmt = workbook.add_format({'num_format': '@'})
        
        for idx in range(len(df.columns)):
            worksheet.set_column(idx, idx, 20, fmt)
            
        writer.close()
        return out

    def _upload_bq(self, df):
        if df.empty: return
        client = bigquery.Client(project=PROJECT_ID)
        
        tbl_stg = f"{TABLE_TARGET}_staging"
        pandas_gbq.to_gbq(df, tbl_stg, project_id=PROJECT_ID, if_exists='replace')
        
        cols = [f"`{c}`" for c in df.columns]
        cond = " AND ".join([f"(T.{c} = S.{c} OR (T.{c} IS NULL AND S.{c} IS NULL))" for c in df.columns])
        
        sql = f"""
        CREATE TABLE IF NOT EXISTS `{TABLE_TARGET}` AS SELECT * FROM `{tbl_stg}` WHERE 1=2;
        
        MERGE `{TABLE_TARGET}` T
        USING `{tbl_stg}` S
        ON {cond}
        WHEN NOT MATCHED THEN
          INSERT ({', '.join(cols)}) VALUES ({', '.join(cols)})
        """
        client.query(sql).result()
        client.delete_table(tbl_stg, not_found_ok=True)

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

    def _move_files_to_network(self, zip_path):
        try:
            base = ROOT_DRIVE / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano"
            target = base / "automacoes" / GLOBAL_CONFIG['area_name'] / "logs" / SCRIPT_NAME / datetime.now().strftime("%Y-%m-%d")
            target.mkdir(parents=True, exist_ok=True)
            if zip_path.exists(): shutil.copy2(zip_path, target)
        except: pass

class PlaywrightWorker:
    def __init__(self):
        self.auth_path = PLAYWRIGHT_SESSION_DIR / f"auth_state_{SCRIPT_NAME}.json"
        PLAYWRIGHT_SESSION_DIR.mkdir(parents=True, exist_ok=True)

    def run(self, usuario, senha, headless):
        t0 = time.time()
        downloaded_file = None
        
        with sync_playwright() as p:
            browser = p.chromium.launch(channel="chrome", headless=headless, args=["--start-maximized"])
            
            context_args = {
                "accept_downloads": True,
                "viewport": {"width": 1920, "height": 1080},
                "locale": "pt-BR"
            }
            if self.auth_path.exists():
                context_args["storage_state"] = str(self.auth_path)
            
            context = browser.new_context(**context_args)
            page = context.new_page()
            
            try:
                LOGGER.info(f"Navegando: {URL_LOGIN}")
                page.goto(URL_LOGIN, timeout=60000)
                
                if page.locator(X_USERNAME).first.is_visible(timeout=5000):
                    page.locator(X_USERNAME).fill(usuario)
                    page.locator(X_PASSWORD).fill(senha)
                    page.locator(X_BTN_ENTRAR).click()
                    page.wait_for_timeout(3000)
                    context.storage_state(path=str(self.auth_path))
                
                LOGGER.info("Indo para Ativos...")
                page.goto(URL_ATIVOS, timeout=60000)
                
                page.locator(X_GERENCIAR_REGISTRO).click()
                page.locator(X_PESQUISAR).click()
                page.wait_for_selector(X_TBODY_ROW1, timeout=30000)
                
                # Export
                page.locator(X_EXPORTAR).click()
                with page.expect_download(timeout=60000) as dl_info:
                    try:
                        page.locator(X_CSV_MENU_ITEM).click()
                    except:
                        page.get_by_text("CSV").click()
                
                dl = dl_info.value
                fname = dl.suggested_filename or "export.csv"
                downloaded_file = INPUT_DIR / fname
                dl.save_as(str(downloaded_file))
                
            finally:
                context.close()
                browser.close()
        
        return str(downloaded_file) if downloaded_file else None

if __name__ == "__main__":
    AutomationTask().run()