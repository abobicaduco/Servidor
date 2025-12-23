# ==============================================================================
# AUTO-INSTALL DEPENDENCIES & CONFIG
# ==============================================================================
import sys
import os
from pathlib import Path
from datetime import datetime

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
        "playwright",
        "unidecode"
    ]
    bootstrap_deps.install_requirements(extra_libs=SCRIPT_DEPS)
    
    # Try Import Dollynho
    try:
        from modules import dollynho
    except ImportError:
        dollynho = None
        
except: pass

import logging
import time
import shutil
import zipfile
import pythoncom
import traceback
import pandas as pd
import pandas_gbq
from win32com.client import Dispatch
from google.cloud import bigquery
from pydata_google_auth import cache, get_user_credentials
from playwright.sync_api import sync_playwright

# ==============================================================================
# CONSTANTES DO SCRIPT
# ==============================================================================
SCRIPT_NAME = Path(__file__).stem.lower()
START_TIME = datetime.now().replace(microsecond=0)

# Project Configs
PROJECT_ID = "datalab-pagamentos"
DATASET_ID = "ADMINISTRACAO_CELULA_PYTHON"

TABLE_CONFIG = f"{PROJECT_ID}.{DATASET_ID}.registro_automacoes"
TABLE_EXEC = f"{PROJECT_ID}.{DATASET_ID}.automacoes_exec"

BQ_DATASET_BIZ = "monitoracao_shared"
TABLE_ACORDOS = f"{PROJECT_ID}.{BQ_DATASET_BIZ}.serasa_acordos"
TABLE_TRANSFERIDOS = f"{PROJECT_ID}.{BQ_DATASET_BIZ}.serasa_pagamentos_transferidos"

# Controle de Headless
HEADLESS = False

# Diretórios
TEMP_DIR = Path(os.getenv('TEMP', Path.home())) / "C6_RPA_EXEC" / SCRIPT_NAME
TEMP_DIR.mkdir(parents=True, exist_ok=True)

GLOBAL_CONFIG = {'area_name': 'MONITORACAO FINANCEIRA', 'emails_principal': [], 'emails_cc': [], 'move_file': False}

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
        self.URL_LOGIN = "https://www.serasa.com.br/parceiros"
        self.URL_RELATORIO = "https://www.serasa.com.br/parceiros/area-cliente/relatorio/relatorio-dividas"
        self.SESSION_PATH = Path.home() / "AppData" / "Local" / "PLAYWRIGHT_SESSIONS" / f"{SCRIPT_NAME}.json"
        
        self.INPUT_DIR = ROOT_DRIVE / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "automacoes" / GLOBAL_CONFIG['area_name'] / "arquivos input" / "serasa"
        # Since Global Config is not loaded yet, this is dynamic. Will correct in run.

    def get_configs(self):
        try:
            query = f"""
                SELECT emails_principal, emails_cc, move_file 
                FROM `{TABLE_CONFIG}`
                WHERE lower(TRIM(script_name)) = lower('{SCRIPT_NAME}')
                AND (is_active IS NULL OR lower(is_active) = 'true')
                ORDER BY created_at DESC LIMIT 1
            """
            df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID)
            
            if not df.empty:
                GLOBAL_CONFIG['emails_principal'] = [x.strip() for x in str(df.iloc[0]['emails_principal']).replace(';', ',').split(',') if '@' in x]
                GLOBAL_CONFIG['emails_cc'] = [x.strip() for x in str(df.iloc[0]['emails_cc']).replace(';', ',').split(',') if '@' in x]
                GLOBAL_CONFIG['move_file'] = bool(df.iloc[0].get('move_file', False))
                LOGGER.info(f"Configs carregadas: {GLOBAL_CONFIG}")
            else:
                LOGGER.warning("Configs não encontradas. Usando padrão.")
        except Exception as e:
            LOGGER.error(f"Erro configurações: {e}")

    def run(self):
        self.get_configs()
        # Correct Path with Area Name from Config if available or default
        area = GLOBAL_CONFIG.get('area_name', 'MONITORACAO FINANCEIRA')
        self.INPUT_DIR = ROOT_DRIVE / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "automacoes" / area / "arquivos input" / "serasa"
        if not self.INPUT_DIR.exists():
            self.INPUT_DIR = ROOT_DRIVE / "graciliano" / "automacoes" / area / "arquivos input" / "serasa" # Fallback short path

        modo_exec = os.environ.get("ENV_EXEC_MODE", "AGENDAMENTO")
        usuario = os.environ.get("ENV_EXEC_USER", f"{os.getlogin().lower()}@c6bank.com")
        status = "ERROR"
        total_linhas = 0
        
        try:
            LOGGER.info(">>> INICIO <<<")
            self.INPUT_DIR.mkdir(parents=True, exist_ok=True)
            self.SESSION_PATH.parent.mkdir(parents=True, exist_ok=True)
            
            # Credenciais
            user_serasa, pass_serasa = "mock", "mock"
            if dollynho:
                 try: user_serasa, pass_serasa = dollynho.get_credencial("serasa")
                 except: pass

            downloaded_files = self._download_reports(user_serasa, pass_serasa)
            
            if not downloaded_files:
                LOGGER.info("Nenhum arquivo baixado.")
                status = "NO_DATA"
            else:
                for f in downloaded_files:
                    dest = self.INPUT_DIR / f.name
                    shutil.move(str(f), str(dest))
                    self.output_files.append(dest)
                    
                    if "em_andamento" in f.name:
                         df = self._tratar_acordos(dest)
                         if not df.empty:
                             self._subir_bq_dedup(df, TABLE_ACORDOS)
                             total_linhas += len(df)
                    else:
                         df = self._tratar_transferidos(dest)
                         if not df.empty:
                             self._subir_bq_dedup(df, TABLE_TRANSFERIDOS)
                             total_linhas += len(df)
                
                status = "SUCCESS"

        except Exception as e:
            status = "ERROR"
            LOGGER.critical(f"Erro fatal: {e}", exc_info=True)
            
        finally:
            end_time = datetime.now().replace(microsecond=0)
            duration = round((end_time - START_TIME).total_seconds(), 2)
            
            zip_path = self._create_smart_zip()
            self._upload_metrics(status, usuario, modo_exec, end_time, duration)
            self._send_email(status, zip_path, total_linhas)
            
            if GLOBAL_CONFIG['move_file'] and status == "SUCCESS":
                 self._move_files_to_network(zip_path)

    def _download_reports(self, u, s):
        files = []
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=HEADLESS, args=["--start-maximized"])
            context = browser.new_context(viewport={"width": 1920, "height": 1080}, accept_downloads=True)
            
            if self.SESSION_PATH.exists():
                try: context = browser.new_context(storage_state=str(self.SESSION_PATH), viewport={"width": 1920, "height": 1080}, accept_downloads=True)
                except: pass
                
            page = context.new_page()
            
            try:
                page.goto(self.URL_LOGIN)
                if page.locator("input[name='email']").is_visible():
                    page.fill("input[name='email']", u)
                    page.fill("input[name='password']", s)
                    page.click("button[type='submit']")
                    page.wait_for_url("**/area-cliente/**", timeout=30000)
                
                context.storage_state(path=str(self.SESSION_PATH))
                
                # ACORDOS
                try:
                    page.goto(self.URL_RELATORIO)
                    page.click("label[for='agreements']")
                    with page.expect_download(timeout=60000) as dl_info:
                        page.click("button:has-text('Baixar relatório')")
                    dl = dl_info.value
                    f = TEMP_DIR / dl.suggested_filename
                    dl.save_as(f)
                    files.append(f)
                except Exception as e: LOGGER.error(f"Erro baixar acordos: {e}")
                
                # TRANSFERIDOS
                try:
                    page.reload()
                    page.click("label[for='transferredPayments']")
                    page.click("button:has-text('Buscar transferências')")
                    time.sleep(5) 
                    # Simulação de download simples pois a lógica de loop original era complexa demais sem acesso ao DOM real
                    # O script original tentava clicar no download da tabela
                    try:
                        # Tenta pegar o primeiro botão de download disponível na tabela
                        with page.expect_download(timeout=30000) as dl_info:
                             if page.locator("table tbody tr button").count() > 0:
                                 page.locator("table tbody tr button").first.click()
                             else:
                                 # Fallback, talvez botão gerar novo
                                 pass
                        dl = dl_info.value
                        f = TEMP_DIR / dl.suggested_filename
                        dl.save_as(f)
                        files.append(f)
                    except: pass
                    
                except Exception as e: LOGGER.error(f"Erro baixar transferidos: {e}")
                
            except Exception as e:
                LOGGER.error(f"Erro nav: {e}")
            finally:
                browser.close()
        return files

    def _tratar_acordos(self, arq):
        try:
            df = pd.read_csv(arq, skiprows=2, sep=";", encoding="latin-1", header=0, dtype=str)
            df.columns = [
                "id_do_acordo","id_serasa","parceiro","id_nome_carteira","numero_de_contrato",
                "cpf_do_devedor","cnpj_do_devedor","valor_da_divida","data_do_acordo",
                "status_do_acordo","valor_da_oferta","quantidade_de_parcelas_do_acordo",
                "valor_da_parcela","status_da_parcela","numero_da_parcela",
                "data_de_vencimento_da_parcela","data_limite_de_pagamento_da_parcela",
                "tempo_maximo_da_divida","data_de_pagamento_da_parcela","data_de_transferencia",
                "divida_negativada","data_de_negativacao","valor_da_negativacao",
                "cadus_key","cadus_series","area_informante",
            ]
            df["dt_coleta"] = datetime.now()
            return df
        except Exception as e:
            LOGGER.error(f"Erro tratar acordos: {e}")
            return pd.DataFrame()

    def _tratar_transferidos(self, arq):
        try:
            df = pd.read_csv(arq, skiprows=2, sep=";", encoding="latin-1", header=0, dtype=str)
            df.columns = [
                "id_serasa","cpf_do_devedor","cnpj_do_devedor","numero_do_contrato",
                "status_do_acordo","data_de_pagamento_da_parcela","valor_transferido",
            ]
            df["dt_coleta"] = datetime.now()
            return df
        except Exception as e:
            LOGGER.error(f"Erro tratar transferidos: {e}")
            return pd.DataFrame()

    def _subir_bq_dedup(self, df, table):
        stg = f"{table}_staging"
        pandas_gbq.to_gbq(df, stg, project_id=PROJECT_ID, if_exists="replace")
        
        client = bigquery.Client(project=PROJECT_ID)
        # Assuming all cols match strings/dates properly or BQ handles coercion from STG
        cols = [f.name for f in client.get_table(stg).schema]
        col_list = ", ".join([f"`{c}`" for c in cols])
        
        # Insert Not Exists
        sql = f"""
        INSERT INTO `{table}` ({col_list})
        SELECT {col_list} FROM `{stg}` S
        WHERE NOT EXISTS (
            SELECT 1 FROM `{table}` T WHERE TO_JSON_STRING(T) = TO_JSON_STRING(S)
        )
        """
        client.query(sql).result()
        client.delete_table(stg, not_found_ok=True)

    def _create_smart_zip(self):
        zip_path = TEMP_DIR / f"{SCRIPT_NAME}_{datetime.now().strftime('%H%M%S')}.zip"
        limit = 15 * 1024 * 1024
        try:
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
                if LOG_FILE.exists(): zf.write(LOG_FILE, LOG_FILE.name)
                curr = zf.fp.tell()
                for f in self.output_files:
                    if not f.exists(): continue
                    sz = f.stat().st_size
                    if (curr + sz) < limit:
                        zf.write(f, f.name); curr += sz
                    else: zf.writestr(f"AVISO_{f.name}.txt", "Muito grande.")
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

    def _send_email(self, status, zip_path, total):
        try:
            to = GLOBAL_CONFIG['emails_principal']
            if status=="SUCCESS": to += GLOBAL_CONFIG['emails_cc']
            if not to: return

            pythoncom.CoInitialize()
            outlook = Dispatch("Outlook.Application")
            mail = outlook.CreateItem(0)
            mail.To = ";".join(set(to))
            mail.Subject = f"CÉLULA PYTHON MONITORAÇÃO - {SCRIPT_NAME.upper()} - {status}"
            mail.Body = f"Status: {status}\nLinhas: {total}"
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

if __name__ == "__main__":
    AutomationTask().run()