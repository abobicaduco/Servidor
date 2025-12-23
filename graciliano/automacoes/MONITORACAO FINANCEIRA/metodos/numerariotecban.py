# ==============================================================================
# AUTO-INSTALL DEPENDENCIES & CONFIG
# ==============================================================================
import sys
import os
from pathlib import Path
from datetime import datetime, date, timedelta

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
        "playwright"
    ]
    bootstrap_deps.install_requirements(extra_libs=SCRIPT_DEPS)
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
        self.DATASET_BQ = "conciliacao_contabil"
        self.TABELA_BQ = "TECBAN_NUMERARIO"
        self.TABELA_BQ_DETALHES = "numerariotecban_detalhes"
        self.URL_INICIAL = "https://numerario.tecban.com.br/portal-2-if/relatoriosIF/extratoIF.xhtml"
        self.IF_ALVO = "0336 - C6 Bank"
        self.PLAYWRIGHT_AUTH_STATE = Path.home() / "AppData" / "Local" / "PLAYWRIGHT_SESSIONS" / f"{SCRIPT_NAME}.json"

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
        modo_exec = os.environ.get("ENV_EXEC_MODE", "AGENDAMENTO")
        usuario = os.environ.get("ENV_EXEC_USER", f"{os.getlogin().lower()}@c6bank.com")
        status = "ERROR"
        linhas_total = 0
        
        try:
            LOGGER.info(">>> INICIO <<<")
            
            dt_fim = date.today() - timedelta(days=1)
            dt_ini = date(dt_fim.year, 1, 1)
            dates = self._get_missing_dates(dt_ini, dt_fim)
            
            if not dates:
                LOGGER.info("Sem datas pendentes.")
                status = "NO_DATA"
            else:
                with sync_playwright() as p:
                    browser = p.chromium.launch(headless=HEADLESS, args=["--start-maximized"])
                    context_args = {"viewport": {"width": 1920, "height": 1080}, "ignore_https_errors": True}
                    
                    if self.PLAYWRIGHT_AUTH_STATE.exists():
                        try: context = browser.new_context(storage_state=str(self.PLAYWRIGHT_AUTH_STATE), **context_args)
                        except: context = browser.new_context(**context_args)
                    else:
                        context = browser.new_context(**context_args)
                    
                    page = context.new_page()
                    self._navigate_and_authenticate(page, context)
                    
                    all_lines = []
                    all_details = []
                    
                    for d in dates:
                        lb, ld = self._consultar_intervalo(page, d, d)
                        all_lines.extend(lb)
                        all_details.extend(ld)
                        
                    browser.close()
                    
                    if all_lines:
                        df_base = pd.DataFrame(all_lines)
                        pandas_gbq.to_gbq(df_base, f"{self.DATASET_BQ}.{self.TABELA_BQ}", project_id=PROJECT_ID, if_exists='append')
                        linhas_total = len(df_base)
                        
                        if all_details:
                            df_det = pd.DataFrame(all_details)
                            pandas_gbq.to_gbq(df_det, f"{self.DATASET_BQ}.{self.TABELA_BQ_DETALHES}", project_id=PROJECT_ID, if_exists='append')
                            
                        client = bigquery.Client(project=PROJECT_ID, credentials=CREDENTIALS)
                        client.query(f"CREATE OR REPLACE TABLE `{PROJECT_ID}.{self.DATASET_BQ}.{self.TABELA_BQ}` AS SELECT DISTINCT * FROM `{PROJECT_ID}.{self.DATASET_BQ}.{self.TABELA_BQ}`").result()
                        
                        status = "SUCCESS"
                    else:
                        status = "NO_DATA"

        except Exception as e:
            status = "ERROR"
            LOGGER.critical(f"Erro fatal: {e}", exc_info=True)
            
        finally:
            end_time = datetime.now().replace(microsecond=0)
            duration = round((end_time - START_TIME).total_seconds(), 2)
            
            zip_path = self._create_smart_zip()
            self._upload_metrics(status, usuario, modo_exec, end_time, duration)
            self._send_email(status, zip_path, linhas_total, duration)
            
            if GLOBAL_CONFIG['move_file'] and status == "SUCCESS":
                 self._move_files_to_network(zip_path)

    def _get_missing_dates(self, di, df):
        try:
            client = bigquery.Client(project=PROJECT_ID, credentials=CREDENTIALS)
            query = f"""
                SELECT DISTINCT SAFE.PARSE_DATE('%Y-%m-%d', data_mov) as d
                FROM `{PROJECT_ID}.{self.DATASET_BQ}.{self.TABELA_BQ}`
                WHERE SAFE.PARSE_DATE('%Y-%m-%d', data_mov) BETWEEN @di AND @df
            """
            job_config = bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("di", "DATE", di),
                bigquery.ScalarQueryParameter("df", "DATE", df)
            ])
            existing = {row.d for row in client.query(query, job_config=job_config) if row.d}
            
            needed = []
            curr = di
            while curr <= df:
                if curr.weekday() < 5 and curr not in existing:
                    needed.append(curr)
                curr += timedelta(days=1)
            return needed
        except Exception as e:
            LOGGER.warning(f"Erro missing dates: {e}")
            return []

    def _navigate_and_authenticate(self, page, context):
        try:
            page.goto(self.URL_INICIAL, timeout=60000)
            time.sleep(2)
            if "login.microsoftonline.com" in page.url.lower():
                try: page.click('div[data-test-id*="@tecban.com"]', timeout=5000)
                except: pass
            
            page.wait_for_selector('xpath=//*[@id="periodoInicio_input"]', timeout=30000)
            self.PLAYWRIGHT_AUTH_STATE.parent.mkdir(parents=True, exist_ok=True)
            context.storage_state(path=str(self.PLAYWRIGHT_AUTH_STATE))
        except Exception as e:
            LOGGER.error(f"Erro nav: {e}")
            raise

    def _consultar_intervalo(self, page, dt_ini, dt_fim):
        linhas, detalhes = [], []
        s_ini, s_fim = dt_ini.strftime("%d/%m/%Y"), dt_fim.strftime("%d/%m/%Y")
        LOGGER.info(f"Consultando {s_ini} a {s_fim}")
        
        try:
            try:
                page.click('#selectIF_label', timeout=5000)
                page.click(f'li[data-label="{self.IF_ALVO}"]', timeout=5000)
            except: pass
            
            page.fill('xpath=//*[@id="periodoInicio_input"]', s_ini); page.keyboard.press("Tab"); time.sleep(0.5)
            page.fill('xpath=//*[@id="periodoFim_input"]', s_fim); page.keyboard.press("Tab"); time.sleep(0.5)
            page.click('#btnConsultar')
            
            start = time.time()
            found = False
            while time.time() - start < 30:
                if page.is_visible("#dlgNegocioException"):
                    try: page.click('#btnDlgNegocioExceptionOK', timeout=2000)
                    except: page.click('#dlgNegocioException .ui-dialog-titlebar-close')
                    return [], []
                if page.is_visible("#extratoDataTable_data"):
                    found = True; break
                time.sleep(0.5)
                
            if not found: return [], []
            
            rows = page.locator("tbody#extratoDataTable_data tr")
            count = rows.count()
            if count == 0: return [], []
            
            for i in range(count):
                row = rows.nth(i)
                tds = row.locator("td")
                if tds.count() < 12: continue
                
                base_data = {
                    "cod_if": tds.nth(1).inner_text().strip(),
                    "nome_if": tds.nth(2).inner_text().strip(),
                    "data_mov": self._fmt_date(tds.nth(3).inner_text().strip()),
                    "saldo_anterior": tds.nth(4).inner_text().strip(),
                    "total_saque": tds.nth(5).inner_text().strip(),
                    "total_deposito": tds.nth(6).inner_text().strip(),
                    "total_remessa": tds.nth(7).inner_text().strip(),
                    "saldo": tds.nth(8).inner_text().strip(),
                    "saldo_if": tds.nth(9).inner_text().strip(),
                    "diferenca": tds.nth(10).inner_text().strip(),
                    "status": tds.nth(11).inner_text().strip(),
                    "dt_coleta": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                base_data["visao_saldo"] = tds.nth(12).inner_text().strip() if tds.count() > 12 else ""
                linhas.append(base_data)
                
                try:
                    link = row.locator('a[id$="linkExtratoDetalhado"]')
                    if link.count() > 0:
                        link.click(); time.sleep(1.5)
                        detalhes.extend(self._extract_details(page, base_data))
                        self._close_modal(page)
                except: pass
                
            return linhas, detalhes

        except Exception as e:
            LOGGER.error(f"Erro consulta: {e}")
            self._close_modal(page)
            return [], []

    def _extract_details(self, page, base_row):
        out = []
        sel = '#extratoDetalhadoPanel_content tbody[id$="_data"]'
        if not page.is_visible(sel): sel = '#dlgExtratoDiarioIF tbody[id$="_data"]'
        
        if page.is_visible(sel):
            rows = page.locator(sel).locator("tr")
            for i in range(rows.count()):
                tds = rows.nth(i).locator("td")
                if tds.count() < 5: continue
                out.append({
                    "cod_if": base_row["cod_if"], "nome_if": base_row["nome_if"], "data_mov": base_row["data_mov"],
                    "tipo_linha": "Detalhe", "if_nome": tds.nth(0).inner_text().strip(), "evento": tds.nth(1).inner_text().strip(),
                    "contabilizacao": tds.nth(2).inner_text().strip(), "qtde": tds.nth(3).inner_text().strip(), "valor": tds.nth(4).inner_text().strip(),
                    "dt_coleta": base_row["dt_coleta"]
                })
        return out

    def _close_modal(self, page):
        try:
             if page.is_visible('#btnDlgNegocioExceptionOK'): page.click('#btnDlgNegocioExceptionOK')
             else: page.keyboard.press("Escape")
        except: pass

    def _fmt_date(self, d_str):
        try: return datetime.strptime(d_str, "%d/%m/%Y").strftime("%Y-%m-%d")
        except: return d_str

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

    def _send_email(self, status, zip_path, linhas, duration):
        try:
            to = GLOBAL_CONFIG['emails_principal']
            if status=="SUCCESS": to += GLOBAL_CONFIG['emails_cc']
            if not to: return

            pythoncom.CoInitialize()
            outlook = Dispatch("Outlook.Application")
            mail = outlook.CreateItem(0)
            mail.To = ";".join(set(to))
            mail.Subject = f"CÉLULA PYTHON MONITORAÇÃO - {SCRIPT_NAME.upper()} - {status}"
            mail.Body = f"Status: {status}\nLinhas: {linhas}\nDuração: {duration}s"
            if zip_path.exists(): mail.Attachments.Add(str(zip_path))
            mail.Send()
        except: pass

    def _move_files_to_network(self, zip_path):
        try:
            base = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano"
            target = base / "automacoes" / GLOBAL_CONFIG['area_name'] / "logs" / SCRIPT_NAME / datetime.now().strftime("%Y-%m-%d")
            target.mkdir(parents=True, exist_ok=True)
            if zip_path.exists(): shutil.copy2(zip_path, target)
        except: pass

if __name__ == "__main__":
    AutomationTask().run()
