# ==============================================================================
# AUTO-INSTALL DEPENDENCIES & CONFIG
# ==============================================================================
import sys
import os
from pathlib import Path
from datetime import datetime

# Adiciona diretório raiz ao path (Híbrido: Relativo ou Padrão C6)
CONFIG_LOADER = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "novo_servidor" / "config_loader.py"
project_root = None

# Se não achou relativo, aponta para o caminho padrão da rede
if not project_root:
    standard_root = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "automacoes" / ""
    if standard_root.exists():
        project_root = standard_root

if project_root:
    sys.path.insert(0, str(project_root))

try:
    import bootstrap_deps

    # ==============================================================================
    # DEPENDÊNCIAS ESPECÍFICAS
    # ==============================================================================
    SCRIPT_DEPS = [
        "pandas",
        "pandas-gbq",
        "pywin32",
        "google-cloud-bigquery",
        "pydata-google-auth",
        "playwright",
        "openpyxl"
    ]
    
    bootstrap_deps.install_requirements(extra_libs=SCRIPT_DEPS)
    from config_loader import Config
    
    # Configuração via Loader (Dinâmica DEV/PROD)
    ROOT_DIR = Config.ROOT_DIR
    PROJECT_ID = Config.PROJECT_ID
    # DATASET_ID = Config.DATASET_ID

except ImportError:
    print("⚠️ Config Loader não encontrado. Usando FALLBACK local.")
    
    # Fallback Hardcoded (Padrão C6 Bank - Assume DEV)
    CONFIG_LOADER = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "novo_servidor"
    PROJECT_ID = 'datalab-pagamentos'
    # DATASET_ID = 'ADMINISTRACAO_CELULA_PYTHON'

# ==============================================================================
# CONSTANTES DO SCRIPT
# ==============================================================================
SCRIPT_NAME = Path(__file__).stem.lower()
START_TIME = datetime.now().replace(microsecond=0)

# Controle de Headless
HEADLESS = False

# Diretórios
TEMP_DIR = Path(os.getenv('TEMP', Path.home())) / "C6_RPA_EXEC" / SCRIPT_NAME
TEMP_DIR.mkdir(parents=True, exist_ok=True)

GLOBAL_CONFIG = {'area_name': 'MONITORACAO FINANCEIRA', 'emails_principal': [], 'emails_cc': [], 'move_file': False}

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

# Modulos opcionais
dollynho = None
try:
    from modules import dollynho
except: pass

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
        self.BQ_TABELA_RAW = f"{PROJECT_ID}.conciliacoes_monitoracao.TASKS_PLANNER_RAW"
        self.BQ_TABELA_DESC = f"{PROJECT_ID}.conciliacoes_monitoracao.TASKS_PLANNER_DESCRICAO"
        self.BQ_PROC_AJUSTE = f"CALL `{PROJECT_ID}.conciliacoes_monitoracao.AJUSTE_TASKS_PLANNER`()"
        self.PLANNER_URL = "https://planner.cloud.microsoft/webui/plan/AV9G9XwJsEukqHvwHND8k2QAGaI1"
        self.PLAYWRIGHT_AUTH_STATE = Path.home() / "AppData" / "Local" / "PLAYWRIGHT_SESSIONS" / f"auth_state_{SCRIPT_NAME}.json"

    def get_configs(self):
        try:
            query = f"""
                SELECT emails_principal, emails_cc, move_file 
                FROM `{Config.TABLE_CONFIG}`
                WHERE lower(TRIM(script_name)) = lower('{SCRIPT_NAME}')
                AND (is_active IS NULL OR lower(is_active) = 'true')
                ORDER BY created_at DESC LIMIT 1
            """
            df = pandas_gbq.read_gbq(query, project_id=Config.PROJECT_ID)
            
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
        usuario = os.environ.get("ENV_EXEC_USER", f"{os.getlogin().lower()}@{Config.COMPANY_DOMAIN}")
        status = "ERROR"
        
        try:
            LOGGER.info(">>> INICIO <<<")
            
            caminho_xlsx = self._baixar_excel()
            
            if not caminho_xlsx or not Path(caminho_xlsx).exists():
                status = "NO_DATA"
                LOGGER.error("Falha download Planner")
            else:
                self.output_files.append(Path(caminho_xlsx))
                df_raw = pd.read_excel(caminho_xlsx, dtype=str)
                
                if df_raw.empty:
                    status = "NO_DATA"
                else:
                    df_main = self._transformar_dataframe_principal(df_raw.copy())
                    df_desc = self._transformar_dataframe_descricao(df_raw.copy())
                    
                    LOGGER.info("Subindo BQ RAW...")
                    pandas_gbq.to_gbq(df_main, self.BQ_TABELA_RAW, project_id=PROJECT_ID, if_exists='replace')
                    
                    LOGGER.info("Subindo BQ DESC...")
                    pandas_gbq.to_gbq(df_desc, self.BQ_TABELA_DESC, project_id=PROJECT_ID, if_exists='replace')
                    
                    LOGGER.info("Procedure ajuste...")
                    bigquery.Client(project=PROJECT_ID, credentials=CREDENTIALS).query(self.BQ_PROC_AJUSTE).result()
                    
                    status = "SUCCESS"

        except Exception as e:
            status = "ERROR"
            LOGGER.critical(f"Erro fatal: {e}", exc_info=True)
            
        finally:
            end_time = datetime.now().replace(microsecond=0)
            duration = round((end_time - START_TIME).total_seconds(), 2)
            
            zip_path = self._create_smart_zip()
            self._upload_metrics(status, usuario, modo_exec, end_time, duration)
            self._send_email(status, zip_path)
            
            if GLOBAL_CONFIG['move_file'] and status == "SUCCESS":
                 self._move_files_to_network(zip_path)

    def _baixar_excel(self) -> str | None:
        xpath_menu = '//*[contains(@class,"linkedBadgeItem")]'
        xpath_export_btn = './/button[contains(@aria-label,"Exportar plano para o Excel")]'
        output_file = TEMP_DIR / f"planner_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=HEADLESS, args=["--start-maximized", "--ignore-certificate-errors"])
                ctx_args = {"accept_downloads": True, "viewport": {"width": 1920, "height": 1080}}
                
                if self.PLAYWRIGHT_AUTH_STATE.exists():
                     ctx_args["storage_state"] = str(self.PLAYWRIGHT_AUTH_STATE)
                     
                context = browser.new_context(**ctx_args)
                page = context.new_page()
                
                LOGGER.info(f"Acessando: {self.PLANNER_URL}")
                page.goto(self.PLANNER_URL, timeout=120000)
                
                if "login.microsoftonline.com" in page.url:
                    LOGGER.info("Login necessario.")
                    if dollynho:
                         try:
                            user, pwd = dollynho.get_credencial("PLANNER_MICROSOFT")
                            page.fill('input[type="email"]', user); page.click('input[type="submit"]'); page.wait_for_timeout(2000)
                            page.fill('input[type="password"]', pwd); page.click('input[type="submit"]')
                            try: page.click('input[id="idSIButton9"]', timeout=5000)
                            except: pass
                            page.wait_for_url("**/plan/**", timeout=60000)
                            context.storage_state(path=str(self.PLAYWRIGHT_AUTH_STATE))
                         except Exception as e: LOGGER.error(f"Erro auto login: {e}")
                    else: LOGGER.warning("Dollynho ausente")

                page.wait_for_selector(f"xpath={xpath_menu}", timeout=60000)
                page.locator(f"xpath={xpath_menu}").first.click()
                
                export_btn = page.locator(f"xpath={xpath_export_btn}").first
                export_btn.wait_for(state="visible", timeout=60000)
                
                with page.expect_download(timeout=180000) as dl:
                    export_btn.click()
                
                download = dl.value
                download.save_as(str(output_file))
                return str(output_file)

        except Exception as e:
            LOGGER.error(f"Erro Playwright: {e}")
            return None

    def _transformar_dataframe_principal(self, df: pd.DataFrame) -> pd.DataFrame:
        if "Descrição" not in df.columns: df["Descrição"] = ""
        df = df.drop(["É Recorrente", "Atrasados", "Itens concluídos da lista de verificação", "Itens da lista de verificação"], axis=1, errors="ignore")
        
        if "Nome da tarefa" in df.columns:
            df["Nome da tarefa"] = df["Nome da tarefa"].astype(str).str.replace("\\n", " ", regex=False)
            
        date_cols = ["Criado em", "Data de início", "Data de conclusão", "Concluído em"]
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], format="%d/%m/%Y", errors="coerce").dt.strftime("%Y-%m-%d")

        flags = ["GG", "G", "M", "P", "Ex", "Priorizada", "Metas", "PRIO_3TRI_2023", "PRIO_4TRI_2023", "PRIO_2S_2024", "PRIO_1TRI_2024"]
        for c in flags: df[c] = False
        if "Rótulos" in df.columns: df["Rótulos"] = df["Rótulos"].astype(str)
        
        def apply_flags(row):
            r = row["Rótulos"]
            if "GG" in r: row["GG"] = True
            if "G" in r: row["G"] = True
            if "M" in r: row["M"] = True
            if "P" in r: row["P"] = True
            if "Ex" in r: row["Ex"] = True
            if "META" in r: row["Metas"] = True
            if "3TRI 2023" in r: row["PRIO_3TRI_2023"] = True
            if "4TRI 2023" in r: row["PRIO_4TRI_2023"] = True
            if "1TRI 2024" in r: row["PRIO_1TRI_2024"] = True
            if "Prioridade 2S 24" in r: row["PRIO_2S_2024"] = True
            return row
        
        df = df.apply(apply_flags, axis=1)

        if "Atribuído a" in df.columns:
            rows = []
            for _, row in df.iterrows():
                names = [n.strip() for n in str(row["Atribuído a"]).split(";") if n.strip()]
                if len(names) > 1:
                    for n in names:
                        nr = row.copy(); nr["Atribuído a"] = n; rows.append(nr)
                else: rows.append(row)
            df = pd.DataFrame(rows)
            
        renames = {
            "Identificação da tarefa": "Identificacao_da_tarefa", "Nome da tarefa": "Nome_da_Tarefa",
            "Nome do Bucket": "Nome_do_Bucket", "Progresso": "Progresso", "Prioridade": "Prioridade",
            "Atribuído a": "Atribuido_a", "Criado por": "Criado_por", "Criado em": "Criado_em",
            "Data de início": "Data_de_inicio", "Data de conclusão": "Data_de_conclusao",
            "Concluído em": "Concluido_em", "Concluída por": "Concluida_por", "Rótulos": "Rotulos", "Descrição": "Descricao",
        }
        df = df.rename(columns=renames)
        for c in flags: 
             if c in df.columns: df[c] = df[c].astype(str)
        return df

    def _transformar_dataframe_descricao(self, df: pd.DataFrame) -> pd.DataFrame:
        ident = df.get("Identificação da tarefa", df.get("Identificacao_da_tarefa", pd.Series([""]*len(df)))).astype(str)
        desc = df.get("Descrição", df.get("Descricao", pd.Series([""]*len(df)))).astype(str)
        return pd.DataFrame({"Identificacao_da_tarefa": ident, "Descricao": desc})

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
            pandas_gbq.to_gbq(df, Config.TABLE_EXEC, project_id=PROJECT_ID, if_exists='append')
        except: pass

    def _send_email(self, status, zip_path):
        try:
            to = GLOBAL_CONFIG['emails_principal']
            if status=="SUCCESS": to += GLOBAL_CONFIG['emails_cc']
            if not to: return

            pythoncom.CoInitialize()
            outlook = Dispatch("Outlook.Application")
            mail = outlook.CreateItem(0)
            mail.To = ";".join(set(to))
            mail.Subject = f"CÉLULA PYTHON MONITORAÇÃO - {SCRIPT_NAME.upper()} - {status}"
            mail.Body = ""
            if zip_path.exists(): mail.Attachments.Add(str(zip_path))
            mail.Send()
        except: pass

    def _move_files_to_network(self, zip_path):
        try:
            base = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano"
            target = base / "automacoes" / GLOBAL_CONFIG['area_name'] / "logs" / SCRIPT_NAME / datetime.now().strftime("%Y-%m-%d")
            target.mkdir(parents=True, exist_ok=True)
            if zip_path.exists(): shutil.copy2(zip_path, target)
            for f in self.output_files:
                if f.exists(): shutil.copy2(f, target)
        except: pass

if __name__ == "__main__":
    AutomationTask().run()
