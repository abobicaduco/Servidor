# ==============================================================================
# AUTO-INSTALL DEPENDENCIES & CONFIG
# ==============================================================================
import sys
import os
from pathlib import Path
from datetime import datetime, timedelta, date
import time
import shutil
import traceback
import logging
import zipfile
import re
import getpass
import hashlib
import io
import requests
import ssl
import certifi
import lxml.etree
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed

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
        "playwright",
        "pyside6"
    ]
    bootstrap_deps.install_requirements(extra_libs=SCRIPT_DEPS)
except: pass

import pandas as pd
import pandas_gbq
import win32com.client as win32
from google.cloud import bigquery
from pydata_google_auth import cache, get_user_credentials
from playwright.sync_api import sync_playwright, Page, Browser, BrowserContext, expect

try:
    from PySide6.QtWidgets import QApplication
    HAS_GUI = True
except ImportError:
    HAS_GUI = False

# ==============================================================================
# CONSTANTES DO SCRIPT
# ==============================================================================
SCRIPT_NAME = Path(__file__).stem.upper()
SCRIPT_NAME_LOWER = SCRIPT_NAME.lower()
TZ = ZoneInfo("America/Sao_Paulo")
START_TIME = datetime.now(TZ).replace(microsecond=0)
AREA_NAME = "BO OFICIOS"

PROJECT_ID = "datalab-pagamentos"
TABLE_CONFIG = f"{PROJECT_ID}.ADMINISTRACAO_CELULA_PYTHON.registro_automacoes"
TABLE_EXEC = f"{PROJECT_ID}.ADMINISTRACAO_CELULA_PYTHON.automacoes_exec"
WALLB_CASOS = f"{PROJECT_ID}.ADMINISTRACAO_CELULA_PYTHON.WallB_casos"

TEMP_DIR = Path(os.getenv('TEMP', Path.home())) / "C6_RPA_EXEC" / SCRIPT_NAME
TEMP_DIR.mkdir(parents=True, exist_ok=True)
USER_DATA_DIR = Path.home() / "AppData" / "Local" / "CELPY" / "chromium_splunk"
DOWNLOADS_DIR = Path.home() / "Downloads"

# Configurações Web
URL_LOGIN = "https://ccs.matera-v2.corp/materaccs/secure/login.jsf"
SPLUNK_URL_SEARCH = "https://siem.corp.c6bank.com/en-US/app/search/search"
SPLUNK_API_BASE = "https://siem.corp.c6bank.com/en-US/splunkd/__raw/services/search/jobs"

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

def get_credencial():
    try:
        import dollynho
        return dollynho.get_credencial("BO OFICIOS")
    except:
        return os.environ.get("MATERA_USER", "dummy"), os.environ.get("MATERA_PASS", "dummy")

USUARIO_MATERA, SENHA_MATERA = get_credencial()

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
            
            # 1. Ler Controles de Arquivos Excel no Diretório de Input
            INPUT_DIR = ROOT_DRIVE / "Mensageria e Cargas Operacionais - 11.CelulaPython/graciliano/automacoes/BO OFICIOS/arquivos_input/encerrar_casos_booficios"
            if not INPUT_DIR.exists(): INPUT_DIR = ROOT_DRIVE / "graciliano/automacoes/BO OFICIOS/arquivos_input/encerrar_casos_booficios"
            
            controles = self.ler_controles_diretorio(INPUT_DIR)
            
            if not controles:
                status = "NO_DATA"
                LOGGER.warning("Nenhum controle encontrado nos excels.")
            else:
                LOGGER.info(f"Total Controles Unicos: {len(controles)}")
                
                # 2. Consultar IDs no Splunk via Controles
                df_ids = self.ler_ids_por_controle_via_splunk(controles)
                
                if df_ids.empty:
                    status = "NO_DATA"
                    LOGGER.warning("Nenhum ID encontrado no Splunk para os controles fornecidos.")
                else:
                    LOGGER.info(f"IDs encontrados no Splunk: {len(df_ids)}")
                    
                    # 3. Coletar Dados Matera
                    df_coleta = self.coletar_matera(df_ids)
                    
                    if df_coleta.empty:
                         status = "NO_DATA"
                         LOGGER.warning("Nenhum dado coletado no Matera.")
                    else:
                         # 4. Enriquecimento
                         df_final = self.enriquecer_final(df_coleta)
                         
                         # Salvar
                         fname = TEMP_DIR / f"resultado_reabertura_{datetime.now().strftime('%H%M%S')}.xlsx"
                         df_final.to_excel(fname, index=False)
                         self.output_files.append(fname)
                         
                         # Opcional: Upload para BigQuery se necessário (mantendo consistencia com manual)
                         # MAS o original não fazia upload para WallB, apenas gerava excel?
                         # O manual faz. O reabrir parece fazer também. Vou fazer upload para tabela de teste ou WallB se requerido.
                         # O original SALVA em POSICAO_DIR.
                         posicao_dir = ROOT_DRIVE / "Catarina Cristina Bernardes De Freitas - Célula Python - Relatórios de Execução/Wall.B/Posição diária/00_ColetaPosicao"
                         posicao_dir.mkdir(parents=True, exist_ok=True)
                         shutil.copy2(fname, posicao_dir / fname.name)
                         LOGGER.info(f"Salvo em: {posicao_dir / fname.name}")
                         
                         status = "SUCCESS"

        except Exception as e:
            status = "ERROR"
            LOGGER.critical(f"Erro fatal: {e}", exc_info=True)
            
        finally:
            end_time = datetime.now(TZ).replace(microsecond=0)
            duration = round((end_time - START_TIME).total_seconds(), 2)
            zip_path = self._create_smart_zip()
            self._upload_metrics(status, usuario_exec, modo_exec, end_time, duration)
            self._send_email(status, zip_path)

    def ler_controles_diretorio(self, diretorio):
        controles = []
        if not diretorio.exists(): return []
        for f in diretorio.glob("*.xls*"):
            try:
                df_dict = pd.read_excel(f, sheet_name=None, dtype=str)
                for k, df in df_dict.items():
                    df.columns = [c.lower().strip() for c in df.columns]
                    if "numero_controle_ccs" in df.columns:
                        vals = df["numero_controle_ccs"].dropna().unique().tolist()
                        controles.extend(vals)
            except Exception as e:
                LOGGER.error(f"Erro arq {f.name}: {e}")
        return list(set(controles))

    def ler_ids_por_controle_via_splunk(self, controles):
        # Splunk Híbrido com Batch
        accum = []
        batches = [controles[i:i + 300] for i in range(0, len(controles), 300)]
        
        with sync_playwright() as p:
            USER_DATA_DIR.mkdir(parents=True, exist_ok=True)
            ctx = p.chromium.launch_persistent_context(user_data_dir=str(USER_DATA_DIR), headless=False, accept_downloads=True)
            page = ctx.new_page()
            
            # Login Splunk
            page.goto(SPLUNK_URL_SEARCH)
            time.sleep(3)
            if "login" in page.url:
                 page.fill("input[name='username']", USUARIO_MATERA)
                 page.fill("input[name='password']", SENHA_MATERA)
                 page.click("input[type=submit]")
                 page.wait_for_url("**/search**")

            cookies = {c['name']: c['value'] for c in ctx.cookies()}
            csrf = page.evaluate("() => document.cookie.match(/splunkweb_csrf_token_8000=([^;]+)/)[1]")
            headers = {"X-Splunk-Form-Key": csrf, "X-Requested-With": "XMLHttpRequest", "User-Agent": "Mozilla/5.0"}
            
            with requests.Session() as s:
                s.cookies.update(cookies)
                
                for batch in batches:
                    vals = ",".join(f"'{c}'" for c in batch)
                    query = f'| dbxquery connection=materadb_prod_adg query="SELECT id_ccs0011 AS ID_EVENTO, num_ctrl_ccs FROM materaccs.cs_evento WHERE num_ctrl_ccs IN ({vals})" maxrows=0'
                    
                    try:
                        resp = s.post(SPLUNK_API_BASE, data={"search": query, "output_mode": "json", "exec_mode": "blocking"}, headers=headers, verify=False, timeout=300)
                        if resp.status_code == 201:
                            sid = resp.json().get('sid')
                            r = s.get(f"{SPLUNK_API_BASE}/{sid}/results?output_mode=csv&count=0", headers=headers, verify=False, timeout=600)
                            batch_df = pd.read_csv(io.BytesIO(r.content), dtype=str)
                            accum.append(batch_df)
                    except Exception as e:
                        LOGGER.error(f"Erro batch splunk: {e}")
                        
            ctx.close()
            
        if not accum: return pd.DataFrame()
        return pd.concat(accum, ignore_index=True)

    def coletar_matera(self, df_ids):
        # Mesma lógica do Manual, reusar se possível ou duplicar simplificada
        registros = []
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            context = browser.new_context()
            page = context.new_page()
            page.goto(URL_LOGIN)
            page.fill('[id="loginForm:login"]', USUARIO_MATERA)
            page.fill('[id="loginForm:senha"]', SENHA_MATERA)
            page.press('[id="loginForm:senha"]', "Enter")
            
            df_ids.columns = [c.upper() for c in df_ids.columns]
            col_id = next((c for c in df_ids.columns if "ID" in c), "ID_EVENTO")
            
            for _, row in df_ids.iterrows():
                try:
                    eid = row[col_id]
                    page.goto(f"https://ccs.matera-v2.corp/materaccs/mensagens/detalhesMsg.jsf?evento={eid}")
                    html = page.content()
                     # Extração simplificada (copiar do manual)
                    def ext(rotulo):
                        try: return html.split(rotulo)[1].split("<td>")[1].split("</td>")[0].strip()
                        except: return ""
                    
                    reg = {
                        "id_evento": eid,
                        "codigo_mensagem": ext("Código Mensagem"),
                        "numero_controle_ccs": ext("Número Controle CCS"),
                         # ... add others
                    }
                    registros.append(reg)
                except: pass
            browser.close()
        return pd.DataFrame(registros)

    def enriquecer_final(self, df):
        # TODO: Implementar enriquecimento igual manual
        return df

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