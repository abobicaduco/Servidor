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
import functools
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from zoneinfo import ZoneInfo
from typing import List, Tuple, Optional

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
from playwright.sync_api import sync_playwright, Page, Browser, BrowserContext, expect, TimeoutError as PWTimeoutError

try:
    from PySide6.QtWidgets import QApplication, QWidget, QVBoxLayout, QLabel, QComboBox, QLineEdit, QPushButton
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
TABELA_TESTE_ALL = "datalab-pagamentos.CELULA_PYTHON_TESTES.materaALLcases"
TABELA_TESTE_OPEN = "datalab-pagamentos.CELULA_PYTHON_TESTES.materaOpenCASES"

TEMP_DIR = Path(os.getenv('TEMP', Path.home())) / "C6_RPA_EXEC" / SCRIPT_NAME
TEMP_DIR.mkdir(parents=True, exist_ok=True)

USER_DATA_DIR = Path.home() / "AppData" / "Local" / "CELPY" / "chromium_splunk"
DOWNLOADS_DIR = Path.home() / "Downloads"

# Configurações Web
URL_LOGIN = "https://ccs.matera-v2.corp/materaccs/secure/login.jsf"
SPLUNK_URL_SEARCH = "https://siem.corp.c6bank.com/en-US/app/search/search"
SPLUNK_API_BASE = "https://siem.corp.c6bank.com/en-US/splunkd/__raw/services/search/jobs"

DATAFRAME_DOIDO = [
    "status_caso", "id_evento", "codigo_mensagem", "numero_controle_ccs", "cnpj_entidade",
    "cnpj_participante", "tipo_pessoa", "cnpj_cpf_cliente", "data_inicio_oficio", "data_fim_oficio",
    "codigo_sistema_envio", "sigla_orgao", "numero_controle_autorizacao", "numero_controle_envio",
    "numero_processo_judicial", "codigo_tribunal", "nome_tribunal", "codigo_vara", "nome_vara",
    "nome_juiz", "descricao_cargo_juiz", "ordem_oficio", "data_limite", "data_bacen",
    "data_movimento_oficio", "status_movimentacao", "ccs0012", "numero_conta",
    "possui_relacionamento", "caso_outros", "dt_coleta",
]

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
# CLASSES AUXILIARES
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
            
            # 1. Coleta Híbrida Splunk
            df_ids = self.obter_dataframe_splunk_hibrido(coleta_ano="2025") # Param can be dynamic
            
            if df_ids.empty:
                status = "NO_DATA"
                LOGGER.warning("Splunk vazio.")
            else:
                # Padronizar
                df_ids.columns = [str(c).upper().strip() for c in df_ids.columns]
                
                # Upload Raw
                try:
                    pandas_gbq.to_gbq(df_ids.astype(str), TABELA_TESTE_ALL, project_id=PROJECT_ID, if_exists="replace")
                except Exception as e: LOGGER.error(f"Erro upload raw: {e}")

                # Filtros
                if "DESCRICAO" in df_ids.columns:
                    df_ids = df_ids[df_ids["DESCRICAO"] != "Finalizada (Mensagem CCS0012 respondida)"]
                
                # Normalize ID Evento
                if 'ID_CCS0011' in df_ids.columns: df_ids.rename(columns={'ID_CCS0011': 'ID_EVENTO'}, inplace=True)
                elif 'ID_EVENTO' not in df_ids.columns:
                    for c in df_ids.columns: 
                        if 'ID_EVENTO' in c: df_ids.rename(columns={c: 'ID_EVENTO'}, inplace=True); break
                
                if 'ID_EVENTO' not in df_ids.columns:
                    raise ValueError("Coluna ID_EVENTO nao encontrada")
                
                LOGGER.info(f"IDs Matera: {len(df_ids)}")
                
                # 2. Coleta Matera
                df_coleta = self.coletar_matera(df_ids)
                
                if df_coleta.empty:
                    status = "NO_DATA"
                else:
                    # Enriquecimento
                    df_coleta = self.enriquecer_dados(df_coleta)

                    # Upload Final
                    try:
                        pandas_gbq.to_gbq(df_coleta.astype(str), TABELA_TESTE_OPEN, project_id=PROJECT_ID, if_exists="replace")
                    except Exception as e: LOGGER.error(f"Erro upload open: {e}")
                    
                    # Salva Excel e Upload WallB
                    self.salvar_e_subir_wallb(df_coleta)
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

    def obter_dataframe_splunk_hibrido(self, coleta_ano="2025"):
        # Lógica simplificada de coleta híbrida (Requests + Playwright Auth)
        # Importante: Requer certificado SSL override
        filtro_ano = f"WHERE TO_CHAR(ce.dt_hr_registro,'YYYY') = '{coleta_ano}'" if coleta_ano else ""
        sql = f"""
        WITH DADOS AS (
            SELECT req.id_req_movto, req.id_ccs0011, req.ind_processamento_manual, req.id_accs100, req.id_ccs0012, 
                   req.id_situacao_req_movto, req.id_cod_ret_sist_info, req.observacao, req.apenas_extrato_movimentacao,
                   req.cod_sist_envio, req.data_movimento_ccs0011, ce.dt_hr_registro, ce.num_ctrl_ccs, csr.descricao,
                   ROW_NUMBER() OVER (PARTITION BY ce.num_ctrl_ccs ORDER BY ce.dt_hr_registro DESC) AS RN
            FROM materaccs.cs_requisicao_movimentacao req
            JOIN materaccs.cs_evento ce ON ce.id_evento = req.id_ccs0011
            JOIN materaccs.cs_situacao_req_movto csr ON csr.id_situacao = req.id_situacao_req_movto
            LEFT JOIN materaccs.cs_accs100 acc ON acc.id_accs100 = req.id_accs100
            {filtro_ano}
        )
        SELECT * FROM DADOS WHERE RN = 1
        """
        query = f'| dbxquery connection=materadb_prod_adg query="{sql.strip()}" maxrows=0'
        
        with sync_playwright() as p:
            USER_DATA_DIR.mkdir(parents=True, exist_ok=True)
            ctx = p.chromium.launch_persistent_context(user_data_dir=str(USER_DATA_DIR), headless=False, accept_downloads=True, viewport={"width":1366,"height":768})
            page = ctx.new_page()
            
            try:
                page.goto(SPLUNK_URL_SEARCH, wait_until="domcontentloaded")
                time.sleep(5) # Auth check
                
                # Check Login
                if "login" in page.url:
                    page.fill("input[name='username']", USUARIO_MATERA)
                    page.fill("input[name='password']", SENHA_MATERA)
                    page.keyboard.press("Enter")
                    page.wait_for_url(lambda u: "login" not in u, timeout=15000)
                
                cookies = {c['name']: c['value'] for c in ctx.cookies()}
                csrf = page.evaluate("() => document.cookie.match(/splunkweb_csrf_token_8000=([^;]+)/)[1]")
                
                headers = {"X-Splunk-Form-Key": csrf, "X-Requested-With": "XMLHttpRequest", "User-Agent": "Mozilla/5.0"}
                
                with requests.Session() as s:
                    s.cookies.update(cookies)
                    payload = {"search": query, "output_mode": "json", "exec_mode": "blocking", "count": 0}
                    resp = s.post(SPLUNK_API_BASE, data=payload, headers=headers, verify=False, timeout=300)
                    if resp.status_code != 201: return pd.DataFrame()
                    
                    sid = resp.json().get('sid')
                    url_res = f"{SPLUNK_API_BASE}/{sid}/results?output_mode=csv&count=0"
                    res = s.get(url_res, headers=headers, verify=False, timeout=600)
                    
                    return pd.read_csv(io.BytesIO(res.content), dtype=str)
                    
            except Exception as e:
                LOGGER.error(f"Erro Splunk: {e}")
                return pd.DataFrame()
            finally:
                ctx.close()

    def coletar_matera(self, df_ids):
        registros = []
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            context = browser.new_context(viewport={"width": 1920, "height": 1080})
            page = context.new_page()
            
            try:
                page.goto(URL_LOGIN)
                page.fill('[id="loginForm:login"]', USUARIO_MATERA)
                page.fill('[id="loginForm:senha"]', SENHA_MATERA)
                page.press('[id="loginForm:senha"]', "Enter")
                page.wait_for_url("**/secure/**")
                
                df_ids["link"] = df_ids["ID_EVENTO"].apply(lambda x: f"https://ccs.matera-v2.corp/materaccs/mensagens/detalhesMsg.jsf?evento={x}")
                
                for _, row in df_ids.iterrows():
                    try:
                        page.goto(row["link"])
                        html = page.content()
                        
                        def ext(rotulo):
                            try: return html.split(rotulo)[1].split("<td>")[1].split("</td>")[0].strip()
                            except: return ""
                            
                        reg = {
                            "status_caso": "ABERTO",
                            "id_evento": row["ID_EVENTO"],
                            "codigo_mensagem": ext("Código Mensagem"),
                            "numero_controle_ccs": ext("Número Controle CCS"),
                            "cnpj_cpf_cliente": ext("CNPJ ou CPF Pessoa").replace(".","").replace("-","").replace("/","").strip(),
                            "data_inicio_oficio": ext("Data Início"),
                            "data_fim_oficio": ext("Data Fim"),
                            "dt_coleta": date.today()
                        }
                        
                        # Caso Outros Check
                        page.goto("https://ccs.matera-v2.corp/materaccs/cadastro/consultaCadastro.jsf")
                        page.fill('xpath=//*[@id="filtroForm:cpfCliente"]', reg["cnpj_cpf_cliente"])
                        page.click('xpath=//*[@id="filtroForm:consultar"]')
                        reg["caso_outros"] = "NAO" if "Titular" in page.content() else "SIM"
                        
                        registros.append(reg)
                    except: pass
                    
            except Exception as e:
                LOGGER.error(f"Erro Matera: {e}")
            finally:
                browser.close()
        
        return pd.DataFrame(registros)

    def enriquecer_dados(self, df):
        if df.empty: return df
        docs = ["'" + str(x) + "'" for x in df["cnpj_cpf_cliente"].unique() if x]
        if not docs: 
            df["numero_conta"] = "0"
            return df
            
        sql = f"SELECT CAST(REGISTER_NUM AS STRING) as doc, CAST(ACCOUNT_NUM AS STRING) as conta FROM `c6-backoffice-prod.conta_corrente.ACCOUNT_REGISTER` WHERE REGISTER_NUM IN ({','.join(docs)})"
        try:
            df_contas = pandas_gbq.read_gbq(sql, project_id=PROJECT_ID)
            mapa = dict(zip(df_contas["doc"], df_contas["conta"]))
            df["numero_conta"] = df["cnpj_cpf_cliente"].apply(lambda x: mapa.get(x, "0"))
            df["possui_relacionamento"] = df["numero_conta"].apply(lambda x: "SIM" if x!="0" else "NAO")
        except:
             df["numero_conta"] = "0"
        return df

    def salvar_e_subir_wallb(self, df):
        try:
            # Fill missing columns
            for c in DATAFRAME_DOIDO:
                if c not in df.columns: df[c] = ""
            
            df_final = df[DATAFRAME_DOIDO].astype(str)
            
            # Save Excel
            dest = ROOT_DRIVE / "Catarina Cristina Bernardes De Freitas - Célula Python - Relatórios de Execução/Wall.B/Posição diária/00_ColetaPosicao"
            dest.mkdir(parents=True, exist_ok=True)
            fname = dest / f"incremento_conta_{datetime.now().strftime('%d.%m.%Y_%H.%M.%S')}.xlsx"
            df_final.to_excel(fname, index=False)
            self.output_files.append(fname)
            
            # Upload
            pandas_gbq.to_gbq(df_final, WALLB_CASOS, project_id=PROJECT_ID, if_exists='replace')
        except Exception as e:
            LOGGER.error(f"Erro salvar/subir: {e}")

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
