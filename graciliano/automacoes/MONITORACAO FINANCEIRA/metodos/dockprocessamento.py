# ==============================================================================
# AUTO-INSTALL DEPENDENCIES & CONFIG
# ==============================================================================
import sys
import os
from pathlib import Path
from datetime import datetime, date, timedelta

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
        "openpyxl",
        "playwright",
        "unidecode"
    ]
    
    bootstrap_deps.install_requirements(extra_libs=SCRIPT_DEPS)
    from config_loader import Config
    
    # Configuração via Loader (Dinâmica DEV/PROD)
    ROOT_DIR = Config.ROOT_DIR
    PROJECT_ID = Config.PROJECT_ID
    DATASET_ID = Config.DATASET_ID

except ImportError:
    print("⚠️ Config Loader não encontrado. Usando FALLBACK local.")
    
    # Fallback Hardcoded (Padrão C6 Bank - Assume DEV)
    CONFIG_LOADER = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "novo_servidor"
    PROJECT_ID = 'datalab-pagamentos'
    DATASET_ID = 'ADMINISTRACAO_CELULA_PYTHON'

# ==============================================================================
# CONSTANTES DO SCRIPT
# ==============================================================================
SCRIPT_NAME = Path(__file__).stem.lower()
START_TIME = datetime.now().replace(microsecond=0)

# Controle de Headless
HEADLESS = True

# Diretórios
TEMP_DIR = Path(os.getenv('TEMP', Path.home())) / "C6_RPA_EXEC" / SCRIPT_NAME
TEMP_DIR.mkdir(parents=True, exist_ok=True)

GLOBAL_CONFIG = {'area_name': 'MONITORACAO FINANCEIRA', 'emails_principal': [], 'emails_cc': [], 'move_file': False}

import logging
import shutil
import zipfile
import pythoncom
import traceback
import re
import unicodedata
import pandas as pd
import pandas_gbq
from win32com.client import Dispatch
from google.cloud import bigquery
from pydata_google_auth import cache, get_user_credentials
from playwright.sync_api import sync_playwright
from unidecode import unidecode

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
        self.DOCK_USER = "RAPHAELA.CASTELLO"
        self.DOCK_PASS = "ep!&@8M8v4"
        self.TABELA_FQN = "datalab-pagamentos.ADMINISTRACAO_CELULA_PYTHON.doki_processamento"
        
        self.SAFE_SCHEMA = [
            ("cpf", "CPF"),
            ("cartao", "Cartão"),
            ("id_cartao", "Id Cartao"),
            ("tipo_cartao", "Tipo Cartao"),
            ("conta", "Conta"),
            ("status_cartao", "Status Cartão"),
            ("bandeira", "Bandeira"),
            ("modalidade", "Modalidade"),
            ("data_transacao", "Data Transação"),
            ("aging_compras", "Aging Compras"),
            ("valor_compra", "Valor Compra"),
            ("valor_contrato", "Valor Contrato"),
            ("valor_us_conciliacao", "Valor U$ Conciliação"),
            ("flag_transacao_com_cvv2", "Flag Transacao Com CVV2"),
            ("tipo_transacao", "Tipo Transação"),
            ("total_parcelas", "Total Parcelas"),
            ("transaction_uuid_codigo_odin", "Transaction UUID/Código Odin"),
            ("transaction_link_id", "Transaction Link ID"),
            ("codigo_autorizacao", "Código Autorização"),
            ("codigo_evento_compra", "Código Evento Compra"),
            ("codigo_contestacao", "Código Contestação"),
            ("internacional", "Internacional"),
            ("cod_modo_entrada", "Cod. Modo Entrada"),
            ("flag_transacao_senha", "Flag Transacao Senha"),
            ("historico", "Historico"),
            ("descricao_modo_entrada", "Descrição Modo Entrada"),
            ("nome_estabelecimento", "Nome Estabelecimento"),
            ("mcc", "MCC"),
            ("data_contestacao", "Data Contestação"),
            ("aging_contestacao", "Aging Contestacao"),
            ("responsavel_abertura", "Responsavel  Abertura"),
            ("status_contestacao", "Status Contestação"),
            ("data_alteracao", "DataAlteração"),
            ("razao_chargeback", "Razão Chargeback"),
            ("descricao_razao", "Descrição Razão"),
            ("mensagem_texto", "Mensagem Texto"),
            ("data_envio_cb", "Data Envio CB"),
            ("responsavel_alteracao", "Responsavel Alteracao"),
            ("report_bandeira", "Report  Bandeira"),
            ("reference_number", "Reference Number"),
            ("transacao_segura", "Transação Segura"),
            ("data_reap", "Data Reap"),
            ("motivo_reap", "Motivo Reap"),
            ("texto_reap", "Texto Reap"),
            ("tipo_contestacao", "Tipo Contestação"),
            ("requestor_id", "Requestor ID"),
        ]
        self.SAFE_ORDER = [s for s, _ in self.SAFE_SCHEMA]

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
            
            hoje = date.today()
            if hoje.weekday() == 0: 
                datas = [hoje - timedelta(days=3), hoje - timedelta(days=2), hoje - timedelta(days=1)]
            else:
                datas = [hoje - timedelta(days=1)]
                
            total_linhas = 0
            
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=HEADLESS, args=["--start-maximized", "--ignore-certificate-errors"])
                context = browser.new_context(viewport={"width": 1920, "height": 1080}, accept_downloads=True, ignore_https_errors=True)
                page = context.new_page()
                
                for d in datas:
                    arq = self._baixar_relatorio(page, d)
                    if arq:
                        self.output_files.append(arq)
                        df = self._tratar_dataframe(arq)
                        if not df.empty:
                            cnt = self._subir_bq(df)
                            total_linhas += cnt
                            LOGGER.info(f"Subido {d}: {cnt} linhas")
                        else:
                            LOGGER.warning(f"DF vazio {d}")
                    else:
                        LOGGER.warning(f"Falha download {d}")
                        
                browser.close()
            
            if not self.output_files:
                status = "ERRO"
            elif total_linhas == 0:
                status = "SEM DADOS"
            else:
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

    def _baixar_relatorio(self, page, data: date):
        try:
            url_auth = f"https://{self.DOCK_USER}:{self.DOCK_PASS}@10.19.254.121/reports/report/C6Bank/Interc%C3%A2mbio/Movimenta%C3%A7%C3%A3o%20de%20Interc%C3%A2mbio"
            LOGGER.info(f"Navegando: {data}")
            page.goto(url_auth, timeout=60000)
            
            frame = page.frame_locator('iframe[src*="ReportViewer.aspx"]')
            d_str = data.strftime("%d/%m/%Y")
            frame.locator('[id$="ctl07_txtValue"]').fill(d_str)
            frame.locator('[id$="ctl09_txtValue"]').fill(d_str)
            
            frame.locator('[id$="ctl04_ctl00"]').click()
            frame.locator('[id$="ctl05_ctl04_ctl00_ButtonLink"]').wait_for(state='visible', timeout=60000)
            
            with page.expect_download(timeout=120000) as dl_info:
                frame.locator('[id$="ctl05_ctl04_ctl00_ButtonLink"]').click()
                frame.locator('a:text("Excel")').click()
                
            dl = dl_info.value
            dest = TEMP_DIR / f"PROCESSAMENTO_{data.strftime('%Y.%m.%d')}.xlsx"
            dl.save_as(str(dest))
            return dest if dest.exists() else None
        except Exception as e:
            LOGGER.error(f"Erro download {data}: {e}")
            return None

    def _normalize_col(self, s: str) -> str:
        if not s: return ""
        s = unidecode(str(s))
        return re.sub(r"\s+", " ", s).strip().lower()

    def _tratar_dataframe(self, caminho: Path) -> pd.DataFrame:
        try:
            df_raw = pd.read_excel(caminho, header=None, dtype=str, engine="openpyxl")
            hdr = 0
            for i in range(min(50, len(df_raw))):
                row = [self._normalize_col(x) for x in df_raw.iloc[i].astype(str).tolist()]
                if "cpf" in row and "cartao" in row:
                    hdr = i; break
            
            df = pd.read_excel(caminho, header=hdr, dtype=str, engine="openpyxl")
            cols = {c: self._normalize_col(c) for c in df.columns}
            df = df.rename(columns=cols)
            
            safe_map = { self._normalize_col(orig): dest for dest, orig in self.SAFE_SCHEMA }
            df = df.rename(columns=safe_map)
            
            for c in self.SAFE_ORDER:
                if c not in df.columns: df[c] = None
            return df[self.SAFE_ORDER]
        except: return pd.DataFrame()

    def _subir_bq(self, df: pd.DataFrame) -> int:
        pandas_gbq.to_gbq(df, self.TABELA_FQN, project_id=PROJECT_ID, if_exists="append")
        client = bigquery.Client(project=PROJECT_ID, credentials=CREDENTIALS)
        query = f"CREATE OR REPLACE TABLE `{self.TABELA_FQN}` AS SELECT DISTINCT * FROM `{self.TABELA_FQN}`"
        client.query(query).result()
        return len(df)

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