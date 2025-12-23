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
        "openpyxl"
    ]
    bootstrap_deps.install_requirements(extra_libs=SCRIPT_DEPS)
except: pass

import logging
import zipfile
import shutil
import pythoncom
import pandas as pd
import pandas_gbq
from win32com.client import Dispatch
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import bigquery
from pydata_google_auth import cache, get_user_credentials

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

BQ_TABELA_ORIGEM = "investimentos.RF_CONCILIACAO_EMISSAO"
BQ_TABELA_DESTINO = f"{PROJECT_ID}.{DATASET_ID}.RF_CONCILIACAO_EMISSAO_EXPORT"
LIMITE_LINHAS_MAX = 900000

# Diretórios
TEMP_DIR = Path(os.getenv('TEMP', Path.home())) / "C6_RPA_EXEC" / SCRIPT_NAME
TEMP_DIR.mkdir(parents=True, exist_ok=True)

GLOBAL_CONFIG = {'area_name': 'BO INVESTIMENTOS', 'emails_principal': [], 'emails_cc': [], 'move_file': False}

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
        qtd_linhas = 0
        observacao = None
        
        try:
            LOGGER.info(">>> INICIO <<<")
            
            client = bigquery.Client(project=PROJECT_ID, credentials=CREDENTIALS)
            
            # 1. Executar ETL
            qtd_linhas = self._executar_etl(client)
            
            if qtd_linhas == 0:
                status = "NO_DATA"
            elif qtd_linhas > LIMITE_LINHAS_MAX:
                status = "SUCCESS" # Tecnicamente sucesso, mas com aviso
                observacao = f"LIMITE EXCEDIDO: {qtd_linhas} > {LIMITE_LINHAS_MAX}"
                self._send_limit_alert(qtd_linhas)
            else:
                # 2. Gerar Excel
                self._gerar_excel_threads()
                status = "SUCCESS"

        except Exception as e:
            status = "ERROR"
            LOGGER.critical(f"Erro fatal: {e}", exc_info=True)
            
        finally:
            end_time = datetime.now().replace(microsecond=0)
            duration = round((end_time - START_TIME).total_seconds(), 2)
            
            zip_path = self._create_smart_zip()
            self._upload_metrics(status, usuario, modo_exec, end_time, duration)
            if observacao: 
                 # Já enviou alerta especifico
                 pass
            else:
                 self._send_email(status, zip_path, qtd_linhas)
            
            if GLOBAL_CONFIG['move_file'] and status == "SUCCESS":
                 self._move_files_to_network(zip_path)

    def _executar_etl(self, client):
        LOGGER.info("Executando CREATE TABLE no BigQuery...")
        client.create_dataset(f"{PROJECT_ID}.{DATASET_ID}", exists_ok=True)
        
        query_etl = f"""
        CREATE OR REPLACE TABLE `{BQ_TABELA_DESTINO}` AS
        WITH base AS (
            SELECT *
            FROM `{PROJECT_ID}.{BQ_TABELA_ORIGEM}`
            WHERE situacao IN ('Sem posição matera','Sem posição cetip')
               OR REGEXP_CONTAINS(lower(situacao), r'verificar')
        ),
        ult AS (SELECT MAX(data_arquivo) AS d FROM base)
        SELECT
            id_papel,
            cod_tit,
            ativo AS codigo_ativo,
            cetip.dt_vcto AS dt_vcto_cetip,
            cetip.dt_emissao AS dt_emissao_cetip,
            cetip.indexador AS indexador_cetip,
            cetip.taxa AS taxa_cetip,
            cetip.vlr_total AS accrual_cetip,
            cetip.quantidade_depositada AS qtd_cetip,
            matera.dt_vcto AS dt_vcto_matera,
            matera.dt_emissao AS dt_emissao_matera,
            matera.indexador AS indexador_matera,
            matera.taxa AS taxa_matera,
            matera.vlr_total AS accrual_matera,
            matera.qtd AS qtd_matera,
            data_arquivo AS dt_arquivo,
            situacao
        FROM base b
        JOIN ult u ON b.data_arquivo = u.d
        """
        client.query(query_etl).result()
        
        df_count = pandas_gbq.read_gbq(f"SELECT COUNT(*) as qtd FROM `{BQ_TABELA_DESTINO}`", project_id=PROJECT_ID)
        qtd = int(df_count.iloc[0]['qtd'])
        LOGGER.info(f"Linhas geradas: {qtd}")
        return qtd

    def _gerar_excel_threads(self):
        LOGGER.info("Baixando dados para DataFrame...")
        df = pandas_gbq.read_gbq(f"SELECT * FROM `{BQ_TABELA_DESTINO}`", project_id=PROJECT_ID)
        df = df.astype(str).replace(["nan", "None", "NaT", "<NA>"], "")
        
        max_rows = 1000000 
        parts = (len(df) // max_rows) + 1
        
        def _save(sub_df, path):
            sub_df.to_excel(path, index=False)
            return path

        with ThreadPoolExecutor(max_workers=min(4, parts + 1)) as executor:
            futures = []
            for i in range(parts):
                start = i * max_rows
                end = start + max_rows
                df_slice = df.iloc[start:end]
                if df_slice.empty: continue
                
                fname = f"ativos_renda_fixa_{datetime.now().strftime('%Y%m%d_%H%M%S')}_parte{i+1}.xlsx"
                fpath = TEMP_DIR / fname
                futures.append(executor.submit(_save, df_slice, fpath))
                
            for f in as_completed(futures):
                self.output_files.append(f.result())

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
            mail.HTMLBody = f"<p>Status: {status}</p><p>Linhas: {total}</p>"
            if zip_path.exists(): mail.Attachments.Add(str(zip_path))
            # Anexar excel solto se pequeno
            for f in self.output_files:
                if f.exists() and f.stat().st_size < 5*1024*1024:
                     mail.Attachments.Add(str(f))
            mail.Send()
        except: pass

    def _send_limit_alert(self, total):
        try:
            to = GLOBAL_CONFIG['emails_principal'] + GLOBAL_CONFIG['emails_cc']
            if not to: return
            pythoncom.CoInitialize()
            outlook = Dispatch("Outlook.Application")
            mail = outlook.CreateItem(0)
            mail.To = ";".join(set(to))
            mail.Subject = f"ATENÇÃO - {SCRIPT_NAME.upper()} - LIMITE EXCEDIDO"
            mail.HTMLBody = f"<p style='color:red'>Limite de linhas excedido: {total}</p><p>Favor verificar base.</p>"
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
