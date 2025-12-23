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
        "openpyxl"
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
SCRIPT_NAME = Path(__file__).stem.lower()
START_TIME = datetime.now().replace(microsecond=0)
AREA_NAME = "BO INVESTIMENTOS"

PROJECT_ID = "datalab-pagamentos"
DATASET_ID = "investimentos"
TABLE_CONFIG = f"{PROJECT_ID}.ADMINISTRACAO_CELULA_PYTHON.registro_automacoes"
TABLE_EXEC = f"{PROJECT_ID}.ADMINISTRACAO_CELULA_PYTHON.automacoes_exec"

BQ_TABLE_SOURCE = "datalab-pagamentos.ADMINISTRACAO_CELULA_PYTHON.TBL_CARTEIRA_INADIMPLENTE"

TEMP_DIR = Path(os.getenv('TEMP', Path.home())) / "C6_RPA_EXEC" / SCRIPT_NAME
TEMP_DIR.mkdir(parents=True, exist_ok=True)

# Colunas na ordem exata solicitada
COLUNAS_BQ = [
    "DATA_REF", "PRODUTO", "DESC_PRODUTO", "TP_PESSOA", "CPF_CNPJ", "CONTRATO",
    "DT_VENCIMENTO", "ATRASO", "VALOR", "SALDO_DEVEDOR", "FL_FPD", "SALDO_EXPOSTO",
    "TELEFONE_1", "TELEFONE_2", "TELEFONE_3", "TELEFONE_4", "TELEFONE_5",
    "DS_GR", "DS_BANCO_SUBSEGMENTO", "UF", "CIDADE", "ACTION_LABEL", "NOME"
]

NOME_ARQUIVO_REF = "BASE TRANSFERIDOS - SAIDA 2025.xlsx"
PATH_POSSIVEIS_REF = [
    ROOT_DRIVE / "BO INVESTIMENTOS - Portabilidade Saída" / NOME_ARQUIVO_REF,
    ROOT_DRIVE / "BO INVESTIMENTOS - Portabilidade Saida" / NOME_ARQUIVO_REF
]
PASTA_DESTINO_FINAL = ROOT_DRIVE / "BO INVESTIMENTOS - Clientes inadimplestes"

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
        
        try:
            LOGGER.info(">>> INICIO <<<")
            
            # 1. Extração BigQuery
            df_bq = self._extrair_dados()
            
            if df_bq.empty:
                status = "NO_DATA"
                LOGGER.warning("Nenhum dado retornado do BigQuery.")
            else:
                # 2. Filtragem Excel
                df_final = self._filtrar_excel(df_bq)
                
                if df_final is not None and not df_final.empty:
                    status = "SUCCESS"
                    
                    fname = f"RELATORIO_FINAL_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                    path_loc = TEMP_DIR / fname
                    df_final.to_excel(path_loc, index=False)
                    self.output_files.append(path_loc)
                    
                    PASTA_DESTINO_FINAL.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(path_loc, PASTA_DESTINO_FINAL / fname)
                    LOGGER.info(f"Arquivo salvo em: {PASTA_DESTINO_FINAL / fname}")
                else:
                    status = "SUCCESS" # Sucesso mesmo sem match
                    LOGGER.info("Nenhum match encontrado com o arquivo de referência.")

        except Exception as e:
            status = "ERROR"
            LOGGER.critical(f"Erro fatal: {e}", exc_info=True)
            
        finally:
            end_time = datetime.now().replace(microsecond=0)
            duration = round((end_time - START_TIME).total_seconds(), 2)
            zip_path = self._create_smart_zip()
            self._upload_metrics(status, usuario_exec, modo_exec, end_time, duration)
            self._send_email(status, zip_path)

    def _extrair_dados(self):
        cols_str = ", ".join(COLUNAS_BQ)
        query = f"SELECT {cols_str} FROM `{BQ_TABLE_SOURCE}` ORDER BY DT_VENCIMENTO DESC, DATA_REF DESC"
        return pandas_gbq.read_gbq(query, project_id=PROJECT_ID)

    def _filtrar_excel(self, df_bq):
        path_ref = None
        for p in PATH_POSSIVEIS_REF:
            if p.exists(): path_ref = p; break
        if not path_ref: 
            for f in ROOT_DRIVE.rglob(NOME_ARQUIVO_REF): path_ref = f; break
        
        if not path_ref:
            LOGGER.warning(f"Arquivo referência {NOME_ARQUIVO_REF} não encontrado.")
            return None

        df_ref = pd.read_excel(path_ref, dtype=str)
        col_cpf_ref = next((c for c in df_ref.columns if 'cpf' in c.lower() or 'doc' in c.lower()), None)
        if not col_cpf_ref: return None

        bq_keys = df_bq['CPF_CNPJ'].astype(str).str.replace(r'\D', '', regex=True).str.lstrip('0')
        ref_keys = df_ref[col_cpf_ref].astype(str).str.replace(r'\D', '', regex=True).str.lstrip('0')
        
        mask = bq_keys.isin(ref_keys)
        return df_bq[mask].copy()[COLUNAS_BQ]

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
