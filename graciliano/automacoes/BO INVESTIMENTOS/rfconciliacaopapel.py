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

try:
    import bootstrap_deps
    SCRIPT_DEPS = [
        "pandas",
        "pandas-gbq",
        "pywin32",
        "google-cloud-bigquery",
        "pydata-google-auth",
        "openpyxl",
        "numpy"
    ]
    bootstrap_deps.install_requirements(extra_libs=SCRIPT_DEPS)
except: pass

import pandas as pd
import pandas_gbq
import win32com.client as win32
from google.cloud import bigquery
from pydata_google_auth import cache, get_user_credentials
import numpy as np

# ==============================================================================
# CONSTANTES DO SCRIPT
# ==============================================================================
SCRIPT_NAME = Path(__file__).stem.upper()
SCRIPT_NAME_LOWER = SCRIPT_NAME.lower()
START_TIME = datetime.now().replace(microsecond=0)
AREA_NAME = "BO INVESTIMENTOS"

PROJECT_ID = "datalab-pagamentos"
DATASET_ID = "investimentos"
TABLE_CONFIG = f"{PROJECT_ID}.ADMINISTRACAO_CELULA_PYTHON.registro_automacoes"
TABLE_EXEC = f"{PROJECT_ID}.ADMINISTRACAO_CELULA_PYTHON.automacoes_exec"

BQ_DATASET_NEGOCIO = "investimentos"
BQ_TABLE_NAME = "RF_CONCILIACAO_PAPEL_EMISSOR_EXTERNO_ARQUIVO_EP"
BQ_FULL_TABLE = f"{PROJECT_ID}.{BQ_DATASET_NEGOCIO}.{BQ_TABLE_NAME}"

# Diretórios
TEMP_DIR = Path(os.getenv('TEMP', Path.home())) / "C6_RPA_EXEC" / SCRIPT_NAME
TEMP_DIR.mkdir(parents=True, exist_ok=True)

INPUT_DIR = ROOT_DRIVE / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "automacoes" / AREA_NAME / "arquivos input" / "RFCONCILIACAOPAPEL"
if not INPUT_DIR.exists():
    INPUT_DIR = ROOT_DRIVE / "graciliano" / "automacoes" / AREA_NAME / "arquivos input" / "RFCONCILIACAOPAPEL"
INPUT_DIR.mkdir(parents=True, exist_ok=True)

GLOBAL_CONFIG = {'area_name': AREA_NAME, 'emails_principal': [], 'emails_cc': [], 'move_file': False}

# Schema de Negócio
SCHEMA_PAPEL = [
    {"name": "data_de_referencia_do_arquivo", "type": "DATE"}, 
    {"name": "status_atual_da_emissao", "type": "STRING"},
    {"name": "escriturador", "type": "STRING"},
    {"name": "agente_fiduciario", "type": "STRING"},
    {"name": "instrucao_cvm", "type": "STRING"},
    {"name": "rating_da_emissao", "type": "STRING"},
    {"name": "emissora", "type": "STRING"},
    {"name": "volume_total_da_emissao_r__", "type": "FLOAT"},
    {"name": "quantidade", "type": "INTEGER"},
    {"name": "descricao_da_serie", "type": "STRING"},
    {"name": "lei_12431", "type": "STRING"},
    {"name": "artigo_lei_12431", "type": "STRING"},
    {"name": "codigo_isin", "type": "STRING"},
    {"name": "codigo_tipo_if", "type": "STRING"},
    {"name": "codigo_instrumento_financeiro", "type": "STRING"},
    {"name": "data_de_emissao", "type": "DATE"},
    {"name": "indexador", "type": "STRING"},
    {"name": "__do_indexador", "type": "FLOAT"},
    {"name": "taxa_de_juros", "type": "FLOAT"},
    {"name": "forma_de_pagamento", "type": "STRING"},
    {"name": "periodicidade_amortizacao", "type": "STRING"},
    {"name": "tipo_de_amortizacao", "type": "STRING"},
    {"name": "repactuacao", "type": "STRING"},
    {"name": "data_de_repactuacao", "type": "DATE"},
    {"name": "incorpora_juros", "type": "STRING"},
    {"name": "data_de_inicio_do_calculo_de_juros", "type": "DATE"},
    {"name": "data_de_vencimento", "type": "DATE"},
    {"name": "classe", "type": "STRING"},
    {"name": "garantia", "type": "STRING"},
    {"name": "base_da_remuneracao", "type": "STRING"},
    {"name": "securitizadora", "type": "STRING"},
    {"name": "devedor", "type": "STRING"},
    {"name": "inadimplencia", "type": "STRING"},
]

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
        total_linhas = 0

        try:
            LOGGER.info(">>> INICIO <<<")
            LOGGER.info(f"Input: {INPUT_DIR}")
            
            arquivos = sorted([f for f in INPUT_DIR.iterdir() if f.is_file() and not f.name.startswith("~$") and f.name.lower() != "thumbs.db"], key=lambda p: p.stat().st_mtime)
            
            if not arquivos:
                status = "NO_DATA"
                LOGGER.warning("Nenhum arquivo encontrado.")
            else:
                validos = 0
                for arq in arquivos:
                    try:
                        df = self._tratar_dataframe(arq)
                        if df.empty: continue
                        
                        validos += 1
                        ins = self._subir_com_merge(df, BQ_FULL_TABLE)
                        total_linhas += ins
                        
                        self._move_file(arq)
                        
                    except Exception as e:
                        LOGGER.error(f"Erro processando {arq.name}: {e}")
                
                if validos == 0: status = "NO_DATA"
                else: status = "SUCCESS"

        except Exception as e:
            status = "ERROR"
            LOGGER.critical(f"Erro fatal: {e}", exc_info=True)
            
        finally:
            end_time = datetime.now().replace(microsecond=0)
            duration = round((end_time - START_TIME).total_seconds(), 2)
            zip_path = self._create_smart_zip()
            self._upload_metrics(status, usuario_exec, modo_exec, end_time, duration)
            self._send_email(status, zip_path, total_linhas)

    def _normalizar_coluna(self, col: str) -> str:
        s = str(col)
        s = unicodedata.normalize('NFKD', s).encode('ASCII', 'ignore').decode('ASCII')
        s = s.lower()
        s = re.sub(r"[^\w]+", "_", s)
        return re.sub(r"_+", "_", s).strip("_")

    def _tratar_dataframe(self, arq: Path) -> pd.DataFrame:
        try:
            df = pd.read_csv(arq, sep=";", dtype=str, encoding="latin1", on_bad_lines="skip", engine="python", header=0)
            df.columns = [self._normalizar_coluna(c) for c in df.columns]
            
            nomes_schema = [f["name"] for f in SCHEMA_PAPEL]
            df = df.loc[:, df.columns.isin(nomes_schema)].copy()
            
            if df.empty: return pd.DataFrame()

            for campo in SCHEMA_PAPEL:
                name = campo["name"]
                if name not in df: continue
                df[name] = df[name].astype(str).str.strip()
                
                if campo["type"] == "INTEGER":
                    df[name] = pd.to_numeric(df[name].str.replace(r"\D", "", regex=True), errors="coerce").astype("Int64")
                elif campo["type"] == "FLOAT":
                    df[name] = pd.to_numeric(df[name].str.replace(",", "."), errors="coerce")
                elif campo["type"] == "DATE":
                    clean = df[name].str.replace(r"\D", "", regex=True)
                    df[name] = pd.to_datetime(clean, format="%Y%m%d", errors="coerce").dt.date
            
            df = df.loc[:, ~df.columns.duplicated()].dropna(how="all")
            df = df.replace({np.nan: None, pd.NA: None, pd.NaT: None, "": None})
            return df
        except Exception as e:
            LOGGER.error(f"Erro tratar {arq.name}: {e}")
            return pd.DataFrame()

    def _subir_com_merge(self, df, tabela_final):
        if df.empty: return 0
        tabela_staging = f"{BQ_DATASET_NEGOCIO}.{BQ_TABLE_NAME}_STAGING_{int(time.time())}"
        client = bigquery.Client(project=PROJECT_ID)
        
        try:
            pandas_gbq.to_gbq(df, tabela_staging, project_id=PROJECT_ID, if_exists='replace')
            
            cols = list(df.columns)
            on_clause = " AND ".join([f"T.`{c}` IS NOT DISTINCT FROM S.`{c}`" for c in cols])
            insert_cols = ", ".join([f"`{c}`" for c in cols])
            insert_vals = ", ".join([f"S.`{c}`" for c in cols])
            
            query = f"""
            MERGE `{tabela_final}` T
            USING `{PROJECT_ID}.{tabela_staging}` S
            ON {on_clause}
            WHEN NOT MATCHED THEN
              INSERT ({insert_cols}) VALUES ({insert_vals})
            """
            job = client.query(query)
            job.result()
            ins = job.num_dml_affected_rows or 0
            
            client.delete_table(f"{PROJECT_ID}.{tabela_staging}", not_found_ok=True)
            return ins
        except Exception as e:
            client.delete_table(f"{PROJECT_ID}.{tabela_staging}", not_found_ok=True)
            raise e

    def _move_file(self, path):
        if GLOBAL_CONFIG['move_file']:
            try:
                dest = ROOT_DRIVE / "Mensageria e Cargas Operacionais - 11.CelulaPython/graciliano/automacoes" / AREA_NAME / "logs" / SCRIPT_NAME / datetime.now().strftime('%Y-%m-%d')
                dest.mkdir(parents=True, exist_ok=True)
                
                dest_file = dest / f"{path.stem}_{datetime.now().strftime('%H%M%S')}{path.suffix}"
                shutil.move(str(path), str(dest_file))
                self.output_files.append(dest_file)
            except: pass

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

    def _send_email(self, status, zip_path, linhas):
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
            mail.Body = f"Status: {status}\nLinhas Inseridas: {linhas}"
            if zip_path.exists(): mail.Attachments.Add(str(zip_path))
            mail.Send()
        except: pass

if __name__ == "__main__":
    AutomationTask().run()