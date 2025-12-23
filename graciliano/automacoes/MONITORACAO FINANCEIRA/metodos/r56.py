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
        "openpyxl",
        "unidecode"
    ]
    bootstrap_deps.install_requirements(extra_libs=SCRIPT_DEPS)
except: pass

import logging
import re
import shutil
import zipfile
import pythoncom
import traceback
import unicodedata
import pandas as pd
import pandas_gbq
from unidecode import unidecode
from win32com.client import Dispatch
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
        self.DATASET_NEGOCIO = "conciliacoes_monitoracao"
        self.TABLE_ID = f"{PROJECT_ID}.{self.DATASET_NEGOCIO}.ARQUIVO_REPASSE_FGTS_R56_ATUALIZADO"
        self.SCHEMA_COLS = [
            "cpf", "data_do_pedido", "identificador_da_solicitacao", "tipo_da_operacao",
            "canal_de_solicitacao", "status_do_periodo", "data_prevista_repasse",
            "valor_cedido_alienado_original", "valor_cedido_alienado_atualizado",
            "numero_do_protocolo", "status_do_protocolo", "data_efetiva_de_pagamento",
            "valor_repassado", "nome_arquivo"
        ]
        
        self.PASTA_R56 = Path.home() / "Meu Drive" / "C6 CTVM" / "BKO FINANCEIRO - R56/DOIDERA_R56"
        if not self.PASTA_R56.exists():
            self.PASTA_R56 = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "BKO FINANCEIRO - R56/DOIDERA_R56"
            
        self.DIR_TRABALHO = self.PASTA_R56 / "HAVOC_novo"
        self.DIR_SAIDA = self.PASTA_R56 / "csv_final"

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
        total_linhas = 0
        
        try:
            LOGGER.info(">>> INICIO <<<")
            
            self.DIR_TRABALHO.mkdir(parents=True, exist_ok=True)
            self.DIR_SAIDA.mkdir(parents=True, exist_ok=True)
            
            # 2. Process Raw CSV chunks
            processed_files = self._process_raw_chunks()
            if not processed_files:
                # Check directly in output
                processed_files = list(self.DIR_SAIDA.glob("*.csv"))
            
            if not processed_files:
                status = "NO_DATA"
                LOGGER.info("Sem arquivos.")
            else:
                df_accum = pd.DataFrame()
                for arq in processed_files:
                    df = self._tratar_dataframe(arq)
                    if not df.empty:
                        df_accum = pd.concat([df_accum, df], ignore_index=True)
                    
                    try: arq.unlink()
                    except: pass
                
                if not df_accum.empty:
                     pandas_gbq.to_gbq(df_accum, self.TABLE_ID, project_id=PROJECT_ID, if_exists="append")
                     total_linhas = len(df_accum)
                     self._deduplicate_bq()
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
            self._send_email(status, zip_path, total_linhas)
            
            if GLOBAL_CONFIG['move_file'] and status == "SUCCESS":
                 self._move_files_to_network(zip_path)

    def _find_header_row(self, caminho: Path) -> int:
        try:
            with open(caminho, encoding="utf-8", errors="ignore") as f:
                for i, linha in enumerate(f):
                    if linha.startswith("CPF;"): return i
        except: pass
        return -1

    def _process_raw_chunks(self) -> list:
        outputs = []
        inputs = list(self.DIR_TRABALHO.glob("*"))
        if not inputs: return []
        
        for f in inputs:
            try:
                cab = self._find_header_row(f)
                if cab < 0: 
                    try: f.unlink()
                    except: pass
                    continue
                
                # Chunk read
                saved = False
                for enc in ("utf-8", "latin1", "windows-1252"):
                    try:
                         for chunk in pd.read_csv(f, sep=";", header=None, skiprows=cab, dtype=str, encoding=enc, chunksize=50000, on_bad_lines="skip"):
                             # Just first chunk for header + data? No, original reads all and concats.
                             pass
                         # Original code reads ALL into memory. Replicating but safer with concat.
                         df = pd.concat([c for c in pd.read_csv(f, sep=";", header=None, skiprows=cab, dtype=str, encoding=enc, chunksize=50000, on_bad_lines="skip")], ignore_index=True)
                         if df.empty: break
                         
                         df.columns = df.iloc[0]
                         df = df.iloc[1:].reset_index(drop=True)
                         df["NOME_ARQUIVO"] = f.name
                         
                         dest = self.DIR_SAIDA / f"{f.stem}.csv"
                         df.to_csv(dest, sep=";", index=False)
                         outputs.append(dest)
                         saved = True
                         break
                    except: continue
                
                try: f.unlink()
                except: pass
            except Exception as e:
                LOGGER.error(f"Erro processar {f.name}: {e}")
        return outputs

    def _tratar_dataframe(self, arq: Path) -> pd.DataFrame:
        try:
            df = pd.read_csv(arq, sep=";", dtype=str)
            
            def norm(c): 
                s = unidecode(str(c)).lower()
                s = re.sub(r"[^\w]+", "_", s).strip("_")
                return re.sub(r"_+", "_", s)
            
            df.columns = [norm(c) for c in df.columns]
            df = df.loc[:, ~df.columns.duplicated()]
            
            cols = [c for c in self.SCHEMA_COLS if c in df.columns]
            return df[cols] if cols else pd.DataFrame()
        except: return pd.DataFrame()

    def _deduplicate_bq(self):
        try:
            client = bigquery.Client(project=PROJECT_ID, credentials=CREDENTIALS)
            tbl = client.get_table(self.TABLE_ID)
            cols = [f"`{f.name}`" for f in tbl.schema]
            if not cols: return
            
            col_list = ", ".join(cols)
            sql = f"""
            DELETE FROM `{self.TABLE_ID}` WHERE STRUCT({col_list}) IN (
              SELECT AS STRUCT {col_list} FROM (
                SELECT {col_list}, ROW_NUMBER() OVER (PARTITION BY {col_list} ORDER BY CURRENT_TIMESTAMP()) AS rn
                FROM `{self.TABLE_ID}`
              ) WHERE rn > 1
            )
            """
            client.query(sql).result()
        except Exception as e:
            LOGGER.error(f"Erro dedup: {e}")

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
            base = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano"
            target = base / "automacoes" / GLOBAL_CONFIG['area_name'] / "logs" / SCRIPT_NAME / datetime.now().strftime("%Y-%m-%d")
            target.mkdir(parents=True, exist_ok=True)
            if zip_path.exists(): shutil.copy2(zip_path, target)
        except: pass

if __name__ == "__main__":
    AutomationTask().run()