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
from concurrent.futures import ThreadPoolExecutor
from google.cloud import bigquery
from pydata_google_auth import cache, get_user_credentials

# ==============================================================================
# CONSTANTES DO SCRIPT
# ==============================================================================
SCRIPT_NAME = Path(__file__).stem.upper()
START_TIME = datetime.now().replace(microsecond=0)

# Project Configs
PROJECT_ID = "datalab-pagamentos"
DATASET_ID = "investimentos"
TABLE_TARGET = f"{PROJECT_ID}.{DATASET_ID}.LC_GARANTIAS_LCI_ARQUIVO"

TABLE_CONFIG = f"{PROJECT_ID}.ADMINISTRACAO_CELULA_PYTHON.registro_automacoes"
TABLE_EXEC = f"{PROJECT_ID}.ADMINISTRACAO_CELULA_PYTHON.automacoes_exec"

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
        # Input Path
        self.INPUT_DIR = ROOT_DRIVE / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "automacoes" / "BO INVESTIMENTOS" / "arquivos input" / "investimentoslci"
        if not self.INPUT_DIR.exists():
             self.INPUT_DIR = ROOT_DRIVE / "graciliano" / "automacoes" / "BO INVESTIMENTOS" / "arquivos input" / "investimentoslci"

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
        
        try:
            LOGGER.info(">>> INICIO <<<")
            
            if not self.INPUT_DIR.exists():
                LOGGER.error(f"Diretório Input não encontrado: {self.INPUT_DIR}")
                status = "ERROR"
                # Check dir creation logic if needed? No, standard structure.
                return

            files = sorted([f for f in self.INPUT_DIR.iterdir() if f.is_file() and not f.name.startswith("~$")], key=lambda x: x.stat().st_mtime) # FIFO - Oldest first
            
            if not files:
                status = "NO_DATA"
            else:
                target_file = files[0]
                LOGGER.info(f"Processando (FIFO): {target_file.name}")
                
                df = self._read_transform(target_file)
                
                if df.empty:
                    status = "NO_DATA"
                else:
                    self._upload_bq(df)
                    status = "SUCCESS"
                    
                # Move
                dest_log = ROOT_DRIVE / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "automacoes" / "BO INVESTIMENTOS" / "logs" / SCRIPT_NAME / datetime.now().strftime("%Y-%m-%d")
                dest_log.mkdir(parents=True, exist_ok=True)
                final_path = dest_log / f"{target_file.stem}_{datetime.now().strftime('%H%M%S')}{target_file.suffix}"
                shutil.move(str(target_file), str(final_path))
                self.output_files.append(final_path)

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

    def _read_transform(self, file_path):
        try:
             # Win32 sanitize
            if file_path.suffix in ['.xls', '.xlsx']:
                self._sanitize_excel(file_path)

            try:
                df = pd.read_csv(file_path, sep="\t", skiprows=6, dtype=str, encoding='latin-1')
            except:
                df = pd.read_excel(file_path, skiprows=6, dtype=str)

            if df.empty: return df

            # Mapping
            mapping = {
                "Conta do Emissor": "Conta_do_Emissor",
                "Código do Crédito": "Codigo_do_Credito",
                "Identificação do Lastro": "Identificacao_do_Lastro",
                "Código IF": "Codigo_IF",
                "Quantidade do IF": "Quantidade_do_IF",
                "Tipo de Crédito": "Tipo_de_Credito",
                "Data da Inclusão": "Data_da_Inclusao",
                "Valor Residual Unitário": "Valor_Residual_Unitario",
                "Data Base do Valor Residual Unitário": "Data_Base_do_Valor_Residual_Unitario",
                "Data da Última Atualização": "Data_da_Ultima_Atualizacao",
                "Data de Exclusão": "Data_de_Exclusao",
                "Lote": "Lote",
                "Valor Contratado": "Valor_Contratado",
                "Data de Contratação da Operação": "Data_de_Contratacao_da_Operacao",
                "Data Vencimento da Operação": "Data_Vencimento_da_Operacao",
                "Natureza do Cliente": "Natureza_do_Cliente",
                "Código do Cliente": "Codigo_do_Cliente",
                "Código do Contrato": "Codigo_do_Contrato",
                "Modalidade da Operação": "Modalidade_da_Operacao",
                "Taxa Referencial ou Indexador": "Taxa_Referencial_ou_Indexador",
                "Percentual do Indexador": "Percentual_do_Indexador",
                "Taxa Efetiva Anual": "Taxa_Efetiva_Anual",
                "Variação": "Variacao",
                "IPOC": "IPOC",
                "Múltiplos IPOC no SCR?": "Multiplos_IPOC_no_SCR",
                "Outros?": "Outros"
            }
            df = df.rename(columns=mapping)
            
            schema_cols = list(mapping.values())
            for col in schema_cols: 
                if col not in df.columns: df[col] = pd.NA
            
            df = df.reindex(columns=schema_cols)
            df["DT_COLETA"] = datetime.now().strftime("%Y-%m-%d")
            
            # Simple Cleaning (Assuming strings for simplicity in refactor, can refine strictly if needed)
            df = df.astype(str).replace({'nan': None, 'NaT': None, 'None': None, '<NA>': None})
            
            return df
        except Exception as e:
            LOGGER.error(f"Erro leitura: {e}")
            return pd.DataFrame()

    def _sanitize_excel(self, path):
        try:
            pythoncom.CoInitialize()
            xl = Dispatch("Excel.Application")
            xl.DisplayAlerts = False
            xl.Visible = False
            wb = xl.Workbooks.Open(str(path))
            wb.Save()
            wb.Close()
            xl.Quit()
        except: pass

    def _upload_bq(self, df):
        if df.empty: return
        staging = f"{TABLE_TARGET}_staging"
        pandas_gbq.to_gbq(df, staging, project_id=PROJECT_ID, if_exists='replace')
        
        # Merge Full Object Equality
        sql = f"""
        INSERT INTO `{TABLE_TARGET}`
        SELECT * FROM `{staging}` S
        WHERE NOT EXISTS (
            SELECT 1 FROM `{TABLE_TARGET}` T
            WHERE TO_JSON_STRING(S) = TO_JSON_STRING(T)
        )
        """
        client = bigquery.Client(project=PROJECT_ID)
        client.query(sql).result()
        client.delete_table(staging, not_found_ok=True)

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
            mail.Body = f"Status: {status}"
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
