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
SCRIPT_NAME = Path(__file__).stem.lower()
START_TIME = datetime.now().replace(microsecond=0)

# Project Configs
PROJECT_ID = "datalab-pagamentos"
DATASET_ID = "investimentos"
TABLE_TARGET = f"{PROJECT_ID}.{DATASET_ID}.RF_ARQUIVO_MOV_B3_LCD_BKUP_TESTE"

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
        self.INPUT_DIR = ROOT_DRIVE / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "automacoes" / "BO INVESTIMENTOS" / "arquivos input" / "investimentosLcd"
        if not self.INPUT_DIR.exists():
             self.INPUT_DIR = ROOT_DRIVE / "graciliano" / "automacoes" / "BO INVESTIMENTOS" / "arquivos input" / "investimentosLcd"
             
        # DE-PARA Colunas
        self.DE_PARA = {
            "Participante": "PARTICIPANTE",
            "Conta": "CONTA",
            "Cód. Movimento": "COD_MOVIMENTO",
            "Tipo Movimento": "TIPO_MOVIMENTO",
            "Papel da Parte": "PAPEL_DA_PARTE",
            "Código IF": "CODIGO_IF",
            "Tipo de IF": "TIPO_DE_IF",
            "Quantidade": "QUANTIDADE",
            "PU": "PU",
            "Valor": "VALOR",
            "Cód. Modalidade Liquidação": "COD_MODALIDADE_LIQUIDACAO",
            "Modalidade Liquidação": "MODALIDADE_LIQUIDACAO",
            "Cód. Status": "COD_STATUS",
            "Status": "STATUS",
            "Meu Número": "MEU_NUMERO",
            "Número Associação": "NUMERO_ASSOCIACAO",
            "Data Liquidação": "DATA_LIQUIDACAO",
            "Data Origem": "DATA_ORIGEM",
            "Contraparte(Nome Simp.)": "CONTRAPARTE_NOME_SIMP",
            "Conta Contraparte": "CONTA_CONTRAPARTE",
            "Detalhes do Erro": "DETALHES_DO_ERRO",
            "Id. Movimento": "ID_MOVIMENTO",
            "Num Ctrl Movimento": "NUM_CTRL_MOVIMENTO"
        }

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
        obs = None
        
        try:
            LOGGER.info(">>> INICIO <<<")
            
            if not self.INPUT_DIR.exists():
                LOGGER.error(f"Diretório Input não encontrado: {self.INPUT_DIR}")
                status = "ERROR"
                return

            files = [f for f in self.INPUT_DIR.iterdir() if f.is_file() and not f.name.startswith("~$") and f.suffix in ['.xls', '.xlsx']]
            
            if not files:
                status = "NO_DATA"
            else:
                dfs = []
                for target_file in files:
                    LOGGER.info(f"Processando: {target_file.name}")
                    df_temp = self._read_transform(target_file)
                    if not df_temp.empty:
                        dfs.append(df_temp)
                        
                        # Move individual processed files already? No, keep logic to move all at end or per file.
                        # Original: Moves all if success?
                        # Step 989 code: moves to self.output_files manually inside loop in main, consolidation happens after loop.
                        # I will add to output list here.
                        
                        dest_log = ROOT_DRIVE / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "automacoes" / "BO INVESTIMENTOS" / "logs" / SCRIPT_NAME / datetime.now().strftime("%Y-%m-%d")
                        dest_log.mkdir(parents=True, exist_ok=True)
                        final_path = dest_log / f"{target_file.stem}_{datetime.now().strftime('%H%M%S')}{target_file.suffix}"
                        try:
                             shutil.move(str(target_file), str(final_path))
                             self.output_files.append(final_path)
                        except Exception as move_err:
                             LOGGER.error(f"Erro ao mover {target_file.name}: {move_err}")

                if not dfs:
                    status = "NO_DATA"
                else:
                    df_final = pd.concat(dfs, ignore_index=True)
                    self._upload_bq(df_final)
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
                 self._move_files_to_network(zip_path) # Also move the already moved input files? 
                 # The method _move_files_to_network typically moves the ZIP to network logs. 
                 # The individual files were moved to LOGS folder (local or network? ROOT_DRIVE logic points to Drive/Network).
                 # So files are safe.

    def _read_transform(self, file_path):
        try:
             # Win32 usage
            if file_path.suffix in ['.xls', '.xlsx']:
                self._sanitize_excel(file_path)

            df = pd.read_excel(file_path, dtype=str)
            if df.empty: return pd.DataFrame()

            # Normalize cols
            cols_norm = {str(c).strip().lower(): c for c in df.columns}
            
            dados = {}
            for col_raw, col_bq in self.DE_PARA.items():
                key_norm = col_raw.strip().lower()
                if key_norm not in cols_norm:
                    LOGGER.error(f"Coluna faltante: {col_raw} em {file_path.name}")
                    return pd.DataFrame()
                dados[col_bq] = df[cols_norm[key_norm]].astype(str)
            
            df_out = pd.DataFrame(dados)
            df_out["NOME_ARQUIVO"] = file_path.name
            df_out["DT_COLETA"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            return df_out.replace({r'\x00': '', 'nan': '', 'NaT': '', 'None': ''}, regex=True)

        except Exception as e:
            LOGGER.error(f"Erro leitura {file_path.name}: {e}")
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
        
        # Dedup logic
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
