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
        self.TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.doki_processamento"
        self.TABLE_STAGING = f"{self.TABLE_ID}_STAGING"
        
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
        total_uploaded = 0
        
        try:
            LOGGER.info(">>> INICIO <<<")
            
            INPUT_DIR = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "automacoes" / GLOBAL_CONFIG['area_name'] / "arquivos input" / "PROCESSAMENTO"
            if not INPUT_DIR.exists():
                INPUT_DIR = Path.home() / "Meu Drive" / "C6 CTVM" / "graciliano" / "automacoes" / GLOBAL_CONFIG['area_name'] / "arquivos input" / "PROCESSAMENTO"
            
            arquivos = sorted([p for p in INPUT_DIR.glob("processamento_*.xlsx") if p.is_file()])
            
            if not arquivos:
                LOGGER.info("Nenhum arquivo encontrado.")
                status = "NO_DATA"
            else:
                client = bigquery.Client(project=PROJECT_ID, credentials=CREDENTIALS)
                for arq in arquivos:
                    df = self._tratar_dataframe(arq)
                    if df.empty: continue
                    
                    pandas_gbq.to_gbq(df, self.TABLE_STAGING, project_id=PROJECT_ID, if_exists="replace")
                    
                    t_concat = ", ".join([f"IFNULL(T.`{c}`, '')" for c in self.SAFE_ORDER])
                    s_concat = ", ".join([f"IFNULL(S.`{c}`, '')" for c in self.SAFE_ORDER])
                    
                    merge_sql = f"""
                    MERGE `{self.TABLE_ID}` T
                    USING `{self.TABLE_STAGING}` S
                    ON MD5(CONCAT({t_concat})) = MD5(CONCAT({s_concat}))
                    WHEN NOT MATCHED THEN
                      INSERT ({', '.join([f'`{c}`' for c in self.SAFE_ORDER])})
                      VALUES ({', '.join([f'S.`{c}`' for c in self.SAFE_ORDER])})
                    """
                    job = client.query(merge_sql)
                    job.result()
                    inserted = job.num_dml_affected_rows or 0
                    
                    LOGGER.info(f"Arquivo {arq.name}: {inserted} inseridos.")
                    total_uploaded += inserted
                    
                    try: arq.unlink()
                    except: pass
                
                if total_uploaded > 0: status = "SUCCESS"
                else: status = "NO_DATA"

        except Exception as e:
            status = "ERROR"
            LOGGER.critical(f"Erro fatal: {e}", exc_info=True)
            
        finally:
            end_time = datetime.now().replace(microsecond=0)
            duration = round((end_time - START_TIME).total_seconds(), 2)
            
            zip_path = self._create_smart_zip()
            self._upload_metrics(status, usuario, modo_exec, end_time, duration)
            self._send_email(status, zip_path, total_uploaded, duration)
            
            if GLOBAL_CONFIG['move_file'] and status == "SUCCESS":
                 self._move_files_to_network(zip_path)

    def _normalize_col(self, s: str) -> str:
        if not s: return ""
        return re.sub(r"\s+", " ", unidecode(str(s)).strip().lower())

    def _find_header_row(self, df: pd.DataFrame) -> int:
        for i in range(min(100, len(df))):
            row = [self._normalize_col(x) for x in df.iloc[i].astype(str).tolist()]
            if "cpf" in row and "cartao" in row: return i
            if "id cartao" in row: return i
        return 0

    def _tratar_dataframe(self, caminho: Path) -> pd.DataFrame:
        LOGGER.info(f"Processando: {caminho.name}")
        try:
            df_raw = pd.read_excel(caminho, header=None, dtype=str, engine="openpyxl")
            hdr = self._find_header_row(df_raw)
            df = pd.read_excel(caminho, header=hdr, dtype=str, engine="openpyxl")
            df = df.dropna(how="all")
            
            df.columns = [self._normalize_col(c) for c in df.columns]
            mapa = {self._normalize_col(orig): dest for dest, orig in self.SAFE_SCHEMA}
            df.columns = [mapa.get(c, c) for c in df.columns]
            
            for c in self.SAFE_ORDER:
                if c not in df.columns: df[c] = None
            return df[self.SAFE_ORDER].astype("string")
        except Exception as e:
            LOGGER.error(f"Erro ao tratar excel {caminho.name}: {e}")
            return pd.DataFrame()

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

    def _send_email(self, status, zip_path, total, duration):
        try:
            to = GLOBAL_CONFIG['emails_principal']
            if status=="SUCCESS": to += GLOBAL_CONFIG['emails_cc']
            if not to: return

            pythoncom.CoInitialize()
            outlook = Dispatch("Outlook.Application")
            mail = outlook.CreateItem(0)
            mail.To = ";".join(set(to))
            mail.Subject = f"CÉLULA PYTHON MONITORAÇÃO - {SCRIPT_NAME.upper()} - {status}"
            mail.Body = f"Status: {status}\nProcessado: {total}\nDuração: {duration}s"
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