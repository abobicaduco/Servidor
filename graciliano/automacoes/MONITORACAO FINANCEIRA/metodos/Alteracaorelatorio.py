# ==============================================================================
# AUTO-INSTALL DEPENDENCIES & CONFIG
# ==============================================================================
import sys
import os
from pathlib import Path
from datetime import datetime

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
SUBIDA_BQ = "append" # ou replace

# Controle de Headless
HEADLESS = False

# Diretórios
TEMP_DIR = Path(os.getenv('TEMP', Path.home())) / "C6_RPA_EXEC" / SCRIPT_NAME
TEMP_DIR.mkdir(parents=True, exist_ok=True)

GLOBAL_CONFIG = {'area_name': 'MONITORACAO FINANCEIRA', 'emails_principal': [], 'emails_cc': [], 'move_file': False}

import logging
import time
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
        # Schema Defines
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
        self.SAFE_ORDER = [dest for dest, _ in self.SAFE_SCHEMA]

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
            
            # --- PATHS ---
            base_dir = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "automacoes"
            if not base_dir.exists(): base_dir = Path.home() / "graciliano" / "automacoes"
            
            INPUT_DIR = base_dir / GLOBAL_CONFIG['area_name'] / "arquivos input" / "ALTERACAO"
            
            BQ_TABLE_NEGOCIO = f"{PROJECT_ID}.{DATASET_ID}.doki_alteracao"
            BQ_TABLE_STAGING = f"{BQ_TABLE_NEGOCIO}_staging"

            arquivos = sorted([p for p in INPUT_DIR.glob("alteracao_*.xlsx") if p.is_file()])
            total_linhas = 0
            
            if not arquivos:
                status = "NO_DATA"
                LOGGER.info("Sem arquivos input.")
            else:
                for arq in arquivos:
                     df = self._ler_arquivo(arq)
                     if df is not None and not df.empty:
                          cnt = self._upload_bq(df, BQ_TABLE_STAGING, BQ_TABLE_NEGOCIO)
                          total_linhas += cnt
                          
                          dest = TEMP_DIR / f"{arq.stem}_{datetime.now().strftime('%H%M%S')}{arq.suffix}"
                          self._move_safe(arq, dest)
                          self.output_files.append(dest)
                          LOGGER.info(f"Processado: {arq.name}")
                     else:
                          LOGGER.warning(f"Vazio/Erro: {arq.name}")
                
                if total_linhas > 0: status = "SUCCESS"
                elif status != "NO_DATA": status = "SUCCESS"

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

    # --- HELPERS ---
    def _move_safe(self, src, dst):
        try: shutil.move(str(src), str(dst))
        except: shutil.copy2(str(src), str(dst))

    def _win_long_path(self, p: Path) -> str:
        s = str(p.resolve())
        return "\\\\?\\" + s if len(s) >= 240 and not s.startswith("\\\\?\\") else s

    def _sanitizar_excel(self, caminho_arquivo: Path):
        if caminho_arquivo.suffix.lower() not in ['.xlsx', '.xls']: return
        try:
            pythoncom.CoInitialize()
            xl = Dispatch("Excel.Application")
            xl.DisplayAlerts = False; xl.Visible = False
            wb = xl.Workbooks.Open(str(caminho_arquivo))
            wb.Save(); wb.Close()
            xl.Quit()
        except: pass

    def _ler_arquivo(self, p: Path) -> pd.DataFrame | None:
        def normalize_col(s: str) -> str:
            if s is None: return ""
            s = unidecode(str(s)).strip().lower()
            return re.sub(r"\s+", " ", s)

        def find_header_row(df: pd.DataFrame) -> int:
            for i in range(min(100, len(df))):
                row = [normalize_col(x) for x in df.iloc[i].tolist()]
                if "cpf" in row and "cartao" in row: return i
            return 0

        try:
            self._sanitizar_excel(p)
            df_raw = pd.read_excel(self._win_long_path(p), header=None, dtype=str, engine="openpyxl")
            hdr = find_header_row(df_raw)
            df = pd.read_excel(self._win_long_path(p), header=hdr, dtype=str, engine="openpyxl")
            
            cols = {c: normalize_col(c) for c in df.columns}
            df.rename(columns=cols, inplace=True)
            
            mapa = {normalize_col(orig): dest for dest, orig in self.SAFE_SCHEMA}
            df.rename(columns=mapa, inplace=True)
            
            for c in self.SAFE_ORDER:
                if c not in df.columns: df[c] = None
            
            df = df[self.SAFE_ORDER].astype("string")
            return df
        except Exception as e:
            LOGGER.error(f"Erro ler {p.name}: {e}")
            return None

    def _upload_bq(self, df, staging_table, target_table):
        pandas_gbq.to_gbq(df, staging_table, project_id=PROJECT_ID, if_exists="replace")
        
        cols_hash = ", ".join([f"IFNULL(t.`{c}`,'')" for c in self.SAFE_ORDER])
        sql = f"""
        INSERT INTO `{target_table}`
        SELECT * FROM `{staging_table}` S
        WHERE NOT EXISTS (
            SELECT 1 FROM `{target_table}` F
            WHERE TO_JSON_STRING(S) = TO_JSON_STRING(F)
        )
        """
        client = bigquery.Client(project=PROJECT_ID, credentials=CREDENTIALS)
        client.query(sql).result()
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