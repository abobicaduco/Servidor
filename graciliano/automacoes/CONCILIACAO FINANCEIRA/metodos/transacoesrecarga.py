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

if not project_root:
    standard_root = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "automacoes" / ""
    if standard_root.exists():
        project_root = standard_root

if project_root:
    sys.path.insert(0, str(project_root))

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
    from config_loader import Config
    
    ROOT_DIR = Config.ROOT_DIR
    PROJECT_ID = Config.PROJECT_ID
    DATASET_ID = Config.DATASET_ID

except ImportError:
    print("⚠️ Config Loader não encontrado. Usando FALLBACK local.")
    CONFIG_LOADER = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "novo_servidor"
    PROJECT_ID = 'datalab-pagamentos' 
    DATASET_ID = 'ADMINISTRACAO_CELULA_PYTHON'

# ==============================================================================
# CONSTANTES DO SCRIPT
# ==============================================================================
SCRIPT_NAME = Path(__file__).stem.lower()
START_TIME = datetime.now().replace(microsecond=0)
SUBIDA_BQ = "append" 
HEADLESS = False

TEMP_DIR = Path(os.getenv('TEMP', Path.home())) / "C6_RPA_EXEC" / SCRIPT_NAME
TEMP_DIR.mkdir(parents=True, exist_ok=True)

GLOBAL_CONFIG = {'area_name': 'CONCILIACAO FINANCEIRA', 'emails_principal': [], 'emails_cc': [], 'move_file': False}

import logging
import time
import shutil
import zipfile
import pythoncom
import traceback
import pandas as pd
import pandas_gbq
from win32com.client import Dispatch
from google.cloud import bigquery
from pydata_google_auth import cache, get_user_credentials
from zoneinfo import ZoneInfo
from datetime import timedelta

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
# BUSINESS LOGIC CONSTANTS
# ==============================================================================
TZ = ZoneInfo("America/Sao_Paulo")
TABELA_ALVO = "datalab-pagamentos.conciliacoes_monitoracao.Rotina_24x7_Recarga"
PROCEDURE_CALL = "CALL `datalab-pagamentos.conciliacoes_monitoracao.C_RECARGA`()"
SCOPES = ["https://www.googleapis.com/auth/bigquery"]
CREDENTIALS = None

if not CREDENTIALS:
    try:
        TOKENS_DIR = Path.home() / "AppData" / "Roaming" / "CELPY" / "tokens"
        CREDENTIALS = get_user_credentials(SCOPES, credentials_cache=cache.ReadWriteCredentialsCache(str(TOKENS_DIR)), auth_local_webserver=True)
        pandas_gbq.context.credentials = CREDENTIALS
    except: pass

pandas_gbq.context.project = PROJECT_ID

CAMINHO_BASE = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "automacoes" / "CONCILIACAO FINANCEIRA"
LOCAL_INPUT_DIR = CAMINHO_BASE / "arquivos_input" / "transacoesrecarga"
LOCK_DIR = CAMINHO_BASE / ".locks"

# ==============================================================================
# AUX CLASSES
# ==============================================================================
class FileLock:
    def __init__(self, path: Path, timeout_s: int = 300, poll_ms: int = 200):
        import msvcrt
        self.path = path
        self.timeout_s = timeout_s
        self.poll_ms = poll_ms
        self._fh = None
        self._msvcrt = msvcrt

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(self.path, "a+b")
        start = time.time()
        while time.time() - start < self.timeout_s:
            try:
                self._msvcrt.locking(self._fh.fileno(), self._msvcrt.LK_NBLCK, 1)
                return self
            except OSError:
                time.sleep(self.poll_ms / 1000.0)
        LOGGER.warning(f"Timeout ao tentar adquirir lock: {self.path}")
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if self._fh:
                self._msvcrt.locking(self._fh.fileno(), self._msvcrt.LK_UNLCK, 1)
                self._fh.close()
        except Exception: pass

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
                LOGGER.warning("Configs não encontradas ou script inativo. Usando padrão.")
        except Exception as e:
            LOGGER.error(f"Erro ao carregar configs: {e}")

    def run(self):
        self.get_configs()
        
        modo_exec = os.environ.get("ENV_EXEC_MODE", "AGENDAMENTO")
        usuario = os.environ.get("ENV_EXEC_USER", f"{os.getlogin().lower()}@{Config.COMPANY_DOMAIN}")
        status = "ERROR"
        
        try:
            LOGGER.info(">>> INICIANDO EXECUÇÃO <<<")
            
            # Outlook Prep
            try:
                pythoncom.CoInitialize()
                try: win32.GetActiveObject("Outlook.Application")
                except: win32.Dispatch("Outlook.Application")
            except: pass

            # Busca Arquivos
            arquivos = self._procurar_arquivos()
            
            if not arquivos:
                status = "NO_DATA"
                LOGGER.info("Sem arquivos para processar.")
            else:
                total_subidas = 0
                for arq in arquivos:
                    self.output_files.append(Path(arq))
                    try:
                        arq_proc = self._regravar_excel(arq)
                        if arq_proc != arq: self.output_files.append(Path(arq_proc))
                        
                        df = self._processar_dataframe(arq_proc)
                        linhas = self._subir_bq(df)
                        total_subidas += linhas
                    except Exception as e:
                        LOGGER.error(f"Erro processando {arq}: {e}")

                if total_subidas > 0:
                    client = bigquery.Client(project=PROJECT_ID, credentials=CREDENTIALS)
                    lock_path = LOCK_DIR / f"{SCRIPT_NAME}_dedup.lock"
                    dedup_sql = f"CREATE OR REPLACE TABLE `{TABELA_ALVO}` AS SELECT DISTINCT * FROM `{TABELA_ALVO}`"
                    
                    with FileLock(lock_path):
                        LOGGER.info("Deduplicando tabela BQ...")
                        client.query(dedup_sql).result()
                    
                    LOGGER.info(f"Executando procedure: {PROCEDURE_CALL}")
                    client.query(PROCEDURE_CALL)
                    status = "SUCCESS"
                elif status != "NO_DATA":
                    status = "SUCCESS" # Arquivos processados mas vazio ou filtro

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

    def _procurar_arquivos(self):
        resultados = []
        if LOCAL_INPUT_DIR.exists():
            for f in sorted(list(LOCAL_INPUT_DIR.glob("*.xls*")) + list(LOCAL_INPUT_DIR.glob("*.csv"))):
                resultados.append(str(f.resolve()))
        
        try:
            pythoncom.CoInitialize()
            app = Dispatch("Outlook.Application")
            ns = app.GetNamespace("MAPI")
            d0 = datetime.now(TZ).replace(hour=0, minute=0, second=0, microsecond=0)
            d1 = d0 + timedelta(days=1)
            restr = f"[ReceivedTime] >= '{d0.strftime('%m/%d/%Y %I:%M %p')}' AND [ReceivedTime] < '{d1.strftime('%m/%d/%Y %I:%M %p')}'"
            assunto_busca = "transacoes_recarga | CONCILIACAO FINANCEIRA - subida de base"

            def coletar(folder):
                items = folder.Items.Restrict(restr)
                items.Sort("[ReceivedTime]", True)
                for i in range(1, items.Count + 1):
                    msg = items.Item(i)
                    if assunto_busca.lower() in (msg.Subject or "").lower() and msg.Attachments.Count > 0:
                        att = msg.Attachments.Item(msg.Attachments.Count)
                        fn = (att.FileName or "").lower()
                        if fn.endswith((".xls", ".xlsx", ".csv")):
                            dest = TEMP_DIR / f"{msg.ReceivedTime:%Y%m%d_%H%M%S}_{att.FileName}"
                            att.SaveAsFile(str(dest))
                            resultados.append(str(dest))
            
            coletar(ns.GetDefaultFolder(6))
        except Exception as e:
            LOGGER.warning(f"Erro outlook: {e}")
        
        return list(set(resultados))

    def _regravar_excel(self, path_str):
        path = Path(path_str)
        if path.suffix.lower() not in (".xls", ".xlsx"): return path_str
        alvo = TEMP_DIR / f"regravado_{path.stem}_{int(time.time() * 1000)}.xlsx"
        try:
            pythoncom.CoInitialize()
            app = Dispatch("Excel.Application")
            app.DisplayAlerts = False; app.Visible = False
            wb = app.Workbooks.Open(Filename=str(path), ReadOnly=False, UpdateLinks=0, Editable=True)
            try: wb.SaveAs(str(alvo), FileFormat=51)
            except: wb.SaveCopyAs(str(alvo))
            wb.Close(SaveChanges=False); app.Quit()
            return str(alvo)
        except: return path_str

    def _processar_dataframe(self, path_str):
        try:
            df = pd.read_excel(path_str)
            df = df.query("Modelo=='ONLINE' and `Status Transação`=='Confirmada' and Tipo=='Compra'")
            col_data = next((c for c in ["Data", "Vencimento", "Data Vencimento Cobrança"] if c in df.columns), None)
            if not col_data: raise ValueError("Coluna de data nao encontrada")

            cols = ["Loja", col_data, "Operadora", "Fone", "Nsu/Referencia", "Tipo Cob.", "Custo", "Face", "Compra", "Nsu Origem", "Série PIN", "Status Transação"]
            df2 = df[cols].astype(str).rename(columns={col_data: "Data"})
            
            df2.insert(2, "Hora", df2["Data"].str[10:])
            df2["Data"] = pd.to_datetime(df2["Data"].str[:10], dayfirst=True, errors="coerce").dt.strftime("%d/%m/%Y")
            df2["Fone"] = df2["Fone"].str.lstrip("+55")
            df2["Custo"] = pd.to_numeric(df2["Custo"].str.replace(",", "."), errors="coerce").astype(str)
            df2["Face"] = pd.to_numeric(df2["Face"].str.replace(",", "."), errors="coerce").astype(str)
            df2.insert(6, "Tipo_Produto", "RECARGA")
            df2["DT_COLETA"] = df2["Data"]
            
            df2.columns = ["LOJA", "DATA", "HORA", "OPERADORA", "NUM_TELEFONE", "NSU", "TIPO_PRODUTO", "TIPO_PAGAMENTO", "CUSTO", "FACE", "COMPRA", "NSU_ORIGEM", "SERIE_PIN", "STATUS_TRANSACAO", "DT_COLETA"]
            return df2
        except Exception as e:
            LOGGER.error(f"Erro processar df: {e}")
            return pd.DataFrame()

    def _subir_bq(self, df):
        if df is None or df.empty: return 0
        try:
            pandas_gbq.to_gbq(df, TABELA_ALVO, project_id=PROJECT_ID, if_exists='append')
            LOGGER.info(f"{len(df)} linhas subidas para BQ.")
            return len(df)
        except Exception as e:
            LOGGER.error(f"Erro BQ: {e}")
            raise

    def _create_smart_zip(self):
        zip_path = TEMP_DIR / f"{SCRIPT_NAME}_{datetime.now().strftime('%H%M%S')}.zip"
        limit = 15 * 1024 * 1024
        try:
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
                if LOG_FILE.exists(): zf.write(LOG_FILE, LOG_FILE.name)
                curr_size = zf.fp.tell()
                for f in self.output_files:
                    if not f.exists(): continue
                    sz = f.stat().st_size
                    if (curr_size + sz) < limit:
                        zf.write(f, f.name); curr_size += sz
                    else:
                        zf.writestr(f"AVISO_ARQUIVO_GRANDE_{f.name}.txt", "Arquivo excede 15MB.")
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
        except Exception as e: LOGGER.error(f"Erro metricas: {e}")

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
            LOGGER.info("Email enviado.")
        except Exception as e: LOGGER.error(f"Erro envio email: {e}")

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
    task = AutomationTask()
    task.run()