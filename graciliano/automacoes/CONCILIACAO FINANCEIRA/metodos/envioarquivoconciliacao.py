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

# 2. Se não achou relativo, aponta para o caminho padrão da rede
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
        "openpyxl"
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
    PROJECT_ID = 'datalab-pagamentos'  # Dev fallback
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

GLOBAL_CONFIG = {'area_name': 'CONCILIACAO FINANCEIRA', 'emails_principal': [], 'emails_cc': [], 'move_file': False}

import logging
import time
import shutil
import zipfile
import pythoncom
import tempfile
import unicodedata
import pandas as pd
import pandas_gbq
from win32com.client import Dispatch
from google.cloud import bigquery
from pydata_google_auth import cache, get_user_credentials

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

try:
    from modules import dollynho
except ImportError:
    dollynho = None

if dollynho:
    try:
        # Placeholder se precisar de credencial web expecífica
        pass 
    except: pass

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
        """Carrega configs do BQ"""
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
                LOGGER.warning("Configs não encontradas no BQ. Usando padrão.")
        except Exception as e:
            LOGGER.error(f"Erro ao carregar configs: {e}")

    def run(self):
        self.get_configs()
        
        # Identity
        modo_exec = os.environ.get("ENV_EXEC_MODE", "AGENDAMENTO")
        usuario = os.environ.get("ENV_EXEC_USER", f"{os.getlogin().lower()}@{Config.COMPANY_DOMAIN}")
        
        status = "ERROR"
        
        try:
            # === LOGICA DE NEGOCIO ORIGINAL ===
            LOGGER.info("Iniciando lógica de negócio...")
            
            # Outlook
            outlook_handler = OutlookHandler()
            arquivos = outlook_handler.baixar_anexos()
            self.output_files.extend(outlook_handler.output_files)
            
            if not arquivos:
                status = "NO_DATA"
                LOGGER.info("Nenhum arquivo encontrado.")
            else:
                files_processed_ok = 0
                bq_manager = BigQueryManager()
                data_processor = DataProcessor()
                uploaded_lines = 0
                
                for path, email_msg in arquivos:
                    try:
                        df = data_processor.ler_arquivo(path)
                        if df is not None and not df.empty:
                            df = data_processor.tratar(df)
                            uploaded = bq_manager.upload_data(df)
                            uploaded_lines += uploaded
                            files_processed_ok += 1
                        
                        if email_msg:
                            try: email_msg.Delete()
                            except: pass
                    except Exception as e:
                        LOGGER.error(f"Erro arq {path}: {e}")
                
                if uploaded_lines > 0:
                    try:
                        bq_manager.run_procedure()
                        status = "SUCCESS"
                    except: status = "ERROR"
                elif files_processed_ok > 0:
                    status = "SUCCESS"
                else:
                    status = "NO_DATA"

        except Exception as e:
            status = "ERROR"
            LOGGER.critical(f"Erro fatal na execução: {e}", exc_info=True)
            
        finally:
            end_time = datetime.now().replace(microsecond=0)
            duration = round((end_time - START_TIME).total_seconds(), 2)
            
            # Smart Zip
            zip_path = self._create_smart_zip()
            
            # Upload Metrics
            self._upload_metrics(status, usuario, modo_exec, end_time, duration)
            
            # Send Email
            self._send_email(status, zip_path)
            
            # Move Files
            if GLOBAL_CONFIG['move_file'] and status == "SUCCESS":
                self._move_files_to_network(zip_path)

    def _create_smart_zip(self):
        zip_path = TEMP_DIR / f"{SCRIPT_NAME}_{datetime.now().strftime('%H%M%S')}.zip"
        limit_bytes = 15 * 1024 * 1024 # 15MB
        
        try:
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
                # Add Log
                if LOG_FILE.exists(): zf.write(LOG_FILE, LOG_FILE.name)
                
                current_size = zf.fp.tell()
                
                for f in self.output_files:
                    if not f.exists(): continue
                    f_size = f.stat().st_size
                    
                    if (current_size + f_size) < limit_bytes:
                        zf.write(f, f.name)
                        current_size += f_size
                    else:
                        LOGGER.warning(f"Arquivo {f.name} muito grande para o ZIP.")
                        zf.writestr(f"AVISO_ARQUIVO_GRANDE_{f.name}.txt", "Arquivo excedeu 15MB.")
        except Exception as e:
            LOGGER.error(f"Erro ao criar ZIP: {e}")
            
        return zip_path

    def _upload_metrics(self, status, usuario, modo_exec, end_time, duration):
        try:
            df_metric = pd.DataFrame([{
                "script_name": SCRIPT_NAME,
                "area_name": GLOBAL_CONFIG['area_name'],
                "start_time": START_TIME,
                "end_time": end_time,
                "duration_seconds": duration,
                "status": status,
                "usuario": usuario,
                "modo_exec": modo_exec
            }])
            
            pandas_gbq.to_gbq(df_metric, Config.TABLE_EXEC, project_id=Config.PROJECT_ID, if_exists='append')
            LOGGER.info("Métricas enviadas com sucesso.")
        except Exception as e:
            LOGGER.error(f"Erro ao enviar métricas: {e}")

    def _send_email(self, status, zip_path):
        try:
            recipients = GLOBAL_CONFIG['emails_principal']
            if status == "SUCCESS":
                recipients += GLOBAL_CONFIG['emails_cc']
            
            if not recipients:
                LOGGER.warning("Sem destinatários para envio de email.")
                return

            pythoncom.CoInitialize()
            outlook = Dispatch("Outlook.Application")
            mail = outlook.CreateItem(0)
            mail.To = ";".join(set(recipients))
            mail.Subject = f"CÉLULA PYTHON MONITORAÇÃO - {SCRIPT_NAME.upper()} - {status}"
            mail.Body = ""
            
            if zip_path.exists():
                mail.Attachments.Add(str(zip_path))
                
            mail.Send()
            LOGGER.info(f"Email enviado para: {mail.To}")
        except Exception as e:
            LOGGER.error(f"Erro ao enviar email: {e}")

    def _move_files_to_network(self, zip_path):
        try:
            network_base = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano"
            network_dir = network_base / "automacoes" / GLOBAL_CONFIG['area_name'] / "logs" / SCRIPT_NAME / datetime.now().strftime("%Y-%m-%d")
            network_dir.mkdir(parents=True, exist_ok=True)
            
            if zip_path.exists(): shutil.copy2(zip_path, network_dir)
            
            for f in self.output_files:
                if f.exists(): shutil.copy2(f, network_dir)
                
            LOGGER.info(f"Arquivos movidos para a rede: {network_dir}")
        except Exception as e:
            LOGGER.error(f"Erro ao mover arquivos para rede: {e}")

# ==============================================================================
# BUSINESS LOGIC CLASSES
# ==============================================================================
class OutlookHandler:
    def __init__(self):
        self.output_files = []

    def _normalize(self, text):
        return unicodedata.normalize("NFKD", str(text)).encode("ASCII", "ignore").decode("ASCII").lower()

    def _buscar_na_caixa(self, folder, hoje):
        encontrados = []
        ASSUNTO_EMAIL_PESQUISA = "Envio de Arquivo de Conciliação C6 – Débito Veicular"
        try:
            items = folder.Items
            items.Sort("[ReceivedTime]", True)
            
            count = 0
            for msg in items:
                count += 1
                if count > 200: break
                
                try:
                    if not hasattr(msg, "ReceivedTime"): continue
                    try: msg_dt = msg.ReceivedTime.astimezone().date()
                    except: msg_dt = msg.ReceivedTime.date()
                    
                    if msg_dt != hoje: continue
                    
                    subj = (msg.Subject or "")
                    if self._normalize(ASSUNTO_EMAIL_PESQUISA) not in self._normalize(subj):
                        continue
                    
                    if msg.Attachments.Count <= 0: continue
                    LOGGER.info(f"Email encontrado: {subj}")
                    
                    for j in range(msg.Attachments.Count, 0, -1):
                        att = msg.Attachments.Item(j)
                        fn = (att.FileName or "").lower()
                        if fn.endswith((".xls", ".xlsx", ".csv")):
                            path = TEMP_DIR / att.FileName
                            att.SaveAsFile(str(path))
                            LOGGER.info(f"Anexo salvo: {path}")
                            encontrados.append((path, msg))
                            self.output_files.append(path)
                except Exception as e:
                    LOGGER.error(f"Erro ao processar mensagem: {e}")
        except Exception as e:
            LOGGER.error(f"Erro ao ler pasta Outlook: {e}")
        return encontrados

    def baixar_anexos(self):
        arquivos_processar = []
        hoje = datetime.now().date()
        
        # 1. Busca Local (Assume path relative to script location for dev/legacy compatibility if standard root fails)
        # But we must use the new standard root if possible or fallback. 
        # Using project_root from header if available
        base = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano"
        local_dir = base / "automacoes" / GLOBAL_CONFIG['area_name'] / "arquivos_input" / "EnvioArquivoConciliacao"
        if not local_dir.exists():
            local_dir = Path.home() / "graciliano/automacoes/CONCILIACAO FINANCEIRA/arquivos_input/EnvioArquivoConciliacao"

        if local_dir.exists():
            for p in local_dir.iterdir():
                if p.is_file():
                    LOGGER.info(f"Arquivo local encontrado: {p.name}")
                    arquivos_processar.append((p, None))
                    self.output_files.append(p)
        
        # 2. Busca Outlook
        try:
            pythoncom.CoInitialize()
            outlook = Dispatch("Outlook.Application")
            ns = outlook.GetNamespace("MAPI")
            
            inbox = ns.GetDefaultFolder(6)
            arquivos_processar.extend(self._buscar_na_caixa(inbox, hoje))
            
            for i in range(1, ns.Folders.Count + 1):
                fld = ns.Folders.Item(i)
                if "Celula Python" in fld.Name: 
                    try:
                        target = fld.Folders["Inbox"]
                        arquivos_processar.extend(self._buscar_na_caixa(target, hoje))
                    except: pass
                    break
        except Exception as e:
            LOGGER.error(f"Erro na conexão Outlook: {e}")
        finally:
            try: pythoncom.CoUninitialize()
            except: pass
        return arquivos_processar

class DataProcessor:
    def ler_arquivo(self, path):
        path = Path(path)
        LOGGER.info(f"Lendo arquivo: {path.name}")
        try:
            if path.suffix.lower() == ".csv":
                return pd.read_csv(path, sep=";", dtype=str).fillna("")
            elif path.suffix.lower() == ".xlsx":
                try: return pd.read_excel(path, sheet_name="Pagamentos Efetuados", engine="openpyxl", dtype=str).fillna("")
                except: return pd.read_excel(path, sheet_name=0, engine="openpyxl", dtype=str).fillna("")
            else:
                return pd.read_excel(path, dtype=str).fillna("")
        except Exception as e:
            LOGGER.error(f"Erro ao ler arquivo {path}: {e}")
            return None

    def tratar(self, df):
        if "dataPagamento" in df.columns:
            try:
                df["dataPagamento"] = pd.to_datetime(df["dataPagamento"], errors='coerce')
                df["dataPagamento"] = df["dataPagamento"].dt.strftime("%Y-%m-%d %H:%M:%S")
                # Drop NaT
                df = df.dropna(subset=["dataPagamento"])
            except: pass
        return df

class BigQueryManager:
    def __init__(self):
        self.client = bigquery.Client(project=PROJECT_ID, credentials=CREDENTIALS)
        self.table_ref = f"{PROJECT_ID}.payments.DEBVCLR_CELCOIN_ARQUIVO"

    def upload_data(self, df):
        if df.empty: return 0
        
        # Deduplicação
        if "dataPagamento" in df.columns:
            try:
                query = f"SELECT DISTINCT dataPagamento FROM `{self.table_ref}`"
                try:
                    result = self.client.query(query).result()
                    # Converte resultados do BQ para string ISO compatível
                    datas_bq = set()
                    for row in result:
                         val = row["dataPagamento"]
                         if val: datas_bq.add(str(val))
                    
                    df = df[~df["dataPagamento"].astype(str).isin(datas_bq)].copy()
                except Exception as e:
                    LOGGER.warning(f"Erro na deduplicação BQ: {e}. Prosseguindo.")
            except: pass

        if df.empty:
            LOGGER.info("Todas as linhas já existem no BigQuery.")
            return 0

        try:
            pandas_gbq.to_gbq(df, "payments.DEBVCLR_CELCOIN_ARQUIVO", project_id=PROJECT_ID, if_exists='append')
            LOGGER.info(f"{len(df)} linhas carregadas em {self.table_ref}")
            return len(df)
        except Exception as e:
            LOGGER.error(f"Erro upload: {e}")
            raise

    def run_procedure(self):
        proc_query = "CALL `datalab-pagamentos.payments.debitos_veiculares`();"
        LOGGER.info(f"Executando procedure: {proc_query}")
        self.client.query(proc_query).result()
        LOGGER.info("Procedure executada com sucesso.")

if __name__ == "__main__":
    task = AutomationTask()
    task.run()
