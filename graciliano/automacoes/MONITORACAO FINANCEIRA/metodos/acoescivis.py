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

# Tentativa de carregar Dollynho (opcional se não usar selenium/login web)
try:
    from modules import dollynho
except ImportError:
    dollynho = None

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
        
        modo_exec = os.environ.get("ENV_EXEC_MODE", "AGENDAMENTO")
        usuario = os.environ.get("ENV_EXEC_USER", f"{os.getlogin().lower()}@{Config.COMPANY_DOMAIN}")
        
        status = "ERROR"
        
        try:
            # === LOGICA DE NEGOCIO ORIGINAL ===
            LOGGER.info("Iniciando lógica de negócio...")
            
            # Caminho Origem
            ARQ_ORIGEM = (
                Path.home()
                / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A"
                / "BKO FINANCEIRO - BASES AÇÕES CIVIS"
                / "Bases TED"
                / "BASE_COMPLETA_RECEBIMENTO_AÇAO_CIVIL_Empilhada.xlsx"
            )
            BQ_DESTINO = "datalab-pagamentos.00_temp.BASE_COMPLETA_AC_EMPILHADA_ORIGEM_SPT_BKO_FIN_TED"

            if not ARQ_ORIGEM.exists():
                LOGGER.warning(f"Arquivo não encontrado: {ARQ_ORIGEM}")
                status = "NO_DATA"
            else:
                self.output_files.append(ARQ_ORIGEM)
                df = self._ler_excel_seguro(ARQ_ORIGEM)
                if df.empty:
                    LOGGER.warning("DataFrame vazio.")
                    status = "NO_DATA"
                else:
                    df = df.rename(columns=self._sanitizar_colunas(df.columns))
                    df = df.astype(str).replace('nan', None)
                    self._upload_staging(df, BQ_DESTINO)
                    status = "SUCCESS"
                    
                    if GLOBAL_CONFIG['move_file']:
                        # Nota: Em refactor.md diz para mover para rede LOGS. 
                        # Mas o script original movia para pasta LOG_DIR local? 
                        # O refactor.md manda mover para pasta logs network. Manteremos output_files para o move final
                        pass 

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

    # =========================
    # LOGICA ESPECIFICA
    # =========================
    def _ascii(self, texto: str) -> str:
        return unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode("ascii")

    def _sanitizar_colunas(self, colunas: list) -> dict:
        mapping = {}
        used = set()
        for col in colunas:
            base = self._ascii(str(col)).strip().replace("@", "at")
            base = re.sub(r"[^A-Za-z0-9_]+", "_", base).lower()
            if not base: base = "coluna"
            if not re.match(r"^[a-z_]", base): base = f"_{base}"
            base = re.sub(r"_+", "_", base).strip("_")
            
            final = base
            idx = 2
            while final in used:
                final = f"{base}_{idx}"
                idx += 1
            used.add(final)
            mapping[col] = final
        return mapping

    def _ler_excel_seguro(self, caminho):
        for i in range(3):
            try:
                return pd.read_excel(caminho, dtype=object, engine="openpyxl")
            except Exception as e:
                LOGGER.warning(f"Tentativa {i+1} falhou: {e}")
                time.sleep(1)
        return pd.DataFrame()

    def _upload_staging(self, df, tabela):
        tabela_staging = f"{tabela}_staging"
        df['dt_coleta'] = pd.Timestamp.now(tz='UTC')
        
        pandas_gbq.to_gbq(df, tabela_staging, project_id=PROJECT_ID, if_exists='replace')
        
        client = bigquery.Client(project=PROJECT_ID)
        query = f"CREATE OR REPLACE TABLE `{tabela}` AS SELECT * FROM `{tabela_staging}`"
        client.query(query).result()
        client.query(f"DROP TABLE IF EXISTS `{tabela_staging}`")

if __name__ == "__main__":
    task = AutomationTask()
    task.run()
