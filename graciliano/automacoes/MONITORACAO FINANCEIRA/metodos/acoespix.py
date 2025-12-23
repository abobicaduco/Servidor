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
from zoneinfo import ZoneInfo

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
TZ = ZoneInfo("America/Sao_Paulo")

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
            
            # --- LÓGICA DE NEGÓCIO ---
            arq = self._localizar_arquivo_base_pix_nova()
            if not arq:
                status = "NO_DATA"
                LOGGER.warning("Arquivo base não encontrado.")
            else:
                self.output_files.append(arq)
                df = self._ler_excel_empilhar(arq)
                
                if df.empty:
                    status = "NO_DATA"
                    LOGGER.warning("DataFrame vazio.")
                else:
                    if "dt_coleta" not in df.columns:
                        df["dt_coleta"] = pd.Timestamp.now(tz="UTC")
                    else:
                        df["dt_coleta"] = pd.to_datetime(df["dt_coleta"], utc=True, errors="coerce").fillna(pd.Timestamp.now(tz="UTC"))
                    
                    BQ_DATASET_DEST = "00_temp"
                    BQ_TABLE_NAME = "BASE_COMPLETA_AC_EMPILHADA_ORIGEM_SPT_BKO_FIN_PIX_NOVA"
                    
                    pandas_gbq.to_gbq(
                        df,
                        f"{BQ_DATASET_DEST}.{BQ_TABLE_NAME}",
                        project_id=PROJECT_ID,
                        if_exists="replace"
                    )
                    status = "SUCCESS"
                    LOGGER.info(f"Upload BQ Replace: {len(df)} linhas")

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
    def _localizar_arquivo_base_pix_nova(self):
        padroes = ["*BASE_PIX_NOVA*.xlsx", "*BASE_PIX_NOVA*.xls"]
        achados = []
        
        POSSIBLE_ROOTS = [
            Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A",
            Path.home() / "Meu Drive/C6 CTVM",
            Path.home() / "C6 CTVM",
            Path.home()
        ]
        
        for raiz in POSSIBLE_ROOTS:
            if not raiz.exists(): continue
            try:
                for pad in padroes:
                    for p in raiz.rglob(pad):
                        if p.is_file(): achados.append(p)
            except: pass
            
        if not achados: return None
        return sorted(achados, key=lambda x: x.stat().st_mtime, reverse=True)[0]

    def _ler_excel_empilhar(self, caminho):
        try:
            xls = pd.ExcelFile(caminho, engine="openpyxl")
        except: return pd.DataFrame()

        acumulado = []
        for aba in xls.sheet_names:
            try:
                df = pd.read_excel(xls, sheet_name=aba, dtype=str, engine="openpyxl").fillna("")
                df = df.rename(columns=self._sanitizar_colunas_unicas(df.columns))
                df["nome_worksheet"] = str(aba).lower()
                acumulado.append(df)
            except: pass
            
        if not acumulado: return pd.DataFrame()
        return pd.concat(acumulado, ignore_index=True)

    def _sanitizar_colunas_unicas(self, cols):
        mapping = {}
        used = set()
        for col in cols:
            base = unicodedata.normalize("NFKD", str(col).strip()).encode("ascii", "ignore").decode("ascii")
            base = base.replace("@", "at")
            base = re.sub(r"[^A-Za-z0-9_]+", "_", base).lower()
            if not base: base = "coluna"
            if not re.match(r"^[a-z_]", base): base = f"_{base}"
            base = re.sub(r"_+", "_", base).strip("_")
            
            final = base
            idx = 2
            while final in used:
                final = f"{base}_{idx}"; idx += 1
            used.add(final)
            mapping[col] = final
        return mapping

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
            LOGGER.info("Email enviado")
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
