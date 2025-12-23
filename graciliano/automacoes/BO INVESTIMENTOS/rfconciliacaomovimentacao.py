# ==============================================================================
# AUTO-INSTALL DEPENDENCIES & CONFIG
# ==============================================================================
import sys
import os
from pathlib import Path
from datetime import datetime, date

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
        "unidecode"
    ]
    bootstrap_deps.install_requirements(extra_libs=SCRIPT_DEPS)
except: pass

import logging
import time
import shutil
import zipfile
import pythoncom
import re
import decimal
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

GLOBAL_CONFIG = {'area_name': 'BO INVESTIMENTOS', 'emails_principal': [], 'emails_cc': [], 'move_file': False}

# Definicao dos Lotes
LOTES = [
    {
        "nome": "Lote_Principal",
        "subpath": "RFCONCILIACAOMOVIMENTACAO",
        "tabela_destino": "investimentos.RF_ARQUIVO_MOV_B3"
    },
    {
        "nome": "Lote_Backup",
        "subpath": "rfconciliacaomovimentacaobackup",
        "tabela_destino": "investimentos.RF_ARQUIVO_MOV_B3_BKUP_CACATUA"
    }
]

COLUNAS_OBRIGATORIAS_ORIGEM = [
    "Participante_Nome_Simpl",
    "Contraparte_Nome_Simpl",
    "Eventos_cursados_pela_Cetip",
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
        area = GLOBAL_CONFIG.get('area_name', 'BO INVESTIMENTOS')
        
        modo_exec = os.environ.get("ENV_EXEC_MODE", "AGENDAMENTO")
        usuario = os.environ.get("ENV_EXEC_USER", f"{os.getlogin().lower()}@c6bank.com")
        status = "ERROR"
        obs = ""
        total_upload = 0
        
        files_moved = []
        
        try:
            LOGGER.info(">>> INICIO <<<")
            success_count = 0
            found_count = 0

            for lote in LOTES:
                lote_name = lote["nome"]
                path_input = ROOT_DRIVE / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "automacoes" / area / "arquivos input" / lote["subpath"]
                
                # Check Path fallback
                if not path_input.exists():
                     path_input = ROOT_DRIVE / "graciliano" / "automacoes" / area / "arquivos input" / lote["subpath"]
                
                table_dest = lote["tabela_destino"]
                if not table_dest.startswith(PROJECT_ID):
                    table_dest = f"{PROJECT_ID}.{table_dest}"
                
                if not path_input.exists():
                    LOGGER.warning(f"Diretório não encontrado: {path_input}")
                    continue
                
                LOGGER.info(f"--- Processando Lote: {lote_name} em {path_input} ---")
                
                files = [f for f in path_input.iterdir() if f.is_file() and not f.name.startswith("~$") and f.name.lower() not in ["thumbs.db", "desktop.ini"]]
                found_count += len(files)
                
                for f in files:
                    try:
                        LOGGER.info(f"Lendo: {f.name}")
                        df = self._ler_arquivo(f)
                        if df.empty:
                            LOGGER.warning("Arquivo vazio/ilegível")
                            continue
                            
                        # Validate
                        if not self._validar_schema(df):
                            LOGGER.warning("Schema inválido")
                            continue
                            
                        # Transform
                        df_final = self._tratar_dataframe(df)
                        if df_final.empty:
                            LOGGER.warning("DF vazio após tratamento")
                            continue
                            
                        # Upload
                        linhas = self._subir_com_staging_e_dedup(df_final, table_dest)
                        total_upload += linhas
                        success_count += 1
                        
                        # Move
                        dest_log = ROOT_DRIVE / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "automacoes" / area / "logs" / SCRIPT_NAME / datetime.now().strftime("%Y-%m-%d")
                        dest_log.mkdir(parents=True, exist_ok=True)
                        final_dest = dest_log / f"{f.stem}_{datetime.now().strftime('%H%M%S')}{f.suffix}"
                        shutil.move(str(f), str(final_dest))
                        files_moved.append(final_dest)
                        
                    except Exception as e:
                        LOGGER.error(f"Erro arquivo {f.name}: {traceback.format_exc()}")
                        
            if success_count > 0:
                status = "SUCCESS"
                obs = f"Linhas processadas: {total_upload}"
            elif found_count > 0:
                status = "NO_DATA" # Encontrou mas falhou processar
                obs = "Arquivos encontrados mas nenhum processado com sucesso"
            else:
                status = "NO_DATA"
                obs = "Nenhum arquivo encontrado"

        except Exception as e:
            status = "ERROR"
            LOGGER.critical(f"Erro fatal: {e}", exc_info=True)
            
        finally:
            end_time = datetime.now().replace(microsecond=0)
            duration = round((end_time - START_TIME).total_seconds(), 2)
            
            # Add moved files to output list for zip if needed (backup)
            self.output_files.extend(files_moved)
            
            zip_path = self._create_smart_zip()
            self._upload_metrics(status, usuario, modo_exec, end_time, duration)
            self._send_email(status, zip_path, obs)
            
            # Files already moved during process

    def _ler_arquivo(self, caminho):
        df = pd.DataFrame()
        try:
            # Excel
            try: return pd.read_excel(caminho, dtype=str)
            except: pass
            
            # CSV
            for enc in ["utf-8", "latin1", "cp1252"]:
                for sep in [";", ",", "\t", "|"]:
                    try:
                        df = pd.read_csv(caminho, sep=sep, encoding=enc, dtype=str)
                        if not df.empty and len(df.columns) > 1: return self._norm_cols(df)
                    except: continue
        except: pass
        return df

    def _norm_cols(self, df):
        cols = []
        for c in df.columns:
            new_c = unidecode(str(c))
            new_c = re.sub(r"[^0-9A-Za-z_]", "_", new_c)
            new_c = re.sub(r"_+", "_", new_c).strip("_")
            cols.append(new_c)
        df.columns = cols
        return df

    def _validar_schema(self, df):
        cols_df = set(self._norm_cols(df).columns)
        # Normalize obligatory list too
        norm_oblig = set()
        for c in COLUNAS_OBRIGATORIAS_ORIGEM:
            n = unidecode(c)
            n = re.sub(r"[^0-9A-Za-z_]", "_", n).strip("_")
            norm_oblig.add(n)
        
        missing = [c for c in norm_oblig if c not in cols_df]
        if missing:
            LOGGER.warning(f"Colunas faltantes: {missing}")
            return False
        return True

    def _tratar_dataframe(self, df):
        df = df.copy()
        df = self._norm_cols(df)
        df = df.loc[:, ~df.columns.str.contains("Unnamed", na=False)]
        
        renames = {
            "Participante_Nome_Simpl": "Participante__Nome_Simpl__",
            "Contraparte_Nome_Simpl": "Contraparte__Nome_Simpl__",
            "Eventos_cursados_pela_Cetip": "Eventos_cursados_pela_Cetip_",
            "Instituicao_Confirmadora_Conta": "Instituicao_Confirmadora_Conta_",
            "Instituicao_Confirmadora_Papel": "Instituicao_Confirmadora_Papel_",
            "ISPB_Liq_Contraparte": "ISPB_Liq__Contraparte",
        }
        # Try exact match or normalized match
        current_cols = df.columns
        final_renames = {}
        for k, v in renames.items():
            # Normalized key
            nk = unidecode(k); nk = re.sub(r"[^0-9A-Za-z_]", "_", nk).strip("_")
            if nk in current_cols:
                final_renames[nk] = v
        
        df.rename(columns=final_renames, inplace=True)
        df = df.replace({"---": None})
        
        # Conversions
        if "Cod_Operacao" in df.columns:
            df["Cod_Operacao"] = pd.to_numeric(df["Cod_Operacao"], errors="coerce").astype("Int64")
            
        for col in ["Quantidade", "PU", "Valor"]:
            if col in df.columns:
                 df[col] = df[col].astype(str).str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
                 df[col] = pd.to_numeric(df[col], errors='coerce')
        
        for col in ["Data_Emissao", "Data_Vencimento", "Data_Liquidacao", "Data_Origem"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], dayfirst=True, errors="coerce").dt.date
        
        df["DT_COLETA"] = datetime.now()
        return df

    def _subir_com_staging_e_dedup(self, df, table):
        if df.empty: return 0
        staging = f"{table}_STAGING_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        # Upload Staging
        df_up = df.copy()
        for col in df_up.columns:
             if pd.api.types.is_object_dtype(df_up[col]):
                 df_up[col] = df_up[col].astype(str).replace("nan", "")
        
        pandas_gbq.to_gbq(df_up, staging, project_id=PROJECT_ID, if_exists='replace')
        
        # Merge/Dedup
        client = bigquery.Client(project=PROJECT_ID)
        # Ensure target exists
        client.query(f"CREATE TABLE IF NOT EXISTS `{table}` AS SELECT * FROM `{PROJECT_ID}.{staging}` WHERE 1=0").result()
        
        sql = f"""
        INSERT INTO `{table}`
        SELECT * FROM `{PROJECT_ID}.{staging}` S
        WHERE NOT EXISTS (
            SELECT 1 FROM `{table}` F
            WHERE TO_JSON_STRING(S) = TO_JSON_STRING(F)
        )
        """
        client.query(sql).result()
        client.delete_table(f"{PROJECT_ID}.{staging}", not_found_ok=True)
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
            pandas_gbq.to_gbq(df, TABLE_EXEC, project_id=PROJECT_ID, if_exists='append')
        except: pass

    def _send_email(self, status, zip_path, obs=""):
        try:
            to = GLOBAL_CONFIG['emails_principal']
            if status=="SUCCESS": to += GLOBAL_CONFIG['emails_cc']
            
            # Add specific hardcoded emails from original script
            EXTRA_EMAILS = ["carlos.lsilva@c6bank.com", "sofia.fernandes@c6bank.com", "antonio.marcoss@c6bank.com"]
            to.extend(EXTRA_EMAILS)
            to = list(set(to))
            if not to: return

            pythoncom.CoInitialize()
            outlook = Dispatch("Outlook.Application")
            mail = outlook.CreateItem(0)
            mail.To = ";".join(to)
            mail.Subject = f"CÉLULA PYTHON MONITORAÇÃO - {SCRIPT_NAME.upper()} - {status}"
            mail.Body = f"Status: {status}\n{obs}"
            if zip_path.exists(): mail.Attachments.Add(str(zip_path))
            mail.Send()
        except: pass

if __name__ == "__main__":
    AutomationTask().run()