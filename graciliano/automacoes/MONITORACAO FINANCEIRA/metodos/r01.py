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
        self.TABELA_ALVO = f"{PROJECT_ID}.{self.DATASET_NEGOCIO}.ARQUIVO_REPASSE_FGTS_R01_ATUALIZADO"
        
        # File specs
        self.SPECS=[(0,1),(1,10),(10,60),(60,71),(71,79),(79,96),(96,104),(104,110),(110,111),(111,112),(112,113),(131,139),(139,180)]
        self.NAMES=["Tipo","Seq","ID","CPF","DataPag","Valor","DataPed","HoraPed","TipoOp","Canal","Prot","DataPrev","Res"]
        
        self.SCHEMA_R01=[
            {"name":"tipo_de_registro","type":"STRING"},
            {"name":"sequencial_do_registro","type":"STRING"},
            {"name":"identificador_da_solicitacao_do_pedido_de_garantia","type":"STRING"},
            {"name":"numero_do_cpf_do_trabalhador","type":"STRING"},
            {"name":"data_efetiva_de_pagamento","type":"STRING"},
            {"name":"valor_repasse","type":"STRING"},
            {"name":"data_do_pedido_de_garantia","type":"STRING"},
            {"name":"hora_do_pedido_de_garantia","type":"STRING"},
            {"name":"tipo_de_operacao_fiduciaria","type":"STRING"},
            {"name":"canal_de_solicitacao","type":"STRING"},
            {"name":"numero_do_protocolo_do_pedido_de_garantia","type":"STRING"},
            {"name":"data_prevista_de_repasse","type":"STRING"},
            {"name":"area_reservada","type":"STRING"},
            {"name":"nome_arquivo","type":"STRING"}
        ]
        
        self.DIR_R01 = Path.home() / "Meu Drive" / "C6 CTVM" / "BKO FINANCEIRO - R01/DOIDERA_R01"
        # Fallback path if drive not mounted standard way
        if not self.DIR_R01.exists():
             self.DIR_R01 = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "BKO FINANCEIRO - R01/DOIDERA_R01"

        self.IN_DIR = self.DIR_R01 / "HAVOC_novo"
        self.OUT_DIR = self.DIR_R01 / "xlsx_final"
        self.PASTA_IN = self.DIR_R01 / "faltam_subir"
        self.PASTA_DONE = self.DIR_R01 / "subida_ja_tratado"

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
            
            # Ensure Dirs
            for p in [self.IN_DIR, self.OUT_DIR, self.PASTA_IN, self.PASTA_DONE]:
                p.mkdir(parents=True, exist_ok=True)
            
            # 1. Process Raw
            self._processar_raw()
            
            # 2. Upload Pending
            total_linhas = self._upload_pending()
            
            if total_linhas > 0: status = "SUCCESS"
            else: status = "NO_DATA"

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

    def _processar_raw(self):
        arquivos = list(self.IN_DIR.glob("*"))
        if not arquivos:
            LOGGER.info("Nenhum arquivo RAW encontrado.")
            return

        for f in arquivos:
            try:
                df = pd.read_fwf(f, colspecs=self.SPECS, header=None, names=self.NAMES, dtype=str)
                df = df[~df["ID"].str.contains("FGTSRepasse", na=False)]
                df["Valor"] = pd.to_numeric(df["Valor"], errors="coerce") / 100
                df["NOME_ARQUIVO"] = f.name
                
                out_file = self.OUT_DIR / f"{f.stem}.xlsx"
                df.to_excel(out_file, index=False)
                
                pending = self.PASTA_IN / out_file.name
                shutil.copy2(out_file, pending)
                
                try: f.unlink()
                except: pass
                
                LOGGER.info(f"Processado: {f.name}")
            except Exception as e:
                LOGGER.error(f"Erro processar {f.name}: {e}")

    def _upload_pending(self):
        files = list(self.PASTA_IN.glob("*.xlsx"))
        if not files: return 0
        
        total = 0
        # Check Exists
        try:
            q = f"SELECT DISTINCT nome_arquivo FROM `{self.TABELA_ALVO}`"
            df_ex = pandas_gbq.read_gbq(q, project_id=PROJECT_ID)
            existentes = set(df_ex['nome_arquivo'].dropna().tolist())
        except: existentes = set()
        
        for f in files:
            if f.name in existentes:
                LOGGER.info(f"Ja existe: {f.name}")
                try: shutil.move(str(f), str(self.PASTA_DONE / f.name))
                except: f.unlink()
                continue
            
            try:
                df = pd.read_excel(f, dtype=str)
                df["nome_arquivo"] = f.name
                cnt = self._subir_bq(df)
                total += cnt
                
                try: shutil.move(str(f), str(self.PASTA_DONE / f.name))
                except: f.unlink()
                self.output_files.append(self.PASTA_DONE / f.name)
            except Exception as e:
                LOGGER.error(f"Erro upload {f.name}: {e}")
        
        return total

    def _subir_bq(self, df):
        # Normalize
        def norm(c): 
            s = unicodedata.normalize("NFKD", str(c)).encode("ascii", "ignore").decode("ascii").lower()
            s = re.sub(r"[^\w]+", "_", s).strip("_")
            return re.sub(r"_+", "_", s)
        
        df.columns = [norm(c) for c in df.columns]
        df = df.drop_duplicates()
        
        map_cols = {
            "tipo": "tipo_de_registro", "seq": "sequencial_do_registro", "id": "identificador_da_solicitacao_do_pedido_de_garantia",
            "cpf": "numero_do_cpf_do_trabalhador", "datapag": "data_efetiva_de_pagamento", "valor": "valor_repasse",
            "dataped": "data_do_pedido_de_garantia", "horaped": "hora_do_pedido_de_garantia", "tipoop": "tipo_de_operacao_fiduciaria",
            "canal": "canal_de_solicitacao", "prot": "numero_do_protocolo_do_pedido_de_garantia", "dataprev": "data_prevista_de_repasse",
            "res": "area_reservada", "nome_arquivo": "nome_arquivo"
        }
        df = df.rename(columns=map_cols)
        
        final_cols = [c["name"] for c in self.SCHEMA_R01 if c["name"] in df.columns]
        df = df[final_cols]
        if df.empty: return 0
        
        pandas_gbq.to_gbq(df, self.TABELA_ALVO, project_id=PROJECT_ID, if_exists="append")
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