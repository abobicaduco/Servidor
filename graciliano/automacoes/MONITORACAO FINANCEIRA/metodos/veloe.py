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
        "unidecode"
    ]
    bootstrap_deps.install_requirements(extra_libs=SCRIPT_DEPS)
except: pass

import logging
import time
import shutil
import zipfile
import pythoncom
import traceback
import uuid
import pandas as pd
import pandas_gbq
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
TABELA_ALVO = f"{PROJECT_ID}.conciliacoes_monitoracao.TAG_VELOE"

# Controle de Headless
HEADLESS = False

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
        self.INPUT_DIR = ROOT_DRIVE / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "automacoes" / GLOBAL_CONFIG['area_name'] / "arquivos input" / "veloe"

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
        # Path Correction
        area = GLOBAL_CONFIG.get('area_name', 'MONITORACAO FINANCEIRA')
        self.INPUT_DIR = ROOT_DRIVE / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "automacoes" / area / "arquivos input" / "veloe"
        if not self.INPUT_DIR.exists():
            self.INPUT_DIR = ROOT_DRIVE / "graciliano" / "automacoes" / area / "arquivos input" / "veloe"
            self.INPUT_DIR.mkdir(parents=True, exist_ok=True)

        modo_exec = os.environ.get("ENV_EXEC_MODE", "AGENDAMENTO")
        usuario = os.environ.get("ENV_EXEC_USER", f"{os.getlogin().lower()}@c6bank.com")
        status = "ERROR"
        total_inseridas = 0
        detalhes_msg = ""
        
        try:
            LOGGER.info(">>> INICIO <<<")
            
            arquivos = sorted(self.INPUT_DIR.glob("*.csv"))
            if not arquivos:
                LOGGER.warning("Nenhum arquivo encontrado.")
                status = "NO_DATA"
            else:
                for arq in arquivos:
                    try:
                        df = self._processar_dataframe(arq)
                        df_filtrado, detalhes = self._filtrar_existentes(df)
                        
                        if detalhes:
                            detalhes_msg += f"\n{arq.name}: Duplicatas filtradas."
                        
                        if not df_filtrado.empty:
                            inseridas = self._subir_com_staging_dedup(df_filtrado)
                            total_inseridas += inseridas
                            if inseridas > 0:
                                self.output_files.append(arq)
                        else:
                            LOGGER.info(f"{arq.name}: Sem linhas novas.")
                            self.output_files.append(arq)
                            
                    except Exception as e:
                        LOGGER.error(f"Erro ao processar {arq.name}: {e}")
                
                if total_inseridas > 0:
                    self._rodar_procedure()
                    status = "SUCCESS"
                else:
                    status = "SUCCESS" # Sucesso técnico mesmo sem dados novos

        except Exception as e:
            status = "ERROR"
            LOGGER.critical(f"Erro fatal: {e}", exc_info=True)
            
        finally:
            end_time = datetime.now().replace(microsecond=0)
            duration = round((end_time - START_TIME).total_seconds(), 2)
            
            zip_path = self._create_smart_zip()
            self._upload_metrics(status, usuario, modo_exec, end_time, duration)
            self._send_email(status, zip_path, total_inseridas, detalhes_msg)
            
            if GLOBAL_CONFIG['move_file'] and status == "SUCCESS":
                 self._move_files_to_network(zip_path)

    def _processar_dataframe(self, arq: Path) -> pd.DataFrame:
        LOGGER.info(f"Lendo {arq.name}")
        try:
            df = pd.read_csv(arq, sep=";", encoding="utf-8")
            
            drop_cols = ["EIXO_CADASTRADO", "EIXO_COBRADO", "HORA_SAIDA", "PERMANENCIA", "AGENCIA", "INFORMACAO_1", "INFORMACAO_2", "INFORMACAO_3", "ID_VELOE"]
            df.drop(columns=[c for c in drop_cols if c in df], inplace=True, errors="ignore")
            
            date_cols = ["DATA_PROCESSAMENTO", "DATA_UTILIZACAO", "DATA_EMISSAO_FATURA", "DATA_VENCIMENTO"]
            for c in date_cols:
                if c in df:
                    df[c] = pd.to_datetime(df[c], errors="coerce").dt.strftime("%d/%m/%Y")
                    
            str_cols = ["VELOE_TAG", "CPF/CNPJ", "CONTA", "ID_TRANSACAO", "PRACA", "PISTA", "CONCESSIONARIAID", "TOTAL_FATURA", "BILLING"]
            for c in str_cols:
                if c in df:
                    df[c] = df[c].astype(str).str.rstrip(".0")
                    
            if "CPF/CNPJ" in df:
                df.rename(columns={"CPF/CNPJ": "CPF"}, inplace=True)
                
            cols_order = ["CPF", "CONTA", "ID_TRANSACAO", "PLACA", "VELOE_TAG", "DATA_UTILIZACAO", "HORA_ENTRADA", "DATA_PROCESSAMENTO", "HORA_PROCESSAMENTO", "ESTABELECIMENTO", "ENDERECO", "VALOR", "TIPO_MOVIMENTO", "TIPO_TRANSACAO", "PRACA", "PISTA", "CONCESSIONARIAID", "BILLING", "FAST_ID", "DATA_EMISSAO_FATURA", "DATA_VENCIMENTO", "TOTAL_FATURA"]
            
            final_cols = [c for c in cols_order if c in df.columns]
            df = df[final_cols]
            
            LOGGER.info(f"DataFrame processado: {len(df)} linhas.")
            return df
        except Exception as e:
            LOGGER.error(f"Erro ao processar dataframe {arq.name}: {e}")
            return pd.DataFrame()

    def _filtrar_existentes(self, df: pd.DataFrame) -> tuple:
        if "DATA_VENCIMENTO" not in df.columns or df.empty: return df, []
        
        datas = sorted(list(set(df["DATA_VENCIMENTO"].dropna().astype(str))))
        if not datas: return df, []
        
        try:
            client = bigquery.Client(project=PROJECT_ID, credentials=CREDENTIALS)
            query = f"""
                SELECT DATA_VENCIMENTO, COUNT(1) as qtd 
                FROM `{TABELA_ALVO}` 
                WHERE DATA_VENCIMENTO IN UNNEST(@datas)
                GROUP BY DATA_VENCIMENTO
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[bigquery.ArrayQueryParameter("datas", "STRING", datas)]
            )
            rows = list(client.query(query, job_config=job_config).result())
            
            mapa = {str(r["DATA_VENCIMENTO"]): r["qtd"] for r in rows}
            if not mapa: return df, []
            
            df_filtrado = df[~df["DATA_VENCIMENTO"].isin(mapa.keys())].copy()
            detalhes = [{"data_vencimento": k, "qt_bq": v} for k,v in mapa.items()]
            
            LOGGER.info(f"Filtrados {len(df)-len(df_filtrado)} registros já existentes.")
            return df_filtrado, detalhes
        except Exception as e:
            LOGGER.error(f"Erro na filtragem BigQuery: {e}")
            return pd.DataFrame(), []

    def _subir_com_staging_dedup(self, df: pd.DataFrame) -> int:
        if df.empty: return 0
        tabela_staging = f"{TABELA_ALVO}_STAGING_{uuid.uuid4().hex[:8]}"
        try:
            LOGGER.info(f"Subindo staging table: {tabela_staging}")
            pandas_gbq.to_gbq(df, tabela_staging, project_id=PROJECT_ID, if_exists='replace')
            
            client = bigquery.Client(project=PROJECT_ID)
            table_ref = client.get_table(tabela_staging)
            cols = [f.name for f in table_ref.schema]
            cols_str = ", ".join([f"`{c}`" for c in cols])
            
            sql = f"""
                INSERT INTO `{TABELA_ALVO}` ({cols_str})
                SELECT {cols_str}
                FROM `{tabela_staging}` S
                WHERE NOT EXISTS (
                    SELECT 1 FROM `{TABELA_ALVO}` T
                    WHERE TO_JSON_STRING(T) = TO_JSON_STRING(S)
                )
            """
            job = client.query(sql)
            job.result()
            inseridas = job.num_dml_affected_rows or 0
            LOGGER.info(f"Inseridas {inseridas} novas linhas.")
            client.delete_table(tabela_staging, not_found_ok=True)
            return inseridas
        except Exception as e:
            LOGGER.error(f"Erro no upload/dedup: {e}")
            try:
                 client = bigquery.Client(project=PROJECT_ID)
                 client.delete_table(tabela_staging, not_found_ok=True)
            except: pass
            return 0

    def _rodar_procedure(self):
        try:
            client = bigquery.Client(project=PROJECT_ID, credentials=CREDENTIALS)
            client.query("CALL `datalab-pagamentos.TAG.VELOE`()").result()
            LOGGER.info("Procedure TAG.VELOE executada.")
        except Exception as e:
            LOGGER.warning(f"Erro ao executar procedure: {e}")

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

    def _send_email(self, status, zip_path, total, msg):
        try:
            to = GLOBAL_CONFIG['emails_principal']
            if status=="SUCCESS": to += GLOBAL_CONFIG['emails_cc']
            if not to: return

            pythoncom.CoInitialize()
            outlook = Dispatch("Outlook.Application")
            mail = outlook.CreateItem(0)
            mail.To = ";".join(set(to))
            mail.Subject = f"CÉLULA PYTHON MONITORAÇÃO - {SCRIPT_NAME.upper()} - {status}"
            mail.Body = f"Status: {status}\nLinhas Inseridas: {total}\n{msg}"
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
