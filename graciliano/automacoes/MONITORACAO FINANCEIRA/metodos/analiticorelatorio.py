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
        self.COL_MAP_DESTINO = {
            "Id_Conta": "id_conta", "Id_TipoTransacaoLojista": "id_tipo_transacao_lojista",
            "Id_EventoCompra": "id_evento_compra", "TransacoesLojistas": "transacoes_lojistas",
            "DescricaoProduto": "descricao_produto", "Bandeira": "bandeira",
            "ReferenceNumber": "reference_number", "ARN": "arn",
            "DataMovimento": "data_movimento", "Parcela": "parcela",
            "ValorParcela": "valor_parcela", "ValorContrato": "valor_contrato",
            "Tipo": "tipo", "nome_arquivo": "nome_arquivo",
        }
        self.COLS_DESTINO_ORDEM = [
            "id_conta", "id_tipo_transacao_lojista", "id_evento_compra", "transacoes_lojistas",
            "descricao_produto", "bandeira", "reference_number", "arn", "data_movimento",
            "parcela", "valor_parcela", "valor_contrato", "tipo", "nome_arquivo"
        ]

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
            base_dir = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "automacoes"
            if not base_dir.exists(): base_dir = Path.home() / "graciliano" / "automacoes"
            
            INPUT_DIR = base_dir / GLOBAL_CONFIG['area_name'] / "arquivos input" / "LOJISTA"
            BQ_TABLE_NEGOCIO = f"{PROJECT_ID}.{DATASET_ID}.dockAnalitico"
            BQ_TABLE_STAGING = f"{BQ_TABLE_NEGOCIO}_staging"

            arquivos = [f for f in INPUT_DIR.glob("*") if f.suffix.lower() in ['.xlsx', '.xls', '.csv']]
            
            if not arquivos:
                status = "NO_DATA"
                LOGGER.info("Sem arquivos input.")
            else:
                try:
                    q = f"SELECT DISTINCT nome_arquivo FROM `{BQ_TABLE_NEGOCIO}`"
                    df_check = pandas_gbq.read_gbq(q, project_id=PROJECT_ID)
                    ja_processados = set(df_check['nome_arquivo'].dropna().tolist())
                except: ja_processados = set()

                dfs = []
                for arq in arquivos:
                    dest = TEMP_DIR / f"{arq.stem}_{datetime.now().strftime('%H%M%S')}{arq.suffix}"
                    if arq.name in ja_processados:
                        LOGGER.info(f"Skip {arq.name} (já processado)")
                        self._move_safe(arq, dest)
                        self.output_files.append(dest)
                    else:
                        df = self._ler_arquivo(arq)
                        if df is not None:
                            df_clean = self._tratar_dataframe_bq(df)
                            dfs.append(df_clean)
                            self._move_safe(arq, dest)
                            self.output_files.append(dest)
                
                if dfs:
                    full = pd.concat(dfs, ignore_index=True)
                    self._upload_bq(full, BQ_TABLE_STAGING, BQ_TABLE_NEGOCIO)
                    status = "SUCCESS"
                elif status != "NO_DATA":
                    status = "SUCCESS" # Tecnicamente sucesso se só tinha duplicados

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
        expected = [
            "Id_Conta", "Id_TipoTransacaoLojista", "Id_EventoCompra", "TransacoesLojistas",
            "DescricaoProduto", "Bandeira", "ReferenceNumber", "ARN", "DataMovimento",
            "Parcela", "ValorParcela", "ValorContrato", "Tipo"
        ]
        aliases = {
            "idconta": "Id_Conta", "conta": "Id_Conta", "contaid": "Id_Conta",
            "idcontalojista": "Id_Conta", "idcontadolojista": "Id_Conta",
            "idtipotransacaolojista": "Id_TipoTransacaoLojista",
            "ideventocompra": "Id_EventoCompra", "transacoeslojistas": "TransacoesLojistas",
            "descricaoproduto": "DescricaoProduto", "bandeira": "Bandeira",
            "referencenumber": "ReferenceNumber", "arn": "ARN",
            "datamovimento": "DataMovimento", "parcela": "Parcela",
            "valorparcela": "ValorParcela", "valorcontrato": "ValorContrato", "tipo": "Tipo",
        }
        def _norm_col(c):
            s = str(c).strip()
            s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
            return re.sub(r"[^a-zA-Z0-9]+", "", s).lower()

        try:
            self._sanitizar_excel(p)
            df = None
            if p.suffix.lower() == ".csv":
                try: df = pd.read_csv(self._win_long_path(p), sep=";", encoding="latin1", dtype=str)
                except: df = pd.read_csv(self._win_long_path(p), sep=",", encoding="latin1", dtype=str)
            else:
                df = pd.read_excel(self._win_long_path(p), dtype=str, engine="openpyxl")
                
            if df is None or df.empty: return None
            
            cols_map = {}
            for c in df.columns:
                nc = _norm_col(c)
                if nc in aliases: cols_map[c] = aliases[nc]
            df.rename(columns=cols_map, inplace=True)
            
            if "Id_Conta" not in df.columns and not df.empty: df["Id_Conta"] = df.iloc[:, 0]
            for c in expected:
                if c not in df.columns: df[c] = None
                
            df = df[expected].copy()
            df["nome_arquivo"] = p.name
            return df
        except Exception as e:
            LOGGER.error(f"Erro ler {p.name}: {e}")
            return None

    def _tratar_dataframe_bq(self, df: pd.DataFrame) -> pd.DataFrame:
        df_bq = df.copy()
        df_bq.rename(columns=self.COL_MAP_DESTINO, inplace=True)
        for c in self.COLS_DESTINO_ORDEM:
            if c not in df_bq.columns: df_bq[c] = None
        df_bq = df_bq[self.COLS_DESTINO_ORDEM].copy()
        
        def parse_date(d):
            s = str(d).strip()
            if not s or s.lower() == 'nan': return None
            for fmt in ["%m/%d/%Y %H:%M:%S", "%d/%m/%Y", "%Y-%m-%d"]:
                try: return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
                except: pass
            return s[:10]
        def norm_dec(x):
            if not x: return None
            s = str(x).strip().replace("R$", "").replace(" ", "")
            if not s or s.lower() == 'nan': return None
            return s.replace(".", "").replace(",", ".")

        df_bq["data_movimento"] = df_bq["data_movimento"].apply(parse_date)
        df_bq["valor_parcela"] = df_bq["valor_parcela"].apply(norm_dec)
        df_bq["valor_contrato"] = df_bq["valor_contrato"].apply(norm_dec)
        return df_bq

    def _upload_bq(self, df, staging, target):
        pandas_gbq.to_gbq(df, staging, project_id=PROJECT_ID, if_exists="replace")
        sql = f"""
        INSERT INTO `{target}`
        SELECT * FROM `{staging}` S
        WHERE NOT EXISTS (
            SELECT 1 FROM `{target}` F
            WHERE TO_JSON_STRING(S) = TO_JSON_STRING(F)
        )
        """
        client = bigquery.Client(project=PROJECT_ID, credentials=CREDENTIALS)
        client.query(sql).result()

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