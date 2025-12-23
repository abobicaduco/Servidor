# ==============================================================================
# AUTO-INSTALL DEPENDENCIES & CONFIG
# ==============================================================================
import sys
import os
from pathlib import Path
from datetime import datetime, date, timedelta

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
        "playwright"
    ]
    
    bootstrap_deps.install_requirements(extra_libs=SCRIPT_DEPS)
    from config_loader import Config
    
    # Configuração via Loader (Dinâmica DEV/PROD)
    ROOT_DIR = Config.ROOT_DIR
    PROJECT_ID = Config.PROJECT_ID
    # DATASET_ID = Config.DATASET_ID # Not used directly here, using hardcoded FQN for source compatibility unless directed otherwise.
    # User FQNs: datalab-pagamentos.conciliacao_contabil.ACAM220_RAW

except ImportError:
    print("⚠️ Config Loader não encontrado. Usando FALLBACK local.")
    
    # Fallback Hardcoded (Padrão C6 Bank - Assume DEV)
    CONFIG_LOADER = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "novo_servidor"
    PROJECT_ID = 'datalab-pagamentos'
    # DATASET_ID = 'ADMINISTRACAO_CELULA_PYTHON'

# ==============================================================================
# CONSTANTES DO SCRIPT
# ==============================================================================
SCRIPT_NAME = Path(__file__).stem.lower()
START_TIME = datetime.now().replace(microsecond=0)

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
import pandas as pd
import pandas_gbq
from win32com.client import Dispatch
from google.cloud import bigquery
from pydata_google_auth import cache, get_user_credentials
from playwright.sync_api import sync_playwright

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
        self.TABELA_FQN = "datalab-pagamentos.conciliacao_contabil.ACAM220_RAW"
        self.URL_ACAM220 = "https://exchange-cambio.prod.core.gondor.infra/WEB/BcACAM220.aspx"
        self.LAG_DIAS_DOWNLOAD = 2
        
        self.SELECTORS = {
            "data_inicio": "#CdData1",
            "data_fim": "#CdData2",
            "botao_ok": "#Bok",
            "status": "#LSTATUS",
            "botao_salvar": "#BDOWNLOADX",
            "elemento_home": "#BENCARGOS"
        }

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
            
            datas = self._datas_execucao()
            if not datas:
                status = "NO_DATA"
                LOGGER.info("Sem datas para processar.")
            else:
                # Garante coluna
                try: bigquery.Client().query(f"ALTER TABLE `{self.TABELA_FQN}` ADD COLUMN IF NOT EXISTS nome_arquivo STRING").result()
                except: pass
                
                total_inserido = 0
                processados = 0
                
                with sync_playwright() as p:
                    browser = p.chromium.launch(headless=HEADLESS, args=["--start-maximized"])
                    context = browser.new_context(viewport={"width": 1920, "height": 1080}, accept_downloads=True)
                    page = context.new_page()
                    
                    for d in datas:
                        arq = self._baixar_arquivo(page, d)
                        if arq:
                            self.output_files.append(arq)
                            linhas = self._ler_txt_tratar(arq)
                            cnt = self._bq_subir_linhas(linhas, arq.name)
                            total_inserido += cnt
                            processados += 1
                            LOGGER.info(f"Processado {d}: {cnt} linhas")
                        else:
                            LOGGER.warning(f"Falha {d}")
                            
                    browser.close()
                
                if processados > 0: status = "SUCCESS"
                elif not self.output_files: status = "ERROR"
                else: status = "SUCCESS"

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

    def _bq_datas_faltantes(self) -> list:
        try:
            sql = """
            WITH base AS (
            SELECT PARSE_DATE('%Y%m%d', REGEXP_EXTRACT(nome_arquivo, r'ACAM220_(\\d{8})\\.txt')) AS data_arquivo
            FROM `datalab-pagamentos.conciliacao_contabil.ACAM220_RAW`
            WHERE REGEXP_CONTAINS(nome_arquivo, r'ACAM220_\\d{8}\\.txt')
            ),
            periodo_completo AS (
            SELECT dia FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2023-01-01', DATE_SUB(DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 MONTH), INTERVAL 1 DAY), INTERVAL 1 DAY)) AS dia
            )
            SELECT dia FROM periodo_completo p LEFT JOIN base b ON p.dia = b.data_arquivo WHERE b.data_arquivo IS NULL ORDER BY dia
            """
            df = pandas_gbq.read_gbq(sql, project_id=PROJECT_ID)
            return df['dia'].tolist()
        except: return []

    def _datas_execucao(self) -> list:
        # if TEST_MODE: return [date.today()] (Standardize manual override later if needed)
        limite = (datetime.now() - timedelta(days=self.LAG_DIAS_DOWNLOAD)).date()
        faltantes = self._bq_datas_faltantes()
        return sorted([d for d in faltantes if isinstance(d, date) and d <= limite])

    def _ler_txt_tratar(self, caminho: Path) -> list:
        texto = caminho.read_text(encoding="utf-8", errors="ignore")
        trocado = texto.replace(",", ".")
        if trocado != texto: caminho.write_text(trocado, encoding="utf-8")
        return [ln.strip() for ln in trocado.splitlines() if ln.strip()]

    def _bq_subir_linhas(self, linhas: list, nome_arquivo: str) -> int:
        if not linhas: return 0
        client = bigquery.Client(project=PROJECT_ID, credentials=CREDENTIALS)
        
        check = client.query(f"SELECT COUNT(1) as qtd FROM `{self.TABELA_FQN}` WHERE nome_arquivo = '{nome_arquivo}'")
        if list(check.result())[0]['qtd'] > 0:
            LOGGER.warning(f"Arquivo pré-existente: {nome_arquivo}")
            return 0

        rows = [{"string_field_0": ln, "nome_arquivo": nome_arquivo} for ln in linhas]
        schema = [bigquery.SchemaField("string_field_0", "STRING"), bigquery.SchemaField("nome_arquivo", "STRING")]
        job_config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_APPEND")
        client.load_table_from_json(rows, self.TABELA_FQN, job_config=job_config).result()
        return len(rows)

    def _baixar_arquivo(self, page, data):
        try:
            try: page.goto(self.URL_ACAM220, timeout=10000)
            except: pass
            
            LOGGER.info("Verificando login...")
            for _ in range(30):
                if page.locator(self.SELECTORS["data_inicio"]).is_visible(): break
                if page.locator(self.SELECTORS["elemento_home"]).is_visible():
                    page.goto(self.URL_ACAM220); time.sleep(2); continue
                time.sleep(2)
            else:
                LOGGER.warning("Timeout Login/Home"); return None

            d_str = data.strftime("%d/%m/%Y")
            page.fill(self.SELECTORS["data_inicio"], d_str)
            page.fill(self.SELECTORS["data_fim"], d_str)
            page.click(self.SELECTORS["botao_ok"])
            
            LOGGER.info(f"Esperando {d_str}...")
            for _ in range(60):
                if "arquivo gerado" in page.inner_text(self.SELECTORS["status"]).lower(): break
                page.wait_for_timeout(1000)
            else:
                LOGGER.warning(f"Timeout geracao {d_str}")
                return None
                
            with page.expect_download(timeout=60000) as info:
                page.click(self.SELECTORS["botao_salvar"])
            
            dl = info.value
            fname = f"ACAM220_{data.strftime('%Y%m%d')}.txt"
            dest = TEMP_DIR / fname
            dl.save_as(str(dest))
            return dest if dest.exists() else None

        except Exception as e:
            LOGGER.error(f"Erro baixar {data}: {e}")
            return None

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
