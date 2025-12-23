# ==============================================================================
# AUTO-INSTALL DEPENDENCIES & CONFIG
# ==============================================================================
import sys
import os
import shutil
import traceback
import logging
import zipfile
import time
import re
import tempfile
import pandas as pd
import pandas_gbq
import win32com.client as win32
from pathlib import Path
from datetime import datetime

# Adiciona diretório raiz ao path (Híbrido: Relativo ou Padrão C6)
CONFIG_LOADER = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "novo_servidor" / "config_loader.py"
project_root = None

# 1. Tenta achar o root relativo
try:
    current_path = Path(__file__).resolve()
    for parent in current_path.parents:
        if (parent / "novo_servidor").exists():
            project_root = parent / "novo_servidor"
            break
except:
    pass

# 2. Se não achou relativo, aponta para o caminho padrão da rede
if not project_root:
    standard_root = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano"
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
        "openpyxl",
        "playwright",
        "google-cloud-bigquery"
    ]
    
    bootstrap_deps.install_requirements(extra_libs=SCRIPT_DEPS)
    from config_loader import Config
    
    # Configuração via Loader (Dinâmica DEV/PROD)
    ROOT_DIR = Config.ROOT_DIR
    PROJECT_ID = Config.PROJECT_ID
    DATASET_ID = Config.DATASET_ID

    # Import Dollynho Seguro
    try:
        from modules import dollynho
    except ImportError:
        dollynho = None

except ImportError:
    print("⚠️ Config Loader não encontrado. Usando FALLBACK local.")
    
    # Fallback Hardcoded
    ROOT_DIR = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano"
    
    class Config:
        PROJECT_ID = 'datalab-pagamentos'
        DATASET_ID = 'ADMINISTRACAO_CELULA_PYTHON'
        ROOT_DIR = ROOT_DIR
        TABLE_CONFIG = f"{PROJECT_ID}.{DATASET_ID}.registro_automacoes"
        TABLE_EXEC = f"{PROJECT_ID}.{DATASET_ID}.automacoes_exec"
        COMPANY_DOMAIN = "c6bank.com"
    
    dollynho = None

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

# ==============================================================================
# CONSTANTES DO SCRIPT
# ==============================================================================
SCRIPT_NAME = Path(__file__).stem.upper()
SCRIPT_NAME_LOWER = Path(__file__).stem.lower()
AREA_NAME = "CONCILIACAO FINANCEIRA" # Ajustado conforme original
START_TIME = datetime.now().replace(microsecond=0)

# Controle de Headless
HEADLESS = False

# Diretórios
TEMP_DIR = Path(os.getenv('TEMP', Path.home())) / "C6_RPA_EXEC" / SCRIPT_NAME
TEMP_DIR.mkdir(parents=True, exist_ok=True)

# Configuração Global
GLOBAL_CONFIG = {
    'area_name': AREA_NAME, 
    'emails_principal': [], 
    'emails_cc': [], 
    'move_file': False
}

# Logger Setup
LOG_FILE = TEMP_DIR / f"{SCRIPT_NAME}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding='utf-8', mode='w')
    ]
)
LOGGER = logging.getLogger(SCRIPT_NAME)

# Query de Negócio
QUERY_NEGOCIO = """
SELECT
  ACCOUNT_ID,
  STATUS_CONTA_DOCK
FROM `datalab-pagamentos.conciliacoes_monitoracao.DASH_CDB_JUB_GARANTIAS`
WHERE DS_STATUS_CONTA IN ('Creliq','Acordo Creliq','Acordo Perda','Perda')
  AND LIMITE_CARTAO=0
  AND status_contrato_cob IN ('Liquidado')
"""

# Sessão Playwright
PLAYWRIGHT_SESSION_DIR = Path.home() / "AppData" / "Local" / "PLAYWRIGHT_SESSIONS"
PLAYWRIGHT_SESSION_FILE = PLAYWRIGHT_SESSION_DIR / f"{SCRIPT_NAME_LOWER}.json"

# ==============================================================================
# FUNÇÕES DE SUPORTE
# ==============================================================================

def get_env_info():
    """Detecta ambiente e usuário de execução"""
    modo = os.environ.get("ENV_EXEC_MODE", "AGENDAMENTO")
    user = os.environ.get("ENV_EXEC_USER")
    
    if not user:
        try:
            user = f"{os.getlogin().lower()}@{Config.COMPANY_DOMAIN}"
        except:
            user = "system@c6bank.com"
            
    return modo, user

def load_configs():
    """Carrega configurações da tabela registro_automacoes"""
    try:
        LOGGER.info(f"Carregando configurações para: {SCRIPT_NAME}...")
        query = f"""
            SELECT emails_principal, emails_cc, move_file 
            FROM `{Config.TABLE_CONFIG}`
            WHERE lower(TRIM(script_name)) = lower('{SCRIPT_NAME_LOWER}')
            AND (is_active IS NULL OR lower(is_active) = 'true')
            ORDER BY created_at DESC LIMIT 1
        """
        try:
            df = pandas_gbq.read_gbq(query, project_id=Config.PROJECT_ID)
        except Exception:
            # Fallback area name se script não encontrado
            query = query.replace(f"lower('{SCRIPT_NAME_LOWER}')", f"lower('{AREA_NAME.lower()}')")
            df = pandas_gbq.read_gbq(query, project_id=Config.PROJECT_ID)

        if not df.empty:
            GLOBAL_CONFIG['emails_principal'] = [x.strip() for x in str(df.iloc[0]['emails_principal']).replace(';', ',').split(',') if '@' in x]
            GLOBAL_CONFIG['emails_cc'] = [x.strip() for x in str(df.iloc[0]['emails_cc']).replace(';', ',').split(',') if '@' in x]
            GLOBAL_CONFIG['move_file'] = bool(df.iloc[0]['move_file']) if pd.notna(df.iloc[0]['move_file']) else False
            LOGGER.info(f"Configs carregadas. Move File: {GLOBAL_CONFIG['move_file']}")
        else:
            LOGGER.warning("Configs não encontradas no BigQuery. Usando padrões locais.")
            GLOBAL_CONFIG['emails_principal'] = ["carlos.lsilva@c6bank.com"]
            
    except Exception as e:
        LOGGER.error(f"Erro ao carregar configs: {e}")
        GLOBAL_CONFIG['emails_principal'] = ["carlos.lsilva@c6bank.com"]

def smart_zip_logs(output_files):
    """Gera zip com logs e outputs respeitando limite de 15MB"""
    try:
        data_str = datetime.now().strftime('%Y-%m-%d')
        network_log_dir = Config.ROOT_DIR / "automacoes" / AREA_NAME / "logs" / SCRIPT_NAME / data_str
        network_log_dir.mkdir(parents=True, exist_ok=True)
        
        zip_name = f"{SCRIPT_NAME}_{datetime.now().strftime('%H%M%S')}.zip"
        zip_path = network_log_dir / zip_name
        
        limit_bytes = 15 * 1024 * 1024 # 15MB
        current_size = 0
        
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            if LOG_FILE.exists():
                zf.write(LOG_FILE, arcname=LOG_FILE.name)
                current_size += LOG_FILE.stat().st_size
            
            for file_path in output_files:
                f_path = Path(file_path)
                if not f_path.exists(): continue
                
                f_size = f_path.stat().st_size
                if current_size + f_size < limit_bytes:
                    zf.write(f_path, arcname=f_path.name)
                    current_size += f_size
                else:
                    msg = f"Arquivo {f_path.name} ignorado pois excede o limite de 15MB total."
                    zf.writestr(f"AVISO_{f_path.name}.txt", msg)
                    LOGGER.warning(msg)
                    
        LOGGER.info(f"Zip gerado com sucesso: {zip_path}")
        return str(zip_path)
    except Exception as e:
        LOGGER.error(f"Erro ao gerar zip: {e}")
        return None

def send_email(status, zip_path):
    """Envia email via Outlook"""
    try:
        outlook = win32.Dispatch('outlook.application')
        mail = outlook.CreateItem(0)
        
        subject_status = "SUCESSO" if status == "SUCCESS" else ("FALHA" if status == "ERROR" else status)
        mail.Subject = f"CÉLULA PYTHON MONITORAÇÃO - {SCRIPT_NAME.upper()} - {subject_status}"
        mail.Body = "" 
        
        recipients = GLOBAL_CONFIG['emails_principal']
        if status == "SUCCESS":
            recipients += GLOBAL_CONFIG['emails_cc']
            
        if not recipients:
            LOGGER.warning("Sem destinatários configurados para envio de email.")
            return

        mail.To = ";".join(list(set(recipients)))
        
        if zip_path and Path(zip_path).exists():
            mail.Attachments.Add(str(zip_path))
            
        mail.Send()
        LOGGER.info(f"Email enviado para: {mail.To}")
    except Exception as e:
        LOGGER.error(f"Erro ao enviar email: {e}")

def upload_metrics(start_time, end_time, status):
    """Sobe métricas para tabela automacoes_exec"""
    try:
        modo, user = get_env_info()
        duration = round((end_time - start_time).total_seconds(), 2)
        
        metrics_data = {
            'script_name': [SCRIPT_NAME],
            'area_name': [AREA_NAME],
            'start_time': [start_time],
            'end_time': [end_time],
            'duration_seconds': [duration],
            'status': [status],
            'usuario': [user],
            'modo_exec': [modo]
        }
        
        df_metrics = pd.DataFrame(metrics_data)
        pandas_gbq.to_gbq(
            df_metrics, 
            Config.TABLE_EXEC, 
            project_id=Config.PROJECT_ID, 
            if_exists='append'
            # use_bqstorage_api=False # Removido para compatibilidade
        )
        LOGGER.info("Métricas registradas com sucesso.")
    except Exception as e:
        LOGGER.error(f"Erro ao subir métricas: {e}")

# ==============================================================================
# LÓGICA DE NEGÓCIO - RUNDECK AUTOMATION
# ==============================================================================

class RundeckAutomation:
    @staticmethod
    def run(csv_path: Path, download_dir: Path) -> Path:
        url_job = "https://tasks.corp/project/attfincards/job/show/a442eba8-ad26-4dfc-b009-6b87a48b7050"
        
        PLAYWRIGHT_SESSION_DIR.mkdir(parents=True, exist_ok=True)
        download_dir.mkdir(parents=True, exist_ok=True)

        with sync_playwright() as p:
            LOGGER.info(f"PLAYWRIGHT: Iniciando browser (Headless: {HEADLESS})")
            # Argumentos padrão C6 para automação
            browser = p.chromium.launch(channel="chrome", headless=HEADLESS, args=["--start-maximized"])
            
            # Carrega sessão se existir
            if PLAYWRIGHT_SESSION_FILE.exists():
                LOGGER.info(f"PLAYWRIGHT: Carregando sessão de {PLAYWRIGHT_SESSION_FILE}")
                context = browser.new_context(
                    viewport={"width": 1920, "height": 1080},
                    accept_downloads=True,
                    storage_state=str(PLAYWRIGHT_SESSION_FILE)
                )
            else:
                context = browser.new_context(
                    viewport={"width": 1920, "height": 1080},
                    accept_downloads=True
                )
                
            page = context.new_page()
            page.set_default_timeout(60000)

            try:
                LOGGER.info(f"RUNDECK: Navegando para {url_job}")
                page.goto(url_job)

                # Verifica Login
                precisa_login = False
                try:
                    # Verifica elemento de login
                    if page.locator("#login").is_visible(timeout=5000):
                        precisa_login = True
                    elif "job/show" not in page.url:
                        precisa_login = True
                except:
                    precisa_login = True

                if precisa_login:
                    LOGGER.info("RUNDECK: Realizando login...")
                    
                    # Recuperação de Credenciais via Dollynho
                    user_site = None
                    pass_site = None
                    
                    if dollynho:
                        try:
                            # Tenta pegar credencial do próprio script
                            cred = dollynho.get_credencial(SCRIPT_NAME)
                            if isinstance(cred, (tuple, list)) and len(cred) >= 2:
                                user_site, pass_site = cred[0], cred[1]
                            elif isinstance(cred, str):
                                # Se retornar só senha
                                user_site = os.getlogin()
                                pass_site = cred
                        except Exception as e:
                            LOGGER.warning(f"Erro ao obter credencial Dollynho: {e}")
                    
                    if not user_site or not pass_site:
                        LOGGER.error("Credenciais não encontradas no Dollynho.")
                        raise ValueError("Credenciais obrigatórias para Rundeck não encontradas.")

                    page.fill("#login", user_site)
                    page.fill("#password", pass_site)
                    page.press("#password", "Enter")
                    page.wait_for_load_state("networkidle")
                    
                    # Salva Sessão após login com sucesso
                    context.storage_state(path=str(PLAYWRIGHT_SESSION_FILE))
                    LOGGER.info("RUNDECK: Sessão salva com sucesso.")
                    
                    if url_job not in page.url:
                        page.goto(url_job)

                # Upload Arquivo
                LOGGER.info("RUNDECK: Realizando upload do CSV.")
                input_selector = "input[type='file'][id$='_CSV']"
                page.wait_for_selector(input_selector, state="attached")
                page.set_input_files(input_selector, str(csv_path))

                # Executar Job
                LOGGER.info("RUNDECK: Clicando em Executar.")
                page.click("#execFormRunButton")

                # Monitoração do Status
                inicio_wait = time.time()
                estado_final = None
                while time.time() - inicio_wait <= 600: # 10 min timeout
                    try:
                        el = page.locator("span.execstate.execstatedisplay.overall")
                        if el.count() > 0:
                            st = el.get_attribute("data-execstate")
                            if st:
                                estado_atual = st.strip().upper()
                                if estado_atual in ["SUCCEEDED", "FAILED", "ABORTED", "CANCELLED"]:
                                    estado_final = estado_atual
                                    break
                    except: pass
                    time.sleep(2)

                LOGGER.info(f"RUNDECK: Estado final do Job: {estado_final}")
                if estado_final != "SUCCEEDED":
                    raise Exception(f"Job Rundeck falhou com status: {estado_final}")

                # Download Output
                LOGGER.info("RUNDECK: Baixando output...")
                page.click("#btn_view_output")
                
                # Tab Execution Log
                xpath_log_tab = "xpath=//button[contains(.,'Execution Log')]"
                page.wait_for_selector(xpath_log_tab, state="visible", timeout=10000)
                page.click(xpath_log_tab)
                
                # Download File
                with page.expect_download(timeout=60000) as download_info:
                    page.click("text=Formatted Text")
                
                download = download_info.value
                nome_arq = f"rundeck_output_{int(time.time())}.txt"
                final_path = download_dir / nome_arq
                download.save_as(str(final_path))
                
                return final_path

            except Exception as e:
                LOGGER.error(f"RUNDECK ERROR: {e}")
                # Captura screenshot em caso de erro
                try:
                    page.screenshot(path=str(TEMP_DIR / "error_screenshot.png"))
                except: pass
                raise
            finally:
                context.close()
                browser.close()

# ==============================================================================
# MAIN
# ==============================================================================
def main():
    status_exec = "ERROR"
    output_files = [] # Arquivos para o Zip
    
    try:
        LOGGER.info(f"=== INICIANDO {SCRIPT_NAME} ===")
        load_configs()
        
        # 1. Busca Dados no BigQuery
        LOGGER.info(f"BQ: Executando query...")
        df = pandas_gbq.read_gbq(
            QUERY_NEGOCIO,
            project_id=Config.PROJECT_ID
            # use_bqstorage_api removido
        )

        if df.empty:
            LOGGER.warning("BQ: Nenhum dado retornado.")
            status_exec = "NO_DATA"
        else:
            LOGGER.info(f"BQ: {len(df)} linhas encontradas.")

            # 2. Geração de Arquivos
            hoje_str = datetime.now().strftime("%Y%m%d")
            
            # Formata CSV conforme regra de negócio
            df_saida = df.rename(columns={"ACCOUNT_ID": "account_id", "STATUS_CONTA_DOCK": "account_status"})
            
            csv_name = f"reversao_dock_{hoje_str}.csv"
            xlsx_name = f"reversao_dock_{hoje_str}.xlsx"
            
            csv_path = TEMP_DIR / csv_name
            xlsx_path = TEMP_DIR / xlsx_name
            
            # Salva
            df_saida.to_csv(csv_path, index=False, sep=";")
            df_saida.to_excel(xlsx_path, index=False)
            
            # Trata Excel (Opcional)
            try:
                xl = win32.Dispatch("Excel.Application")
                xl.DisplayAlerts = False
                xl.Visible = False
                wb = xl.Workbooks.Open(str(xlsx_path))
                wb.Save()
                wb.Close()
                xl.Quit()
            except Exception as e_excel:
                LOGGER.warning(f"Erro ao regravar Excel via COM (ignorado): {e_excel}")

            output_files.append(str(csv_path))
            output_files.append(str(xlsx_path))

            # 3. Automação Web (Rundeck)
            LOGGER.info("Iniciando automação Web (Rundeck)...")
            
            # Diretório temporário para download do rundeck
            rundeck_dl_dir = TEMP_DIR / "rundeck_dl"
            rundeck_dl_dir.mkdir(exist_ok=True)
            
            txt_output = RundeckAutomation.run(csv_path, rundeck_dl_dir)
            
            if txt_output and txt_output.exists():
                output_files.append(str(txt_output))
            
            status_exec = "SUCCESS"
            
            # Mover arquivos para rede se configurado
            if GLOBAL_CONFIG['move_file']:
                network_dest = Config.ROOT_DIR / "automacoes" / AREA_NAME / "logs" / SCRIPT_NAME / datetime.now().strftime('%Y-%m-%d')
                network_dest.mkdir(parents=True, exist_ok=True)
                for f_path in output_files:
                    try:
                        f = Path(f_path)
                        shutil.copy2(f, network_dest / f.name)
                    except Exception as e:
                        LOGGER.error(f"Erro ao mover arquivo {Path(f_path).name}: {e}")

    except Exception as e:
        status_exec = "ERROR"
        LOGGER.error(f"Erro Crítico: {traceback.format_exc()}")
    
    finally:
        end_time = datetime.now().replace(microsecond=0)
        
        # Gera Zip
        zip_path = smart_zip_logs(output_files)
        
        # Envia Email
        send_email(status_exec, zip_path)
        
        # Sobe Métricas
        upload_metrics(START_TIME, end_time, status_exec)
        
        # Cleanup Temp
        try:
            shutil.rmtree(TEMP_DIR, ignore_errors=True)
        except: pass
        
        LOGGER.info(f"=== FIM DA EXECUÇÃO | Status: {status_exec} ===")

if __name__ == "__main__":
    main()