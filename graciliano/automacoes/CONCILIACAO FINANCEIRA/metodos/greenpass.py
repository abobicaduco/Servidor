# ==============================================================================
# AUTO-INSTALL DEPENDENCIES & CONFIG
# ==============================================================================
import sys
import os
import shutil
import logging
import getpass
import time
import tempfile
import traceback
import zipfile
import pythoncom
import pandas as pd
import pandas_gbq
from pathlib import Path
from datetime import datetime
from google.cloud import bigquery
from win32com.client import Dispatch

# Adiciona diretório raiz ao path (Híbrido: Relativo ou Padrão C6)
CONFIG_LOADER = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "novo_servidor" / "config_loader.py"
project_root = None

# 2. Se não achou relativo, aponta para o caminho padrão da rede
if not project_root:
    standard_root = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "automacoes"
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
        "google-cloud-bigquery", 
        "pywin32", 
        "openpyxl", 
        "xlrd"
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
    ROOT_DIR = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano"
    PROJECT_ID = 'datalab-pagamentos'
    DATASET_ID = 'ADMINISTRACAO_CELULA_PYTHON'
    
    class Config:
        TABLE_CONFIG = f"{PROJECT_ID}.{DATASET_ID}.registro_automacoes"
        TABLE_EXEC = f"{PROJECT_ID}.{DATASET_ID}.automacoes_exec"
        COMPANY_DOMAIN = "c6bank.com"

# ==============================================================================
# CONSTANTES DO SCRIPT
# ==============================================================================
SCRIPT_NAME = Path(__file__).stem.upper()
AREA_NAME = "CONCILIACAO FINANCEIRA"
START_TIME = datetime.now().replace(microsecond=0)
SUBIDA_BQ = "append" 

# Diretórios
TEMP_DIR = Path(os.getenv('TEMP', Path.home())) / "C6_RPA_EXEC" / SCRIPT_NAME
TEMP_DIR.mkdir(parents=True, exist_ok=True)

# Path Específico do Input
PASTA_INPUT = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "automacoes" / AREA_NAME / "arquivos input" / "greenpass"

# BigQuery Target
BQ_TARGET_TABLE = f"{PROJECT_ID}.conciliacoes_monitoracao.TAG_GREENPASS"

# Globais de Configuração
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
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
LOGGER = logging.getLogger(SCRIPT_NAME)

# ==============================================================================
# 1. FUNÇÕES AUXILIARES & CONFIG
# ==============================================================================

def get_configs():
    """Carrega configs do BQ"""
    LOGGER.info("Carregando configurações do BigQuery...")
    try:
        # CORREÇÃO 1: Alterado metodo_automacao para script_name
        query = f"""
            SELECT emails_principal, emails_cc, move_file 
            FROM `{Config.TABLE_CONFIG}`
            WHERE lower(TRIM(script_name)) = lower('{Path(__file__).stem}')
            AND (is_active IS NULL OR lower(is_active) = 'true')
            LIMIT 1
        """
        
        # Note: use_bqstorage_api=False é mantido aqui pois é read_gbq (onde funciona)
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, use_bqstorage_api=False)
        
        if not df.empty:
            GLOBAL_CONFIG['emails_principal'] = [x.strip() for x in str(df.iloc[0]['emails_principal']).replace(';', ',').split(',') if '@' in x]
            GLOBAL_CONFIG['emails_cc'] = [x.strip() for x in str(df.iloc[0]['emails_cc']).replace(';', ',').split(',') if '@' in x]
            
            move_val = df.iloc[0].get('move_file', 'false')
            GLOBAL_CONFIG['move_file'] = str(move_val).lower() in ['true', '1', 't']
            
            LOGGER.info(f"Configs carregadas. Move File: {GLOBAL_CONFIG['move_file']}")
        else:
            LOGGER.warning("Configs não encontradas no BQ. Usando padrão (Sem emails, sem move).")
    except Exception as e:
        LOGGER.error(f"Erro ao carregar configs: {e}")

def detect_env():
    """Detecta Usuario e Modo de Execução"""
    user = os.environ.get("ENV_EXEC_USER", f"{getpass.getuser().lower()}@{Config.COMPANY_DOMAIN}")
    mode = os.environ.get("ENV_EXEC_MODE", "AGENDAMENTO")
    return user, mode

def regravar_excel_safe(caminho_original: Path) -> Path:
    """
    Cria uma CÓPIA do arquivo para uma pasta temporária e salva como xlsx limpo via Win32.
    """
    if caminho_original.suffix.lower() not in ['.xls', '.xlsx']:
        return caminho_original

    timestamp = datetime.now().strftime('%H%M%S%f')
    path_input_temp = TEMP_DIR / f"input_{timestamp}_{caminho_original.name}"
    path_output_repaired = TEMP_DIR / f"repaired_{timestamp}_{caminho_original.stem}.xlsx"
    
    try:
        shutil.copy2(caminho_original, path_input_temp)
    except Exception as e:
        LOGGER.warning(f"Não foi possível criar cópia temporária: {e}")
        return caminho_original

    try:
        pythoncom.CoInitialize()
        xl = Dispatch("Excel.Application")
        xl.DisplayAlerts = False
        xl.Visible = False
        
        wb = None
        try:
            wb = xl.Workbooks.Open(str(path_input_temp))
            wb.SaveAs(str(path_output_repaired), FileFormat=51) # xlsx
            return path_output_repaired
        except Exception as e_wb:
            LOGGER.debug(f"Win32 falhou na conversão: {e_wb}")
            return caminho_original
        finally:
            if wb: wb.Close(SaveChanges=False)
            xl.Quit()
            
    except Exception as e:
        LOGGER.debug(f"Erro genérico no Win32: {e}")
        return caminho_original
    finally:
        pythoncom.CoUninitialize()
        if path_input_temp.exists():
            try: path_input_temp.unlink()
            except: pass

def transformar_dados(df: pd.DataFrame, nome_arquivo: str) -> pd.DataFrame:
    if df.empty: return df

    cols_negocio = [
        "NSU", "DATA_TRANSACAO", "TIPO_TRANSACAO", "NATUREZA_OPERACAO",
        "VALOR", "COMISSAO_TRANSACAO", "DATA_EVENTO", "NUMERO_SERIE",
        "PLACA", "CONVENIADA", "LOCAL"
    ]
    
    if len(df.columns) >= 11:
        df.columns = cols_negocio + list(df.columns[11:])
        df = df[cols_negocio]
    else:
        LOGGER.warning(f"Arquivo {nome_arquivo} tem menos colunas ({len(df.columns)}) que o esperado.")
        df.columns = cols_negocio[:len(df.columns)]
    
    df["NSU"] = df["NSU"].astype(str).str.strip()
    df["VALOR"] = df["VALOR"].astype(str).str.replace(",", ".", regex=False)
    df["VALOR"] = pd.to_numeric(df["VALOR"], errors="coerce").round(2)
    
    df = df.drop_duplicates(subset=["NSU"])
    
    return df

def processar_arquivo_individual(caminho: Path) -> pd.DataFrame:
    arquivo_leitura = caminho
    arquivo_temporario = None

    try:
        if caminho.suffix.lower() in ['.xls', '.xlsx']:
            path_safe = regravar_excel_safe(caminho)
            if path_safe != caminho:
                arquivo_temporario = path_safe
                arquivo_leitura = path_safe

        LOGGER.info(f"Lendo: {caminho.name}")

        if caminho.suffix.lower() == ".csv":
            df = pd.read_csv(arquivo_leitura, sep=";", dtype=str).fillna("")
        else:
            df = pd.read_excel(arquivo_leitura, engine="openpyxl", dtype=str).fillna("")
        
        return transformar_dados(df, caminho.name)

    except Exception:
        LOGGER.error(f"Erro ao processar {caminho.name}: {traceback.format_exc()}")
        return pd.DataFrame()
    finally:
        if arquivo_temporario and arquivo_temporario.exists():
            try: arquivo_temporario.unlink()
            except: pass

# ==============================================================================
# 2. LOGICA PRINCIPAL (ETL)
# ==============================================================================
def main():
    LOGGER.info(f"=== INICIO: {SCRIPT_NAME} ===")
    
    user, mode = detect_env()
    get_configs()
    
    status_exec = "ERROR"
    output_files = [] 
    processed_count = 0
    observacao = ""
    
    try:
        # --- 1. Leitura de Arquivos ---
        if not PASTA_INPUT.exists():
            LOGGER.error(f"Pasta de input não encontrada: {PASTA_INPUT}")
            status_exec = "ERROR"
            observacao = "Pasta de input não encontrada."
            PASTA_INPUT.mkdir(parents=True, exist_ok=True)
            return

        arquivos = [p for p in PASTA_INPUT.iterdir() if p.is_file() and p.suffix.lower() in [".xls", ".xlsx", ".csv"]]
        
        if not arquivos:
            LOGGER.warning("Nenhum arquivo encontrado na pasta de input.")
            status_exec = "NO_DATA"
            run_final_dedup_only(status_exec)
            if status_exec == "NO_DATA": status_exec = "SUCCESS" 
            return

        LOGGER.info(f"Arquivos encontrados: {len(arquivos)}")
        dfs_final = []
        
        # --- 2. Processamento ---
        for arq in arquivos:
            df_temp = processar_arquivo_individual(arq)
            if not df_temp.empty:
                dfs_final.append(df_temp)
                output_files.append(arq)
        
        if not dfs_final:
            LOGGER.warning("Arquivos lidos, mas nenhum dado válido extraído.")
            status_exec = "NO_DATA"
            return

        df_consolidado = pd.concat(dfs_final, ignore_index=True)
        processed_count = len(df_consolidado)
        LOGGER.info(f"Total de linhas consolidadas: {processed_count}")

        # --- 3. Carga no BigQuery ---
        tabela_staging = f"{BQ_TARGET_TABLE}_staging"
        
        LOGGER.info(f"Subindo Staging: {tabela_staging}")
        # CORREÇÃO 2: Removido use_bqstorage_api=False (inválido no to_gbq)
        pandas_gbq.to_gbq(
            df_consolidado,
            tabela_staging,
            project_id=PROJECT_ID,
            if_exists='replace'
        )

        # --- 4. Merge/Insert ---
        cols_sql = "NSU, DATA_TRANSACAO, TIPO_TRANSACAO, NATUREZA_OPERACAO, VALOR, COMISSAO_TRANSACAO, DATA_EVENTO, NUMERO_SERIE, PLACA, CONVENIADA, LOCAL"
        
        sql_merge = f"""
            INSERT INTO `{BQ_TARGET_TABLE}` ({cols_sql})
            SELECT {cols_sql}
            FROM `{tabela_staging}` S
            WHERE NOT EXISTS (
                SELECT 1 FROM `{BQ_TARGET_TABLE}` F
                WHERE F.NSU = S.NSU 
            )
        """
        
        LOGGER.info("Executando Merge no BigQuery...")
        client = bigquery.Client(project=PROJECT_ID)
        client.query(sql_merge).result()
        
        client.query(f"DROP TABLE IF EXISTS `{tabela_staging}`")

        # --- 5. Query Final de Deduplicação ---
        LOGGER.info("Executando Query Final de Deduplicação da Tabela Mestra...")
        query_final = f"""
            CREATE OR REPLACE TABLE `{BQ_TARGET_TABLE}` AS
            SELECT DISTINCT *
            FROM `{BQ_TARGET_TABLE}`;
        """
        client.query(query_final).result()
        
        status_exec = "SUCCESS"
        LOGGER.info("Fluxo concluído com sucesso.")

    except Exception as e:
        status_exec = "ERROR"
        observacao = str(e)
        LOGGER.error(f"Erro Fatal: {traceback.format_exc()}")
    
    finally:
        finalize_execution(status_exec, output_files, user, mode, observacao)

def run_final_dedup_only(current_status):
    """Executa a query final de dedup mesmo se não houver arquivos"""
    try:
        LOGGER.info("Rodando query final de manutenção (Dedup)...")
        client = bigquery.Client(project=PROJECT_ID)
        query_final = f"""
            CREATE OR REPLACE TABLE `{BQ_TARGET_TABLE}` AS
            SELECT DISTINCT *
            FROM `{BQ_TARGET_TABLE}`;
        """
        client.query(query_final).result()
        LOGGER.info("Query final executada com sucesso.")
    except Exception as e:
        LOGGER.error(f"Erro na query final: {e}")

# ==============================================================================
# 3. FINALIZAÇÃO (Zip, Move, Email, Metrics)
# ==============================================================================
def finalize_execution(status, output_files, user, mode, observacao):
    end_time = datetime.now().replace(microsecond=0)
    duration = round((end_time - START_TIME).total_seconds(), 2)
    
    # 1. Smart Zip
    zip_path = create_smart_zip(output_files)
    
    # 2. Move Files
    if GLOBAL_CONFIG['move_file'] and status == "SUCCESS":
        move_to_network(output_files)
    
    # 3. Email
    send_email_outlook(status, zip_path)
    
    # 4. Upload Metrics
    upload_metrics(status, user, mode, duration)

def create_smart_zip(files_to_zip):
    try:
        timestamp_str = datetime.now().strftime("%H%M%S")
        zip_filename = TEMP_DIR / f"{SCRIPT_NAME}_{timestamp_str}.zip"
        limit_bytes = 15 * 1024 * 1024 # 15MB
        
        with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zf:
            if LOG_FILE.exists():
                zf.write(LOG_FILE, arcname=LOG_FILE.name)
                current_size = LOG_FILE.stat().st_size
            else:
                current_size = 0
                
            for file_path in files_to_zip:
                if not file_path.exists(): continue
                
                f_size = file_path.stat().st_size
                if (current_size + f_size) < limit_bytes:
                    zf.write(file_path, arcname=file_path.name)
                    current_size += f_size
                else:
                    warning_text = f"Arquivo {file_path.name} ignorado pois excede o limite de 15MB do Outlook. Consulte a rede."
                    zf.writestr(f"AVISO_ARQUIVO_GRANDE_{file_path.name}.txt", warning_text)
                    LOGGER.warning(f"Arquivo {file_path.name} ignorado no zip (Tamanho excedido).")
        
        return zip_filename
    except Exception as e:
        LOGGER.error(f"Erro ao criar zip: {e}")
        return None

def move_to_network(files):
    network_log_dir = ROOT_DIR / "automacoes" / AREA_NAME / "logs" / SCRIPT_NAME / datetime.now().strftime("%Y-%m-%d")
    network_log_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp_mov = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    for f in files:
        try:
            if f.exists():
                dest = network_log_dir / f"{f.stem}_{timestamp_mov}{f.suffix}"
                shutil.move(str(f), str(dest))
                LOGGER.info(f"Movido para rede: {dest}")
        except Exception as e:
            LOGGER.error(f"Erro ao mover {f.name}: {e}")

def send_email_outlook(status, attachment_path):
    try:
        pythoncom.CoInitialize()
        outlook = Dispatch("Outlook.Application")
        mail = outlook.CreateItem(0)
        
        subject = f"CÉLULA PYTHON MONITORAÇÃO - {SCRIPT_NAME} - [{status}]"
        
        to_list = GLOBAL_CONFIG['emails_principal']
        cc_list = []
        if status == "SUCCESS":
            cc_list = GLOBAL_CONFIG['emails_cc']
            
        if not to_list:
            LOGGER.warning("Sem destinatários configurados. Email não enviado.")
            return

        mail.To = ";".join(to_list)
        mail.CC = ";".join(cc_list)
        mail.Subject = subject
        mail.Body = "" 
        
        if attachment_path and attachment_path.exists():
            mail.Attachments.Add(str(attachment_path))
            
        mail.Send()
        LOGGER.info(f"Email enviado para {len(to_list)} destinatários.")
        
    except Exception as e:
        LOGGER.error(f"Erro ao enviar email via Outlook: {e}")
    finally:
        pythoncom.CoUninitialize()

def upload_metrics(status, user, mode, duration):
    try:
        df_metric = pd.DataFrame([{
            "script_name": SCRIPT_NAME,
            "area_name": AREA_NAME,
            "start_time": START_TIME,
            "end_time": datetime.now().replace(microsecond=0),
            "duration_seconds": duration,
            "status": status,
            "usuario": user,
            "modo_exec": mode
        }])
        
        # CORREÇÃO 2: Removido use_bqstorage_api=False
        pandas_gbq.to_gbq(
            df_metric,
            Config.TABLE_EXEC,
            project_id=PROJECT_ID,
            if_exists='append'
        )
        LOGGER.info("Métricas de execução subidas com sucesso.")
    except Exception as e:
        LOGGER.error(f"Erro ao subir métricas: {e}")

if __name__ == "__main__":
    main()
