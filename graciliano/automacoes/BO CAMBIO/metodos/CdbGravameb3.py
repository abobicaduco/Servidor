import sys
import os
import shutil
import logging
import traceback
import getpass
import time
import zipfile
import re
import pandas as pd
import pandas_gbq
import win32com.client as win32
import pythoncom
import tempfile
import pytz
from pathlib import Path
from datetime import datetime
from google.cloud import bigquery
from typing import List, Tuple, Optional

# ==================================================================================================
# CONSTANTES E CONFIGURAÇÕES
# ==================================================================================================

SCRIPT_NAME = "CdbGravameb3"
AREA_NAME = "BO CAMBIO"

TZ = pytz.timezone("America/Sao_Paulo")
ENV_EXEC_USER = os.getenv("ENV_EXEC_USER", getpass.getuser()).lower()
ENV_EXEC_MODE = os.getenv("ENV_EXEC_MODE", "MANUAL").upper()
TEST_MODE = False

# Paths
HOME = Path.home()
POSSIBLE_ROOTS = [
    HOME / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A",
    HOME / "Meu Drive/C6 CTVM",
    HOME / "C6 CTVM",
    HOME / "SharePoint",
    HOME / "OneDrive - C6 Bank S.A",
    HOME,
]
ROOT_DRIVE = next((p for p in POSSIBLE_ROOTS if p.exists() and (p / "Mensageria e Cargas Operacionais - 11.CelulaPython").exists()), POSSIBLE_ROOTS[0])

# Specific Paths
BASE_DIR = ROOT_DRIVE / "Mensageria e Cargas Operacionais - 11.CelulaPython/graciliano"
if not BASE_DIR.exists():
    BASE_DIR = ROOT_DRIVE / "graciliano"

AUTOMACOES_DIR = BASE_DIR / "automacoes"
LOG_DIR = AUTOMACOES_DIR / AREA_NAME / "LOGS" / SCRIPT_NAME / datetime.now(TZ).strftime("%Y-%m-%d")
TEMP_DIR = Path(os.getenv('TEMP', Path.home())) / "C6_RPA_EXEC" / SCRIPT_NAME

# Business Paths
INPUT_DIR = AUTOMACOES_DIR / AREA_NAME / "arquivos input" / "cdb_gravame_b3"

# BigQuery
PROJECT_ID = "datalab-pagamentos"
DATASET_ID = "ADMINISTRACAO_CELULA_PYTHON"
DATA_DATASET = "INVESTIMENTOS"
DATA_TABELA = "GRAVAME_"
TABELA_REFERENCIA = f"{PROJECT_ID}.{DATA_DATASET}.{DATA_TABELA}"

REGISTRO_AUTOMACOES_TABLE = f"{PROJECT_ID}.ADMINISTRACAO_CELULA_PYTHON.registro_automacoes"
AUTOMACOES_EXEC_TABLE = f"{PROJECT_ID}.ADMINISTRACAO_CELULA_PYTHON.automacoes_exec"

SUBIDA_BQ_MODE = "replace"

SCHEMA_FIELDS = [
    "Contrato_de_Garantia", "Status_do_Contrato_de_Garantia", "Tipo_IF", "Codigo_IF",
    "Data_de_Vencimento_do_Ativo", "Quantidade", "Conta_Garantia_Parte", "CPFCNPJ_Parte",
    "Conta_Garantia_Contraparte", "CPFCNPJ_Contraparte", "Constituicao_do_Gravame",
    "Contrato_de_Garantia_com_Pluralidade_de_Credores", "Grau_de_Penhor", "Data", "Hora",
    "Conta_Parte", "Conta_Contraparte", "Conta_Origem", "CPFCNPJ_Origem", "Conta_Destino",
    "CPFCNPJ_Destino", "PU_do_ativo_Valor_Base_Remanescente", "Eventos_para_o_Garantido",
    "Data_de_Emissao_Registro", "Descricao_do_Objeto_do_Contrato", "Indexador", "Taxa_de_Juros"
]

# ==================================================================================================
# FUNÇÕES DE SUPORTE
# ==================================================================================================

def setup_logger():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    log_filename = f"{SCRIPT_NAME}_{datetime.now(TZ).strftime('%H%M%S')}.log"
    log_path = LOG_DIR / log_filename
    
    logger = logging.getLogger(SCRIPT_NAME)
    logger.setLevel(logging.INFO)
    logger.handlers = []
    
    formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    
    file_handler = logging.FileHandler(log_path, encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    
    return logger, log_path

def load_config(logger):
    if TEST_MODE:
        return {"emails_principal": ["carlos.lsilva@c6bank.com"], "emails_cc": [], "move_file": False, "is_active": True}
    try:
        query = f"""
            SELECT emails_principal, emails_cc, move_file, is_active
            FROM `{REGISTRO_AUTOMACOES_TABLE}`
            WHERE lower(TRIM(script_name)) = lower('{SCRIPT_NAME}')
            ORDER BY created_at DESC LIMIT 1
        """
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, use_bqstorage_api=False)
        if not df.empty:
            row = df.iloc[0]
            val_move = row.get("move_file", False)
            if isinstance(val_move, str): val_move = val_move.lower() in ('true', '1')
            else: val_move = bool(val_move)
            
            val_active = row.get("is_active", "true")
            if isinstance(val_active, str): val_active = val_active.lower() in ('true', '1', 'ativo')
            else: val_active = bool(val_active)

            return {
                "emails_principal": [e.strip() for e in row.get("emails_principal", "").split(";") if "@" in e],
                "emails_cc": [e.strip() for e in row.get("emails_cc", "").split(";") if "@" in e],
                "move_file": val_move,
                "is_active": val_active
            }
    except Exception as e:
        logger.warning(f"Erro config BQ: {e}")
    return {"emails_principal": ["carlos.lsilva@c6bank.com"], "emails_cc": [], "move_file": False, "is_active": True}

def record_metrics(logger, start_time, end_time, status, error_msg=""):
    if TEST_MODE: return
    try:
        duration = (end_time - start_time).total_seconds()
        metrics = {
            "script_name": SCRIPT_NAME,
            "area_name": AREA_NAME,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": duration,
            "status": status,
            "usuario": ENV_EXEC_USER,
            "modo_exec": ENV_EXEC_MODE,
        }
        pandas_gbq.to_gbq(pd.DataFrame([metrics]), AUTOMACOES_EXEC_TABLE, project_id=PROJECT_ID, if_exists="append", use_bqstorage_api=False)
    except Exception as e:
        logger.error(f"Erro metrics: {e}")

def send_email_outlook(logger, subject, body, to_list, cc_list=None, attachments=None):
    if not to_list or TEST_MODE: return
    try:
        pythoncom.CoInitialize()
        outlook = win32.Dispatch("Outlook.Application")
        mail = outlook.CreateItem(0)
        mail.Subject = subject
        if body: mail.HTMLBody = body
        mail.To = ";".join(to_list)
        if cc_list: mail.CC = ";".join(cc_list)
        if attachments:
            for att in attachments:
                if Path(att).exists(): mail.Attachments.Add(str(att))
        mail.Send()
    except Exception as e:
        logger.error(f"Erro email: {e}")
    finally:
         try: pythoncom.CoUninitialize()
         except: pass

def smart_zip_logs(output_files: list) -> str:
    zip_filename = f"{SCRIPT_NAME}_{datetime.now(TZ).strftime('%H%M%S')}.zip"
    zip_path = TEMP_DIR / zip_filename
    
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        current_logs = list(LOG_DIR.glob(f"{SCRIPT_NAME}_*.log"))
        if current_logs:
            latest = max(current_logs, key=os.path.getctime)
            zf.write(latest, arcname=latest.name)
            
        for f in output_files:
            if Path(f).exists():
                zf.write(f, arcname=Path(f).name)
                
    return str(zip_path)

# ==================================================================================================
# LÓGICA DE NEGÓCIO
# ==================================================================================================

def tratar_dataframe(df: pd.DataFrame, logger) -> pd.DataFrame:
    logger.info("Iniciando tratamento de DataFrame...")
    
    # Normalize Headers
    df = df.rename(columns=lambda c: str(c).strip())
    df = df.rename(columns=lambda c: re.sub(r"[^0-9A-Za-z_]", "_", c))
    df = df.rename(columns=lambda c: re.sub(r"_+", "_", c).strip("_"))
    
    # Remove Extras
    extras = [c for c in df.columns if c not in SCHEMA_FIELDS]
    if extras:
        logger.info(f"Removendo colunas extras: {extras}")
        df = df.drop(columns=extras)
    
    # Add Missing
    for col in SCHEMA_FIELDS:
        if col not in df.columns:
            df[col] = None
    
    # Reorder
    df = df[SCHEMA_FIELDS]
    
    # Numeric
    numeric_cols = ["Quantidade", "Grau_de_Penhor", "Conta_Parte", "Conta_Contraparte", 
                    "Conta_Origem", "Conta_Destino"]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
            
    logger.info("Tratamento concluído.")
    return df

def main():
    start_time = datetime.now(TZ)
    logger, log_path = setup_logger()
    logger.info(f"Inicio {SCRIPT_NAME}")
    
    status = "SUCESSO"
    error_msg = ""
    linhas_processadas = 0
    output_files = []
    
    try:
        config = load_config(logger)
        
        if not INPUT_DIR.exists():
            INPUT_DIR.mkdir(parents=True, exist_ok=True)
            status = "NO_DATA"
            logger.warning(f"Diretorio criado, vazio: {INPUT_DIR}")
        else:
            files = sorted([p for p in INPUT_DIR.glob("*") if p.is_file()], key=lambda p: p.stat().st_mtime)
            
            if not files:
                 status = "NO_DATA"
                 logger.warning("Nenhum arquivo input.")
            else:
                 # Original logic: process ONLY last file
                 caminho_arquivo = files[-1]
                 logger.info(f"Processando arquivo: {caminho_arquivo.name}")
                 
                 temp_copy = TEMP_DIR / caminho_arquivo.name
                 shutil.copy2(caminho_arquivo, temp_copy)
                 output_files.append(temp_copy)
                 
                 try:
                     df = pd.read_csv(str(caminho_arquivo), sep=";", encoding="latin-1", low_memory=False)
                 except:
                     logger.info("Falha CSV, tentando Excel...")
                     df = pd.read_excel(str(caminho_arquivo), engine="openpyxl")
                     
                 df_tratado = tratar_dataframe(df, logger)
                 linhas_processadas = len(df_tratado)
                 
                 if linhas_processadas == 0:
                     status = "NO_DATA"
                 else:
                     logger.info(f"Subindo BQ: {TABELA_REFERENCIA} mode={SUBIDA_BQ_MODE}")
                     if_exists_param = 'replace' if SUBIDA_BQ_MODE.lower() == 'replace' else 'append'
                     
                     pandas_gbq.to_gbq(
                         df_tratado,
                         TABELA_REFERENCIA,
                         project_id=PROJECT_ID,
                         if_exists=if_exists_param,
                         use_bqstorage_api=False
                     )
                     
                     if config["move_file"]:
                         try:
                             dest = LOG_DIR / caminho_arquivo.name
                             shutil.move(str(caminho_arquivo), str(dest))
                             logger.info("Arquivo movido rede.")
                         except Exception as e:
                             logger.error(f"Erro move: {e}")

    except Exception as e:
        status = "ERRO"
        error_msg = str(e)
        logger.error(f"Fatal: {traceback.format_exc()}")
        
    end_time = datetime.now(TZ)
    zip_path = smart_zip_logs([str(p) for p in output_files])
    
    body = f"""
    <html><body>
    <h2>Execução {SCRIPT_NAME}</h2>
    <p>Status: {status}</p>
    <p>Linhas: {linhas_processadas}</p>
    </body></html>
    """
    send_email_outlook(logger, f"{SCRIPT_NAME} - {status}", body, config["emails_principal"], config["emails_cc"], [zip_path])
    record_metrics(logger, start_time, end_time, status, error_msg)

if __name__ == "__main__":
    if "--test" in sys.argv: TEST_MODE = True
    main()