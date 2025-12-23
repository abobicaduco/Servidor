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
from typing import List, Tuple, Optional, Dict, Any

# ==================================================================================================
# CONSTANTES E CONFIGURAÇÕES
# ==================================================================================================

SCRIPT_NAME = "mastercardrejects"
AREA_NAME = "BO CARTOES"

TZ = pytz.timezone("America/Sao_Paulo")
ENV_EXEC_USER = os.getenv("ENV_EXEC_USER", getpass.getuser()).lower()
ENV_EXEC_MODE = os.getenv("ENV_EXEC_MODE", "MANUAL").upper()
TEST_MODE = False

# Paths - Robust Detection
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
INPUT_DIR = ROOT_DRIVE / "SharePoint BO cartões - Bases Mastercard Contestacao" / "Rejects"
if not INPUT_DIR.exists():
    INPUT_DIR = Path(os.getenv("CELPY_INPUT_DIR", INPUT_DIR))

# BigQuery
PROJECT_ID = "datalab-pagamentos"
DATASET_ID = "ADMINISTRACAO_CELULA_PYTHON"

REGISTRO_AUTOMACOES_TABLE = f"{PROJECT_ID}.ADMINISTRACAO_CELULA_PYTHON.registro_automacoes"
AUTOMACOES_EXEC_TABLE = f"{PROJECT_ID}.ADMINISTRACAO_CELULA_PYTHON.automacoes_exec"

BQ_PROJECT_BUSINESS = "datalab-pagamentos"
BQ_DATASET_BUSINESS = "22_cargas_stage"
BQ_TABLE_BUSINESS = "BASE_MASTERCARD_REJECTS"
TABELA_FINAL = f"{BQ_PROJECT_BUSINESS}.{BQ_DATASET_BUSINESS}.{BQ_TABLE_BUSINESS}"
SUBIDA_BQ = "append" 

SCHEMA_COLS = [
    "CLAIM_ID", "CHARGEBACK_REFERENCE_NUMBER", "ACCOUNT_NUMBER", "ACQUIRER_REFERENCE_NUMBER",
    "CHARGEBACK_AMOUNT", "CURRENCY_CODE", "SENDER_ICA", "CHARGEBACK_REASON_CODE",
    "REJECTION_DATE", "REJECT_REASON_CODE", "FUNCTION_CODE", "ARQUIVO_NOME",
    "DT_COLETA"
]

HEADER_TARGET_MAP = {
    "CLAIMID": "CLAIM_ID", "CLAIM_ID": "CLAIM_ID",
    "CHARGEBACKREFERENCENUMBER": "CHARGEBACK_REFERENCE_NUMBER", "CHARGEBACK_REFERENCE_NUMBER": "CHARGEBACK_REFERENCE_NUMBER",
    "ACCOUNTNUMBER": "ACCOUNT_NUMBER", "ACCOUNT_NUMBER": "ACCOUNT_NUMBER",
    "ACQUIRER_REFERENCE_NUMBER": "ACQUIRER_REFERENCE_NUMBER", "ACQUIRERREFERENCENUMBER": "ACQUIRER_REFERENCE_NUMBER",
    "CHARGEBACK_AMOUNT": "CHARGEBACK_AMOUNT", "CHARGEBACKAMOUNT": "CHARGEBACK_AMOUNT",
    "CURRENCY_CODE": "CURRENCY_CODE", "CURRENCYCODE": "CURRENCY_CODE",
    "SENDER_ICA": "SENDER_ICA", "SENDERICA": "SENDER_ICA",
    "CHARGEBACK_REASON_CODE": "CHARGEBACK_REASON_CODE", "CHARGEBACKREASONCODE": "CHARGEBACK_REASON_CODE",
    "REJECTION_DATE": "REJECTION_DATE", "REJECTIONDATE": "REJECTION_DATE",
    "REJECT_REASON_CODE": "REJECT_REASON_CODE", "REJECTREASONCODE": "REJECT_REASON_CODE",
    "FUNCTION_CODE": "FUNCTION_CODE", "FUNCTIONCODE": "FUNCTION_CODE"
}

# ==================================================================================================
# FUNÇÕES DE SUPORTE
# ==================================================================================================

def setup_logger():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    try: INPUT_DIR.mkdir(parents=True, exist_ok=True)
    except: pass
    
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
        try:
            df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, use_bqstorage_api=False)
        except Exception:
            # Fallback to AREA NAME search if SCRIPT NAME not found (legacy behavior support)
            query = query.replace(f"lower('{SCRIPT_NAME}')", f"lower('{AREA_NAME.lower()}')")
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

def normalizar_header_cell(x):
    s = "" if pd.isna(x) else str(x)
    s = s.replace('"', " ").replace("\n", " ").replace("\r", " ")
    s = s.upper() 
    s = re.sub(r"[^A-Z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s

def detecting_linha_header(df_raw: pd.DataFrame):
    best_row = -1
    best_score = -1
    best_norm = []
    targets = set(HEADER_TARGET_MAP.keys())
    
    max_check = min(20, max(1, len(df_raw))) 
    
    for i in range(max_check):
        row_vals = df_raw.iloc[i, :].tolist()
        norm = [normalizar_header_cell(v) for v in row_vals]
        matches = [v for v in norm if v in targets]
        score = len(matches)
        
        if score > best_score and score > 0:
            best_score = score
            best_row = i
            best_norm = norm
            
    return best_row, best_norm

def mapear_colunas(norm_headers):
    idx_to_target = {}
    for idx, val in enumerate(norm_headers):
        if val in HEADER_TARGET_MAP:
            idx_to_target[idx] = HEADER_TARGET_MAP[val]
    return idx_to_target

def ler_arquivo_generico(path: Path, logger) -> pd.DataFrame:
    logger.info(f"Processando arquivo: {path.name}")
    is_excel = path.suffix.lower() in ['.xls', '.xlsx']
    
    try:
        if is_excel:
            df_raw = pd.read_excel(path, header=None, dtype=object)
        else:
            try:
                df_raw = pd.read_csv(path, header=None, dtype=object, sep=None, engine='python', encoding="utf-8")
            except UnicodeDecodeError:
                df_raw = pd.read_csv(path, header=None, dtype=object, sep=None, engine='python', encoding="latin1")
    except Exception as e:
        logger.error(f"Erro leitura {path.name}: {e}")
        return pd.DataFrame(columns=SCHEMA_COLS)

    hdr_row, norm_hdrs = detecting_linha_header(df_raw)
    
    if hdr_row < 0:
        logger.warning(f"Header não encontrado em {path.name}")
        return pd.DataFrame(columns=SCHEMA_COLS)
    
    idx_map = mapear_colunas(norm_hdrs)
    if not idx_map:
        return pd.DataFrame(columns=SCHEMA_COLS)

    dados = df_raw.iloc[hdr_row + 1 :].copy()
    
    cols_out = {}
    for idx, tgt in idx_map.items():
        if idx < len(dados.columns):
            cols_out[tgt] = dados.iloc[:, idx].astype(object)
    
    out = pd.DataFrame(cols_out)
    
    for c in SCHEMA_COLS:
        if c not in out.columns:
            out[c] = None
    
    out = out[SCHEMA_COLS]

    for c in out.columns:
        out[c] = out[c].apply(lambda v: str(v).strip() if pd.notna(v) else None)
        out[c] = out[c].replace({'': None, 'None': None, 'nan': None, 'NaN': None})

    colunas_chave = [c for c in ["CLAIM_ID", "CHARGEBACK_REFERENCE_NUMBER", "ACCOUNT_NUMBER", "ACQUIRER_REFERENCE_NUMBER"] if c in out.columns]
    out = out.dropna(subset=colunas_chave, how='all')

    if not out.empty:
        out["ARQUIVO_NOME"] = str(path.name)
        out["DT_COLETA"] = datetime.now(TZ).replace(microsecond=0)
    else:
        return pd.DataFrame(columns=SCHEMA_COLS)

    return out

def subir_para_bigquery(df: pd.DataFrame, logger):
    if df.empty:
        logger.warning("DataFrame vazio. Nada para subir.")
        return 0

    logger.info(f"Upload BQ: {TABELA_FINAL} | Modo: {SUBIDA_BQ} | Linhas: {len(df)}")
    
    df_bq = df.astype(str)
    df_bq = df_bq.replace({'None': None, 'nan': None, 'NaN': None, '<NA>': None})
    
    if "DT_COLETA" in df_bq.columns:
        df_bq["DT_COLETA"] = pd.to_datetime(df_bq["DT_COLETA"], errors='coerce')

    try:
        pandas_gbq.to_gbq(
            df_bq,
            TABELA_FINAL,
            project_id=PROJECT_ID,
            if_exists=SUBIDA_BQ,
            use_bqstorage_api=False
        )
        logger.info("Upload BigQuery concluído com sucesso.")
        return len(df)
    except Exception as e:
        logger.error(f"Erro fatal no upload BQ: {e}")
        raise e

def main():
    start_time = datetime.now(TZ)
    logger, log_path = setup_logger()
    logger.info(f"Inicio {SCRIPT_NAME}")
    
    status = "SUCESSO"
    error_msg = ""
    processed_files = []
    
    try:
        config = load_config(logger)
        
        if not INPUT_DIR.exists():
            status = "ERRO_DIR"
            logger.error(f"Input dir não encontrado: {INPUT_DIR}")
        else:
            extensoes = ['.csv', '.xlsx', '.xls']
            arquivos = sorted([p for p in INPUT_DIR.rglob("*") if p.is_file() and p.suffix.lower() in extensoes and not p.name.startswith('~$')])
            
            if not arquivos:
                status = "NO_DATA"
                logger.info("Nenhum arquivo encontrado.")
            else:
                frames = []
                for f in arquivos:
                    try:
                        df_part = ler_arquivo_generico(f, logger)
                        if not df_part.empty:
                            frames.append(df_part)
                            processed_files.append(f)
                            logger.info(f"Lido: {f.name} - {len(df_part)} linhas")
                        else:
                            logger.info(f"Ignorado: {f.name}")
                    except Exception as e:
                        logger.error(f"Erro processar {f.name}: {e}")
                
                if not frames:
                    status = "NO_DATA"
                else:
                    df_final = pd.concat(frames, ignore_index=True)
                    inserted = subir_para_bigquery(df_final, logger)
                    
                    if inserted > 0:
                        if config["move_file"]:
                            dest_dir = LOG_DIR
                            for f in processed_files:
                                try:
                                    dest = dest_dir / f.name
                                    shutil.copy2(str(f), str(dest))
                                    logger.info(f"Movido: {f.name}")
                                except Exception as e:
                                    logger.error(f"Erro move {f.name}: {e}")
                    else:
                        status = "NO_DATA"

    except Exception as e:
        status = "ERRO"
        error_msg = str(e)
        logger.error(f"Fatal: {traceback.format_exc()}")
        
    end_time = datetime.now(TZ)
    zip_path = smart_zip_logs([]) 
    
    body = f"""
    <html><body>
    <h2>Execução {SCRIPT_NAME}</h2>
    <p>Status: {status}</p>
    </body></html>
    """
    send_email_outlook(logger, f"{SCRIPT_NAME} - {status}", body, config["emails_principal"], config["emails_cc"], [zip_path])
    record_metrics(logger, start_time, end_time, status, error_msg)

if __name__ == "__main__":
    if "--test" in sys.argv: TEST_MODE = True
    main()
