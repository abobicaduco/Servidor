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

SCRIPT_NAME = "mastercardincoming"
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
# Original: "SharePoint BO cartões - Bases Mastercard Contestacao" / "Base Geral Incoming Outgoing"
# Using ROOT_DRIVE which usually points to "C6 CTVM..."
INPUT_DIR = ROOT_DRIVE / "SharePoint BO cartões - Bases Mastercard Contestacao" / "Base Geral Incoming Outgoing"

# BigQuery
PROJECT_ID = "datalab-pagamentos"
DATASET_ID = "ADMINISTRACAO_CELULA_PYTHON"

REGISTRO_AUTOMACOES_TABLE = f"{PROJECT_ID}.ADMINISTRACAO_CELULA_PYTHON.registro_automacoes"
AUTOMACOES_EXEC_TABLE = f"{PROJECT_ID}.ADMINISTRACAO_CELULA_PYTHON.automacoes_exec"

BQ_PROJECT_BUSINESS = "datalab-pagamentos"
BQ_DATASET_BUSINESS = "22_cargas_stage"
BQ_TABLE_BUSINESS = "BASE_MASTERCARD_INCOMING"
TABELA_FINAL = f"{BQ_PROJECT_BUSINESS}.{BQ_DATASET_BUSINESS}.{BQ_TABLE_BUSINESS}"
SUBIDA_BQ = "append" # APPEND ou REPLACE

SCHEMA_COLS = [
    "ACCOUNT_NUMBER", "ARN", "CURRENCY", "REASON_CODE", "CHARGEBACK_REFERENCE_NUMBER",
    "CLAIM_ID", "PROCESSING_DATE", "ACQUIRER_ICA", "ISSUER_ICA", "BIN_ARID",
    "DIRECTION", "ITEM_TYPE", "AMOUNT", "DT_COLETA", "ARQUIVO_NOME"
]

HEADER_TARGET_MAP = {
    "ACCOUNT_NUMBER": "ACCOUNT_NUMBER",
    "ARN": "ARN",
    "AMOUNT": "AMOUNT",
    "CURRENCY": "CURRENCY",
    "REASON_CODE": "REASON_CODE",
    "CHARGEBACK_REFERENCE_NUMBER": "CHARGEBACK_REFERENCE_NUMBER",
    "CLAIM_ID": "CLAIM_ID",
    "PROCESSING_DATE": "PROCESSING_DATE",
    "ACQUIRER_ICA": "ACQUIRER_ICA",
    "ISSUER_ICA": "ISSUER_ICA",
    "BIN_ARID": "BIN_ARID",
    "DIRECTION": "DIRECTION",
    "ITEM_TYPE": "ITEM_TYPE",
}

# ==================================================================================================
# FUNÇÕES DE SUPORTE
# ==================================================================================================

def setup_logger():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    
    # Try to create Input Dir if possible, but might be restricted if SharePoint
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
# LÓGICA DE NEGÓCIO - PARSER
# ==================================================================================================

def normalizar_header_cell(x) -> str:
    s = "" if pd.isna(x) else str(x)
    s = s.replace('"', " ").replace("\n", " ").replace("\r", " ")
    s = re.sub(r"\s+", " ", s).strip()
    s = s.upper() 
    s = re.sub(r"[^A-Z0-9]+", "_", s) 
    s = re.sub(r"_+", "_", s).strip("_")
    return s

def detecting_linha_header(df_raw: pd.DataFrame):
    best_row = -1
    best_score = -1
    best_norm = []
    targets = set(HEADER_TARGET_MAP.keys())
    
    max_check = min(100, len(df_raw)) 
    
    for i in range(max_check):
        row_vals = df_raw.iloc[i, :].tolist()
        norm = [normalizar_header_cell(v) for v in row_vals]
        score = sum(1 for v in norm if v in targets)
        if score > best_score and score >= 3:
            best_score = score
            best_row = i
            best_norm = norm
            
    return best_row, best_norm, list(targets)

def mapear_colunas(norm_headers):
    idx_to_target = {}
    for idx, val in enumerate(norm_headers):
        if val in HEADER_TARGET_MAP:
            idx_to_target[idx] = HEADER_TARGET_MAP[val]
    return idx_to_target

def _posprocessar_saida(out: pd.DataFrame, path: Path) -> pd.DataFrame:
    out["ARQUIVO_NOME"] = str(path.name)
    out["DT_COLETA"] = pd.Timestamp.now(tz=TZ).replace(microsecond=0)
    
    for c in out.columns:
        if c != "DT_COLETA":
            out[c] = out[c].astype(object).where(pd.notna(out[c]), None)
            out[c] = out[c].apply(lambda v: str(v) if v is not None else None)
    
    for col in SCHEMA_COLS:
        if col not in out.columns:
            out[col] = None
            
    out = out[SCHEMA_COLS]
    return out

def ler_csv_robusto(path: Path, logger) -> pd.DataFrame:
    encodings = ['utf-8', 'utf-8-sig', 'latin1', 'cp1252']
    separators = [None, ';', ',', '\t'] 
    
    df_raw = pd.DataFrame()
    sucesso = False
    
    for enc in encodings:
        if sucesso: break
        for sep in separators:
            try:
                temp_df = pd.read_csv(
                    path, 
                    header=None, 
                    dtype=object, 
                    engine="python", 
                    sep=sep, 
                    encoding=enc, 
                    on_bad_lines='skip'
                )
                if not temp_df.empty and temp_df.shape[1] > 1:
                    hdr, _, _ = detecting_linha_header(temp_df)
                    if hdr >= 0:
                        df_raw = temp_df
                        sucesso = True
                        break
            except Exception:
                continue

    if df_raw.empty:
        logger.warning(f"FALHA CSV|Nenhum encoding/sep funcionou para: {path.name}")
        return pd.DataFrame(columns=SCHEMA_COLS)

    hdr_row, norm_hdrs, _ = detecting_linha_header(df_raw)
    idx_map = mapear_colunas(norm_hdrs)
    
    dados = df_raw.iloc[hdr_row + 1 :].copy()
    dados = dados.dropna(how="all")
    
    cols_out = {}
    for idx, tgt in idx_map.items():
        if idx < dados.shape[1]:
            cols_out[tgt] = dados.iloc[:, idx].astype(object)
            
    out = pd.DataFrame(cols_out)
    
    for c in HEADER_TARGET_MAP.values():
        if c not in out.columns:
            out[c] = None
            
    base_cols = list(HEADER_TARGET_MAP.values())
    out = out[base_cols]
    out = out.dropna(how="all", subset=[c for c in base_cols if c in out.columns])
    
    out = _posprocessar_saida(out, path)
    return out

def ler_planilha_xlsx(path: Path, sheet_name: str, logger) -> pd.DataFrame:
    try:
        df_raw = pd.read_excel(path, sheet_name=sheet_name, header=None, dtype=object, engine="openpyxl")
    except Exception as e:
        logger.warning(f"Erro Excel sheet={sheet_name}: {e}")
        return pd.DataFrame(columns=SCHEMA_COLS)
    
    if df_raw.empty:
        return pd.DataFrame(columns=SCHEMA_COLS)

    hdr_row, norm_hdrs, _ = detecting_linha_header(df_raw)
    if hdr_row < 0:
        return pd.DataFrame(columns=SCHEMA_COLS)
        
    idx_map = mapear_colunas(norm_hdrs)
    dados = df_raw.iloc[hdr_row + 1 :].copy()
    dados = dados.dropna(how="all")
    
    cols_out = {}
    for idx, tgt in idx_map.items():
        if idx < dados.shape[1]:
            cols_out[tgt] = dados.iloc[:, idx].astype(object)
            
    out = pd.DataFrame(cols_out)
    
    for c in HEADER_TARGET_MAP.values():
        if c not in out.columns:
            out[c] = None
            
    base_cols = list(HEADER_TARGET_MAP.values())
    out = out[base_cols]
    out = out.dropna(how="all", subset=[c for c in base_cols if c in out.columns])
    
    out = _posprocessar_saida(out, path)
    return out

def subir_dados(df: pd.DataFrame, logger):
    if df.empty: return 0
    
    logger.info(f"Iniciando carga no BigQuery. Modo: {SUBIDA_BQ}. Linhas: {len(df)}")
    
    df_bq = df.copy()
    df_bq.columns = [c.lower() for c in df_bq.columns]
    
    for col in df_bq.columns:
        if col != 'dt_coleta':
            df_bq[col] = df_bq[col].astype(str).replace({'None': None, 'nan': None})
    
    if 'dt_coleta' in df_bq.columns:
        df_bq['dt_coleta'] = pd.to_datetime(df_bq['dt_coleta'])

    try:
        pandas_gbq.to_gbq(
            df_bq,
            TABELA_FINAL,
            project_id=BQ_PROJECT_BUSINESS,
            if_exists=SUBIDA_BQ.lower(),
            use_bqstorage_api=False
        )
        logger.info(f"Carga concluída com sucesso em {TABELA_FINAL}")
        return len(df)
    except Exception as e:
        logger.error(f"Erro no upload BigQuery: {e}")
        raise e

def executar_proc_atualizacao(logger):
    try:
        proc_query = "CALL `datalab-pagamentos.22_cargas_stage.ATUALIZACAO_BASES_MASTERCARD_CHARGEBACK`();"
        logger.info(f"Executando PROC: {proc_query}")
        
        client = bigquery.Client(project=BQ_PROJECT_BUSINESS)
        query_job = client.query(proc_query)
        query_job.result() 
        
        logger.info("PROC executada com sucesso.")
        return True
    except Exception as e:
        logger.error(f"Erro ao executar PROC: {e}")
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
             logger.error(f"Input dir não encontrado: {INPUT_DIR}")
             status = "ERRO_DIR"
        else:
            arquivos = []
            for padrao in ("*.xlsx", "*.csv"):
                arquivos.extend([p for p in INPUT_DIR.rglob(padrao) if p.is_file()])
            
            if not arquivos:
                status = "NO_DATA"
                logger.info("Nenhum arquivo encontrado.")
            else:
                frames = []
                for i, f in enumerate(arquivos, 1):
                    try:
                        dfp = pd.DataFrame()
                        if f.suffix.lower() == ".xlsx":
                            xl = pd.ExcelFile(f, engine="openpyxl")
                            for sh in xl.sheet_names:
                                temp_df = ler_planilha_xlsx(f, sh, logger)
                                if not temp_df.empty:
                                    dfp = pd.concat([dfp, temp_df]) if not dfp.empty else temp_df
                        else:
                            dfp = ler_csv_robusto(f, logger)
                            
                        if not dfp.empty:
                            frames.append(dfp)
                            processed_files.append(f)
                            logger.info(f"Processado [{i}/{len(arquivos)}]: {f.name}")
                        else:
                            logger.warning(f"Vazio/Erro [{i}/{len(arquivos)}]: {f.name}")
                            
                    except Exception as e:
                         logger.error(f"Erro arquivo {f.name}: {e}")

                df_final = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=SCHEMA_COLS)
                
                if df_final.empty:
                    status = "NO_DATA"
                else:
                    inserted = subir_dados(df_final, logger)
                    if inserted > 0:
                        executar_proc_atualizacao(logger)
                        
                        if config["move_file"]:
                            dest_dir = LOG_DIR
                            for f in processed_files:
                                try:
                                    dest = dest_dir / f.name
                                    shutil.copy2(str(f), str(dest))
                                    logger.info(f"Movido: {f.name}")
                                except Exception as e:
                                    logger.error(f"Erro mover {f.name}: {e}")
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