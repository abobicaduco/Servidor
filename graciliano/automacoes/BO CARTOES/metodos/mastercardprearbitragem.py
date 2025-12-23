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

SCRIPT_NAME = "mastercardprearbitragem"
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
INPUT_DIR = ROOT_DRIVE / "SharePoint BO cartões - Bases Mastercard Contestacao" / "Reconciliacao"

# BigQuery
PROJECT_ID = "datalab-pagamentos"
DATASET_ID = "ADMINISTRACAO_CELULA_PYTHON"

REGISTRO_AUTOMACOES_TABLE = f"{PROJECT_ID}.ADMINISTRACAO_CELULA_PYTHON.registro_automacoes"
AUTOMACOES_EXEC_TABLE = f"{PROJECT_ID}.ADMINISTRACAO_CELULA_PYTHON.automacoes_exec"

BQ_PROJECT_BUSINESS = "datalab-pagamentos"
BQ_DATASET_BUSINESS = "22_cargas_stage"
BQ_TABLE_BUSINESS = "BASE_MASTERCARD_RECONCILIACAO"
TABELA_FINAL = f"{BQ_PROJECT_BUSINESS}.{BQ_DATASET_BUSINESS}.{BQ_TABLE_BUSINESS}"
SUBIDA_BQ = "append" # APPEND ou REPLACE

SCHEMA_COLS = [
    "FILE_NAME", "FILE_ID", "MESSAGE_NUMBER", "CLAIMCBKID", "ITEMCBKID",
    "CARD_ISSUER_REFERENCE_DATA", "ACQUIRER_REFERENCE_NUMBER", "MTI",
    "PRIMARY_ACCOUNT_NUMBER", "PROCESSING_CODE", "FUNCTION_CODE",
    "MESSAGE_REASON_CODE", "AMOUNT_TRANSACTION", "AMOUNT_RECONCILIATION",
    "AMOUNT_CARDHOLDER_BILLING", "TRANSACTION_CURRENCY_CODE",
    "RECONCILIATION_CURRENCY_CODE", "CARDHOLDER_BILLING_CURRENCY_CODE",
    "TRANSACTION_CURRENCY_EXP", "RECONCILIATION_CURRENCY_EXP",
    "CARDHOLDER_BILLING_CURRENCY_EXP", "RETRIEVAL_REFERENCE_NUMBER",
    "CARD_ACCEPTOR_BUSINESS_CODE", "CARD_ACCEPTOR_ID",
    "CARD_ACCEPTOR_NAME_LOCATION", "DATE_TIME_OF_TRANSACTION",
    "TRANSACTION_ORIGINATOR_INSTITUTION_ID_CODE",
    "TRANSACTION_DESTINATION_INSTITUTION_ID_CODE", "TRANSACTION_STATUS",
    "REASON_FOR_REJECTION", "REVERSAL_FLAG", "ARQUIVO", "DT_COLETA", "ARQUIVO_NOME",
]

HEADER_SYNONYMS = {
    "FILENAME": "FILE_NAME", "FILE_NAME": "FILE_NAME", "FILEID": "FILE_ID", "FILE_ID": "FILE_ID",
    "MESSAGENUMBER": "MESSAGE_NUMBER", "MESSAGE_NUMBER": "MESSAGE_NUMBER",
    "CLAIMCBKID": "CLAIMCBKID", "CLAIMBKID": "CLAIMCBKID", "ITEMCBKID": "ITEMCBKID",
    "CARDISSUERREFERENCEDATA": "CARD_ISSUER_REFERENCE_DATA", "CARD_ISSUER_REFERENCE_DATA": "CARD_ISSUER_REFERENCE_DATA",
    "ACQUIRERREFERENCENUMBER": "ACQUIRER_REFERENCE_NUMBER", "ACQUIRER_REFERENCE_NUMBER": "ACQUIRER_REFERENCE_NUMBER",
    "MTI": "MTI", "MIT": "MTI",
    "PRIMARYACCOUNTNUMBER": "PRIMARY_ACCOUNT_NUMBER", "PRIMARY_ACCOUNT_NUMBER": "PRIMARY_ACCOUNT_NUMBER",
    "PROCESSINGCODE": "PROCESSING_CODE", "PROCESSING_CODE": "PROCESSING_CODE",
    "FUNCTIONCODE": "FUNCTION_CODE", "FUNCTION_CODE": "FUNCTION_CODE",
    "MESSAGEREASONCODE": "MESSAGE_REASON_CODE", "MESSAGE_REASON_CODE": "MESSAGE_REASON_CODE",
    "AMOUNTTRANSACTION": "AMOUNT_TRANSACTION", "AMOUNT_TRANSACTION": "AMOUNT_TRANSACTION",
    "AMOUNTRECONCILIATION": "AMOUNT_RECONCILIATION", "AMOUNT_RECONCILIATION": "AMOUNT_RECONCILIATION",
    "AMOUNTCARDHOLDERBILLING": "AMOUNT_CARDHOLDER_BILLING", "AMOUNT_CARDHOLDER_BILLING": "AMOUNT_CARDHOLDER_BILLING",
    "TRANSACTIONCURRENCYCODE": "TRANSACTION_CURRENCY_CODE", "TRANSACTION_CURRENCY_CODE": "TRANSACTION_CURRENCY_CODE",
    "RECONCILIATIONCURRENCYCODE": "RECONCILIATION_CURRENCY_CODE", "RECONCILIATION_CURRENCY_CODE": "RECONCILIATION_CURRENCY_CODE",
    "CARDHOLDERBILLINGCURRENCYCODE": "CARDHOLDER_BILLING_CURRENCY_CODE", "CARDHOLDER_BILLING_CURRENCY_CODE": "CARDHOLDER_BILLING_CURRENCY_CODE",
    "TRANSACTIONCURRENCYEXP": "TRANSACTION_CURRENCY_EXP", "TRANSACTION_CURRENCY_EXP": "TRANSACTION_CURRENCY_EXP",
    "RECONCILIATIONCURRENCYEXP": "RECONCILIATION_CURRENCY_EXP", "RECONCILIATION_CURRENCY_EXP": "RECONCILIATION_CURRENCY_EXP",
    "CARDHOLDERBILLINGCURRENCYEXP": "CARDHOLDER_BILLING_CURRENCY_EXP", "CARDHOLDER_BILLING_CURRENCY_EXP": "CARDHOLDER_BILLING_CURRENCY_EXP",
    "RETRIEVALREFERENCENUMBER": "RETRIEVAL_REFERENCE_NUMBER", "RETRIEVAL_REFERENCE_NUMBER": "RETRIEVAL_REFERENCE_NUMBER",
    "CARDACCEPTORBUSINESSCODE": "CARD_ACCEPTOR_BUSINESS_CODE", "CARD_ACCEPTOR_BUSINESS_CODE": "CARD_ACCEPTOR_BUSINESS_CODE",
    "CARDACCEPTORID": "CARD_ACCEPTOR_ID", "CARD_ACCEPTOR_ID": "CARD_ACCEPTOR_ID",
    "CARDACCEPTORNAMELOCATION": "CARD_ACCEPTOR_NAME_LOCATION", "CARD_ACCEPTOR_NAME_LOCATION": "CARD_ACCEPTOR_NAME_LOCATION",
    "DATETIMEOFTRANSACTION": "DATE_TIME_OF_TRANSACTION", "DATE_TIME_OF_TRANSACTION": "DATE_TIME_OF_TRANSACTION",
    "TRANSACTIONORIGINATORINSTITUTIONIDCODE": "TRANSACTION_ORIGINATOR_INSTITUTION_ID_CODE",
    "TRANSACTION_ORIGINATOR_INSTITUTION_ID_CODE": "TRANSACTION_ORIGINATOR_INSTITUTION_ID_CODE",
    "TRANSACTIONDESTINATIONINSTITUTIONIDCODE": "TRANSACTION_DESTINATION_INSTITUTION_ID_CODE",
    "TRANSACION_DESTINATION_INSTITUTION_ID_CODE": "TRANSACTION_DESTINATION_INSTITUTION_ID_CODE",
    "TRANSACTION_DESTINATION_INSTITUTION_ID_CODE": "TRANSACTION_DESTINATION_INSTITUTION_ID_CODE",
    "TRANSACTIONSTATUS": "TRANSACTION_STATUS", "TRANSACTION_STATUS": "TRANSACTION_STATUS",
    "REASONFORREJECTION": "REASON_FOR_REJECTION", "REASON_FOR_REJECTION": "REASON_FOR_REJECTION",
    "REVERSALFLAG": "REVERSAL_FLAG", "REVERSAL_FLAG": "REVERSAL_FLAG",
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
    s = s.replace('"', " ").replace("’", "'").replace("`", "'")
    s = re.sub(r"\s+", " ", s).strip()
    s = s.upper()
    s = re.sub(r"[^A-Z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    s = s.lower()
    return s

def detectar_linha_header(df_raw):
    best_row = -1
    best_score = -1
    best_norm = []
    targets = {k.lower() for k in HEADER_SYNONYMS.keys()}
    max_check = min(50, max(1, len(df_raw)))
    for i in range(max_check):
        vals = df_raw.iloc[i, :].tolist()
        norm = [normalizar_header_cell(v) for v in vals]
        score = sum(1 for v in norm if v in targets)
        if score > best_score:
            best_score = score
            best_row = i
            best_norm = norm
    return best_row, best_norm, list(targets)

def mapear_colunas(norm_headers):
    idx_to_target = {}
    for idx, v in enumerate(norm_headers):
        for k, target in HEADER_SYNONYMS.items():
            if v == k.lower():
                idx_to_target[idx] = target
                break
    return idx_to_target

def converter_xls_para_xlsx(path: Path, logger) -> Path:
    try:
        pythoncom.CoInitialize()
        new_path = path.with_suffix(".xlsx")
        try:
            if new_path.exists():
                new_path.unlink()
        except: pass
        
        excel = win32.Dispatch("Excel.Application")
        excel.Visible = False
        excel.DisplayAlerts = False
        wb = excel.Workbooks.Open(str(path))
        wb.SaveAs(str(new_path), 51)
        wb.Close(False)
        excel.Quit()
        return new_path if new_path.exists() else path
    except Exception as e:
        logger.error(f"Erro ao converter XLS {path}: {e}")
        return path
    finally:
        try: pythoncom.CoUninitialize()
        except: pass

def ler_planilha_excel(path: Path, logger) -> pd.DataFrame:
    ext = path.suffix.lower()
    try:
        engine = "openpyxl" if ext in (".xlsx", ".xlsm") else "xlrd"
        xl = pd.ExcelFile(path, engine=engine)
    except Exception:
        if ext == ".xls":
            path = converter_xls_para_xlsx(path, logger)
            xl = pd.ExcelFile(path, engine="openpyxl")
        else:
            return pd.DataFrame()

    dfs_path = []
    for sheet_name in xl.sheet_names:
        try:
            df_raw = pd.read_excel(path, sheet_name=sheet_name, header=None, dtype=object, engine="openpyxl" if path.suffix in [".xlsx", ".xlsm"] else "xlrd")
            hdr_row, norm_hdrs, _ = detectar_linha_header(df_raw)
            if hdr_row < 0: continue
            
            idx_map = mapear_colunas(norm_hdrs)
            if not idx_map: continue

            dados = df_raw.iloc[hdr_row + 1 :].copy()
            dados = dados.dropna(how="all")
            cols_out = {}
            for idx, tgt in idx_map.items():
                cols_out[tgt] = dados.iloc[:, idx].astype(object)
            out = pd.DataFrame(cols_out)
            
            for c in SCHEMA_COLS:
                if c not in out.columns and c not in {"ARQUIVO", "DT_COLETA", "ARQUIVO_NOME"}:
                    out[c] = None
            
            for c in out.columns:
                out[c] = out[c].astype(object).where(pd.notna(out[c]), None)
                out[c] = out[c].apply(lambda v: str(v) if v is not None else None)
            
            dfs_path.append(out)
        except Exception as e:
            logger.error(f"Erro sheet {sheet_name} de {path.name}: {e}")

    if not dfs_path: return pd.DataFrame(columns=SCHEMA_COLS)
    
    final_df_path = pd.concat(dfs_path, ignore_index=True)
    final_df_path["ARQUIVO"] = str(path)
    final_df_path["ARQUIVO_NOME"] = path.name
    final_df_path["DT_COLETA"] = datetime.now(TZ).replace(microsecond=0)
    
    cols_existentes = [c for c in SCHEMA_COLS if c in final_df_path.columns]
    final_df_path = final_df_path[cols_existentes]
    
    final_df_path = final_df_path.dropna(how="all", subset=[k for k in SCHEMA_COLS if k in final_df_path.columns and k not in {"ARQUIVO", "DT_COLETA", "ARQUIVO_NOME"}])
    
    return final_df_path

def subir_dados(df: pd.DataFrame, logger):
    if df.empty: return 0
    
    logger.info(f"Upload BQ: {TABELA_FINAL} | Modo: {SUBIDA_BQ}")
    
    try:
        pandas_gbq.to_gbq(
            df,
            TABELA_FINAL,
            project_id=PROJECT_ID,
            if_exists=SUBIDA_BQ,
            use_bqstorage_api=False
        )
        logger.info("Upload OK.")
        return len(df)
    except Exception as e:
        logger.error(f"Erro Upload BQ: {e}")
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
             logger.error(f"Dir not found: {INPUT_DIR}")
        else:
            arquivos = sorted([p for p in INPUT_DIR.rglob("*") if p.is_file() and p.suffix.lower() in (".xlsx", ".xlsm", ".xls") and not p.name.startswith("~$")])
            
            if not arquivos:
                status = "NO_DATA"
                logger.info("Nenhum arquivo encontrado.")
            else:
                frames = []
                for path in arquivos:
                    try:
                        logger.info(f"Processando: {path.name}")
                        df_file = ler_planilha_excel(path, logger)
                        if not df_file.empty:
                            frames.append(df_file)
                            processed_files.append(path)
                        else:
                            logger.warning(f"Vazio/Irregular: {path.name}")
                    except Exception as e:
                         logger.error(f"Erro {path.name}: {e}")

                if not frames:
                    status = "NO_DATA"
                else:
                    df_total = pd.concat(frames, ignore_index=True)
                    if "DT_COLETA" in df_total.columns:
                         df_total["DT_COLETA"] = pd.to_datetime(df_total["DT_COLETA"])
                         
                    inserted = subir_dados(df_total, logger)
                    
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