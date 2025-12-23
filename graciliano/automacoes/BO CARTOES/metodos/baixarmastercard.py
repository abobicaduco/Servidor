import sys
import os
import shutil
import logging
import traceback
import getpass
import time
import zipfile
import re
import requests
import subprocess
import pandas as pd
import pandas_gbq
import win32com.client as win32
import pythoncom
import tempfile
import pytz
from pathlib import Path
from datetime import datetime, date
from urllib.parse import quote
from google.cloud import bigquery
from typing import List, Tuple, Optional, Dict, Any

# ==================================================================================================
# CONSTANTES E CONFIGURAÇÕES
# ==================================================================================================

SCRIPT_NAME = "baixarmastercard"
AREA_NAME = "BO CARTOES"

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
INPUT_DIR = AUTOMACOES_DIR / AREA_NAME / "arquivos input"

# BigQuery
PROJECT_ID = "datalab-pagamentos"
DATASET_ID = "ADMINISTRACAO_CELULA_PYTHON"

REGISTRO_AUTOMACOES_TABLE = f"{PROJECT_ID}.ADMINISTRACAO_CELULA_PYTHON.registro_automacoes"
AUTOMACOES_EXEC_TABLE = f"{PROJECT_ID}.ADMINISTRACAO_CELULA_PYTHON.automacoes_exec"

BUCKET = "c6-storage-data-prod-238420-conductor-prod"
PREFIX = "sftp/DaProducao/T461"
CHUNK_SIZE = 1024 * 1024
SCOPES = ["https://www.googleapis.com/auth/cloud-platform", "openid", "email"]
USER_PROJECT = os.environ.get("GCS_USER_PROJECT", "").strip() or None

DESTINO_MAP = {
    "1SWCHD53": "datalab-pagamentos.ARQUIVOS_MASTERCARD.SWCHD53",
    "1SWCHD53_IND": "datalab-pagamentos.ARQUIVOS_MASTERCARD.SWCHD53_INDICE",
    "1SWCHD363": "datalab-pagamentos.ARQUIVOS_MASTERCARD.SWCHD363_NOVO",
    "1SWCHD353": "datalab-pagamentos.ARQUIVOS_MASTERCARD.SWCHD353",
    "1SWCHD353_IND": "datalab-pagamentos.ARQUIVOS_MASTERCARD.SWCHD353_INDICE",
}
EXTRA_TABELAS_DISTINCT = ["monitoracao_shared.SWCHD363_NOVO"]

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
# GSC / AUTH
# ==================================================================================================

def try_gcloud_token(logger):
    for cmd in (["gcloud", "auth", "print-access-token", "--quiet"], ["gcloud.cmd", "auth", "print-access-token", "--quiet"]):
        try:
            out = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=10)
            tok = out.stdout.strip()
            if tok:
                logger.info("Token obtido via gcloud")
                return tok
        except Exception:
            continue
    return None

def get_access_token(logger):
    env_tok = os.environ.get("GCS_BEARER_TOKEN", "").strip()
    if env_tok: return env_tok
    
    tok = try_gcloud_token(logger)
    if tok: return tok
    
    try:
        import pydata_google_auth
        from google.auth.transport.requests import Request
        creds = pydata_google_auth.get_user_credentials(SCOPES, use_local_webserver=True)
        if not creds.valid:
            if getattr(creds, "expired", False) and getattr(creds, "refresh_token", None):
                creds.refresh(Request())
            else:
                creds = pydata_google_auth.get_user_credentials(SCOPES, use_local_webserver=True)
        return creds.token
    except Exception as e:
        logger.warning(f"Erro token pydata: {e}")
        return None

def _req(url, headers=None, params=None, stream=False, timeout=60):
    return requests.get(url, headers=headers or {}, params=params or {}, stream=stream, timeout=timeout)

def list_objects(logger, bucket, prefix, token):
    url = f"https://storage.googleapis.com/storage/v1/b/{bucket}/o"
    params = {"prefix": prefix, "fields": "items(name,updated,size,metadata),nextPageToken"}
    if USER_PROJECT: params["userProject"] = USER_PROJECT
    headers = {"Authorization": f"Bearer {token}"}
    items = []
    page = None
    
    while True:
        if page: params["pageToken"] = page
        r = _req(url, headers=headers, params=params, timeout=60)
        if r.status_code != 200:
            raise RuntimeError(f"GCS LIST Fail: {r.status_code} {r.text[:200]}")
        data = r.json()
        items.extend(data.get("items", []))
        page = data.get("nextPageToken")
        if not page: break
        
    logger.info("Objetos listados com prefixo '%s': %d", prefix, len(items))
    return items

def download_object(logger, bucket, object_name, token, dest_path):
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    url = f"https://storage.googleapis.com/download/storage/v1/b/{bucket}/o/{quote(object_name, safe='')}"
    params = {"alt": "media"}
    if USER_PROJECT: params["userProject"] = USER_PROJECT
    headers = {"Authorization": f"Bearer {token}"}
    
    with _req(url, headers=headers, params=params, stream=True, timeout=300) as r:
        if r.status_code != 200:
            raise RuntimeError(f"GCS DOWNLOAD Fail: {r.status_code}")
        
        tmp = dest_path.with_suffix(dest_path.suffix + ".part")
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                if chunk: f.write(chunk)
        tmp.replace(dest_path)
        
    logger.info("Download concluído: %s (%s bytes)", dest_path.name, dest_path.stat().st_size)
    return dest_path

def identificar_arquivos_faltantes(logger):
    logger.info("bq|verificacao|inicio")
    try:
        data_inicio = date(2025, 1, 1)
        data_fim = datetime.now().date() - pd.Timedelta(days=1)
        
        if data_inicio > data_fim:
            logger.info("Data inicio > data fim. Nada a verificar.")
            return []
            
        datas_esperadas = set()
        curr = data_inicio
        while curr <= data_fim:
            datas_esperadas.add(curr)
            curr += pd.Timedelta(days=1)
            
        tabelas = list(set(list(DESTINO_MAP.values()) + EXTRA_TABELAS_DISTINCT))
        datas_encontradas = set()
        
        for tabela in tabelas:
            sql = f"""
                SELECT DISTINCT CAST(MOVIMENTO AS DATE) as mov 
                FROM `{tabela}` 
                WHERE MOVIMENTO >= '2025-01-01'
            """
            try:
                df = pandas_gbq.read_gbq(sql, project_id=PROJECT_ID, use_bqstorage_api=False)
                if not df.empty and 'mov' in df.columns:
                    lista_datas = pd.to_datetime(df['mov']).dt.date.tolist()
                    datas_encontradas.update(lista_datas)
                    logger.info(f"Tabela {tabela}: {len(lista_datas)} datas encontradas.")
            except Exception as e_sql:
                logger.warning(f"Erro ao consultar tabela {tabela}: {e_sql}")

        datas_faltantes = sorted(list(datas_esperadas - datas_encontradas))
        logger.info(f"Total datas faltantes: {len(datas_faltantes)}")
        
        nomes_arquivos = []
        for d in datas_faltantes:
            data_arquivo = d + pd.Timedelta(days=1)
            nome = f"T461-{data_arquivo.strftime('%d-%m-%Y')}.zip"
            nomes_arquivos.append(nome)
            
        return nomes_arquivos
    except Exception as e:
        logger.error(f"Erro crítico ao identificar faltantes: {e}")
        return []

# ==================================================================================================
# MAIN
# ==================================================================================================

def main():
    start_time = datetime.now(TZ)
    logger, log_path = setup_logger()
    logger.info(f"Inicio {SCRIPT_NAME}")
    
    status = "SUCESSO"
    error_msg = ""
    observacao = ""
    
    try:
        config = load_config(logger)
        
        # 1. Identificar Faltantes
        lista_arquivos_alvo = identificar_arquivos_faltantes(logger)
        
        if not lista_arquivos_alvo:
            status = "NO_DATA"
            observacao = "Nenhuma pendência encontrada."
            logger.info(observacao)
        else:
            # 2. Auth GCS
            token = get_access_token(logger)
            if not token:
                raise RuntimeError("Não foi possível obter token GCS.")
            
            # 3. List
            try:
                items_bucket = list_objects(logger, BUCKET, PREFIX, token)
                mapa_bucket = {it.get("name", "").split("/")[-1]: it for it in items_bucket}
            except Exception as e:
                raise RuntimeError(f"Falha ao listar bucket: {e}")
                
            # 4. Download
            erros_download = 0
            arquivos_baixados = []
            
            for nome_arq in lista_arquivos_alvo:
                obj_info = mapa_bucket.get(nome_arq)
                if not obj_info:
                    logger.warning(f"Arquivo solicitado {nome_arq} não encontrado no bucket.")
                    erros_download += 1
                    continue
                
                try:
                    destino = INPUT_DIR / nome_arq # Downloading directly to input dir
                    download_object(logger, BUCKET, obj_info["name"], token, destino)
                    arquivos_baixados.append(destino.name)
                except Exception as e:
                    logger.error(f"Erro download {nome_arq}: {e}")
                    erros_download += 1

            if arquivos_baixados:
                 status = "SUCESSO"
                 observacao = f"Baixados: {len(arquivos_baixados)} | Erros: {erros_download}"
            else:
                if erros_download > 0:
                    status = "FALHA"
                    observacao = "Arquivos solicitados não encontrados."
                else:
                    status = "NO_DATA"

    except Exception as e:
        status = "ERRO"
        error_msg = str(e)
        observacao = error_msg[:200]
        logger.error(f"Fatal: {traceback.format_exc()}")
        
    end_time = datetime.now(TZ)
    zip_path = smart_zip_logs([]) # No output files to zip besides log
    
    body = f"""
    <html><body>
    <h2>Execução {SCRIPT_NAME}</h2>
    <p>Status: {status}</p>
    <p>Obs: {observacao}</p>
    </body></html>
    """
    send_email_outlook(logger, f"{SCRIPT_NAME} - {status}", body, config["emails_principal"], config["emails_cc"], [zip_path])
    record_metrics(logger, start_time, end_time, status, error_msg)

if __name__ == "__main__":
    if "--test" in sys.argv: TEST_MODE = True
    main()
