import sys
import os
import shutil
import traceback
import logging
import zipfile
import time
import subprocess
import gzip
import pandas as pd
import pandas_gbq
import win32com.client as win32
import pythoncom
import pytz
from pathlib import Path
from datetime import datetime, date
from google.cloud import bigquery

# ==================================================================================================
# CONSTANTES E CONFIGURAÇÕES
# ==================================================================================================

SCRIPT_NAME = "consigclt"
AREA_NAME = "BO CONSIGNADO"

TZ = pytz.timezone("America/Sao_Paulo")
ENV_EXEC_USER = os.getenv("ENV_EXEC_USER", os.getlogin().lower() if 'os' in locals() and hasattr(os, 'getlogin') else "unknown")
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
INPUT_DIR = AUTOMACOES_DIR / AREA_NAME / "arquivos input" / "INSS"

# BigQuery
PROJECT_ID = "datalab-pagamentos"
DATASET_ID = "ADMINISTRACAO_CELULA_PYTHON"
BQ_DATASET_NEGOCIO = "conciliacoes_monitoracao"

REGISTRO_AUTOMACOES_TABLE = f"{PROJECT_ID}.ADMINISTRACAO_CELULA_PYTHON.registro_automacoes"
AUTOMACOES_EXEC_TABLE = f"{PROJECT_ID}.ADMINISTRACAO_CELULA_PYTHON.automacoes_exec"

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
             # Fallback
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

def buscar_anexos_outlook(destino_dir: Path, logger) -> list:
    """Busca anexos .csv.gz no Outlook (Pastas específicas C6)"""
    salvos = []
    if TEST_MODE: return []
    
    try:
        pythoncom.CoInitialize()
        app = win32.Dispatch("Outlook.Application")
        ns = app.GetNamespace("MAPI")
        pastas = []
        
        # Tenta localizar pasta específica
        try:
            for i in range(1, ns.Folders.Count + 1):
                fld = ns.Folders.Item(i)
                if "Celula Python Monitoracao" in getattr(fld, "Name", ""):
                    pastas.append(fld.Folders["Inbox"])
                    break
        except: pass
        
        # Fallback Inbox Padrão
        try:
            pastas.append(ns.GetDefaultFolder(6))
        except: pass
        
        hoje = datetime.now().date()
        
        for inbox in pastas:
            try:
                items = inbox.Items
                items.Sort("[ReceivedTime]", True)
                for msg in items:
                    if getattr(msg, "Class", 0) != 43: continue 
                    rt = getattr(msg, "ReceivedTime", None)
                    if not rt: continue
                    
                    # Converte pywintypes.datetime para datetime.date
                    if hasattr(rt, 'date'):
                         rt_date = rt.date()
                    else:
                         rt_date = datetime.fromtimestamp(rt).date()
                         
                    if rt_date != hoje:
                        if rt_date < hoje: break 
                        continue
                    
                    atts = getattr(msg, "Attachments", None)
                    if not atts or atts.Count <= 0: continue
                    
                    for j in range(1, atts.Count + 1):
                        att = atts.Item(j)
                        fn = str(att.FileName or "")
                        if fn.lower().endswith(".csv.gz") and "FECOAPI01" in fn:
                            destino = destino_dir / fn
                            k = 1
                            while destino.exists():
                                destino = destino_dir / f"{Path(fn).stem} ({k}){Path(fn).suffix}"
                                k += 1
                            att.SaveAsFile(str(destino))
                            salvos.append(destino)
                            logger.info(f"ANEXO_SALVO: {destino.name}")
            except Exception as e:
                 # logger.debug(f"Iter msg error: {e}")
                 continue
            
    except Exception as e:
        logger.error(f"Erro ao buscar outlook: {e}")
    finally:
        try: pythoncom.CoUninitialize()
        except: pass
    return salvos

def extrair_arquivo(arquivo_gz: Path, destino_dir: Path, logger) -> Path:
    destino_dir.mkdir(parents=True, exist_ok=True)
    
    # 1. Tentar gzip nativo (Mais rápido e confiável que subprocess se for gzip puro)
    try:
        destino_csv = destino_dir / arquivo_gz.stem 
        if destino_csv.suffix.lower() != '.csv': 
             destino_csv = destino_csv.with_suffix('.csv')
             
        with gzip.open(arquivo_gz, "rb") as f_in, open(destino_csv, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        if destino_csv.exists():
            return destino_csv
    except Exception as e:
        logger.warning(f"Erro extração gzip nativo: {e}. Tentando 7zip.")

    # 2. Fallback 7-Zip
    candidatos_7z = ["7z", "7z.exe", r"C:\Program Files\7-Zip\7z.exe"]
    for exe in candidatos_7z:
        try:
            cmd = [exe, "x", "-y", f"-o{str(destino_dir)}", str(arquivo_gz)]
            subprocess.run(cmd, capture_output=True, check=True)
            
            csvs = list(destino_dir.glob("*.csv"))
            if csvs:
                return max(csvs, key=lambda p: p.stat().st_mtime)
        except Exception:
            continue
            
    return None

def ler_csv_customizado(caminho: Path, logger) -> pd.DataFrame:
    cols_esperadas = [
        "cbc", "nu_contrato", "vl_parcela_paga", "dt_pagamento_guia",
        "dt_repasse_if", "nu_guia", "mes_referencia", "ID"
    ]
    
    try:
        try:
            df = pd.read_csv(caminho, sep=";", dtype=str, encoding="latin1", header=None, keep_default_na=False)
        except:
            df = pd.read_csv(caminho, sep=";", dtype=str, encoding="utf-8", header=None, keep_default_na=False)
            
        if df.empty: return pd.DataFrame()
        
        primeira_linha = [str(x).strip().lower() for x in df.iloc[0].tolist()]
        if "cbc" in primeira_linha and "nu_contrato" in primeira_linha:
            df = df.iloc[1:].copy()
            
        if 6 <= df.shape[1] <= 10:
            df = df.iloc[:, :8].copy()
            while df.shape[1] < 8:
                df[f"extra_{df.shape[1]}"] = ""
            df.columns = cols_esperadas
            return df
        
        return df 
        
    except Exception as e:
        logger.error(f"Erro ler CSV {caminho.name}: {e}")
        return pd.DataFrame()

def classificar_arquivo(df: pd.DataFrame):
    cols = [str(c).strip().lower() for c in df.columns]
    
    if "dt_repasse_if" in cols and "mes_referencia" in cols:
        return "CONSIGCLTREPASSE"
        
    escrituracao_keys = {"competencia_referencia", "processamento_dataprev", "processamento_ambiente_nacional", "id_evento"}
    if any(c in cols for c in escrituracao_keys):
        return "CONSIGCLTESCRITUR"
        
    return "CONSIGCLTREPASSE" if df.shape[1] <= 10 else "CONSIGCLTESCRITUR"

def subir_bq_com_staging(df: pd.DataFrame, table_name: str, logger):
    colunas_padrao = [
        "nome_arquivo", "cbc", "nu_contrato", "competencia_referencia",
        "valor_parcela_desconto", "processamento_ambiente_nacional",
        "processamento_dataprev", "id_evento", "id_evento_retificacao",
        "id_evento_exclusao", "cpf_trabalhador", "matricula", "ID",
        "mes_referencia", "dt_coleta"
    ]
    
    df_upload = df.copy()
    for col in colunas_padrao:
        if col not in df_upload.columns:
            df_upload[col] = None
            
    df_upload = df_upload[colunas_padrao]
    
    table_fqn = f"{PROJECT_ID}.{BQ_DATASET_NEGOCIO}.{table_name}"
    table_staging = f"{BQ_DATASET_NEGOCIO}.{table_name}_STAGING"
    
    logger.info(f"Subindo {len(df_upload)} linhas para {table_staging}")
    pandas_gbq.to_gbq(
        df_upload,
        destination_table=table_staging,
        project_id=PROJECT_ID,
        if_exists="replace",
        use_bqstorage_api=False
    )
    
    cols_sql = ", ".join([f"`{c}`" for c in colunas_padrao])
    join_cond = " AND ".join([f"(T.`{c}` = S.`{c}` OR (T.`{c}` IS NULL AND S.`{c}` IS NULL))" for c in colunas_padrao])
    
    query = f"""
    INSERT INTO `{table_fqn}` ({cols_sql})
    SELECT * FROM `{PROJECT_ID}.{table_staging}` S
    WHERE NOT EXISTS (
        SELECT 1 FROM `{table_fqn}` T
        WHERE {join_cond}
    )
    """
    
    client = bigquery.Client(project=PROJECT_ID)
    try: client.get_table(table_fqn)
    except:
        schema = [bigquery.SchemaField(c, "STRING") for c in colunas_padrao]
        t = bigquery.Table(table_fqn, schema=schema)
        client.create_table(t)
        logger.info(f"Tabela criada: {table_fqn}")

    job = client.query(query)
    job.result()
    
    return len(df_upload)

def main():
    start_time = datetime.now(TZ)
    logger, log_path = setup_logger()
    logger.info(f"Inicio {SCRIPT_NAME}")
    
    status = "SUCESSO"
    error_msg = ""
    processed_files = []
    total_linhas = 0
    
    try:
        config = load_config(logger)
        
        INPUT_DIR.mkdir(parents=True, exist_ok=True)
        logger.info("Buscando anexos Outlook...")
        try:
             buscar_anexos_outlook(INPUT_DIR, logger)
        except Exception as e:
             logger.warning(f"Erro busca outlook: {e}")
        
        arquivos = sorted(
            list(INPUT_DIR.glob("*.csv.gz")) + list(INPUT_DIR.glob("*.xlsx")),
            key=lambda x: x.stat().st_mtime
        )
        
        if not arquivos:
            status = "NO_DATA"
            logger.info("Nenhum arquivo encontrado.")
        else:
            logger.info(f"Arquivos: {len(arquivos)}")
            for arq in arquivos:
                try:
                    logger.info(f"Processando: {arq.name}")
                    
                    df = None
                    if arq.suffix.lower() in [".xlsx", ".xls"]:
                        df = pd.read_excel(arq, dtype=str, engine="openpyxl").fillna("")
                    else:
                        csv_path = extrair_arquivo(arq, TEMP_DIR, logger)
                        if csv_path:
                            df = ler_csv_customizado(csv_path, logger)
                            try: csv_path.unlink() 
                            except: pass
                    
                    if df is None or df.empty:
                        logger.warning(f"Vazio/Inválido: {arq.name}")
                        continue
                        
                    target_table = classificar_arquivo(df)
                    logger.info(f"Tabela Destino: {target_table}")
                    
                    if "nome_arquivo" not in df.columns: df.insert(0, "nome_arquivo", arq.name)
                    else: df["nome_arquivo"] = arq.name
                        
                    if "dt_coleta" not in df.columns: df["dt_coleta"] = datetime.now().strftime("%Y-%m-%d")
                    
                    inseridas = subir_bq_com_staging(df, target_table, logger)
                    total_linhas += inseridas
                    processed_files.append(arq)
                    
                    if config["move_file"]:
                         try:
                            # Move para LOG_DIR ao invés de estrutura complexa de rede
                            dest = LOG_DIR / arq.name
                            shutil.copy2(str(arq), str(dest))
                            if arq.exists(): arq.unlink()
                            logger.info(f"Movido: {arq.name}")
                         except Exception as e:
                            logger.error(f"Erro mover {arq.name}: {e}")

                except Exception as e:
                    logger.error(f"Erro {arq.name}: {e}")

            if total_linhas == 0:
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
    <p>Linhas Inseridas: {total_linhas}</p>
    </body></html>
    """
    send_email_outlook(logger, f"{SCRIPT_NAME} - {status}", body, config["emails_principal"], config["emails_cc"], [zip_path])
    record_metrics(logger, start_time, end_time, status, error_msg)

if __name__ == "__main__":
    if "--test" in sys.argv: TEST_MODE = True
    main()
