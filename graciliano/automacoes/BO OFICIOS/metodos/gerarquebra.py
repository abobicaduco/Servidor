import getpass
import logging
import os
import re
import shutil
import sys
import tempfile
import time
import traceback
import zipfile
from datetime import datetime
from pathlib import Path
from subprocess import Popen
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo
from datetime import timedelta

import pandas as pd
import pandas_gbq
import pythoncom
import pytz
from pydata_google_auth import cache as pydata_cache
from pydata_google_auth import get_user_credentials
from win32com.client import Dispatch
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

# ==============================================================================
# CONFIGURAÇÃO DE AMBIENTE
# ==============================================================================
NOME_AUTOMACAO = "BO OFICIOS"
NOME_SCRIPT = Path(__file__).stem.upper()
TZ = ZoneInfo("America/Sao_Paulo")

# Robust Path Detection
POSSIBLE_ROOTS = [
    Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano",
    Path.home() / "Meu Drive/C6 CTVM" / "graciliano",
    Path.home() / "C6 CTVM/graciliano",
]
BASE_DIR = next((p for p in POSSIBLE_ROOTS if p.exists()), Path.home() / "graciliano")
MODULES_PATH = BASE_DIR / "novo_servidor" / "modules"
if str(MODULES_PATH) not in sys.path:
    sys.path.insert(0, str(MODULES_PATH))

# Import Modules
try:
    from modules.TratamentoDados1 import TratamentoDados
    from modules.Machadao1 import Machadao
    from modules.Relatorio import Relatorio
except ImportError:
    try:
        from TratamentoDados1 import TratamentoDados
        from Machadao1 import Machadao
        from Relatorio import Relatorio
    except:
        print("CRÍTICO: Erro ao importar módulos de negócio.")

# Paths
BASE_AUTOM = BASE_DIR / "automacoes" / NOME_AUTOMACAO
LOG_DAY_DIR = BASE_AUTOM / "logs" / NOME_SCRIPT / datetime.now(TZ).strftime("%Y-%m-%d")
INPUT_DIR = BASE_AUTOM / "arquivos_input"
LOCKS_DIR = BASE_AUTOM / ".locks"
TOKENS_DIR = Path.home() / "AppData" / "Roaming" / "CELPY" / "tokens"
DOWNLOADS_BASE = Path.home() / "Downloads" / NOME_SCRIPT.lower()
INPUT_XLSX_ORIG = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Catarina Cristina Bernardes De Freitas - Célula Python - Relatórios de Execução" / "Wall.B" / "PENDENCIAS - INPUT PYTHON" / "Dados Machadao.xlsx"
INPUT_XLSX = INPUT_XLSX_ORIG if INPUT_XLSX_ORIG.exists() else BASE_AUTOM / "arquivos_input" / "Dados Machadao.xlsx"

PROJECT_ID = "datalab-pagamentos"
SCOPES = ["https://www.googleapis.com/auth/bigquery"]

for d in [BASE_AUTOM, LOG_DAY_DIR, INPUT_DIR, LOCKS_DIR, TOKENS_DIR, DOWNLOADS_BASE]:
    d.mkdir(parents=True, exist_ok=True)

# Credentials
try:
    CREDENTIALS = get_user_credentials(SCOPES, credentials_cache=pydata_cache.ReadWriteCredentialsCache(str(TOKENS_DIR)), auth_local_webserver=True)
    pandas_gbq.context.credentials = CREDENTIALS
    pandas_gbq.context.project = PROJECT_ID
except:
    CREDENTIALS = None

# ==============================================================================
# FUNÇÕES DE SUPORTE
# ==============================================================================

def setup_logger():
    ts_nome = datetime.now(TZ).strftime("%Y%m%d_%H%M%S")
    log_file = LOG_DAY_DIR / f"{NOME_SCRIPT.lower()}_{ts_nome}.log"
    
    logger = logging.getLogger(NOME_SCRIPT)
    logger.setLevel(logging.INFO)
    logger.handlers = []
    
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    
    return logger, log_file

def get_config(logger):
    try:
        sql = f"""
            SELECT emails_principais, emails_cc 
            FROM `datalab-pagamentos.ADMINISTRACAO_CELULA_PYTHON.Registro_automacoes` 
            WHERE TRIM(LOWER(script_name)) = '{NOME_SCRIPT.lower()}'
            LIMIT 1
        """
        df = pandas_gbq.read_gbq(sql, project_id=PROJECT_ID)
        if df.empty: return [], []
        def parse(raw):
            if not raw or str(raw).lower() == 'nan': return []
            return [x.strip() for x in str(raw).replace(';',',').split(',') if '@' in x]
        return parse(df.iloc[0]['emails_principais']), parse(df.iloc[0]['emails_cc'])
    except Exception as e:
        logger.error(f"Erro config: {e}")
        return ["carlos.lsilva@c6bank.com"], []

def send_email_outlook(logger, status, subject, body, to, cc, attachments):
    try:
        pythoncom.CoInitialize()
        outlook = Dispatch("Outlook.Application")
        mail = outlook.CreateItem(0)
        mail.To = ";".join(to)
        if status == "SUCESSO" and cc: mail.CC = ";".join(cc)
        mail.Subject = subject
        mail.HTMLBody = body
        for att in attachments:
            if Path(att).exists(): mail.Attachments.Add(str(att))
        mail.Send()
        logger.info("Email enviado via Outlook.")
    except Exception as e:
        logger.error(f"Erro email: {e}")

def main():
    logger, log_file = setup_logger()
    logger.info(f"Iniciando execução. Input: {INPUT_XLSX}")
    
    start_time = time.time()
    dt_inicio = datetime.now(TZ)
    status_final = "FALHA"
    anexos = []
    
    try:
        if not INPUT_XLSX.exists():
            logger.warning(f"Arquivo de input não encontrado: {INPUT_XLSX}")
            status_final = "SEM DADOS"
            return

        df = pd.read_excel(INPUT_XLSX, dtype=str)
        if df.empty:
            logger.info("Planilha vazia.")
            status_final = "SEM DADOS"
            return
        
        logger.info(f"Planilha carregada. Linhas: {len(df)}")
        col_ini = next((c for c in df.columns if 'inicio' in c.lower() or 'inicial' in c.lower()), None)
        col_fim = next((c for c in df.columns if 'fim' in c.lower() or 'final' in c.lower()), None)
        
        if not col_ini or not col_fim:
            raise ValueError("Colunas Inicio/Fim nao encontradas")

        try:
            shutil.rmtree(DOWNLOADS_BASE, ignore_errors=True)
            DOWNLOADS_BASE.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            logger.warning(f"Erro ao limpar temp: {e}")

        # Processamento
        trat = TratamentoDados(DOWNLOADS_BASE)
        df["cpf_cnpj"] = df["cpf_cnpj"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(11)
        
        logger.info("Coletando numeros de conta...")
        df["numero_conta"] = trat.coletaNumConta(df["cpf_cnpj"].tolist())
        
        df_validos = df[df["numero_conta"] != 0].copy()
        processados = 0
        erros = 0
        
        for controle, grupo in df_validos.groupby("numero_controle_envio"):
            try:
                pasta_ctrl = DOWNLOADS_BASE / str(controle)
                pasta_ctrl.mkdir(parents=True, exist_ok=True)
                
                m = Machadao(
                    lista_numero_conta=[int(float(c)) for c in grupo["numero_conta"].tolist()],
                    data_inicial=grupo[col_ini].astype(str).tolist(),
                    data_final=grupo[col_fim].astype(str).tolist(),
                    numero_controle_envio=[str(controle)] * len(grupo),
                    endereco_salvar=str(pasta_ctrl),
                )
                m.assis(oficio_ccs=False)
                processados += 1
            except Exception as e:
                erros += 1
                logger.error(f"Erro controle {controle}: {e}")

        # Zipar
        zip_path = DOWNLOADS_BASE / f"GERARQUEBRA_{dt_inicio.strftime('%Y%m%d_%H%M')}.zip"
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            for root, _, files in os.walk(DOWNLOADS_BASE):
                for f in files:
                    if f != zip_path.name and not f.endswith(".zip"):
                         zf.write(os.path.join(root, f), os.path.relpath(os.path.join(root, f), DOWNLOADS_BASE))
        
        if zip_path.exists(): anexos.append(str(zip_path))
        
        status_final = "SUCESSO" if erros == 0 else "SUCESSO PARCIAL"
        if processados == 0 and len(df) > 0: status_final = "FALHA"

    except Exception as e:
        status_final = "FALHA"
        logger.error(f"Erro fatal: {traceback.format_exc()}")
        
    finally:
        # Finalização
        duration = time.time() - start_time
        tempo_exec_str = str(timedelta(seconds=int(duration)))
        usuario_exec = os.getenv("USUARIO_EXEC", getpass.getuser())
        if "@" not in usuario_exec: usuario_exec += "@c6bank.com"
        
        recip_to, recip_cc = get_config(logger)
        
        # Email
        body = f"""
        <html><body>
            <h3>Status: {status_final}</h3>
            <p>Tempo: {tempo_exec_str}</p>
            <p>Log em anexo.</p>
        </body></html>
        """
        if os.path.exists(str(log_file)): anexos.append(str(log_file))
        send_email_outlook(logger, status_final, f"{NOME_AUTOMACAO} - {NOME_SCRIPT} - {status_final}", body, recip_to, recip_cc, anexos)
        
        # Metricas
        try:
            df_m = pd.DataFrame([{
                "nome_automacao": NOME_AUTOMACAO,
                "metodo_automacao": NOME_SCRIPT,
                "status": status_final,
                "tempo_exec": tempo_exec_str,
                "data_exec": dt_inicio.strftime("%Y-%m-%d"),
                "hora_exec": dt_inicio.strftime("%H:%M:%S"),
                "usuario": usuario_exec,
                "log_path": str(log_file)
            }])
            pandas_gbq.to_gbq(df_m, "datalab-pagamentos.ADMINISTRACAO_CELULA_PYTHON.automacoes_exec", project_id=PROJECT_ID, if_exists="append")
            logger.info("Metricas OK.")
        except Exception as e:
            logger.error(f"Erro metricas: {e}")

if __name__ == "__main__":
    main()
