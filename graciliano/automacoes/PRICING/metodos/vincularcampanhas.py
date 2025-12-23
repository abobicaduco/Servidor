import argparse
import getpass
import importlib.util
import json
import logging
import os
import shutil
import sys
import threading
import time
import tkinter as tk
from tkinter import simpledialog, messagebox
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import google.auth
import google.auth.exceptions
import google.auth.transport.requests
import pandas as pd
import polars as pl
import requests
from playwright.sync_api import (
    BrowserContext,
    Page,
    TimeoutError as PWTimeoutError,
    sync_playwright,
)

# -----------------------------------------------------------------------------
# 1. CONFIGURAÇÃO DE PATH E IMPORTAÇÃO DO MÓDULO UTILITÁRIO
# -----------------------------------------------------------------------------
# Tenta localizar o caminho correto do novo_servidor/modules
POSSIBLE_ROOTS = [
    Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano",
    Path.home() / "Meu Drive/C6 CTVM" / "graciliano",
    Path.home() / "C6 CTVM/graciliano",
]
BASE_GRACILIANO = next((p for p in POSSIBLE_ROOTS if p.exists()), None)

if BASE_GRACILIANO:
     UTIL_PATH = BASE_GRACILIANO / "novo_servidor" / "modules"
else:
     # Fallback
     UTIL_PATH = Path.home() / "graciliano" / "novo_servidor" / "modules"

sys.path.append(str(UTIL_PATH))

try:
    import _utilAutomacoesExec
except ImportError:
    logging.warning("Modulo _utilAutomacoesExec nao encontrado. Funcionalidades de metricas e email podem falhar.")
    _utilAutomacoesExec = None

# Import Dollynho local ou do modules
def _carregar_dollynho() -> Any:
    candidatos = [
        Path(__file__).with_name("dollynho.py"),
        Path(__file__).parent / "dollynho.py",
        UTIL_PATH / "dollynho.py",
    ]
    dolly_path = next((p for p in candidatos if p.is_file()), None)
    if dolly_path is None:
         # Mock se nao achar (para nao quebrar execução local sem dollynho)
         class MockDollynho:
             def get_credencial(self, name=None): return "user_mock", "pass_mock"
         return MockDollynho()

    spec = importlib.util.spec_from_file_location("dollynho", dolly_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("FALHA AO CARREGAR DOLLYNHO.PY")
    
    mod = importlib.util.module_from_spec(spec)
    sys.modules["dollynho"] = mod
    spec.loader.exec_module(mod)
    return mod

dollynho = _carregar_dollynho()

# -----------------------------------------------------------------------------
# 2. CONSTANTES GLOBAIS
# -----------------------------------------------------------------------------
NOME_AUTOMACAO = "PRICING"
NOME_SCRIPT = Path(__file__).stem.upper()
SCRIPT_STEM = Path(__file__).stem
NOME_SERVIDOR = "Servidor.py"

TZ = ZoneInfo("America/Sao_Paulo")
INICIO_EXEC_SP = datetime.now(TZ)
DATA_EXEC = INICIO_EXEC_SP.date().isoformat()
HORA_EXEC = INICIO_EXEC_SP.strftime("%H:%M:%S")

# Configurações do Negócio
BQ_PROJECT_ID = "datalab-pagamentos"
BQ_SOURCE_PROJECT_ID = "c6-banco-comercial-analytics"
BQ_LOCATION = "US"
BQ_DATASET_DESTINO = "ADMINISTRACAO_CELULA_PYTHON"
BQ_TABLE_DESTINO = "VincularCampanhas"

RETCODE_SUCESSO = 0
RETCODE_FALHA = 1
RETCODE_SEMDADOSPARAPROCESSAR = 2

LIMITE_ABAS = 5
HEADLESS_DEFAULT = False

# Seletores Rundeck
X_RD_LOGIN_USUARIO = {"css": "#login"}
X_RD_LOGIN_SENHA = {"css": "#password"}
X_RD_BTN_ENTRAR = {"css": "#btn-login"}
X_RD_BTN_RUN = {"css": "#execFormRunButton"}
X_RD_STATUS_OK = {"css": "span.execstate.overall[data-execstate='SUCCEEDED']"}
X_RD_INTERSTITIAL = {"css": "#main-frame-error"}
X_RD_BTN_LOG = {"css": "#btn_view_output"}
X_RD_LOG_TEXT = {"css": "span.execution-log__content-text"}
X_RD_INPUT_FILE = "input[name='extra.option.FILE']"

# VARIAVEL SOLICITADA PELO USUARIO
DATA_ESPECIFICA = True

# -----------------------------------------------------------------------------
# 3. CAMINHOS E PASTAS
# -----------------------------------------------------------------------------
BASE_DIR = BASE_GRACILIANO / "automacoes" / NOME_AUTOMACAO if BASE_GRACILIANO else Path.cwd()

PASTA_INPUT = BASE_DIR / "arquivos input" / SCRIPT_STEM
PASTA_LOGS = BASE_DIR / "logs" / SCRIPT_STEM / DATA_EXEC
PASTA_SESSIONS = BASE_DIR / "PLAYWRIGHT_SESSIONS" / SCRIPT_STEM

# -----------------------------------------------------------------------------
# 4. CLASSES E FUNÇÕES AUXILIARES
# -----------------------------------------------------------------------------

class Execucao:
    def __init__(self):
        self.modo_execucao = "AUTO"
        self.observacao = "AUTO"
        self.usuario = f"{getpass.getuser()}@c6bank.com"

    def is_servidor(self) -> bool:
        return (
            len(sys.argv) > 1
            or os.getenv("SERVIDOR_ORIGEM") is not None
            or os.getenv("MODO_EXECUCAO") is not None
            or os.getenv("ENV_EXEC_MODE") == "AGENDAMENTO"
        )
            
    def detectar(self) -> Tuple[str, str, str]:
        if self.is_servidor():
            return "AUTO", "AUTO", self.usuario
        # Se for manual local, assumimos modo SOLICITACAO ou AUTO
        return "AUTO", "AUTO", self.usuario

def _setup_logger() -> Tuple[logging.Logger, Path]:
    PASTA_LOGS.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(TZ).strftime("%Y%m%d_%H%M%S")
    log_path = PASTA_LOGS / f"{SCRIPT_STEM}_{ts}.log"

    logger = logging.getLogger(NOME_SCRIPT)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    
    for h in list(logger.handlers):
        logger.removeHandler(h)

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(sh)
    
    return logger, log_path

def obter_destinatarios(logger: logging.Logger) -> tuple[list[str], list[str]]:
    """Busca destinatários (TO e CC) na tabela Registro_automacoes."""
    import pandas_gbq
    
    proj = "datalab-pagamentos"
    nome_script_filtro = SCRIPT_STEM

    sql = (
        "SELECT emails_principais, emails_cc "
        "FROM `datalab-pagamentos.ADMINISTRACAO_CELULA_PYTHON.Registro_automacoes` "
        f"WHERE lower(TRIM(metodo_automacao)) = lower(TRIM('{nome_script_filtro}')) "
        "AND (status_automacao IS NULL OR lower(status_automacao) NOT IN ('INATIVA','DESATIVADA','DESLIGADA')) "
        "ORDER BY SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', data_lancamento) DESC "
        "LIMIT 1"
    )
    try:
        df = pandas_gbq.read_gbq(sql, project_id=proj, dialect='standard') # creds implicitas ou via env
    except Exception as e:
        logger.error(f"FALHA AO LER DESTINATARIOS DO BQ: {e}")
        return [], []
        
    if df.empty:
        logger.warning(f"NAO ENCONTREI DESTINATARIOS NO BQ PARA metodo_automacao={nome_script_filtro}")
        return [], []
        
    def _clean_emails(raw):
        if not raw or str(raw).lower() == 'nan': return []
        return [x.strip() for x in str(raw).replace(';', ',').split(',') if '@' in x]

    to_col = _clean_emails(df.iloc[0].get("emails_principais"))
    cc_col = _clean_emails(df.iloc[0].get("emails_cc"))
    
    return to_col, cc_col

def formatar_tempo(seconds: float) -> str:
    s = int(max(0, round(seconds)))
    h = s // 3600
    m = (s % 3600) // 60
    ss = s % 60
    return f"{h:02d}:{m:02d}:{ss:02d}"

# -----------------------------------------------------------------------------
# 5. INTEGRACAO TKINTER (DATA ESPECIFICA)
# -----------------------------------------------------------------------------
def solicitar_data_tkinter() -> Optional[str]:
    """
    Abre janela Tkinter para user inserir data.
    Retorna YYYY-MM-DD ou None se cancelar.
    Salva ultima data em arquivo temp para persistencia simples.
    """
    try:
        root = tk.Tk()
        root.withdraw() # Esconde janela principal
        
        # Sobrepor
        root.attributes("-topmost", True)
        
        # Arquivo de persistencia de data
        cache_file = BASE_DIR / "last_date_config.txt"
        last_date = date.today().isoformat()
        if cache_file.exists():
            try: last_date = cache_file.read_text().strip()
            except: pass
            
        user_input = simpledialog.askstring(
            title="Data de Processamento",
            prompt="Insira a data de corte (YYYY-MM-DD):",
            initialvalue=last_date,
            parent=root
        )
        
        root.destroy()
        
        if not user_input:
            return None
            
        # Validacao simples
        try:
            val_dt = datetime.strptime(user_input.strip(), "%Y-%m-%d").date()
            # Salvar cache
            try: cache_file.write_text(val_dt.isoformat())
            except: pass
            return val_dt.isoformat()
        except ValueError:
            # Fallback erro
            root2 = tk.Tk()
            root2.withdraw()
            root2.attributes("-topmost", True)
            messagebox.showerror("Erro", "Formato inválido! Use YYYY-MM-DD.")
            root2.destroy()
            return None
            
    except Exception as e:
        print(f"Erro Tkinter: {e}")
        return None

# -----------------------------------------------------------------------------
# 6. CLASSES DE NEGÓCIO E PLAYWRIGHT
# -----------------------------------------------------------------------------

class BigQueryHelper:
    @staticmethod
    def _get_access_token() -> str:
        tok = os.getenv("GCP_ACCESS_TOKEN") or os.getenv("BQ_TOKEN")
        if tok: return tok.strip()
        try:
            creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/bigquery"])
            if not creds.valid:
                req = google.auth.transport.requests.Request()
                creds.refresh(req)
            return creds.token
        except Exception:
            pass
        return ""

    @staticmethod
    def query_polars(logger: logging.Logger, sql: str, project_id: str = BQ_PROJECT_ID) -> pl.DataFrame:
        # Simplificado: Usar pandas_gbq e converter para polars se preciso, 
        # ou usar o request se pandas_gbq nao estiver disponivel
        try:
            df = pandas_gbq.read_gbq(sql, project_id=project_id, dialect='standard')
            return pl.from_pandas(df)
        except Exception as e:
            logger.error(f"Erro BQ Query (PandasGBQ): {e}")
            return pl.DataFrame()

    @staticmethod
    def insert_rows(logger: logging.Logger, rows: List[dict], project_id: str, dataset_id: str, table_id: str) -> int:
        if not rows: return 0
        df = pd.DataFrame(rows)
        try:
            pandas_gbq.to_gbq(
                df, 
                f"{dataset_id}.{table_id}", 
                project_id=project_id, 
                if_exists='append'
            )
            return len(df)
        except Exception as e:
            logger.error(f"Erro Insert BQ: {e}")
            return 0

class PlaywrightWorker:
    def __init__(self, logger: logging.Logger, headless: bool = True):
        self.logger = logger
        self.headless = headless

    def _get_element(self, page: Page, spec: Any):
        if isinstance(spec, dict):
            if "css" in spec: return page.locator(spec["css"])
            if "xpath" in spec: return page.locator(f"xpath={spec['xpath']}")
        if isinstance(spec, str):
            if spec.startswith("//"): return page.locator(f"xpath={spec}")
            return page.locator(spec)
        return page.locator(str(spec))

    def login_rundeck(self, page: Page, user: str, pwd: str):
        page.goto("https://tasks.corp/user/login", wait_until="domcontentloaded")
        if "/user/login" not in page.url:
            return

        u = self._get_element(page, X_RD_LOGIN_USUARIO)
        p = self._get_element(page, X_RD_LOGIN_SENHA)
        b = self._get_element(page, X_RD_BTN_ENTRAR)
        
        u.wait_for(state="visible", timeout=10000)
        u.fill(user)
        p.fill(pwd)
        
        if b.count() > 0:
            b.click()
        else:
            p.press("Enter")
            
        page.wait_for_url("**/user/login", wait_until="domcontentloaded", timeout=15000)

    def run_job(self, page: Page, job_url: str, params: List[dict], file_path: Optional[Path]) -> Tuple[str, str]:
        page.goto(job_url, wait_until="domcontentloaded")
        
        for p in params:
            if p["campo"].upper() == "CAMPAIGN_ID":
                loc = page.locator("input[name='extra.option.CAMPAIGN_ID']")
                if loc.count() > 0:
                    loc.fill(str(p["valor"]))
        
        if file_path and file_path.exists():
            try:
                self._get_element(page, X_RD_INPUT_FILE).set_input_files(str(file_path))
            except Exception as e:
                self.logger.warning(f"Erro upload: {e}")

        btn_run = self._get_element(page, X_RD_BTN_RUN)
        if btn_run.count() > 0:
            btn_run.click()
        else:
            return "FALHA_INIT", "Botao Run nao encontrado"

        start = time.time()
        final_status = "UNKNOWN"
        logs = ""
        
        while time.time() - start < 600: 
            try:
                if self._get_element(page, X_RD_STATUS_OK).count() > 0:
                    final_status = "SUCCEEDED"
                    break
                if page.locator(".execstate[data-execstate='FAILED']").count() > 0:
                    final_status = "FAILED"
                    break
                time.sleep(2)
            except Exception:
                pass
        
        try:
            btn_log = self._get_element(page, X_RD_BTN_LOG)
            if btn_log.count() > 0:
                btn_log.click()
                time.sleep(1)
                logs = "\n".join(page.locator(X_RD_LOG_TEXT["css"]).all_text_contents())
        except Exception:
            pass
            
        return final_status, logs

def worker_processar_campanha(cid: str, csv_path: Path, cred_user: str, cred_pass: str, logger: logging.Logger) -> dict:
    res = {"campaign_id": cid, "status": "ERROR", "log": ""}
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(channel="chrome", headless=HEADLESS_DEFAULT)
            context = browser.new_context(viewport={"width": 1920, "height": 1080}, accept_downloads=True)
            page = context.new_page()
            
            worker = PlaywrightWorker(logger)
            worker.login_rundeck(page, cred_user, cred_pass)
            
            job_url = "https://tasks.corp/project/attfincards/job/show/bcd7569b-ddf2-4a4b-8561-5f0b6926175c"
            status, logs = worker.run_job(
                page, job_url, 
                [{"campo": "CAMPAIGN_ID", "valor": cid}], 
                csv_path
            )
            
            res["status"] = status
            res["log"] = logs
            
            context.close()
            browser.close()
    except Exception as e:
        res["log"] = str(e)
    
    return res

# -----------------------------------------------------------------------------
# 7. LÓGICA DE NEGÓCIO PRINCIPAL
# -----------------------------------------------------------------------------

def baixar_dados_input(logger: logging.Logger, data_corte: str) -> pl.DataFrame:
    logger.info(f"Baixando dados para corte: {data_corte}")
    
    sql_corte = f"SELECT Data_Vencimento FROM `{BQ_SOURCE_PROJECT_ID}.SHARED_OPS.TB_DATA_CORTE_FATURAS` WHERE DATA_GERACAO_ROBO = '{data_corte}'"
    df_corte = BigQueryHelper.query_polars(logger, sql_corte)
    
    if df_corte.is_empty():
        logger.warning("Sem data de vencimento encontrada.")
        return pl.DataFrame()
        
    vencimento_raw = df_corte[0, "Data_Vencimento"]
    logger.info(f"Vencimento encontrado: {vencimento_raw}")
    
    sql_publico = f"SELECT ACCOUNT_ID, CAMPAIGN_ID FROM `{BQ_SOURCE_PROJECT_ID}.SHARED_OPS.TB_PUBLICO_PARCELAMENTO_FATURA_PF` WHERE DT_VENCIMENTOCOBRANCA = '{vencimento_raw}'"
    df_publico = BigQueryHelper.query_polars(logger, sql_publico)
    
    if df_publico.is_empty():
        logger.warning("Sem publico para processar.")
        return pl.DataFrame()
        
    PASTA_INPUT.mkdir(parents=True, exist_ok=True)
    parquet_path = PASTA_INPUT / "base.parquet"
    df_publico.write_parquet(parquet_path)
    
    pdf = df_publico.to_pandas()
    pdf["CAMPAIGN_ID"] = pdf["CAMPAIGN_ID"].fillna(0).astype(int)
    
    for cid, grp in pdf.groupby("CAMPAIGN_ID"):
        fpath = PASTA_INPUT / f"{cid}.csv"
        grp[["ACCOUNT_ID"]].to_csv(fpath, index=False, header=False)
    
    return df_publico.with_columns(pl.lit(str(vencimento_raw)).alias("DT_VENCIMENTO"))

def processar_campanhas(logger: logging.Logger, df_input: pl.DataFrame, cred_user: str, cred_pass: str) -> List[dict]:
    campanhas = df_input["CAMPAIGN_ID"].unique().to_list()
    campanhas = [str(int(c)) for c in campanhas if c is not None]
    
    logger.info(f"Iniciando processamento de {len(campanhas)} campanhas com {LIMITE_ABAS} threads.")
    resultados = []
    
    with ThreadPoolExecutor(max_workers=LIMITE_ABAS) as executor:
        futures = []
        for cid in campanhas:
            csv_path = PASTA_INPUT / f"{cid}.csv"
            futures.append(executor.submit(
                worker_processar_campanha, 
                cid, csv_path, cred_user, cred_pass, logger
            ))
            
        for f in as_completed(futures):
            resultados.append(f.result())
            
    return resultados

def consolidar_e_publicar_bq(logger: logging.Logger, df_input: pl.DataFrame, resultados: List[dict], data_corte: str) -> int:
    status_map = {r["campaign_id"]: r["status"] for r in resultados}
    ts_now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    run_id = datetime.now(TZ).strftime("%Y%m%d_%H%M%S")
    vencimento = df_input[0, "DT_VENCIMENTO"] if "DT_VENCIMENTO" in df_input.columns else ""

    rows_to_insert = []
    input_rows = df_input.to_dicts()
    
    for row in input_rows:
        cid = str(int(row.get("CAMPAIGN_ID", 0) or 0))
        st = status_map.get(cid, "UNKNOWN")
        item = {
            "DATA_GERACAO_ROBO": data_corte,
            "DT_VENCIMENTOCOBRANCA": vencimento,
            "CAMPAIGN_ID": int(cid),
            "ACCOUNT_ID": str(row.get("ACCOUNT_ID", "")),
            "DT_COLETA": ts_now,
            "JOB_STATUS": st,
            "RUN_ID": run_id,
            "SCRIPT": NOME_SCRIPT
        }
        rows_to_insert.append(item)
        
    logger.info(f"Inserindo {len(rows_to_insert)} linhas no BQ...")
    inserted = BigQueryHelper.insert_rows(
        logger, rows_to_insert, 
        BQ_PROJECT_ID, "ADMINISTRACAO_CELULA_PYTHON", "VincularCampanhas"
    )
    return inserted

def mover_arquivos_processados(logger: logging.Logger):
    PASTA_INPUT.mkdir(parents=True, exist_ok=True)
    PASTA_LOGS.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(TZ).strftime("%Y%m%d_%H%M%S")
    
    for f in PASTA_INPUT.glob("*"):
        if f.is_file():
            try:
                novo_nome = PASTA_LOGS / f"{f.stem}_{ts}{f.suffix}"
                shutil.move(str(f), str(novo_nome))
            except Exception as e:
                logger.warning(f"Erro ao mover arquivo {f.name}: {e}")

# -----------------------------------------------------------------------------
# 8. FUNÇÃO PRINCIPAL
# -----------------------------------------------------------------------------

def main() -> int:
    logger, log_path = _setup_logger()
    exec_obj = Execucao()
    modo_execucao, observacao, usuario = exec_obj.detectar()
    
    # Parser CLI
    parser = argparse.ArgumentParser()
    parser.add_argument("param", nargs="?", help="Data Corte YYYY-MM-DD")
    parser.add_argument("--no-baixar", action="store_true", help="Pular download BQ")
    args, _ = parser.parse_known_args()
    
    data_corte = args.param or date.today().isoformat()
    pular_download = args.no_baixar

    # ==== LÓGICA DE DATA ESPECÍFICA (User Request) ====
    # Se DATA_ESPECIFICA for True, e não estiver rodando via servidor (agendamento),
    # abrimos a janela do Tkinter para o usuário escolher a data.
    if DATA_ESPECIFICA and not exec_obj.is_servidor():
        dt_selecionada = solicitar_data_tkinter()
        if dt_selecionada:
            data_corte = dt_selecionada
            logger.info(f"DATA SELECIONADA VIA TKINTER: {data_corte}")
        else:
            logger.warning("Seleção de data cancelada pelo usuário. Usando data padrao/atual.")

    logger.info("=" * 50)
    logger.info(f"INICIO PROCESSAMENTO: {NOME_SCRIPT}")
    logger.info(f"MODO: {modo_execucao} | USER: {usuario}")
    logger.info(f"DATA CORTE: {data_corte}")
    logger.info("=" * 50)

    try:
        cred_user, cred_pass = dollynho.get_credencial("VincularCampanhas")
        if not cred_user or not cred_pass: raise ValueError("Credenciais vazias")
    except Exception as e:
        logger.error(f"Falha credenciais: {e}")
        return RETCODE_FALHA

    t0 = time.perf_counter()
    status_final = "FALHA"
    retcode = RETCODE_FALHA
    linhas_processadas = 0
    linhas_inseridas = 0
    
    try:
        df_input = pl.DataFrame()
        if not pular_download:
            df_input = baixar_dados_input(logger, data_corte)
        else:
            pq_path = PASTA_INPUT / "base.parquet"
            if pq_path.exists():
                df_input = pl.read_parquet(pq_path)
        
        linhas_processadas = df_input.height
        
        if linhas_processadas == 0:
            status_final = "SEM DADOS PARA PROCESSAR"
            retcode = RETCODE_SEMDADOSPARAPROCESSAR
            logger.info("Sem dados.")
        else:
            resultados = processar_campanhas(logger, df_input, cred_user, cred_pass)
            linhas_inseridas = consolidar_e_publicar_bq(logger, df_input, resultados, data_corte)
            
            try:
                res_df = pd.DataFrame(resultados)
                res_path = PASTA_LOGS / f"resultados_{datetime.now(TZ).strftime('%Y%m%d_%H%M%S')}.xlsx"
                res_df.to_excel(res_path, index=False)
            except Exception: pass

            status_final = "SUCESSO"
            retcode = RETCODE_SUCESSO
            
    except Exception as e:
        logger.exception("ERRO FATAL NA EXECUCAO")
        status_final = "FALHA"
        retcode = RETCODE_FALHA
    
    finally:
        tempo_exec = formatar_tempo(time.perf_counter() - t0)
        mover_arquivos_processados(logger)

        if _utilAutomacoesExec:
            try:
                dest_to, dest_cc = obter_destinatarios(logger)
                lista_emails = list(set(dest_to + dest_cc)) if status_final == "SUCESSO" else list(set(dest_to))
                
                client = _utilAutomacoesExec.AutomacoesExecClient(logger)
                client.publicar(
                    nome_automacao=NOME_AUTOMACAO,
                    metodo_automacao=SCRIPT_STEM,
                    status=status_final,
                    tempo_exec=tempo_exec,
                    data_exec=DATA_EXEC,
                    hora_exec=HORA_EXEC,
                    usuario=usuario,
                    log_path=str(log_path),
                    destinatarios=lista_emails,
                    send_email=True,
                    observacao=f"Data Corte: {data_corte}"
                )
            except Exception as e:
                logger.error(f"Falha ao publicar metricas: {e}")
        
        for h in logger.handlers:
            h.flush()
            h.close()

    return retcode

if __name__ == "__main__":
    sys.exit(main())