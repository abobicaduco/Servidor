import sys
import os
import re
import shutil
import random
import json
import time
import getpass
import logging
import traceback
import tkinter as tk
from tkinter import simpledialog, messagebox
import pandas as pd
import pandas_gbq
from pathlib import Path
from datetime import datetime, date
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed

# Imports de Automação e Interface
from playwright.sync_api import sync_playwright, expect, TimeoutError as PWTimeoutError

# Configuração de Caminhos e Imports Locais
# ----------------------------------------------------------------------------------------------------------------------
POSSIBLE_ROOTS = [
    Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano",
    Path.home() / "Meu Drive/C6 CTVM" / "graciliano",
    Path.home() / "C6 CTVM/graciliano",
]
BASE_PATH = next((p for p in POSSIBLE_ROOTS if p.exists()), None)

if BASE_PATH:
     UTIL_PATH = BASE_PATH / "novo_servidor" / "modules"
else:
     # Fallback
     UTIL_PATH = Path.home() / "graciliano" / "novo_servidor" / "modules"

sys.path.append(str(UTIL_PATH))

try:
    import _utilAutomacoesExec
except ImportError:
    logging.warning("Modulo _utilAutomacoesExec nao encontrado.")
    _utilAutomacoesExec = None

# Custom Dollynho Loader
def _carregar_dollynho():
    candidatos = [
        Path(__file__).with_name("dollynho.py"),
        Path(__file__).parent / "dollynho.py",
        UTIL_PATH / "dollynho.py",
    ]
    dolly_path = next((p for p in candidatos if p.is_file()), None)
    
    if dolly_path:
        import importlib.util
        spec = importlib.util.spec_from_file_location("dollynho", dolly_path)
        if spec and spec.loader:
            mod = importlib.util.module_from_spec(spec)
            sys.modules["dollynho"] = mod
            spec.loader.exec_module(mod)
            return mod
    
    # Mock
    class MockDollynho:
        def get_credencial(self, name=None): return "user_mock", "pass_mock"
    return MockDollynho()

cofre = _carregar_dollynho()

# Constantes Globais
# ----------------------------------------------------------------------------------------------------------------------
TZ = ZoneInfo("America/Sao_Paulo")
NOME_SERVIDOR = "Servidor.py"
NOME_AUTOMACAO = "PRICING"
NOME_SCRIPT = Path(__file__).stem
APP_STEM = NOME_SCRIPT.lower()

# Configurações BigQuery
BQ_PROJECT = "datalab-pagamentos"
BQ_LOCATION = "US"
DATASET_ID = "ADMINISTRACAO_CELULA_PYTHON"
TABLE_ID = APP_STEM

# Configurações Playwright e Web
JOB_URL = "https://tasks.corp/project/corecardstax/job/show/868dfdb5-59fa-4654-a411-933ccb85755e"
HEADLESS_DEFAULT = True
TIMEOUT_MS = 60000
SLOW_MO_MS = 0

# Configurações de Negócio
RANDOM_CONFERENCE = True
QTDE_ALEATORIA = 3
TABLES_USED = [
    "c6-banco-comercial-analytics.SHARED_OPS.TB_DATA_CORTE_FATURAS",
    "c6-banco-comercial-analytics.SHARED_OPS.TB_PUBLICO_PARCELAMENTO_FATURA_PF",
]

# Definição de Pastas
PASTA_AUTOMACAO = BASE_PATH / "automacoes" / NOME_AUTOMACAO if BASE_PATH else Path.cwd()
PASTA_INPUT = PASTA_AUTOMACAO / "arquivos input" / NOME_SCRIPT
PASTA_LOGS_BASE = PASTA_AUTOMACAO / "logs" / NOME_SCRIPT
PASTA_SAIDA_BASE = PASTA_AUTOMACAO / NOME_SCRIPT.lower()
PASTA_SESSIONS = PASTA_AUTOMACAO / "PLAYWRIGHT_SESSIONS" / NOME_SCRIPT

# Setup Inicial de Pastas
for p in [PASTA_INPUT, PASTA_LOGS_BASE, PASTA_SAIDA_BASE, PASTA_SESSIONS]:
    try: p.mkdir(parents=True, exist_ok=True)
    except: pass

# Definição de Logs e Arquivos do Dia
DATA_HOJE_STR = datetime.now(TZ).strftime("%d.%m.%Y")
PASTA_LOGS_DIA = PASTA_LOGS_BASE / DATA_HOJE_STR
PASTA_LOGS_DIA.mkdir(exist_ok=True)
LOG_FILE_PATH = PASTA_LOGS_DIA / f"{APP_STEM}_{datetime.now(TZ).strftime('%Y%m%d_%H%M%S')}.log"

PASTA_SAIDA_DIA = PASTA_SAIDA_BASE / DATA_HOJE_STR
PASTA_SAIDA_DIA.mkdir(exist_ok=True)

# Seletores Web (Originais)
X_LOGIN_USER = {"css": "input#login[name='j_username']"}
X_LOGIN_PASS = {"css": "input#password[name='j_password']"}
X_LOGIN_BTN = {"css": "button#btn-login[type='submit']"}
X_INPUT_ACCOUNT = {"css": "input[name='extra.option.ACCOUNT_ID']"}
X_INPUT_DATE = {"css": "input[name='extra.option.DATE']"}
X_SELECT_DETAIL = {"css": "select[name='followdetail']"}
X_BTN_RUN = {"css": "#execFormRunButton"}
X_STATUS_SPAN = {"css": "span.execstate.execstatedisplay.overall"}
X_BTN_LOG = {"text": "Execution Log"}
X_LINK_HTML = {"text": "HTML"}
X_TAB_OUTPUT = {"css": "#btn_view_output"}
X_EXEC_DROPDOWN = {"role": "button", "name": "Execution Log"}

# Configuração do Logger
# ----------------------------------------------------------------------------------------------------------------------
logger = logging.getLogger(APP_STEM)
logger.setLevel(logging.INFO)
logger.handlers = []
logger.propagate = False
_fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
_fh = logging.FileHandler(LOG_FILE_PATH, encoding="utf-8")
_fh.setFormatter(_fmt)
_sh = logging.StreamHandler(sys.stdout)
_sh.setFormatter(_fmt)
logger.addHandler(_fh)
logger.addHandler(_sh)


# Classes e Funções Auxiliares
# ----------------------------------------------------------------------------------------------------------------------
class Execucao:
    """Gerencia contexto de execução (Servidor vs Local) e parâmetros."""
    
    @staticmethod
    def is_servidor():
        return (
            os.getenv("SERVIDOR_ORIGEM") == NOME_SERVIDOR 
            or "--executado-por-servidor" in sys.argv
            or os.getenv("ENV_EXEC_MODE") == "AGENDAMENTO"
        )

    @staticmethod
    def detectar():
        if Execucao.is_servidor():
            return {
                "modo": "AUTO",
                "usuario": f"{getpass.getuser()}@c6bank.com",
                "headless": HEADLESS_DEFAULT,
                "data_corte": datetime.now(TZ).date().isoformat(),
                "qtde": QTDE_ALEATORIA,
                "nivel_log": "INFO"
            }
        
        # Interface Manual Tkinter (Substituindo PySide6)
        try:
            root = tk.Tk()
            root.withdraw()
            root.attributes("-topmost", True)
            
            resp_mode = messagebox.askyesno("Modo de Execução", "Deseja executar em modo AUTO? (Não para Manual)")
            
            data_str = simpledialog.askstring("Data Corte", "Data de Corte (DD/MM/YYYY):", initialvalue=datetime.now(TZ).strftime("%d/%m/%Y"))
            if not data_str:
                sys.exit(0)
                
            qtde = QTDE_ALEATORIA
            if RANDOM_CONFERENCE:
                q_str = simpledialog.askstring("Quantidade", "Quantidade de amostras:", initialvalue=str(QTDE_ALEATORIA))
                if q_str and q_str.isdigit():
                    qtde = int(q_str)
            
            root.destroy()
            
            try:
                data_iso = datetime.strptime(data_str, "%d/%m/%Y").strftime("%Y-%m-%d")
            except:
                data_iso = data_str
                
            return {
                "modo": "AUTO" if resp_mode else "SOLICITACAO",
                "usuario": f"{getpass.getuser()}@c6bank.com",
                "headless": False,
                "data_corte": data_iso,
                "qtde": qtde,
                "nivel_log": "INFO"
            }
            
        except Exception as e:
            logger.warning(f"Erro GUI local: {e}. Usando defaults.")
            return {
                "modo": "AUTO",
                "usuario": f"{getpass.getuser()}@c6bank.com",
                "headless": False,
                "data_corte": datetime.now(TZ).date().isoformat(),
                "qtde": QTDE_ALEATORIA,
                "nivel_log": "INFO"
            }

def obter_destinatarios(logger_inst: logging.Logger) -> tuple[list[str], list[str]]:
    proj = "datalab-pagamentos"
    nome_script_filtro = Path(__file__).stem 

    sql = (
        "SELECT emails_principais, emails_cc "
        "FROM `datalab-pagamentos.ADMINISTRACAO_CELULA_PYTHON.Registro_automacoes` "
        f"WHERE lower(TRIM(metodo_automacao)) = lower(TRIM('{nome_script_filtro}')) "
        "AND (status_automacao IS NULL OR lower(status_automacao) NOT IN ('INATIVA','DESATIVADA','DESLIGADA')) "
        "ORDER BY SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', data_lancamento) DESC "
        "LIMIT 1"
    )
    try:
        df = pandas_gbq.read_gbq(sql, project_id=proj, dialect='standard')
    except Exception as e:
        logger_inst.error(f"FALHA AO LER DESTINATARIOS DO BQ: {e}")
        return [], []
        
    if df.empty:
        logger_inst.warning(f"NAO ENCONTREI DESTINATARIOS NO BQ PARA metodo_automacao={nome_script_filtro}")
        return [], []
        
    def _clean_emails(raw):
        if not raw or str(raw).lower() == 'nan': return []
        return [x.strip() for x in str(raw).replace(';', ',').split(',') if '@' in x]

    to_col = _clean_emails(df.iloc[0].get("emails_principais"))
    cc_col = _clean_emails(df.iloc[0].get("emails_cc"))
    
    return to_col, cc_col

def gerenciador_arquivos(arquivo_origem: Path, logger_inst: logging.Logger):
    if not arquivo_origem.exists():
        return
    timestamp = datetime.now(TZ).strftime("%Y%m%d_%H%M%S")
    destino = PASTA_LOGS_DIA / f"{arquivo_origem.stem}_{timestamp}{arquivo_origem.suffix}"
    try:
        shutil.move(str(arquivo_origem), str(destino))
        logger_inst.info(f"ARQUIVO MOVIDO: {arquivo_origem.name} -> {destino.name}")
    except Exception as e:
        logger_inst.error(f"ERRO AO MOVER ARQUIVO {arquivo_origem}: {e}")

# Funções de BigQuery (Leitura e Upload com Staging)
# ----------------------------------------------------------------------------------------------------------------------
def bq_query_pandas(sql, project_id=BQ_PROJECT):
    return pandas_gbq.read_gbq(sql, project_id=project_id, dialect='standard')

def get_data_vencimento(data_corte):
    sql = f"""
      SELECT Data_Vencimento
        FROM `c6-banco-comercial-analytics.SHARED_OPS.TB_DATA_CORTE_FATURAS`
       WHERE DATA_GERACAO_ROBO = '{data_corte}'
    """
    df = bq_query_pandas(sql)
    if df.empty:
        return None
    return pd.to_datetime(df.loc[0, "Data_Vencimento"]).strftime("%Y-%m-%d")

def load_base(data_v):
    sql = f"""
        SELECT ACCOUNT_ID, CAMPAIGN_ID
          FROM `c6-banco-comercial-analytics.SHARED_OPS.TB_PUBLICO_PARCELAMENTO_FATURA_PF`
         WHERE DT_VENCIMENTOCOBRANCA = '{data_v}'
    """
    return bq_query_pandas(sql)

def subir_staging_merge(df, project_id, dataset_id, table_id, keys):
    if df.empty:
        return 0
    
    df.columns = [str(c).replace(' ', '_').replace('.', '_').upper() for c in df.columns]
    df = df.astype(str)
    
    timestamp = datetime.now(TZ).strftime("%Y%m%d%H%M%S")
    table_full = f"{project_id}.{dataset_id}.{table_id}"
    staging_table = f"{dataset_id}.staging_{table_id}_{timestamp}"
    staging_full = f"{project_id}.{staging_table}"
    
    logger.info(f"INICIANDO UPLOAD: {len(df)} linhas para {staging_full}")
    
    try:
        pandas_gbq.to_gbq(
            df, 
            destination_table=staging_table,
            project_id=project_id,
            if_exists='replace'
        )
    except Exception as e:
        logger.error(f"FALHA NO UPLOAD PARA STAGING: {e}")
        raise

    match_cond = " AND ".join([f"T.{k} = S.{k}" for k in keys])
    cols = ", ".join(df.columns)
    vals = ", ".join([f"S.{c}" for c in df.columns])
    
    sql_merge = f"""
    MERGE `{table_full}` T
    USING `{staging_full}` S
    ON {match_cond}
    WHEN NOT MATCHED THEN
      INSERT ({cols}) VALUES ({vals})
    """
    
    logger.info("EXECUTANDO MERGE NO BIGQUERY...")
    try:
        import google.auth
        from google.cloud import bigquery
        creds, _ = google.auth.default()
        client = bigquery.Client(project=project_id, credentials=creds)
        query_job = client.query(sql_merge)
        query_job.result() 
        logger.info("MERGE CONCLUIDO COM SUCESSO")
    except Exception as e:
        logger.error(f"FALHA NO MERGE: {e}")
        try: client.query(f"DROP TABLE IF EXISTS `{staging_full}`")
        except: pass
        raise e

    try:
        client.query(f"DROP TABLE IF EXISTS `{staging_full}`")
    except:
        pass
        
    return len(df)

# Funções Playwright (Automação Web)
# ----------------------------------------------------------------------------------------------------------------------
def locator_from(page, spec):
    if isinstance(spec, dict):
        if "role" in spec and "name" in spec:
            return page.get_by_role(spec["role"], name=spec["name"])
        if "text" in spec:
            return page.get_by_text(spec["text"], exact=True)
        if "label" in spec:
            return page.get_by_label(spec["label"])
        if "css" in spec:
            return page.locator(spec["css"])
    if isinstance(spec, str):
        if spec.startswith("//"): return page.locator(f"xpath={spec}")
        return page.locator(spec)
    raise ValueError(f"Spec inválida: {spec}")

def login_rundeck(page, user, pwd):
    logger.info(f"LOGIN RUNDECK: {JOB_URL} user={user}")
    page.goto(JOB_URL, wait_until="domcontentloaded")
    
    try:
        if "/user/login" in page.url or locator_from(page, X_LOGIN_USER).count() > 0:
            locator_from(page, X_LOGIN_USER).wait_for(state="visible")
            locator_from(page, X_LOGIN_USER).fill(user)
            locator_from(page, X_LOGIN_PASS).fill(pwd)
            locator_from(page, X_LOGIN_BTN).click()
            page.wait_for_load_state("domcontentloaded")
            expect(page).not_to_have_url(re.compile(r"/user/login"), timeout=TIMEOUT_MS)
    except Exception:
        raise PWTimeoutError("Falha no login Rundeck (timeout ou erro)")
    logger.info("LOGIN RUNDECK: SUCESSO")

def run_campaign(page, account_id, data_v, camp):
    logger.info(f"RUN_CAMPAIGN: ACC={account_id} CAMP={camp} DT={data_v}")
    page.goto(JOB_URL, wait_until="domcontentloaded")
    
    locator_from(page, X_INPUT_ACCOUNT).wait_for()
    locator_from(page, X_INPUT_ACCOUNT).fill(str(account_id))
    
    locator_from(page, X_INPUT_DATE).wait_for()
    locator_from(page, X_INPUT_DATE).fill(str(data_v))
    
    try:
        page.evaluate("""() => { 
            const sel = document.querySelector("select[name='followdetail']");
            if (sel) { sel.value = 'output'; sel.dispatchEvent(new Event('change', {bubbles:true})); }
        }""")
    except: pass
    
    clicked = False
    for attempt in range(5):
        try:
            res = page.evaluate("""() => {
                const btn = document.querySelector('#execFormRunButton');
                if (!btn || btn.disabled) return 'fail';
                btn.click(); return 'clicked';
            }""")
            if res == 'clicked':
                clicked = True
                break
        except: pass
        time.sleep(1)
        
    if not clicked:
        try:
            page.evaluate("document.querySelector('#execFormRunButton').focus()")
            page.keyboard.press("Enter")
            clicked = True
        except: pass

    state = "UNKNOWN"
    start_time = time.time()
    while time.time() - start_time < 600:
        try:
            span = locator_from(page, X_STATUS_SPAN)
            if span.is_visible():
                state = (span.get_attribute("data-execstate") or "").upper()
                if state in ("SUCCEEDED", "FAILED"): break
        except: pass
        time.sleep(2)
        
    raw_html = ""
    if state == "SUCCEEDED":
        try:
            page.evaluate("const a = document.querySelector('#btn_view_output'); if(a) a.click();")
            try: locator_from(page, X_EXEC_DROPDOWN).click(timeout=3000)
            except: pass
            
            with page.context.expect_page() as new_page_info:
                try: page.get_by_role("menuitem", name="HTML").click(timeout=5000)
                except: page.locator("ul.dropdown-menu").get_by_text("HTML").click(timeout=5000)
            
            log_page = new_page_info.value
            log_page.wait_for_load_state()
            raw_html = log_page.locator("body").inner_text()
            log_page.close()
        except Exception as e:
            logger.error(f"ERRO AO EXTRAIR HTML LOG: {e}")
            
    return state, raw_html

# Função Principal
# ----------------------------------------------------------------------------------------------------------------------
def main():
    logger.info("INICIANDO EXECUCAO")
    
    params = Execucao.detectar()
    logger.setLevel(getattr(logging, params["nivel_log"]))
    
    modo = params["modo"]
    usuario = params["usuario"]
    data_corte = params["data_corte"]
    headless = params["headless"]
    
    inicio = datetime.now(TZ)
    status_final = "FALHA"
    observacao = "Execucao Automatica" if modo == "AUTO" else "Solicitacao Manual"
    tabela_ref = f"{BQ_PROJECT}.{DATASET_ID}.{TABLE_ID}"
    
    df_result = pd.DataFrame()
    path_output_xlsx = None
    lista_emails = []
    
    try:
        try:
            user_dolly, pwd_dolly = cofre.get_credencial(NOME_SCRIPT)
        except:
            user_dolly, pwd_dolly = cofre.get_credencial() 
            
        data_v = get_data_vencimento(data_corte)
        if not data_v:
            logger.warning(f"SEM DATA VENCIMENTO PARA CORTE {data_corte}")
            status_final = "SEM DADOS"
            return
            
        df_base = load_base(data_v)
        if df_base.empty:
            logger.warning("BASE VAZIA DO BIGQUERY")
            status_final = "SEM DADOS"
            return

        entries = []
        if RANDOM_CONFERENCE and params["qtde"] > 0:
            for c, grp in df_base.groupby("CAMPAIGN_ID"):
                subs = grp["ACCOUNT_ID"].dropna().tolist()
                if subs:
                    amostra = random.sample(subs, min(params["qtde"], len(subs)))
                    entries.extend([(acc, c) for acc in amostra])
        else:
            entries = [(r.ACCOUNT_ID, r.CAMPAIGN_ID) for r in df_base.itertuples()]
            
        if not entries:
            status_final = "SEM DADOS"
            return
            
        logger.info(f"TOTAL PARA PROCESSAR: {len(entries)}")

        session_path = PASTA_SESSIONS / f"auth_state_{NOME_SCRIPT}.json"
        
        resultados = []
        with sync_playwright() as pw:
            browser = pw.chromium.launch(channel="chrome", headless=headless, slow_mo=SLOW_MO_MS)
            
            context_args = {"viewport": {"width": 1920, "height": 1080}, "accept_downloads": True}
            if session_path.exists():
                context = browser.new_context(storage_state=str(session_path), **context_args)
            else:
                context = browser.new_context(**context_args)
                
            page = context.new_page()
            
            try:
                login_rundeck(page, user_dolly, pwd_dolly)
                context.storage_state(path=str(session_path))
            except Exception as e:
                logger.error(f"LOGIN FALHOU: {e}")
                raise e
            
            for i, (acc, camp) in enumerate(entries, 1):
                try:
                    logger.info(f"PROCESSANDO [{i}/{len(entries)}] ACC={acc}")
                    st, log_html = run_campaign(page, acc, data_v, camp)
                except Exception as e:
                    logger.error(f"ERRO ITEM {acc}: {e}")
                    st, log_html = "ERROR", str(e)
                
                dt_fmt = data_v
                try: dt_fmt = datetime.strptime(data_v, "%Y-%m-%d").strftime("%d-%m-%Y")
                except: pass
                
                resultados.append({
                    "ACCOUNT_ID": str(acc),
                    "CAMPANHA": str(int(camp)) if str(camp).replace('.','').isdigit() else str(camp),
                    "STATUS": "SUCCEEDED" if st == "SUCCEEDED" else "FALHA",
                    "DATA": dt_fmt,
                    "LOGS": log_html
                })
                
            context.close()
            browser.close()
            
        df_result = pd.DataFrame(resultados)
        path_output_xlsx = PASTA_SAIDA_DIA / f"campanhas_conferidas_{datetime.now(TZ).strftime('%Y%m%d_%H%M%S')}.xlsx"
        df_result.to_excel(path_output_xlsx, index=False)
        logger.info(f"ARQUIVO GERADO: {path_output_xlsx}")
        
        try:
            chaves_dedup = ["ACCOUNT_ID", "CAMPANHA", "DATA"]
            inseridos = subir_staging_merge(df_result, BQ_PROJECT, DATASET_ID, TABLE_ID, chaves_dedup)
            logger.info(f"LINHAS INSERIDAS/ATUALIZADAS NO BQ: {inseridos}")
        except Exception as e:
            logger.error(f"FALHA CRITICA NO UPLOAD BQ: {e}")
            
        status_final = "SUCESSO"
        
    except Exception as e:
        logger.error("ERRO FATAL NA EXECUCAO", exc_info=True)
        status_final = "FALHA"
        
    finally:
        fim = datetime.now(TZ)
        tempo_exec = str(fim - inicio).split('.')[0]
        
        dest_to, dest_cc = obter_destinatarios(logger)
        lista_final_emails = dest_to + dest_cc
        if status_final != "SUCESSO" and not lista_final_emails:
             lista_final_emails = ["carlos.lsilva@c6bank.com", "sofia.fernandes@c6bank.com", "antonio.marcoss@c6bank.com"]

        try:
            if _utilAutomacoesExec:
                client = _utilAutomacoesExec.AutomacoesExecClient(logger)
                client.publicar(
                    nome_automacao=NOME_AUTOMACAO,
                    metodo_automacao=NOME_SCRIPT,
                    status=status_final,
                    tempo_exec=tempo_exec,
                    data_exec=inicio.strftime("%Y-%m-%d"),
                    hora_exec=inicio.strftime("%H:%M:%S"),
                    usuario=usuario,
                    log_path=str(LOG_FILE_PATH),
                    destinatarios=lista_final_emails,
                    send_email=True,
                    observacao=f"{observacao}. Processados: {len(df_result)}",
                    anexos=[str(path_output_xlsx)] if path_output_xlsx and path_output_xlsx.exists() else []
                )
        except Exception as e:
            logger.error(f"FALHA AO PUBLICAR METRICAS: {e}")
            
        if path_output_xlsx and path_output_xlsx.exists():
            pass

if __name__ == "__main__":
    sys.exit(main())