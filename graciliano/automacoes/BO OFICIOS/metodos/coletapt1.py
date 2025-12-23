import os
import sys
import logging
import traceback
import tempfile
import shutil
import time
import functools
import re
import getpass
import glob
from pathlib import Path
from datetime import datetime, date
from collections import defaultdict
from collections import defaultdict
from typing import List, Tuple, Optional
from zoneinfo import ZoneInfo
from datetime import timedelta

# --- IMPORTS DE TERCEIROS ---
import pandas as pd
import pandas_gbq
from playwright.sync_api import sync_playwright, Page, Browser, BrowserContext, expect, TimeoutError as PWTimeoutError
# --- CONFIGURAÇÃO DE AMBIENTE ---
SCRIPT_NAME = Path(__file__).stem.upper()
APP_STEM = Path(__file__).stem.lower()
NOME_AUTOMACAO = "BO OFICIOS"
TZ = ZoneInfo("America/Sao_Paulo")
INICIO_EXEC_SP = datetime.now(TZ)

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

try:
    import dollynho
except ImportError:
    dollynho = None

# --- CONSTANTES ---
GOOGLE_CLOUD_PROJECT = "datalab-pagamentos"
WALLB_CASOS = "ADMINISTRACAO_CELULA_PYTHON.WallB_casos"
TABELA_TESTE_ALL = "datalab-pagamentos.CELULA_PYTHON_TESTES.materaALLcases"
TABELA_TESTE_OPEN = "datalab-pagamentos.CELULA_PYTHON_TESTES.materaOpenCASES"

# Variável de Controle de Ano (Pode vir de ENV)
COLETA_ANO = os.environ.get("COLETA_ANO", "2025") 
if COLETA_ANO == "ALL": COLETA_ANO = ""

# Caminhos
LOG_DIR = BASE_DIR / "automacoes" / NOME_AUTOMACAO / "logs" / SCRIPT_NAME / INICIO_EXEC_SP.strftime("%Y-%m-%d")
DOWNLOADS_DIR = Path.home() / "Downloads"
USER_DATA_DIR = Path.home() / "AppData" / "Local" / "CELPY" / "chromium_splunk"
POSICAO_DIR = BASE_DIR / "automacoes" / NOME_AUTOMACAO / "arquivos_input" / "00_ColetaPosicao" # Redirecionado para pasta da automacao

# Configurações Web
URL_LOGIN = "https://ccs.matera-v2.corp/materaccs/secure/login.jsf"
HEADLESS = os.environ.get("HEADLESS", "false").lower() == "true"
TIMEOUT_MS = 60000
SPLUNK_URL_SEARCH = "https://siem.corp.c6bank.com/en-US/app/search/search"
SPLUNK_HEADLESS = HEADLESS # Usa mesma config
SPLUNK_TIMEOUT_MS = 300000 
SPLUNK_SEARCH_BTN_XPATH = "/html/body/div[3]/div[2]/div/div[1]/div[2]/form/table/tbody/tr/td[4]/a"

DATAFRAME_DOIDO = [
    "status_caso", "id_evento", "codigo_mensagem", "numero_controle_ccs", "cnpj_entidade",
    "cnpj_participante", "tipo_pessoa", "cnpj_cpf_cliente", "data_inicio_oficio", "data_fim_oficio",
    "codigo_sistema_envio", "sigla_orgao", "numero_controle_autorizacao", "numero_controle_envio",
    "numero_processo_judicial", "codigo_tribunal", "nome_tribunal", "codigo_vara", "nome_vara",
    "nome_juiz", "descricao_cargo_juiz", "ordem_oficio", "data_limite", "data_bacen",
    "data_movimento_oficio", "status_movimentacao", "ccs0012", "numero_conta",
    "possui_relacionamento", "caso_outros", "dt_coleta",
]

# Métricas Internas
_time_acc: defaultdict = defaultdict(float)
_status_acc: dict = {}
_tb_acc: dict = {}

def get_credencial():
    if dollynho:
        try: return dollynho.get_credencial("BO OFICIOS") # ou SCRIPT_NAME
        except: pass
    return os.environ.get("MATERA_USER", "dummy"), os.environ.get("MATERA_PASS", "dummy")

USUARIO_MATERA, SENHA_MATERA = get_credencial()
TEMP_DIR = Path(tempfile.mkdtemp(prefix=f"{APP_STEM}_"))

# --- CLASSE DE ENRIQUECIMENTO ---
class Enriquecedor:
    @staticmethod
    def consultar_contas(logger: logging.Logger, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or "cnpj_cpf_cliente" not in df.columns:
            return df
        logger.info("ENRIQUECEDOR|Consultando números de conta no BigQuery...")
        docs_raw = df["cnpj_cpf_cliente"].dropna().unique().tolist()
        docs = [str(d).replace(".", "").replace("-", "").replace("/", "").strip() for d in docs_raw if d]
        if not docs:
            df["numero_conta"] = "0"
            return df
        
        docs_fmt = "', '".join(docs)
        query = f"SELECT CAST(REGISTER_NUM AS STRING) as doc, CAST(ACCOUNT_NUM AS STRING) as conta FROM `c6-backoffice-prod.conta_corrente.ACCOUNT_REGISTER` WHERE REGISTER_NUM IN ('{docs_fmt}')"
        try:
            df_contas = pandas_gbq.read_gbq(query, project_id=GOOGLE_CLOUD_PROJECT)
            mapa_contas = dict(zip(df_contas["doc"], df_contas["conta"]))
            df["numero_conta"] = df["cnpj_cpf_cliente"].apply(lambda x: mapa_contas.get(str(x).replace(".","").replace("-","").replace("/","").strip(), "0"))
            logger.info(f"ENRIQUECEDOR|Mapeamento concluído. Contas encontradas: {len(df_contas)}")
        except Exception as e:
            logger.error(f"ENRIQUECEDOR|Erro BQ: {e}")
            df["numero_conta"] = "0"
        return df

class Execucao:
    def __init__(self):
        self.modo_execucao = os.environ.get("MODO_EXECUCAO", "AUTO")
        self.usuario = os.environ.get("USUARIO_EXEC", getpass.getuser()).lower()
        if "@" not in self.usuario: self.usuario += "@c6bank.com"

# --- FUNÇÕES AUXILIARES ---
def _setup_logger() -> tuple[logging.Logger, Path]:
    try: LOG_DIR.mkdir(parents=True, exist_ok=True)
    except: pass
    log_filename = f"{SCRIPT_NAME}_{INICIO_EXEC_SP.strftime('%Y%m%d_%H%M%S')}.log"
    log_path = LOG_DIR / log_filename
    logger = logging.getLogger(SCRIPT_NAME)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    logger.handlers = []
    
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(formatter)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger, log_path

def obter_destinatarios(logger: logging.Logger) -> tuple[list[str], list[str]]:
    try:
        sql = f"SELECT emails_principais, emails_cc FROM `datalab-pagamentos.ADMINISTRACAO_CELULA_PYTHON.Registro_automacoes` WHERE lower(TRIM(metodo_automacao)) = lower(TRIM('{APP_STEM}')) LIMIT 1"
        df = pandas_gbq.read_gbq(sql, project_id=GOOGLE_CLOUD_PROJECT)
        if df.empty: return [], []
        def cln(r): return [x.strip() for x in str(r).replace(';',',').split(',') if '@' in x]
        return cln(df.iloc[0,0]), cln(df.iloc[0,1])
    except Exception as e:
        logger.error(f"Erro ao obter destinatarios: {e}")
        return [], []

def medir_tempo(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        inicio = time.perf_counter()
        status, tb = "SUCESSO", ""
        try: return func(*args, **kwargs)
        except Exception:
            status, tb = "FALHA", traceback.format_exc()
            raise
        finally:
            dur = time.perf_counter() - inicio
            _time_acc[func.__name__] += dur
            _status_acc[func.__name__] = status
            if status == "FALHA": _tb_acc[func.__name__] = tb
            logging.getLogger(SCRIPT_NAME).info("ETAPA %s: %s (%.2fs)", func.__name__, status, dur)
    return wrapper

# --- LÓGICA DE NEGÓCIO ---
def extrair_campo(html: str, rotulo: str) -> str:
    try:
        idx = html.index(rotulo)
        bloco = html[idx: idx + 300]
        return bloco.split("<td>")[1].split("</td>")[0].strip()
    except: return ""

def limpar_identificador(raw: str) -> str:
    clean = str(raw or "").replace(".", "").replace("-", "").replace("/", "").strip()
    return clean.zfill(11) if clean else ""

def immortal_action(action, desc: str, retries: int = 5):
    last = None
    logger = logging.getLogger(SCRIPT_NAME)
    for i in range(1, retries + 1):
        try: return action()
        except Exception as e:
            last = e
            logger.info("RETRY %s (%d/%d): %s", desc, i, retries, e)
            time.sleep(0.5)
    raise last if last else RuntimeError(f"Falha em {desc}")

@medir_tempo
def criar_contexto() -> tuple:
    p = sync_playwright().start()
    browser = p.chromium.launch(headless=HEADLESS, channel="chrome", args=["--start-maximized"])
    context = browser.new_context(accept_downloads=True, viewport={"width": 1920, "height": 1080})
    page = context.new_page()
    page.set_default_timeout(TIMEOUT_MS)
    return p, browser, context, page

@medir_tempo
def efetuar_login(page: Page, user: str, pwd: str):
    immortal_action(lambda: page.goto(URL_LOGIN, wait_until="domcontentloaded"), "goto login")
    immortal_action(lambda: page.locator('[id="loginForm:login"]').fill(user), "user")
    immortal_action(lambda: page.locator('[id="loginForm:senha"]').fill(pwd), "pwd")
    immortal_action(lambda: page.locator('[id="loginForm:senha"]').press("Enter"), "enter")
    expect(page).to_have_url(re.compile(r"/secure"), timeout=TIMEOUT_MS)

# SPLUNK HELPERS (MANTIDOS ORIGINAIS MAS SIMPLIFICADOS)
def _splunk_ace_set_value(page: Page, text: str) -> bool:
    try:
        # Tenta injetar via JS ace
        page.evaluate(f"ace.edit(document.querySelector('.ace_editor')).setValue(`{text}`)")
        return True
    except:
        try: page.locator("textarea").first.fill(text); return True
        except: return False

@medir_tempo
def obter_dataframe_splunk_inicial() -> pd.DataFrame:
    logger = logging.getLogger(SCRIPT_NAME)
    filtro_sql = f"WHERE TO_CHAR(ce.dt_hr_registro,'YYYY') = '{COLETA_ANO}'" if COLETA_ANO else ""
    sql_base = """
WITH DADOS AS (
    SELECT req.id_req_movto, req.id_ccs0011, req.ind_processamento_manual, req.id_accs100, req.id_ccs0012, 
           req.id_situacao_req_movto, req.id_cod_ret_sist_info, req.observacao, req.apenas_extrato_movimentacao,
           req.cod_sist_envio, req.data_movimento_ccs0011, ce.dt_hr_registro, ce.num_ctrl_ccs, csr.descricao,
           ROW_NUMBER() OVER (PARTITION BY ce.num_ctrl_ccs ORDER BY ce.dt_hr_registro DESC) AS RN
    FROM materaccs.cs_requisicao_movimentacao req
    JOIN materaccs.cs_evento ce ON ce.id_evento = req.id_ccs0011
    JOIN materaccs.cs_situacao_req_movto csr ON csr.id_situacao = req.id_situacao_req_movto
    LEFT JOIN materaccs.cs_accs100 acc ON acc.id_accs100 = req.id_accs100
    {filtro_ano}
)
SELECT * FROM DADOS WHERE RN = 1
"""
    sql_final = sql_base.format(filtro_ano=filtro_sql).replace("\n", " ").strip()
    query_text = f'| dbxquery connection=materadb_prod_adg query="{sql_final}" maxrows=0'
    
    # Executa Playwright Splunk
    USER_DATA_DIR.mkdir(parents=True, exist_ok=True)
    p = sync_playwright().start()
    ctx = p.chromium.launch_persistent_context(user_data_dir=str(USER_DATA_DIR), headless=SPLUNK_HEADLESS, channel="chrome", viewport={"width":1920,"height":1080})
    page = ctx.new_page()
    df_result = pd.DataFrame()
    
    try:
        logger.info("SPLUNK|Acessando...")
        page.goto(SPLUNK_URL_SEARCH, wait_until="domcontentloaded")
        time.sleep(2)
        if _splunk_ace_set_value(page, query_text):
            page.locator(f"xpath={SPLUNK_SEARCH_BTN_XPATH}").click()
            # Espera resultado
            page.wait_for_selector(".results-table", timeout=SPLUNK_TIMEOUT_MS)
            # Export
            page.get_by_role("button", name=re.compile("Export", re.I)).click()
            page.get_by_label(re.compile("CSV", re.I)).first.check()
            with page.expect_download(timeout=60000) as dl_info:
                page.locator(".modal-btn-primary").click()
            
            fpath = DOWNLOADS_DIR / f"splunk_res_{datetime.now().strftime('%H%M%S')}.csv"
            dl_info.value.save_as(fpath)
            try: df_result = pd.read_csv(fpath, dtype=str)
            except: df_result = pd.read_csv(fpath, dtype=str, sep=";")
            logger.info(f"SPLUNK|Linhas: {len(df_result)}")
    except Exception as e:
        logger.error(f"SPLUNK|Erro: {e}")
    finally:
        ctx.close(); p.stop()
    return df_result

@medir_tempo
def coletar_dados_via_links(page: Page, df_links: pd.DataFrame) -> pd.DataFrame:
    logger = logging.getLogger(SCRIPT_NAME)
    registros = []
    for i, row in enumerate(df_links.itertuples(index=False), start=1):
        link = getattr(row, "link")
        id_evento = getattr(row, "ID_EVENTO")
        try:
            immortal_action(lambda: page.goto(link, wait_until="domcontentloaded"), f"detalhe {id_evento}")
            html = page.content()
            registros.append({
                "status_caso": "ABERTO",
                "id_evento": id_evento,
                "codigo_mensagem": extrair_campo(html, "Código Mensagem"),
                "numero_controle_ccs": extrair_campo(html, "Número Controle CCS"),
                "cnpj_entidade": extrair_campo(html, "CNPJ Base Entidade Responsável"),
                "cnpj_participante": extrair_campo(html, "CNPJ Base Participante"),
                "tipo_pessoa": extrair_campo(html, "Tipo Pessoa"),
                "cnpj_cpf_cliente": limpar_identificador(extrair_campo(html, "CNPJ ou CPF Pessoa")),
                "data_inicio_oficio": extrair_campo(html, "Data Início"),
                "data_fim_oficio": extrair_campo(html, "Data Fim"),
                "codigo_sistema_envio": extrair_campo(html, "Sistema Envio"),
                "sigla_orgao": extrair_campo(html, "Sigla Órgão CCS Destino"),
                "numero_controle_autorizacao": extrair_campo(html, "Número Controle Autorização Quebra Sigilo"),
                "numero_controle_envio": extrair_campo(html, "Número Controle Envio"),
                "numero_processo_judicial": extrair_campo(html, "Número Processo Judicial"),
                "codigo_tribunal": extrair_campo(html, "Código Tribunal"),
                "nome_tribunal": extrair_campo(html, "Nome Tribunal"),
                "codigo_vara": extrair_campo(html, "Código Vara Tribunal"),
                "nome_vara": extrair_campo(html, "Nome Vara Tribunal"),
                "nome_juiz": extrair_campo(html, "Nome Juiz"),
                "descricao_cargo_juiz": extrair_campo(html, "Descrição Cargo Juiz"),
                "ordem_oficio": extrair_campo(html, "Texto Observação Ordem"),
                "data_limite": extrair_campo(html, "Data Hora Limite Resposta"),
                "data_bacen": extrair_campo(html, "Data Hora Bacen"),
                "data_movimento_oficio": extrair_campo(html, "Data Movimento"),
                "status_movimentacao": extrair_campo(html, "Status"),
                "ccs0012": False,
            })
            if i % 50 == 0: logger.info("COLETA: %d/%d", i, len(df_links))
        except Exception as e:
            logger.error("Erro coleta ID %s: %s", id_evento, e)
    return pd.DataFrame(registros)

@medir_tempo
def salvar_excel(df: pd.DataFrame) -> Path:
    POSICAO_DIR.mkdir(parents=True, exist_ok=True)
    arquivo = POSICAO_DIR / f"incremento_conta_{datetime.now().strftime('%d.%m.%Y_%H.%M.%S')}.xlsx"
    df.to_excel(arquivo, index=False)
    return arquivo

def _upload_and_notify(xlsx_path: Path):
    logger = logging.getLogger(SCRIPT_NAME)
    df = pd.read_excel(xlsx_path, dtype=str).fillna("")
    df.columns = [str(c).strip().lower() for c in df.columns]
    for c in DATAFRAME_DOIDO: 
        if c not in df.columns: df[c] = ""
    df["dt_coleta"] = date.today()
    pandas_gbq.to_gbq(df[DATAFRAME_DOIDO].astype(str), WALLB_CASOS, project_id=GOOGLE_CLOUD_PROJECT, if_exists="replace")
    logger.info("Upload Wall.B concluído.")

def main():
    logger, log_path = _setup_logger()
    exec_obj = Execucao()
    logger.info(f"INICIO {APP_STEM} | USER: {exec_obj.usuario}")
    
    p = browser = context = page = None
    status_final = "FALHA"
    inicio = time.time()
    
    try:
        df_ids = obter_dataframe_splunk_inicial()
        if df_ids.empty:
            logger.warning("SPLUNK VAZIO")
            status_final = "SEM DADOS PARA PROCESSAR"
            return 0
        
        df_ids.columns = [str(c).upper().strip() for c in df_ids.columns]
        
        # Filtros
        if "DESCRICAO" in df_ids.columns:
            df_ids = df_ids[df_ids["DESCRICAO"] != "Finalizada (Mensagem CCS0012 respondida)"]
        
        # Renomeia ID
        if 'ID_CCS0011' in df_ids.columns: df_ids.rename(columns={'ID_CCS0011': 'ID_EVENTO'}, inplace=True)
        elif 'ID_EVENTO' not in df_ids.columns:
             for c in df_ids.columns: 
                 if 'ID_EVENTO' in c: df_ids.rename(columns={c: 'ID_EVENTO'}, inplace=True); break
        
        if 'ID_EVENTO' not in df_ids.columns: raise ValueError("Coluna ID_EVENTO não encontrada")

        p, browser, context, page = criar_contexto()
        efetuar_login(page, USUARIO_MATERA, SENHA_MATERA)
        
        df_ids["link"] = df_ids["ID_EVENTO"].apply(lambda x: f"https://ccs.matera-v2.corp/materaccs/mensagens/detalhesMsg.jsf?evento={x}")
        df_coleta = coletar_dados_via_links(page, df_ids[["ID_EVENTO", "link"]])
        
        if df_coleta.empty:
            status_final = "SEM DADOS PARA PROCESSAR"
            return 2

        # Enriquecimento
        df_coleta = Enriquecedor.consultar_contas(logger, df_coleta)
        df_coleta["possui_relacionamento"] = df_coleta["numero_conta"].apply(lambda x: "SIM" if x and str(x) != "0" else "NAO")
        
        # Uploads
        try: pandas_gbq.to_gbq(df_coleta.astype(str), TABELA_TESTE_OPEN, project_id=GOOGLE_CLOUD_PROJECT, if_exists="replace")
        except Exception as e: logger.error(f"Erro upload open cases: {e}")

        xlsx = salvar_excel(df_coleta)
        _upload_and_notify(xlsx)
        
        status_final = "SUCESSO"
        return 0

    except Exception as e:
        logger.error(f"FATAL: {traceback.format_exc()}")
        status_final = "FALHA"
        return 1
    finally:
        try: 
            if context: context.close()
            if browser: browser.close()
            if p: p.stop()
            shutil.rmtree(TEMP_DIR, ignore_errors=True)
        except: pass

        # Publicação (Direct BQ Metrics)
        try:
            duration = time.time() - inicio
            user = exec_obj.usuario
            
            df_metrics = pd.DataFrame([{
                "nome_automacao": NOME_AUTOMACAO,
                "metodo_automacao": SCRIPT_NAME,
                "status": status_final,
                "tempo_exec": str(timedelta(seconds=int(duration))),
                "data_exec": datetime.now(TZ).strftime("%Y-%m-%d"),
                "hora_exec": datetime.now(TZ).strftime("%H:%M:%S"),
                "usuario": user,
                "log_path": str(log_path)
            }])
            
            pandas_gbq.to_gbq(
                df_metrics,
                "datalab-pagamentos.ADMINISTRACAO_CELULA_PYTHON.automacoes_exec",
                project_id="datalab-pagamentos",
                if_exists="append"
            )
            logger.info("Métricas enviadas para automacoes_exec.")
            
        except Exception as e:
            logger.error(f"Erro publicação métricas: {e}")

if __name__ == "__main__":
    sys.exit(main())
