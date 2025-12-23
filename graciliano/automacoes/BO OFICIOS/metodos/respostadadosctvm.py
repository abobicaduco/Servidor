import sys
import os
import time
import zipfile
import shutil
import logging
import traceback
import tempfile
import re
import csv
import datetime
import importlib.util
from pathlib import Path
from subprocess import Popen
import unicodedata

# Bibliotecas de terceiros
import pandas as pd
import pythoncom
import win32com.client as win32
from win32com.client import Dispatch, gencache
import pandas_gbq
from pydata_google_auth import cache, get_user_credentials
from unidecode import unidecode
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError
from google.cloud import bigquery

# ==============================================================================
# CONFIGURAÇÕES GERAIS E DO STEP 1 (GERAÇÃO)
# ==============================================================================

# Configuração de Logger Global (Inicialmente para o Step 1)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger_gen = logging.getLogger("GENERATOR")
logger_gen.propagate = False
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
if not logger_gen.handlers:
    logger_gen.addHandler(console_handler)

APP_STEM_GEN = "RespostaDadosCTVM" # Nome fixo para evitar confusão no merge
TMP_DIR_GEN = Path(tempfile.mkdtemp(prefix=f"{APP_STEM_GEN}_"))
LOG_PATH_GEN = TMP_DIR_GEN / f"{APP_STEM_GEN}{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.log"

file_handler_gen = logging.FileHandler(LOG_PATH_GEN, encoding="utf-8")
file_handler_gen.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger_gen.addHandler(file_handler_gen)

TOKENS_DIR = Path.home() / "AppData" / "Roaming" / "CELPY" / "tokens"
TOKENS_DIR.mkdir(parents=True, exist_ok=True)
SCOPES = ["https://www.googleapis.com/auth/bigquery"]
CREDENTIALS = get_user_credentials(SCOPES, credentials_cache=cache.ReadWriteCredentialsCache(str(TOKENS_DIR)), auth_local_webserver=True)

BASE_WALLB = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Catarina Cristina Bernardes De Freitas - Célula Python - Relatórios de Execução" / "Wall.B"
MODEL_DOCX = BASE_WALLB / "arquivos" / "TRATAMENTOBACEN" / "Carta Corretora.docx"

# Caminhos compartilhados
DEST_ZIP_DIR = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "automacoes" / "BO OFICIOS" / "zipados_ctvm"
DEST_ZIP_DIR.mkdir(parents=True, exist_ok=True)

ARQUIVOS_INPUT_DIR = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "automacoes" / "BO OFICIOS" / "arquivos_input"
ARQUIVOS_INPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_XLSX = ARQUIVOS_INPUT_DIR / "preparar_ctvm.xlsx"

COLETA_CCS = ""
CNPJ_ALVO = "32345784"

# ==============================================================================
# FUNÇÕES DO STEP 1 (GERADOR DE CARTAS E ZIPS)
# ==============================================================================

def _bq_quote(s: str) -> str:
    return "'" + str(s).replace("'", "''") + "'"

def _parse_emails(s: str) -> list[str]:
    if not s:
        return []
    rx = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")
    out: list[str] = []
    for part in str(s).split(","):
        e = part.strip().lower()
        if e and rx.match(e):
            out.append(e)
    return out

def _dedup(emails: list[str]) -> list[str]:
    seen = set()
    res: list[str] = []
    for e in emails:
        if e not in seen:
            seen.add(e)
            res.append(e)
    return res

def _parse_coleta_ccs(v: str) -> tuple[int, int, str]:
    mapa = {"JANEIRO":1,"FEVEREIRO":2,"MARCO":3,"ABRIL":4,"MAIO":5,"JUNHO":6,"JULHO":7,"AGOSTO":8,"SETEMBRO":9,"OUTUBRO":10,"NOVEMBRO":11,"DEZEMBRO":12}
    if "_" not in v:
        raise ValueError("COLETA_CCS inválido. Use MES_ANO, ex.: AGOSTO_2025")
    mes_txt, ano_txt = v.split("_", 1)
    mes = mapa[unidecode(mes_txt.strip().lower())]
    ano = int(ano_txt.strip())
    mes_ini = f"{ano:04d}-{mes:02d}-01"
    return ano, mes, mes_ini

def obter_destinatarios_gen(logger: logging.Logger) -> tuple[list[str], list[str]]:
    # Usa nome original do script 1 para buscar no banco, se necessário ajuste para APP_STEM_GEN
    nome_busca = Path(__file__).stem.lower() 
    proj = os.environ.get("BQ_BILLING_PROJECT") or os.environ.get("GOOGLE_CLOUD_PROJECT") or "datalab-pagamentos"
    sql = (
        "SELECT emails_principais, emails_cc FROM datalab-pagamentos.ADMINISTRACAO_CELULA_PYTHON.Registro_automacoes "
        f"WHERE lower(TRIM(metodo_automacao)) = lower(TRIM({_bq_quote(nome_busca)})) "
        "AND (status_automacao IS NULL OR lower(status_automacao) NOT IN ('INATIVA','DESATIVADA','DESLIGADA')) "
        "ORDER BY SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', data_lancamento) DESC LIMIT 1"
    )
    logger.info("OBTENDO DESTINATARIOS | APP_STEM=%s", nome_busca)
    try:
        df = pandas_gbq.read_gbq(sql, project_id=proj, credentials=CREDENTIALS, dialect="standard")
    except Exception:
        logger.error("ERRO AO CONSULTAR DESTINATARIOS NO BIGQUERY", exc_info=True)
        return [], []
    if df.empty:
        logger.warning("NENHUM REGISTRO DE DESTINATARIOS ENCONTRADO")
        return [], []
    to_raw = str(df.iloc[0].get("emails_principais") or "")
    cc_raw = str(df.iloc[0].get("emails_cc") or "")
    todos = _dedup(_parse_emails(to_raw) + _parse_emails(cc_raw))
    logger.info("DESTINATARIOS RESOLVIDOS | QTD=%d", len(todos))
    return todos, []

def enviar_email_geracao(resumo: dict, anexos: list[Path], logger: logging.Logger) -> None:
    try:
        pythoncom.CoInitialize()
    except Exception:
        pass
    try:
        outlook = Dispatch("Outlook.Application")
    except Exception:
        logger.warning("OUTLOOK INDISPONIVEL; EMAIL NAO ENVIADO")
        try:
            pythoncom.CoUninitialize()
        except Exception:
            pass
        return
    try:
        destinatarios, _ = obter_destinatarios_gen(logger)
        if not destinatarios:
            logger.warning("SEM DESTINATARIOS; EMAIL NAO ENVIADO")
            return
        agora = datetime.datetime.now()
        assunto = f"Célula Python Monitoração - GERADOR CARTAS - {resumo.get('status','')} - {agora.strftime('%d/%m/%Y')} - {agora.strftime('%H:%M:%S')}"
        corpo = (
            "<html><body style='font-family:Arial, sans-serif;'>"
            f"<p><b>Candidatas:</b> {int(resumo.get('candidatas',0))}</p>"
            f"<p><b>Novas:</b> {int(resumo.get('novas',0))}</p>"
            f"<p><b>Existentes:</b> {int(resumo.get('existentes',0))}</p>"
            f"<p><b>Filtro:</b> {resumo.get('filtro','')}</p>"
            "</body></html>"
        )
        mail = outlook.CreateItem(0)
        mail.To = "; ".join(destinatarios)
        mail.Subject = assunto
        mail.HTMLBody = corpo
        if LOG_PATH_GEN.exists():
            try:
                mail.Attachments.Add(str(LOG_PATH_GEN))
                logger.info("ANEXADO LOG_PATH: %s", LOG_PATH_GEN)
            except Exception:
                logger.warning("FALHA AO ANEXAR LOG_PATH")
        for p in anexos or []:
            try:
                pth = Path(p)
                if pth.exists() and pth.is_file():
                    mail.Attachments.Add(str(pth))
                    logger.info("ANEXO ADICIONADO: %s", pth)
            except Exception:
                logger.warning("FALHA AO ANEXAR ARQUIVO: %s", p)
        mail.Send()
        logger.info("EMAIL ENVIADO: %s", assunto)
    except Exception:
        logger.error("FALHA AO ENVIAR EMAIL", exc_info=True)
    finally:
        try:
            pythoncom.CoUninitialize()
        except Exception:
            pass

def _close_excel_if_open(target: Path) -> None:
    try:
        xl = win32.GetObject(Class="Excel.Application")
    except Exception:
        xl = None
    if xl is None:
        try:
            xl = gencache.EnsureDispatch("Excel.Application")
            if not xl.Workbooks.Count:
                xl.Quit()
                return
        except Exception:
            return
    try:
        tgt = str(target.resolve())
        for wb in list(xl.Workbooks):
            try:
                if str(wb.FullName).lower() == tgt.lower():
                    wb.Close(SaveChanges=False)
                    logger_gen.info("FECHADO EXCEL: %s", tgt)
                    break
            except Exception:
                continue
    except Exception:
        pass

def _remove_existing_file(target: Path) -> None:
    if not target.exists():
        return
    try:
        target.unlink()
        logger_gen.info("ARQUIVO ANTIGO REMOVIDO: %s", target)
    except PermissionError:
        logger_gen.warning("ARQUIVO EM USO. TENTANDO FECHAR NO EXCEL: %s", target)
        _close_excel_if_open(target)
        time.sleep(0.5)
        target.unlink(missing_ok=True)
        if target.exists():
            raise RuntimeError(f"Falha ao remover {target}")

def _salvar_planilha_final(df: pd.DataFrame) -> Path:
    ARQUIVOS_INPUT_DIR.mkdir(parents=True, exist_ok=True)
    _remove_existing_file(OUTPUT_XLSX)
    with pd.ExcelWriter(str(OUTPUT_XLSX), engine="openpyxl") as w:
        df.to_excel(w, index=False)
    logger_gen.info("PLANILHA FINAL SALVA: %s", OUTPUT_XLSX)
    return OUTPUT_XLSX

def _criar_carta(modelo: Path, destino: Path, dados: dict) -> None:
    from docx import Document as _D
    d = _D(str(modelo))
    for p in d.paragraphs:
        t = p.text
        t = t.replace("[PROCESSO]", dados.get("numero_processo_judicial",""))
        t = t.replace("[CONTROLE_ENVIO]", dados.get("numero_controle_envio",""))
        t = t.replace("[LOCAL]", "São Paulo")
        t = t.replace("[DIA]", dados.get("dia",""))
        t = t.replace("[MES]", dados.get("mes",""))
        t = t.replace("[ANO]", dados.get("ano",""))
        p.text = t
    d.save(str(destino))

def _zipar_cartas(pasta: Path, out_zip: Path) -> None:
    with zipfile.ZipFile(str(out_zip), "w", compression=zipfile.ZIP_STORED) as zf:
        for arq in pasta.glob("*.docx"):
            zf.write(str(arq), arq.name)

def step1_geracao() -> int:
    logger = logger_gen
    logger.info(">>> INICIANDO STEP 1: GERAÇÃO DE CARTAS E ZIPS <<<")
    inicio = datetime.datetime.now()
    dia = f"{inicio.day}"
    ano_txt = f"{inicio.year}"
    MESES = {1:"Janeiro",2:"Fevereiro",3:"Março",4:"Abril",5:"Maio",6:"Junho",7:"Julho",8:"Agosto",9:"Setembro",10:"Outubro",11:"Novembro",12:"Dezembro"}
    mes_txt = MESES[inicio.month]
    nome_script = "GERADOR_CTVM" # Nome para pastas temp
    ts = inicio.strftime("%d.%m.%Y_%H%M%S")

    temp_root = Path(tempfile.gettempdir()) / f"{nome_script}_{ts}"
    cartas_dir = temp_root / "cartas"
    zips_dir = temp_root / "zips"
    cartas_dir.mkdir(parents=True, exist_ok=True)
    zips_dir.mkdir(parents=True, exist_ok=True)

    try:
        sql_date_filter = ""
        if COLETA_CCS:
            _, _, mes_ini = _parse_coleta_ccs(COLETA_CCS)
            sql_date_filter = f"""
            WHERE dt_bacen BETWEEN DATE '{mes_ini}'
                AND DATE_SUB(DATE_ADD(DATE_TRUNC(DATE '{mes_ini}', MONTH), INTERVAL 1 MONTH), INTERVAL 1 DAY)
            """
        else:
            logger.info("COLETA_CCS vazia: Buscando todo histórico do CNPJ.")

        proj = os.environ.get("BQ_BILLING_PROJECT") or os.environ.get("GOOGLE_CLOUD_PROJECT") or "datalab-pagamentos"
        
        sql = f"""
        WITH base AS (
          SELECT
            *,
            COALESCE(
              SAFE.PARSE_DATE('%Y-%m-%d', SUBSTR(CAST(data_bacen AS STRING), 1, 10)),
              SAFE.PARSE_DATE('%d/%m/%Y', SUBSTR(CAST(data_bacen AS STRING), 1, 10)),
              DATE(SAFE_CAST(data_bacen AS DATETIME))
            ) AS dt_bacen
          FROM `datalab-pagamentos.ADMINISTRACAO_CELULA_PYTHON.WallB_casos`
          WHERE cnpj_participante = {_bq_quote(CNPJ_ALVO)}
        )
        SELECT *
        FROM base
        {sql_date_filter}
        """

        logger.info("LENDO CASOS NO BQ | CNPJ=%s | COLETA_CCS=%s", CNPJ_ALVO, COLETA_CCS if COLETA_CCS else "[TODOS]")
        df = pandas_gbq.read_gbq(sql, project_id=proj, credentials=CREDENTIALS, dialect="standard")
        logger.info("LINHAS RETORNADAS DO BQ: %d", len(df))

        if df.empty:
            resumo = {"candidatas": 0, "novas": 0, "existentes": 0, "status": "Sem dados", "filtro": COLETA_CCS}
            try:
                enviar_email_geracao(resumo, [LOG_PATH_GEN], logger)
            except Exception:
                pass
            return 2

        resultados = []
        for _, row in df.iterrows():
            ctrl_envio = str(row.get("numero_controle_envio", ""))
            ctrl_proc = str(row.get("numero_processo_judicial", ""))
            ctrl_ccs = str(row.get("numero_controle_ccs", ""))
            subpasta = cartas_dir / ctrl_envio
            subpasta.mkdir(parents=True, exist_ok=True)
            destino = subpasta / f"Carta Corretora_CCS{ctrl_ccs}.docx"
            payload = {"numero_controle_envio": ctrl_envio, "numero_processo_judicial": ctrl_proc, "dia": dia, "mes": mes_txt, "ano": ano_txt}
            try:
                _criar_carta(MODEL_DOCX, destino, payload)
                status, msg = "SUCESSO", ""
                logger.info("CARTA CRIADA: %s", destino.name)
            except Exception:
                status, msg = "ERRO_CRIACAO", traceback.format_exc()
                logger.error("FALHA AO CRIAR CARTA %s", destino.name, exc_info=True)
            out = {k: ("" if pd.isna(v) else v) for k, v in row.to_dict().items()}
            out["STATUS"] = status
            out["MSG"] = msg
            resultados.append(out)

        for pasta in cartas_dir.iterdir():
            if not pasta.is_dir():
                continue
            zip_path = zips_dir / f"{pasta.name}.zip"
            try:
                _zipar_cartas(pasta, zip_path)
                dst = DEST_ZIP_DIR / zip_path.name
                if dst.exists():
                    dst.unlink()
                shutil.copy2(str(zip_path), str(dst))
                logger.info("ZIP CRIADO E COPIADO: %s", zip_path.name)
            except Exception:
                logger.error("ERRO AO ZIPAR %s", pasta.name, exc_info=True)
                for r in resultados:
                    if str(r.get("numero_controle_envio")) == pasta.name:
                        r["STATUS"], r["MSG"] = "ERRO_ZIP", "falha ao zipar"

        df_res = pd.DataFrame(resultados)
        cols = [c for c in ["numero_controle_ccs","numero_controle_envio","cnpj_participante","data_bacen","id_evento","numero_processo_judicial","STATUS","MSG"] if c in df_res.columns]
        df_email = df_res[cols]
        arq_final = _salvar_planilha_final(df_email)

        total = len(df_email)
        novas = int((df_email["STATUS"] == "SUCESSO").sum()) if "STATUS" in df_email.columns else total
        resumo = {"candidatas": total, "novas": novas, "existentes": total - novas, "status": "Sucesso" if novas > 0 else "Sem novas", "filtro": COLETA_CCS}
        try:
            enviar_email_geracao(resumo, [arq_final], logger)
        except Exception:
            logger.warning("FALHA AO ENVIAR EMAIL DE GERAÇÃO", exc_info=True)
        return 0

    except Exception:
        logger.error("FALHA GERAL NO STEP 1", exc_info=True)
        try:
            enviar_email_geracao({"candidatas":0,"novas":0,"existentes":0,"status":"Falha","filtro":COLETA_CCS}, [LOG_PATH_GEN], logger)
        except Exception:
            pass
        return 1
    finally:
        try:
            if temp_root.exists():
                shutil.rmtree(temp_root, ignore_errors=True)
        except Exception:
            pass

# ==============================================================================
# CONFIGURAÇÕES E FUNÇÕES DO STEP 2 (ROBÔ WEB)
# ==============================================================================

LOGIN_URL = "https://ccs.matera-v2.corp/materaccs/secure/login.jsf"
REQ_URL   = "https://ccs.matera-v2.corp/materaccs/movimentacao/requisicoes_movimentacao.jsf"

BASE_NAME_WEB = "ROBO_WEB_CTVM"
RUN_STAMP_WEB = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
DOWNLOADS_WEB = Path.home() / "Downloads"
CSV_PATH_WEB  = DOWNLOADS_WEB / f"{BASE_NAME_WEB}_{RUN_STAMP_WEB}.csv"
LOG_PATH_WEB  = DOWNLOADS_WEB / f"{BASE_NAME_WEB}_{RUN_STAMP_WEB}.log"

# Configuração do Logger do Step 2 (Separado)
logger_web = logging.getLogger("WEB_AUTOMATION")
logger_web.setLevel(logging.INFO)
logger_web.propagate = False
for h in list(logger_web.handlers):
    logger_web.removeHandler(h)
fmt_web = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
sh_web = logging.StreamHandler(sys.stdout)
sh_web.setFormatter(fmt_web)
logger_web.addHandler(sh_web)
fh_web = logging.FileHandler(LOG_PATH_WEB, encoding="utf-8")
fh_web.setFormatter(fmt_web)
logger_web.addHandler(fh_web)

log = logger_web # Alias usado no código do script 2

DOLLY_CANDIDATOS = [
    Path(__file__).parent / "dollynho.py",
    Path.home()
    / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A"
    / "Mensageria e Cargas Operacionais - 11.CelulaPython"
    / "graciliano" / "novo_servidor" / "modules" / "dollynho.py",
]

ZIP_DIR = DEST_ZIP_DIR # Reutiliza a variável do Step 1 para garantir consistência

DOLLY_PATH = next((p for p in DOLLY_CANDIDATOS if p.is_file()), None)
if DOLLY_PATH is None:
    # Não levanta erro imediato para não quebrar o Step 1 se o Step 2 falhar na importação,
    # mas será validado no inicio do Step 2
    pass

BQ_PROJECT    = "datalab-pagamentos"
BQ_DATASET    = "ADMINISTRACAO_CELULA_PYTHON"
BQ_TABLE      = "WallE_respostas"
BQ_FQN        = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

# Variáveis globais de credencial (serão preenchidas no step2_web)
USUARIO, SENHA = None, None

SEL_USER = "#loginForm\\:login"
SEL_PASS = "#loginForm\\:senha"
SEL_OK   = "#loginForm\\:loginAction"
SEL_MENU_WRAP = "#header__id5_menu"
SEL_MENU_MOV_TXT = "text=Movimentação"
SEL_CONGLOMERADO = "#filtroForm\\:conglomerado"
SEL_DT_INI       = "#filtroForm\\:dtInicial"
SEL_DT_FIM       = "#filtroForm\\:dtFinal"
SEL_NUMCTRL_CCS  = "#filtroForm\\:numCtrlCcs"
SEL_CONSULTAR    = "#filtroForm\\:consultar"
SEL_TABELA = "#listaForm\\:requisicoesMovimentacaoTable"
SEL_TBODY_ROWS = "#listaForm\\:requisicoesMovimentacaoTable tbody tr"
SEL_THEAD_TH   = "#listaForm\\:requisicoesMovimentacaoTable thead th"
SEL_100_FILE_INPUT = ".resumable-browse input[type='file'][accept='.zip']"
SEL_100_PROGRESS   = ".resumable-file-progress"
SEL_100_UPLOAD_BTN = "#importarForm\\:upload-btn"
SEL_100_MSG_OK     = "span.msgWarning"
SEL_012_NUM    = "#ccs0012Form\\:numeroControleEntrega"
SEL_012_SIT    = "#ccs0012Form\\:situacao"
SEL_012_DTHR   = "#ccs0012Form\\:dtHrEntrega"
SEL_012_OBS    = "#ccs0012Form\\:txtObsResp"
SEL_012_ENVIAR = "#ccs0012Form\\:enviar"
CONSULTA_100_URL = "https://ccs.matera-v2.corp/materaccs/movimentacao/consulta_accs100.jsf"
SEL_100_CONSULTAR_BTN = "#filtroForm\\:consultar"
SEL_100_ARQUIVOS_ROWS = "#listaForm\\:arquivosTable tbody tr"

def load_dollynho():
    global USUARIO, SENHA
    if DOLLY_PATH is None:
        raise RuntimeError(f"Falha ao localizar dollynho.py. Verifique caminhos: {DOLLY_CANDIDATOS}")
    spec = importlib.util.spec_from_file_location("dollynho", DOLLY_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Falha ao carregar: {DOLLY_PATH}")
    dollynho = importlib.util.module_from_spec(spec)
    sys.modules["dollynho"] = dollynho
    spec.loader.exec_module(dollynho)
    # Metodo pode ser o nome do arquivo, mas vamos usar um fixo ou derivado
    metodo_busca = Path(__file__).stem
    # Tenta obter credenciais. Se não funcionar com nome do arquivo, ajusta hardcoded se necessario.
    try:
        USUARIO, SENHA = dollynho.get_credencial(metodo_busca)
        log.info(f"DOLLYNH0 carregado. Credenciais obtidas para '{metodo_busca}'.")
    except Exception as e:
        log.warning(f"Falha ao obter credenciais para {metodo_busca}: {e}. Tentando fallback...")
        raise e

def safe_back_to_lista(page):
    try:
        page.goto(REQ_URL, wait_until="domcontentloaded")
    except Exception:
        pass

def ensure_requisicoes_ready(page, max_wait: int = 30) -> bool:
    t0 = time.time()
    while time.time() - t0 < max_wait:
        try:
            if hasattr(page, "is_closed") and page.is_closed():
                time.sleep(0.5)
                continue
            if "login.jsf" in (page.url or ""):
                try:
                    login_loop(page)
                except Exception:
                    time.sleep(0.5)
                    continue
            if "requisicoes_movimentacao.jsf" not in (page.url or ""):
                try:
                    page.goto(REQ_URL, wait_until="domcontentloaded")
                except Exception:
                    time.sleep(0.5)
                    continue
            loc = page.locator(SEL_NUMCTRL_CCS)
            try:
                loc.wait_for(state="visible", timeout=2000)
            except Exception:
                time.sleep(0.5)
                continue
            if loc.is_disabled():
                try: page.keyboard.press("Escape")
                except Exception: pass
                time.sleep(0.3)
                if loc.is_disabled():
                    time.sleep(0.5)
                    continue
            return True
        except Exception:
            time.sleep(0.5)
    return False

def fields_present(page) -> bool:
    try:
        return (
            page.locator(SEL_USER).count() > 0 and
            page.locator(SEL_PASS).count() > 0 and
            page.locator(SEL_OK).count()   > 0
        )
    except Exception:
        return False

def clear_and_fill(page, sel, value) -> bool:
    try:
        loc = page.locator(sel)
        loc.wait_for(state="visible", timeout=6000)
    except Exception:
        return False
    try:
        loc.click(timeout=2000)
        page.keyboard.press("Control+A")
        page.keyboard.press("Delete")
        loc.fill(value, timeout=4000)
        return (loc.input_value() or "") == value
    except Exception:
        pass
    try:
        page.evaluate(
            """([selector, val]) => {
                const el = document.querySelector(selector);
                if (!el) return;
                el.value = val;
                el.dispatchEvent(new Event('input', {bubbles:true}));
                el.dispatchEvent(new Event('change', {bubbles:true}));
            }""",
            [sel, value]
        )
        time.sleep(0.1)
        return (loc.input_value() or "") == value
    except Exception:
        return False

def clean_text(s: str) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFC", s)
    s = s.replace("\u00A0", " ")
    return " ".join(s.split())

def capturar_protocolo_pos_envio_100(page) -> str | None:
    try:
        page.goto(CONSULTA_100_URL, wait_until="domcontentloaded")
    except Exception:
        pass
    while True:
        try:
            if page.locator(SEL_100_CONSULTAR_BTN).count() > 0:
                break
        except Exception:
            pass
        time.sleep(1)
    try:
        btn = page.locator(SEL_100_CONSULTAR_BTN)
        if btn.count() > 0:
            btn.click()
    except Exception:
        pass
    for _ in range(120):
        try:
            rows = page.locator(SEL_100_ARQUIVOS_ROWS)
            if rows.count() > 0:
                tds = rows.nth(0).locator("td")
                if tds.count() >= 9:
                    protocolo = clean_text(tds.nth(8).inner_text() or "")
                    if protocolo:
                        log.info(f"Protocolo (1ª linha ACCS100): {protocolo}")
                        return protocolo
        except Exception:
            pass
        time.sleep(1)
    log.warning("Não foi possível capturar o Nr. Protocolo na consulta do ACCS100.")
    return None

def coletar_dt_entrega_detalhes(page, timeout: int = 10) -> str | None:
    try:
        rows = page.locator(SEL_TBODY_ROWS)
        try:
            if rows.count() == 0:
                return None
        except Exception:
            return None
        r0 = rows.nth(0)
        img = r0.locator("img[title='Detalhes CCS0012']")
        if img.count() == 0:
            img = r0.locator("img[src*='detalhes.gif']")
            if img.count() == 0:
                return None
        try:
            with page.expect_popup() as pinfo:
                img.first.click()
            pop = pinfo.value
        except Exception:
            return None
        data_txt = None
        t0 = time.time()
        limite = min(timeout, 3)
        while time.time() - t0 < limite:
            try:
                alvo = pop.locator("xpath=//td[@class='formLabel' and normalize-space()='Data Hora Entrega']/following-sibling::td[1]")
                if alvo.count() > 0:
                    raw = (alvo.nth(0).inner_text() or "").strip()
                    if raw:
                        data_txt = raw
                        break
                    else:
                        break
                else:
                    break
            except Exception:
                break
            time.sleep(0.2)
        try:
            pop.get_by_role("button", name="Fechar").click(timeout=600)
        except Exception:
            try: pop.close()
            except Exception: pass
        if not data_txt:
            return None
        try:
            dt = datetime.datetime.strptime(data_txt, "%d/%m/%Y %H:%M:%S")
        except ValueError:
            try:
                dt = datetime.datetime.strptime(data_txt, "%d/%m/%Y")
            except Exception:
                return None
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None

def login_loop(page):
    log.info("Iniciando fluxo de login...")
    page.set_default_timeout(99999)
    while True:
        try:
            page.goto(LOGIN_URL, wait_until="domcontentloaded")
            log.info("Página de login carregada.")
        except Exception as e:
            log.warning(f"Falha ao abrir login: {e}. Nova tentativa em 1s.")
            time.sleep(1); continue
        misses = 0
        while not fields_present(page):
            misses += 1
            if misses % 6 == 0:
                log.info("Campos de login não visíveis; recarregando a página de login.")
                try: page.reload(wait_until="domcontentloaded")
                except Exception: pass
            time.sleep(0.3)
        ok = False
        for i in range(3):
            try:
                u_ok = clear_and_fill(page, SEL_USER, USUARIO)
                p_ok = clear_and_fill(page, SEL_PASS, SENHA)
                log.info(f"Tentativa 1/3 — usuário_ok={u_ok}, senha_ok={p_ok}")
                if u_ok and p_ok:
                    ok = True; break
            except (PWTimeoutError, Exception) as e:
                log.warning(f"Falha ao preencher credenciais (tentativa {i+1}/3): {e}")
            time.sleep(0.3)
        if not ok:
            log.info("Preenchimento falhou 3x. Recarregando tela de login...")
            try: page.reload(wait_until="domcontentloaded")
            except Exception: pass
            continue
        btn = page.locator(SEL_OK)
        try:
            log.info("Clicando em OK do login...")
            if btn.is_enabled(): btn.click()
            else: btn.click(force=True)
        except Exception as e:
            log.warning(f"Falha ao clicar OK: {e}. Recarregando login.")
            try: page.reload(wait_until="domcontentloaded")
            except Exception: pass
            continue
        try:
            page.wait_for_url(lambda url: "login.jsf" not in url, timeout=8000)
            log.info("URL pós-login detectada.")
        except Exception:
            log.info("URL ainda contém login.jsf; verificando menu...")
        try:
            if page.locator(SEL_MENU_WRAP).count() > 0 or page.locator(SEL_MENU_MOV_TXT).count() > 0:
                log.info("Menu pós-login detectado. Login concluído.")
                return
        except Exception:
            pass
        log.info("Menu pós-login não visível ainda; repetindo ciclo de login.")

def goto_requisicoes(page):
    log.info("Navegando para a tela de Requisições...")
    page.goto(REQ_URL, wait_until="domcontentloaded")
    if not ensure_requisicoes_ready(page, max_wait=30):
        raise RuntimeError("Elementos da tela de Requisições não foram encontrados (após tentativas).")
    log.info("Tela de Requisições carregada com sucesso.")

def carregar_dataframe_web():
    # Usa o arquivo gerado no Step 1
    origem = OUTPUT_XLSX
    if not origem.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {origem}")
    temp_dir = Path(tempfile.gettempdir())
    destino = temp_dir / "preparar_copia_web.xlsx"
    shutil.copy2(origem, destino)
    log.info(f"Cópia do Excel criada para web em: {destino}")
    df = pd.read_excel(destino, dtype=str, keep_default_na=False)
    cols = ["numero_controle_ccs", "numero_controle_envio"]
    faltantes = [c for c in cols if c not in df.columns]
    if faltantes:
        raise KeyError(f"Colunas ausentes no Excel: {faltantes}")
    df = df[cols].copy()
    log.info(f"Planilha carregada: {len(df)} linhas.")
    return df

def precheck_missing_zips(df: pd.DataFrame):
    missing_list = []
    missing_ccs = set()
    for _, row in df.iterrows():
        ccs = (row.get("numero_controle_ccs") or "").strip()
        envio = (row.get("numero_controle_envio") or "").strip()
        if not ccs or not envio:
            continue
        zp = ZIP_DIR / f"{envio}.zip"
        if not zp.exists():
            missing_list.append({
                "numero_controle_ccs": ccs,
                "numero_controle_envio": envio,
                "zip_path": str(zp),
            })
            missing_ccs.add(ccs)
    return missing_list, missing_ccs

def consultar_por_ccs(page, ccs: str, timeout: int = 30) -> bool:
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            if not ensure_requisicoes_ready(page, max_wait=5):
                time.sleep(0.5)
                continue
            ok = clear_and_fill(page, SEL_NUMCTRL_CCS, ccs)
            if not ok:
                time.sleep(0.3)
                continue
            try:
                btn = page.locator(SEL_CONSULTAR)
                if btn.count() == 0:
                    btn = page.get_by_role("button", name="Consultar")
                if btn.count() > 0:
                    btn.click(timeout=1500) if btn.is_enabled() else btn.click(force=True, timeout=1500)
                else:
                    page.locator(SEL_NUMCTRL_CCS).press("Enter")
            except Exception:
                pass
            t1 = time.time()
            while time.time() - t1 < 5:
                try:
                    if page.locator(SEL_TABELA).count() > 0:
                        return True
                except Exception:
                    break
                time.sleep(0.2)
        except Exception:
            pass
        time.sleep(0.5)
    return False

def ler_cabecalhos(page):
    ths = page.locator(SEL_THEAD_TH)
    headers = []
    try:
        n = ths.count()
        for i in range(n):
            txt = ths.nth(i).inner_text()
            headers.append(clean_text(txt))
    except Exception:
        pass
    return headers

def ler_primeira_linha(page):
    try:
        rows = page.locator(SEL_TBODY_ROWS)
        try:
            if rows.count() == 0:
                return None
        except Exception:
            return None
        r = rows.nth(0)
        tds = r.locator("td")
        try:
            n = tds.count()
        except Exception:
            return None
        vals = []
        for i in range(n):
            try:
                vals.append(clean_text(tds.nth(i).inner_text()))
            except Exception:
                vals.append("")
        extras = {}
        try:
            c8 = tds.nth(7)
            img8 = c8.locator("img")
            a8   = c8.locator("a")
            extras["col8_img_title"] = clean_text(img8.nth(0).get_attribute("title") or "") if img8.count()>0 else ""
            extras["col8_img_alt"]   = clean_text(img8.nth(0).get_attribute("alt")   or "") if img8.count()>0 else ""
            extras["col8_link_text"] = clean_text(a8.nth(0).inner_text() or "") if a8.count()>0 else ""
        except Exception:
            extras["col8_img_title"] = ""
            extras["col8_img_alt"]   = ""
            extras["col8_link_text"] = ""
        try:
            img9 = tds.nth(8).locator("img")
            if img9.count() > 0:
                extras["col9_img_alt"]   = clean_text(img9.nth(0).get_attribute("alt") or "")
                extras["col9_img_title"] = clean_text(img9.nth(0).get_attribute("title") or "")
            else:
                a9 = tds.nth(8).locator("a")
                extras["col9_link_text"] = clean_text((a9.nth(0).inner_text() or "")) if a9.count()>0 else ""
        except Exception:
            pass
        try:
            a10 = tds.nth(9).locator("a")
            extras["col10_link_text"] = clean_text(a10.nth(0).inner_text() or "") if a10.count()>0 else ""
        except Exception:
            extras["col10_link_text"] = ""
        return vals, extras
    except Exception:
        return None

def garantir_primeira_linha_corresponde(page, ccs: str, timeout: int = 30) -> bool:
    t0 = time.time()
    while time.time() - t0 < timeout:
        pl = None
        try:
            pl = ler_primeira_linha(page)
        except Exception:
            pl = None
        if not pl:
            if not consultar_por_ccs(page, ccs, timeout=5):
                time.sleep(0.3)
                continue
            time.sleep(0.2)
            continue
        vals, _ = pl
        try:
            if len(vals) >= 5 and (vals[4] or "").strip() == ccs:
                return True
        except Exception:
            pass
        consultar_por_ccs(page, ccs, timeout=5)
        time.sleep(0.2)
    return False

def precisa_enviar_100(extras) -> bool:
    return (
        (extras.get("col8_img_alt","")    == "Fazer upload do arquivo ACCS100") or
        (extras.get("col8_img_title","") == "Fazer upload do arquivo ACCS100") or
        (extras.get("col8_link_text","") == "Fazer upload do arquivo ACCS100")
    )

def precisa_enviar_012(vals, extras) -> bool:
    col8_title = (extras.get("col8_img_title") or "").strip()
    col10_txt  = (extras.get("col10_link_text") or "").strip()
    ja_tem_100 = "Não é possível realizar o upload de arquivo ACCS100" in col8_title
    botao_enviar = (col10_txt == "Enviar")
    return ja_tem_100 and botao_enviar

def enviar_accs100(page, ccs: str, nome_envio: str) -> bool:
    log.info(f"[{ccs}] Abrindo tela de upload ACCS100...")
    zip_path = ZIP_DIR / f"{nome_envio}.zip"
    if not zip_path.exists():
        log.warning(f"[{ccs}] ZIP não encontrado: {zip_path}")
        return False
    try:
        r0 = page.locator(SEL_TBODY_ROWS).nth(0)
        a8 = r0.locator("td").nth(7).locator("a")
        if a8.count() == 0:
            log.warning(f"[{ccs}] Link de upload (coluna 8) não encontrado.")
            return False
        a8.first.click()
    except Exception as e:
        log.warning(f"[{ccs}] Falha ao clicar no link de upload: {e}")
        return False
    start = time.time()
    while time.time() - start < 15:
        try:
            if page.locator(SEL_100_FILE_INPUT).count() > 0:
                break
        except Exception:
            pass
        time.sleep(0.3)
    else:
        log.warning(f"[{ccs}] Tela de upload ACCS100 não apareceu.")
        safe_back_to_lista(page)
        return False
    try:
        page.set_input_files(SEL_100_FILE_INPUT, str(zip_path))
    except Exception as e:
        log.warning(f"[{ccs}] Falha ao setar arquivo no input: {e}")
        safe_back_to_lista(page)
        return False
    up_start = time.time()
    while True:
        if time.time() - up_start > 90:
            log.warning(f"[{ccs}] Upload não concluiu em 90s (timeout).")
            safe_back_to_lista(page)
            return False
        try:
            btn = page.locator(SEL_100_UPLOAD_BTN)
            prog = page.locator(SEL_100_PROGRESS)
            progress_done = prog.count() > 0 and "(concluído)" in (prog.inner_text() or "")
            enabled = (btn.count() > 0 and btn.is_enabled())
            if progress_done and enabled:
                break
        except Exception:
            pass
        time.sleep(0.5)
    try:
        b = page.locator(SEL_100_UPLOAD_BTN)
        if b.count() == 0:
            log.warning(f"[{ccs}] Botão 'Fazer upload' não encontrado.")
            safe_back_to_lista(page)
            return False
        b.click() if b.is_enabled() else b.click(force=True)
    except Exception as e:
        log.warning(f"[{ccs}] Falha ao clicar em 'Fazer upload': {e}")
        safe_back_to_lista(page)
        return False
    ok_start = time.time()
    while True:
        if time.time() - ok_start > 60:
            log.warning(f"[{ccs}] Mensagem de sucesso do upload não apareceu (timeout).")
            safe_back_to_lista(page)
            return False
        try:
            if page.locator(SEL_100_MSG_OK).count() > 0:
                txt = clean_text(page.locator(SEL_100_MSG_OK).inner_text() or "")
                if "O upload do arquivo ACCS100 foi concluído." in txt:
                    log.info(f"[{ccs}] Upload ACCS100 concluído.")
                    return True
        except Exception:
            pass
        time.sleep(0.5)

def enviar_ccs012(page, ccs: str, envio: str | None = None, numero_protocolo: str | None = None) -> bool:
    log.info(f"[{ccs}] Abrindo formulário CCS0012 (clicando em 'Enviar')...")
    try:
        rows = page.locator(SEL_TBODY_ROWS)
        if rows.count() == 0:
            log.warning("Não há linhas na tabela para clicar em Enviar.")
            return False
        r0 = rows.nth(0)
        a10 = r0.locator("td").nth(9).locator("a")
        if a10.count() == 0:
            log.warning("Link 'Enviar' não encontrado na coluna 10.")
            return False
        a10.first.click()
    except Exception as e:
        log.warning(f"Falha ao clicar em 'Enviar': {e}")
        return False
    start = time.time()
    ok = False
    while time.time() - start < 8:
        try:
            ok = (
                page.locator(SEL_012_NUM).count() > 0 and
                page.locator(SEL_012_SIT).count() > 0 and
                page.locator(SEL_012_DTHR).count() > 0 and
                page.locator(SEL_012_OBS).count() > 0
            )
            if ok:
                break
        except Exception:
            pass
        time.sleep(0.2)
    if not ok:
        log.warning("Formulário CCS0012 não apareceu.")
        return False
    num_val = (numero_protocolo or "0000000000").strip()
    for _ in range(50):
        try:
            if not clear_and_fill(page, SEL_012_NUM, num_val):
                continue
            if page.locator(SEL_012_NUM).input_value() != num_val:
                continue
            agora = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            if not clear_and_fill(page, SEL_012_DTHR, agora):
                continue
            if page.locator(SEL_012_DTHR).input_value() != agora:
                continue
            caso = (envio or ccs).strip()
            obs_field = page.locator(SEL_012_OBS)
            texto_atual = obs_field.input_value()
            espaco = "" if (texto_atual or "").strip() == "" else " "
            novo_texto = (texto_atual or "") + espaco + caso
            if not clear_and_fill(page, SEL_012_OBS, novo_texto):
                continue
            if obs_field.input_value() != novo_texto:
                continue
            break
        except Exception:
            time.sleep(0.1)
    else:
        log.warning("Não consegui validar campos do CCS0012 (NUM/DTHR/OBS).")
        return False
    try:
        btn = page.locator(SEL_012_ENVIAR)
        if btn.count() == 0:
            log.warning("Botão 'Enviar' do CCS0012 não encontrado.")
            return False
        btn.click() if btn.is_enabled() else btn.click(force=True)
    except Exception as e:
        log.warning(f"Falha ao clicar em 'Enviar' do CCS0012: {e}")
        return False
    start = time.time()
    while time.time() - start < 12:
        try:
            if page.locator(SEL_TABELA).count() > 0:
                log.info(f"[{ccs}] CCS0012 enviado; voltou para a lista.")
                return True
        except Exception:
            pass
        time.sleep(0.3)
    log.warning("Não voltou para a lista após enviar CCS0012.")
    return False

def append_csv(out_path: Path, headers: list, vals: list, ccs: str, envio: str, dt_entrega_detalhes: str | None = None):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not out_path.exists()
    row_dict = {}
    for i, h in enumerate(headers):
        row_dict[h] = vals[i] if i < len(vals) else ""
    row_dict["pesquisado_numero_controle_ccs"] = (ccs or "").strip()
    row_dict["pesquisado_numero_controle_envio"] = (envio or "").strip()
    row_dict["capturado_em"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    row_dict["dt_entrega_detalhes"] = (dt_entrega_detalhes or "").strip()
    fieldnames = list(row_dict.keys())
    with out_path.open("a", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            w.writeheader()
        w.writerow(row_dict)

def bq_upsert_from_csv(csv_path: Path):
    import os
    os.environ.setdefault("GOOGLE_CLOUD_PROJECT", BQ_PROJECT)
    if not csv_path.exists():
        log.warning(f"CSV para BigQuery não encontrado: {csv_path}")
        return False
    try:
        df = pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig")
    except Exception as e:
        log.exception(f"Falha ao ler CSV para BQ: {e}")
        return False
    req_cols = {"Número Controle CCS", "Dt. Hr. Recebimento CCS0011", "Status Requisição Movimentação", "capturado_em"}
    faltam = [c for c in req_cols if c not in df.columns]
    if faltam:
        log.error(f"CSV não tem as colunas exigidas para BQ ({faltam}). Nada será enviado.")
        return False
    try:
        working = pd.DataFrame({
            "numero_controle_ccs": df["Número Controle CCS"].fillna("").astype(str).str.strip(),
            "status_requisicao_movimentacao": df["Status Requisição Movimentação"].fillna("").astype(str),
            "numero_controle_envio": df.get("pesquisado_numero_controle_envio", "").fillna("").astype(str).str.strip()
        })
        working["data_bacen"] = pd.to_datetime(
            df["Dt. Hr. Recebimento CCS0011"].fillna(""), errors="coerce", dayfirst=True
        ).dt.date
        entrega = pd.to_datetime(df.get("dt_entrega_detalhes", "").fillna(""), errors="coerce").dt.date
        captura = pd.to_datetime(df["capturado_em"].fillna(""), errors="coerce").dt.date
        working["dt_coleta"] = entrega.where(pd.notna(entrega), captura)
        working["capturado_em_ts"] = pd.to_datetime(df["capturado_em"].fillna(""), errors="coerce")
    except Exception as e:
        log.exception(f"Falha na normalização para BQ: {e}")
        return False
    working = working[working["numero_controle_ccs"].astype(str).str.strip() != ""].copy()
    if working.empty:
        log.info("Nenhuma linha válida para enviar ao BigQuery (chave vazia).")
        return True
    working.sort_values(["numero_controle_ccs", "capturado_em_ts"], inplace=True)
    working = working.drop_duplicates(subset=["numero_controle_ccs"], keep="last").copy()
    out = working.drop(columns=["capturado_em_ts"])
    client = bigquery.Client(project=BQ_PROJECT, location="US")
    staging = f"{BQ_FQN}__stg_{RUN_STAMP_WEB}"
    schema = [
        bigquery.SchemaField("numero_controle_ccs", "STRING"),
        bigquery.SchemaField("data_bacen", "DATE"),
        bigquery.SchemaField("dt_coleta", "DATE"),
        bigquery.SchemaField("status_requisicao_movimentacao", "STRING"),
        bigquery.SchemaField("numero_controle_envio", "STRING"),
    ]
    try:
        job = client.load_table_from_dataframe(
            out,
            staging,
            job_config=bigquery.LoadJobConfig(
                schema=schema,
                write_disposition="WRITE_TRUNCATE",
            ),
        )
        job.result()
        log.info(f"Staging carregado no BQ: {staging}")
    except Exception as e:
        log.exception(f"Falha ao carregar staging no BQ: {e}")
        return False
    merge_sql = f"""
    MERGE `{BQ_FQN}` T
    USING `{staging}` S
    ON T.numero_controle_ccs = S.numero_controle_ccs
    WHEN MATCHED THEN
      UPDATE SET
        T.data_bacen = S.data_bacen,
        T.dt_coleta = S.dt_coleta,
        T.status_requisicao_movimentacao = S.status_requisicao_movimentacao,
        T.numero_controle_envio = S.numero_controle_envio
    WHEN NOT MATCHED THEN
      INSERT (numero_controle_ccs, data_bacen, dt_coleta, status_requisicao_movimentacao, numero_controle_envio)
      VALUES (S.numero_controle_ccs, S.data_bacen, S.dt_coleta, S.status_requisicao_movimentacao, S.numero_controle_envio)
    """
    try:
        client.query(merge_sql).result()
        log.info(f"MERGE concluído em {BQ_FQN}")
    except Exception as e:
        log.exception(f"Falha no MERGE para {BQ_FQN}: {e}")
        try:
            client.delete_table(staging, not_found_ok=True)
        except Exception:
            pass
        return False
    try:
        client.delete_table(staging, not_found_ok=True)
        log.info("Staging removido.")
    except Exception as e:
        log.warning(f"Não foi possível remover staging {staging}: {e}")
    return True

def processar_lista(page, df):
    headers_cache = None
    enviados_100 = 0
    enviados_012 = 0
    missing_list, missing_ccs_set = precheck_missing_zips(df)
    if missing_list:
        log.warning(f"Encontrados {len(missing_list)} ZIP(s) ausentes antes do processamento.")
    failed_100_list = []
    total = len(df)
    coletadas_desde_flush = 0

    for idx, row in df.iterrows():
        try:
            if hasattr(page, "is_closed") and page.is_closed():
                raise RuntimeError(f"RESTART_BROWSER_AT:{idx}")
            ccs = (row.get("numero_controle_ccs") or "").strip()
            envio = (row.get("numero_controle_envio") or "").strip()
            if not ccs:
                log.warning(f"Linha {idx+1}/{total}: numero_controle_ccs vazio. Pulando.")
                continue
            log.info(f"[{idx+1}/{total}] Processando CCS: {ccs}")

            if not consultar_por_ccs(page, ccs, timeout=30):
                log.warning(f"CCS '{ccs}': não consegui carregar a tabela em 30s. Pulando.")
                continue
            if not garantir_primeira_linha_corresponde(page, ccs, timeout=30):
                log.warning(f"CCS '{ccs}': 1ª linha não corresponde após 30s. Pulando.")
                continue

            if headers_cache is None:
                headers_cache = ler_cabecalhos(page)
                log.info(f"Cabeçalhos: {headers_cache}")

            pl = ler_primeira_linha(page)
            if not pl:
                log.warning("Tabela vazia após confirmar CCS. Pulando.")
                continue
            vals, extras = pl

            ja_tentei_012_pos_100 = False
            if precisa_enviar_100(extras):
                if not envio:
                    log.warning(f"[{ccs}] numero_controle_envio vazio — não é possível enviar ACCS100.")
                elif ccs in missing_ccs_set:
                    log.warning(f"[{ccs}] ZIP ausente (pré-checado). Pulando ACCS100.")
                else:
                    log.info(f"[{ccs}] Enviando ACCS100 (arquivo: {envio}.zip).")
                    ok100 = False
                    t0 = time.time()
                    while time.time() - t0 < 30 and not ok100:
                        try:
                            ok100 = enviar_accs100(page, ccs, envio)
                        except Exception:
                            ok100 = False
                        if not ok100:
                            time.sleep(1)
                    if ok100:
                        enviados_100 += 1
                        protocolo = capturar_protocolo_pos_envio_100(page)
                        safe_back_to_lista(page)
                        consultar_por_ccs(page, ccs, timeout=30)
                        garantir_primeira_linha_corresponde(page, ccs, timeout=30)
                        pl = ler_primeira_linha(page) or pl
                        vals, extras = pl
                        if precisa_enviar_012(vals, extras):
                            log.info(f"[{ccs}] 'Enviar' disponível para CCS0012 — enviando com protocolo capturado.")
                            ok12 = False
                            t1 = time.time()
                            while time.time() - t1 < 30 and not ok12:
                                try:
                                    ok12 = enviar_ccs012(page, ccs, envio, numero_protocolo=protocolo)
                                except Exception:
                                    ok12 = False
                                if not ok12:
                                    time.sleep(1)
                            if ok12:
                                enviados_012 += 1
                                ja_tentei_012_pos_100 = True
                                consultar_por_ccs(page, ccs, timeout=30)
                                garantir_primeira_linha_corresponde(page, ccs, timeout=30)
                                pl = ler_primeira_linha(page) or pl
                                vals, extras = pl
                    else:
                        failed_100_list.append({
                            "numero_controle_ccs": ccs,
                            "numero_controle_envio": envio,
                            "motivo": "falha_upload_ou_timeout"
                        })
                        safe_back_to_lista(page)

            if not ja_tentei_012_pos_100 and precisa_enviar_012(vals, extras):
                log.info(f"[{ccs}] 'Enviar' disponível para CCS0012 — enviando (sem protocolo capturado).")
                ok12 = False
                t2 = time.time()
                while time.time() - t2 < 30 and not ok12:
                    try:
                        ok12 = enviar_ccs012(page, ccs, envio, numero_protocolo=None)
                    except Exception:
                        ok12 = False
                    if not ok12:
                        time.sleep(1)
                if ok12:
                    enviados_012 += 1
                    consultar_por_ccs(page, ccs, timeout=30)
                    garantir_primeira_linha_corresponde(page, ccs, timeout=30)
                    pl = ler_primeira_linha(page) or pl
                    vals, extras = pl

            dt_entrega = coletar_dt_entrega_detalhes(page, timeout=3)
            append_csv(CSV_PATH_WEB, headers_cache, vals, ccs, envio, dt_entrega_detalhes=dt_entrega)
            if dt_entrega:
                coletadas_desde_flush += 1
                if coletadas_desde_flush % 100 == 0:
                    try:
                        log.info("Flush parcial para o BigQuery (100 capturas).")
                        bq_upsert_from_csv(CSV_PATH_WEB)
                    except Exception as e:
                        log.warning(f"Falha no flush parcial para o BigQuery: {e}")
            log.info(f"[{ccs}] Linha registrada em: {CSV_PATH_WEB}")

        except Exception as e:
            msg = str(e)
            if ("Target page, context or browser has been closed" in msg or
                "TargetClosedError" in msg or
                "browser has been closed" in msg or
                "Context was closed" in msg):
                log.warning(f"Navegador/Contexto fechado durante o índice {idx+1}/{total}. Reiniciando.")
                raise RuntimeError(f"RESTART_BROWSER_AT:{idx}")
            log.warning(f"[{idx+1}/{total}] Erro não fatal ao processar {row.get('numero_controle_ccs')}: {e}. Seguindo para o próximo.")
            continue

    return enviados_100, enviados_012, missing_list, failed_100_list

def enviar_email_web(success: bool, runtime_str: str, enviados_100: int, enviados_012: int,
                        missing_list: list, failed_100_list: list):
    assunto_status = "sucesso" if success else "falha"
    agora = datetime.datetime.now().strftime("%d/%m/%Y - %H:%M:%S")
    subject = f"CÉLULA PYTHON MONITORAÇÃO - {BASE_NAME_WEB} - {assunto_status} - {agora}"
    body_text = (
        "Tempo de execução: " + runtime_str + "\n"
        f"Arquivos 100 enviados: {enviados_100}\n"
        f"Arquivos 12 enviados: {enviados_012}\n"
    )
    def table_html(items, title):
        if not items:
            return ""
        rows = "".join(
            f"<tr>"
            f"<td>{i.get('numero_controle_ccs','')}</td>"
            f"<td>{i.get('numero_controle_envio','')}</td>"
            f"<td>{i.get('zip_path','')}</td>"
            f"<td>{i.get('motivo','')}</td>"
            f"</tr>"
            for i in items
        )
        return f"""
        <h3>{title} ({len(items)})</h3>
        <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;">
          <thead>
            <tr>
              <th>numero_controle_ccs</th>
              <th>numero_controle_envio (zip)</th>
              <th>caminho esperado</th>
              <th>motivo</th>
            </tr>
          </thead>
          <tbody>{rows}</tbody>
        </table>
        """
    missing_render = [dict(x, motivo="zip_nao_encontrado") for x in (missing_list or [])]
    body_html = f"""
    <html>
      <body>
        <p><b>Tempo de execução:</b> {runtime_str}<br/>
           <b>Arquivos 100 enviados:</b> {enviados_100}<br/>
           <b>Arquivos 12 enviados:</b> {enviados_012}</p>
        {table_html(missing_render, "CCS não processados por ZIP ausente")}
        {table_html(failed_100_list or [], "Falhas no upload do ACCS100 (ZIP presente)")}
      </body>
    </html>
    """
    try:
        import win32com.client as win32
    except Exception as e:
        log.error(f"pywin32 não disponível para enviar e-mail: {e}")
        return False
    try:
        recipients = ["carlos.lsilva@c6bank.com", "sofia.fernandes@c6bank.com", "antonio.marcoss@c6bank.com"]
        outlook = win32.Dispatch("Outlook.Application")
        mail = outlook.CreateItem(0)
        for addr in recipients:
            try:
                mail.Recipients.Add(addr)
            except Exception:
                pass
        try:
            mail.Recipients.ResolveAll()
        except Exception:
            pass
        if not getattr(mail, "To", None):
            try:
                mail.To = "; ".join(recipients)
            except Exception:
                pass
        mail.Subject = subject
        mail.Body = body_text
        mail.HTMLBody = body_html
        try:
            if CSV_PATH_WEB.exists():
                mail.Attachments.Add(Source=str(CSV_PATH_WEB))
        except Exception as e:
            log.warning(f"Falha ao anexar CSV: {e}")
        try:
            if LOG_PATH_WEB.exists():
                mail.Attachments.Add(Source=str(LOG_PATH_WEB))
        except Exception as e:
            log.warning(f"Falha ao anexar LOG: {e}")
        mail.Send()
        log.info("E-mail enviado via Outlook.")
        return True
    except Exception as e:
        log.exception(f"Falha ao enviar e-mail via Outlook: {e}")
        return False

def step2_web() -> int:
    log.info(">>> INICIANDO STEP 2: AUTOMAÇÃO WEB <<<")
    
    # Carrega dollynho e credenciais
    try:
        load_dollynho()
    except Exception as e:
        log.critical(f"Abortando Step 2 por falha nas credenciais/modulo: {e}")
        return 1

    start_ts = time.time()
    enviados_100_total = 0
    enviados_012_total = 0
    success = False
    missing_acum = []
    failed_100_acum = []

    try:
        df = carregar_dataframe_web()
    except Exception as e:
        log.exception(f"Falha ao carregar o DataFrame para Web: {e}")
        df = pd.DataFrame(columns=["numero_controle_ccs", "numero_controle_envio"])

    if df.empty:
        log.warning("DataFrame vazio para automação web. Encerrando Step 2.")
        return 0

    restante = df.copy()

    with sync_playwright() as pw:
        while True:
            browser = None
            ctx = None
            try:
                browser = pw.chromium.launch(headless=False, channel="chrome")
                ctx = browser.new_context(ignore_https_errors=True)
                page = ctx.new_page()
                login_loop(page)
                goto_requisicoes(page)
                e100, e012, miss_list, fail_list = processar_lista(page, restante)
                enviados_100_total += e100
                enviados_012_total += e012
                missing_acum.extend(miss_list or [])
                failed_100_acum.extend(fail_list or [])
                success = True
                break
            except RuntimeError as e:
                msg = str(e)
                if msg.startswith("RESTART_BROWSER_AT:"):
                    try:
                        at = int(msg.split(":", 1)[1])
                    except Exception:
                        at = 0
                    if at < 0:
                        at = 0
                    if at >= len(restante):
                        success = True
                        break
                    restante = restante.iloc[at:].copy()
                    log.info(f"Reiniciando navegador e retomando a partir do índice {at} ({len(restante)} restantes).")
                    try:
                        if ctx: ctx.close()
                    except Exception: pass
                    try:
                        if browser: browser.close()
                    except Exception: pass
                    continue
                else:
                    log.exception(f"Erro em execução: {e}")
                    success = False
                    break
            except Exception as e:
                log.exception(f"Falha no fluxo: {e}")
                success = False
                break
            finally:
                try:
                    if ctx: ctx.close()
                except Exception: pass
                try:
                    if browser: browser.close()
                except Exception: pass

    elapsed = time.time() - start_ts
    runtime_str = f"{elapsed:.2f} segundos"
    try:
        try:
            bq_ok = bq_upsert_from_csv(CSV_PATH_WEB)
            log.info(f"Envio final ao BigQuery: {'ok' if bq_ok else 'falhou'}")
        except Exception as e:
            log.exception(f"Erro inesperado no envio final ao BigQuery: {e}")
        enviar_email_web(success, runtime_str, enviados_100_total, enviados_012_total, missing_acum, failed_100_acum)
    except Exception as e:
        log.exception(f"Erro ao montar/enviar e-mail: {e}")

    try:
        for h in list(logger_web.handlers):
            try:
                h.flush()
                h.close()
            except Exception:
                pass
            logger_web.removeHandler(h)
    except Exception:
        pass

    # Limpeza de arquivos temporários do step 2
    for path in [CSV_PATH_WEB, LOG_PATH_WEB]:
        try:
            if path.exists():
                os.remove(path)
        except Exception as e:
            try:
                print(f"Não foi possível remover {path}: {e}")
            except Exception:
                pass
    rc = 0 if success else 1
    return rc

# ==============================================================================
# ORQUESTRADOR PRINCIPAL
# ==============================================================================

def main_orchestrator():
    print("="*60)
    print("INICIANDO PROCESSO UNIFICADO")
    print("="*60)
    
    # 1. Executa Geração de Cartas e Zips
    status_gen = step1_geracao()
    
    if status_gen == 0:
        print("\n" + "="*60)
        print("STEP 1 CONCLUÍDO COM SUCESSO (Arquivos Gerados).")
        print("INICIANDO STEP 2 (AUTOMAÇÃO WEB)...")
        print("="*60 + "\n")
        
        # 2. Executa Automação Web
        status_web = step2_web()
        return status_web
        
    elif status_gen == 2:
        print("\n" + "="*60)
        print("STEP 1 FINALIZADO: SEM DADOS PARA PROCESSAR.")
        print("ENCERRANDO SEM EXECUTAR STEP 2.")
        print("="*60 + "\n")
        return 0
        
    else:
        print("\n" + "="*60)
        print(f"STEP 1 FALHOU COM CÓDIGO {status_gen}.")
        print("ABORTANDO PROCESSO.")
        print("="*60 + "\n")
        return 1

if __name__ == "__main__":
    sys.exit(main_orchestrator())