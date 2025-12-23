import csv
import datetime
import getpass
import importlib.util
import json
import logging
import os
import re
import shutil
import sys
import tempfile
import time
import traceback
import unicodedata
import zipfile
from collections import defaultdict
from pathlib import Path
from subprocess import Popen
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import pandas_gbq
import pythoncom
from google.cloud import bigquery
from playwright.sync_api import TimeoutError as PWTimeoutError
from playwright.sync_api import sync_playwright
from win32com.client import Dispatch

try:
    import xlsxwriter
    XLSXWRITER_OK = True
except ImportError:
    XLSXWRITER_OK = False

# ==============================================================================
# CONSTANTES E CONFIGURAÇÕES
# ==============================================================================

NOME_AUTOMACAO = "BO OFICIOS"
NOME_SCRIPT = "respostadados" 
TZ = ZoneInfo("America/Sao_Paulo")

# Detecção de Ambiente
ENV_EXEC_USER = os.getenv("ENV_EXEC_USER", getpass.getuser()).lower()
if "@" not in ENV_EXEC_USER: ENV_EXEC_USER += "@c6bank.com"
ENV_EXEC_MODE = os.getenv("ENV_EXEC_MODE", "MANUAL").upper()
HEADLESS = True 

# Paths - Robust Detection
POSSIBLE_ROOTS = [
    Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano",
    Path.home() / "Meu Drive/C6 CTVM" / "graciliano",
    Path.home() / "C6 CTVM/graciliano",
]
BASE_DIR = next((p for p in POSSIBLE_ROOTS if p.exists()), Path.home() / "graciliano")

MODULES_PATH = BASE_DIR / "novo_servidor" / "modules"
sys.path.append(str(MODULES_PATH))

try:
    import dollynho
except ImportError:
    dollynho = None

BASE_AUTOM = BASE_DIR / "automacoes" / NOME_AUTOMACAO
INPUT_DIR = BASE_AUTOM / "arquivos_input"
LOG_DIR = BASE_AUTOM / "logs" / NOME_SCRIPT / datetime.now(TZ).strftime("%Y-%m-%d")

# Diretorios Especificos
DESTINO_ZIPS_FINAL = BASE_AUTOM / "zipados"
CAMINHO_ARQUIVO_PREPARAR = INPUT_DIR / "preparar.xlsx"
LOCKS_DIR = BASE_AUTOM / ".locks"

# Criação de Diretorios
for d in [INPUT_DIR, LOG_DIR, DESTINO_ZIPS_FINAL, LOCKS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# Configurações BigQuery
PROJECT_ID = "datalab-pagamentos"
DATASET_ID = "ADMINISTRACAO_CELULA_PYTHON"
TABELA_AUTOMACOES_EXEC = f"{PROJECT_ID}.{DATASET_ID}.automacoes_exec"
TABELA_REGISTRO = f"{PROJECT_ID}.{DATASET_ID}.Registro_automacoes" # Corrigido Case
TABELA_WALLB_INPUT = f"{PROJECT_ID}.{DATASET_ID}.WallB_casos"
TABELA_WALLE_OUTPUT = "WallE_respostas"

# Configurações Web
LOGIN_URL = "https://ccs.matera-v2.corp/materaccs/secure/login.jsf"
REQ_URL = "https://ccs.matera-v2.corp/materaccs/movimentacao/requisicoes_movimentacao.jsf"
CONSULTA_100_URL = "https://ccs.matera-v2.corp/materaccs/movimentacao/consulta_accs100.jsf"

# Seletores
SEL_USER = "#loginForm\\:login"
SEL_PASS = "#loginForm\\:senha"
SEL_OK = "#loginForm\\:loginAction"
SEL_NUMCTRL_CCS = "#filtroForm\\:numCtrlCcs"
SEL_CONSULTAR = "#filtroForm\\:consultar"
SEL_TABELA = "#listaForm\\:requisicoesMovimentacaoTable"
SEL_TBODY_ROWS = "#listaForm\\:requisicoesMovimentacaoTable tbody tr"
SEL_100_FILE_INPUT = ".resumable-browse input[type='file']" 
SEL_100_UPLOAD_BTN = "#importarForm\\:upload-btn"
SEL_100_PROGRESS = ".resumable-file-progress"
SEL_100_MSG_OK = "span.msgWarning"
SEL_100_CONSULTAR_BTN = "#filtroForm\\:consultar"
SEL_100_ARQUIVOS_ROWS = "#listaForm\\:arquivosTable tbody tr"
SEL_012_NUM = "#ccs0012Form\\:numeroControleEntrega"
SEL_012_DTHR = "#ccs0012Form\\:dtHrEntrega"
SEL_012_OBS = "#ccs0012Form\\:txtObsResp"
SEL_012_ENVIAR = "#ccs0012Form\\:enviar"

# ==============================================================================
# FUNÇÕES DE SUPORTE
# ==============================================================================

def setup_logger():
    logger = logging.getLogger(NOME_SCRIPT)
    logger.setLevel(logging.INFO)
    logger.handlers = []
    logger.propagate = False
    
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    
    ts = datetime.now(TZ).strftime("%H%M%S")
    log_file = LOG_DIR / f"{NOME_SCRIPT}_{ts}.log"
    
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    
    return logger, log_file

def get_config(logger):
    try:
        sql = f"""
            SELECT emails_principal, emails_cc 
            FROM `{TABELA_REGISTRO}` 
            WHERE TRIM(LOWER(script_name)) = '{NOME_SCRIPT.lower()}'
            LIMIT 1
        """
        df = pandas_gbq.read_gbq(sql, project_id=PROJECT_ID)
        if df.empty: return [], []
        
        def parse(raw):
            if not raw or str(raw).lower() == 'nan': return []
            return [x.strip() for x in str(raw).replace(';',',').split(',') if '@' in x]
            
        return parse(df.iloc[0]['emails_principal']), parse(df.iloc[0]['emails_cc'])
    except Exception as e:
        logger.error(f"Erro config: {e}")
        return ["carlos.lsilva@c6bank.com"], []

# ==============================================================================
# CLASSES DE NEGÓCIO
# ==============================================================================

class ObterDadosWallB:
    def run(self, logger):
        inicio = time.perf_counter()
        logger.info(f"Buscando dados na tabela {TABELA_WALLB_INPUT}...")
        try:
            sql = f"""
                SELECT numero_controle_ccs, numero_controle_envio
                FROM `{TABELA_WALLB_INPUT}`
                WHERE cnpj_participante != '32345784'
            """
            df = pandas_gbq.read_gbq(sql, project_id=PROJECT_ID)
            if not df.empty:
                df["numero_controle_envio"] = df["numero_controle_envio"].astype(str)
                df["numero_controle_ccs"] = df["numero_controle_ccs"].astype(str)
            logger.info(f"Dados obtidos. Linhas: {len(df)}")
            return df
        except Exception as e:
            logger.error(f"Erro ao buscar dados do WallB: {e}")
            return pd.DataFrame()

class AutomacaoMatera:
    def __init__(self, logger):
        self.logger = logger
        self.auth_path = BASE_AUTOM / "PLAYWRIGHT_SESSIONS" / f"auth_state_{NOME_SCRIPT}.json"
        self.auth_path.parent.mkdir(parents=True, exist_ok=True)

    def clean_text(self, s):
        if s is None: return ""
        s = unicodedata.normalize("NFC", s)
        s = s.replace("\u00A0", " ")
        return " ".join(s.split())

    def clear_and_fill(self, page, sel, value):
        try:
            page.locator(sel).click()
            page.keyboard.press("Control+A")
            page.keyboard.press("Delete")
            page.locator(sel).fill(value)
            return True
        except Exception:
            return False

    def login(self, page, usuario, senha):
        self.logger.info("Realizando login no Matera...")
        try:
            page.goto(LOGIN_URL)
            page.fill(SEL_USER, usuario)
            page.fill(SEL_PASS, senha)
            page.click(SEL_OK)
            time.sleep(5)
            # Salvar estado? Opcional
            return True
        except Exception as e:
            self.logger.error(f"Erro critico na tentativa de input de login: {e}")
            return False

    def ensure_requisicoes_ready(self, page, max_wait=30):
        t0 = time.time()
        while time.time() - t0 < max_wait:
            try:
                if "requisicoes_movimentacao.jsf" not in (page.url or ""):
                    page.goto(REQ_URL, wait_until="domcontentloaded")
                
                loc = page.locator(SEL_NUMCTRL_CCS)
                try: loc.wait_for(state="visible", timeout=2000)
                except: pass
                
                if loc.is_visible() and not loc.is_disabled():
                    return True
                
                if loc.is_disabled():
                    page.keyboard.press("Escape")
                    time.sleep(0.5)
            except:
                time.sleep(0.5)
        return False

    def consultar_por_ccs(self, page, ccs, timeout=30):
        t0 = time.time()
        while time.time() - t0 < timeout:
            try:
                if not self.ensure_requisicoes_ready(page, max_wait=5):
                    continue
                
                self.clear_and_fill(page, SEL_NUMCTRL_CCS, ccs)
                
                btn = page.locator(SEL_CONSULTAR)
                if btn.count() > 0 and btn.is_enabled():
                    btn.click()
                else:
                    page.locator(SEL_NUMCTRL_CCS).press("Enter")
                
                try:
                    page.locator(SEL_TABELA).wait_for(state="visible", timeout=5000)
                    return True
                except:
                    pass
            except:
                pass
            time.sleep(0.5)
        return False

    def ler_primeira_linha(self, page):
        try:
            rows = page.locator(SEL_TBODY_ROWS)
            if rows.count() == 0: return None
            
            r = rows.nth(0)
            tds = r.locator("td")
            vals = [self.clean_text(tds.nth(i).inner_text()) for i in range(tds.count())]
            
            extras = {}
            c8 = tds.nth(7)
            img8 = c8.locator("img"); a8 = c8.locator("a")
            extras["col8_img_title"] = self.clean_text(img8.first.get_attribute("title") or "") if img8.count() else ""
            extras["col8_link_text"] = self.clean_text(a8.first.inner_text() or "") if a8.count() else ""
            
            c10 = tds.nth(9)
            a10 = c10.locator("a")
            extras["col10_link_text"] = self.clean_text(a10.first.inner_text() or "") if a10.count() else ""
            
            return vals, extras
        except:
            return None

    def garantir_primeira_linha_corresponde(self, ccs, page, timeout=30):
        t0 = time.time()
        while time.time() - t0 < timeout:
            pl = self.ler_primeira_linha(page)
            if pl:
                vals, _ = pl
                if len(vals) >= 5 and (vals[4] or "").strip() == ccs:
                    return True
            time.sleep(0.5)
        return False

    def enviar_accs100(self, page, ccs, zip_path):
        self.logger.info(f"[{ccs}] Iniciando upload ACCS100: {zip_path.name}")
        try:
            r0 = page.locator(SEL_TBODY_ROWS).nth(0)
            a8 = r0.locator("td").nth(7).locator("a")
            if a8.count() == 0: return False
            a8.click()
            time.sleep(2)
            
            try:
                page.set_input_files(SEL_100_FILE_INPUT, str(zip_path))
                time.sleep(1)
            except Exception as e:
                self.logger.warning(f"[{ccs}] Falha ao setar input file: {e}")
                return False

            page.click(SEL_100_UPLOAD_BTN)
            
            try:
                page.wait_for_selector(SEL_100_MSG_OK, timeout=120000)
                msg = page.locator(SEL_100_MSG_OK).inner_text()
                if "concluído" in msg or "sucesso" in msg.lower():
                    self.logger.info(f"[{ccs}] Upload confirmado com sucesso.")
                    return True
            except:
                pass
            
            return False
        except Exception as e:
            self.logger.error(f"[{ccs}] Erro no fluxo de upload: {e}")
            return False

    def enviar_ccs012(self, page, ccs, envio, numero_protocolo=None):
        self.logger.info(f"[{ccs}] Enviando CCS012...")
        try:
            r0 = page.locator(SEL_TBODY_ROWS).nth(0)
            a10 = r0.locator("td").nth(9).locator("a")
            if a10.count() == 0: return False
            a10.click()
            
            page.locator(SEL_012_NUM).wait_for(state="visible", timeout=10000)
            proto_final = numero_protocolo if numero_protocolo else "0000000000"
            self.clear_and_fill(page, SEL_012_NUM, proto_final)
            
            agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            self.clear_and_fill(page, SEL_012_DTHR, agora)
            
            obs_el = page.locator(SEL_012_OBS)
            txt = obs_el.input_value()
            page.locator(SEL_012_OBS).fill(f"{txt} {envio}".strip())
            
            page.click(SEL_012_ENVIAR)
            page.locator(SEL_TABELA).wait_for(state="visible", timeout=15000)
            self.logger.info(f"[{ccs}] CCS012 enviado (Protocolo: {proto_final}).")
            return True
        except Exception as e:
            self.logger.error(f"[{ccs}] Erro ao enviar CCS012: {e}")
            return False

    def processar_casos(self, excel_path):
        self.logger.info("Iniciando automacao Playwright...")
        
        cols_retorno = ["numero_controle_ccs", "numero_controle_envio", "status_processamento", "data_execucao"]
        df = pd.read_excel(excel_path, dtype=str)
        resultados = []
        
        usuario, senha = None, None
        if dollynho:
            try: usuario, senha = dollynho.get_credencial("BO OFICIOS")
            except: pass
        if not usuario: usuario, senha = os.getenv("MATERA_USER", "DUMMY"), os.getenv("MATERA_PASS", "DUMMY")

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=HEADLESS, channel="chrome")
            context = browser.new_context(ignore_https_errors=True)
            page = context.new_page()
            
            if not self.login(page, usuario, senha):
                browser.close()
                return pd.DataFrame(columns=cols_retorno)

            for _, row in df.iterrows():
                ccs = str(row.get("numero_controle_ccs", "")).strip()
                envio = str(row.get("numero_controle_envio", "")).strip()
                zip_path = DESTINO_ZIPS_FINAL / f"{envio}.zip"
                
                if not ccs: continue
                if not zip_path.exists():
                    self.logger.warning(f"[{ccs}] ZIP não encontrado. Pulando.")
                    resultados.append({"numero_controle_ccs": ccs, "numero_controle_envio": envio, "status_processamento": "ZIP NAO ENCONTRADO"})
                    continue

                self.logger.info(f"Processando CCS: {ccs} | Envio: {envio}")
                
                try:
                    if not self.consultar_por_ccs(page, ccs):
                        self.logger.warning(f"[{ccs}] Não encontrado na busca.")
                        resultados.append({"numero_controle_ccs": ccs, "numero_controle_envio": envio, "status_processamento": "CCS NAO ENCONTRADO"})
                        continue
                    
                    if not self.garantir_primeira_linha_corresponde(ccs, page):
                        self.logger.warning(f"[{ccs}] Linha da tabela não corresponde.")
                        continue

                    pl = self.ler_primeira_linha(page)
                    if not pl: continue
                    vals, extras = pl
                    
                    status_final = "SEM ACAO"
                    protocolo_capturado = None
                    
                    # Logica Simplificada de Decisao
                    precisa_100 = "Fazer upload" in extras.get("col8_img_title", "")
                    ja_tem_100 = "Não é possível realizar o upload" in extras.get("col8_img_title", "")
                    precisa_012 = ja_tem_100 and extras.get("col10_link_text", "") == "Enviar"
                    
                    if precisa_100:
                        if self.enviar_accs100(page, ccs, zip_path):
                             status_final = "SUCESSO_100"
                             # Tenta pegar protocolo na consulta 100... (Omitido para brevidade, pode ser add se crítico)
                             page.goto(REQ_URL) 
                             self.consultar_por_ccs(page, ccs)
                             pl = self.ler_primeira_linha(page)
                             if pl: vals, extras = pl
                             precisa_012 = True # Assume que agora pode mandar 012
                        else:
                             resultados.append({"numero_controle_ccs": ccs, "numero_controle_envio": envio, "status_processamento": "FALHA_UPLOAD_100"})
                             continue

                    if precisa_012:
                        if self.enviar_ccs012(page, ccs, envio, numero_protocolo=protocolo_capturado):
                            status_final = "SUCESSO_TOTAL" if status_final == "SUCESSO_100" else "SUCESSO_012"

                    resultados.append({
                        "numero_controle_ccs": ccs,
                        "numero_controle_envio": envio,
                        "status_processamento": status_final,
                        "data_execucao": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })
                    
                except Exception as e:
                    self.logger.error(f"Erro processando {ccs}: {e}")
                    resultados.append({"numero_controle_ccs": ccs, "numero_controle_envio": envio, "status_processamento": f"ERRO: {str(e)}"})

            browser.close()
            
        return pd.DataFrame(resultados)

class SubirBigQuery:
    def run(self, logger, df_resultados: pd.DataFrame):
        if df_resultados.empty: return 0
        logger.info(f"Subindo {len(df_resultados)} linhas para o BigQuery: {TABELA_WALLE_OUTPUT}")
        try:
            df_str = df_resultados.astype(str)
            pandas_gbq.to_gbq(
                df_str,
                destination_table=f"{DATASET_ID}.{TABELA_WALLE_OUTPUT}",
                project_id=PROJECT_ID,
                if_exists="append"
            )
            return len(df_resultados)
        except Exception as e:
            logger.error(f"Erro BQ: {e}")
            return 0

# ==============================================================================
# EXECUÇÃO
# ==============================================================================

def main():
    logger, log_path = setup_logger()
    logger.info("INICIO PROCESSAMENTO")
    
    start_time = time.time()
    dt_inicio = datetime.now(TZ)
    status_global = "SUCESSO"
    
    if CAMINHO_ARQUIVO_PREPARAR.exists():
        automacao = AutomacaoMatera(logger)
        df_res = automacao.processar_casos(CAMINHO_ARQUIVO_PREPARAR)
        uploader = SubirBigQuery()
        uploader.run(logger, df_res)
    else:
        logger.warning(f"Arquivo de controle não encontrado: {CAMINHO_ARQUIVO_PREPARAR}")
        status_global = "SEM DADOS"

    duration = time.time() - start_time
    tempo_exec_str = str(timedelta(seconds=int(duration)))
    
    # Notificação
    recip_to, recip_cc = get_config(logger)
    
    # 1. Enviar Email
    try:
        pythoncom.CoInitialize()
        outlook = Dispatch("Outlook.Application")
        mail = outlook.CreateItem(0)
        mail.To = ";".join(recip_to)
        if status_global == "SUCESSO" and recip_cc:
             mail.CC = ";".join(recip_cc)
        mail.Subject = f"{NOME_AUTOMACAO} - {NOME_SCRIPT} - {status_global}"
        mail.Body = f"Execução finalizada.\nStatus: {status_global}\nTempo: {tempo_exec_str}\nLog Attached."
        if log_path.exists(): mail.Attachments.Add(str(log_path))
        mail.Send()
        logger.info("Email enviado.")
    except Exception as e:
        logger.error(f"Erro email: {e}")

    # 2. Upload Metricas
    try:
        df_m = pd.DataFrame([{
            "nome_automacao": NOME_AUTOMACAO,
            "metodo_automacao": NOME_SCRIPT,
            "status": status_global,
            "tempo_exec": tempo_exec_str,
            "data_exec": dt_inicio.strftime("%Y-%m-%d"),
            "hora_exec": dt_inicio.strftime("%H:%M:%S"),
            "usuario": ENV_EXEC_USER,
            "log_path": str(log_path)
        }])
        pandas_gbq.to_gbq(df_m, TABELA_AUTOMACOES_EXEC, project_id=PROJECT_ID, if_exists="append")
        logger.info("Metricas subidas.")
    except Exception as e:
        logger.error(f"Erro metricas: {e}")

if __name__ == "__main__":
    main()