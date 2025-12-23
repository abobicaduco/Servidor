import logging
import os
import sys
import shutil
import time
import zipfile
import json
import getpass
import pandas as pd
import pythoncom
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

# ==============================================================================
# 0. VARIÁVEIS GLOBAIS OBRIGATÓRIAS E CONFIGURAÇÕES
# ==============================================================================
REGRAVEL_EXCEL = False   
SUBIDA_BQ = "append"     
HEADLESS = True  # Default to True for Server

# Definições de Diretórios
NOME_SERVIDOR = "Servidor.py" 
NOME_SCRIPT = Path(__file__).stem.upper()
NOME_AUTOMACAO = "BO OFICIOS"

# Paths - Robust Detection
POSSIBLE_ROOTS = [
    Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano",
    Path.home() / "Meu Drive/C6 CTVM" / "graciliano",
    Path.home() / "C6 CTVM/graciliano",
]
BASE_DIR = next((p for p in POSSIBLE_ROOTS if p.exists()), Path.home() / "graciliano")

MODULES_PATH = BASE_DIR / "novo_servidor" / "modules"
BASE_AUTOM = BASE_DIR / "automacoes" / NOME_AUTOMACAO

# Caminho de Logs
HOJE_STR = datetime.now().strftime("%Y-%m-%d")
LOG_DIR = BASE_AUTOM / "logs" / NOME_SCRIPT / HOJE_STR
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Caminhos Específicos do Processo
INPUT_DIR = BASE_AUTOM / "arquivos input"
CAMINHO_EXCEL = INPUT_DIR / "abrir_casos.xlsx"
PASTA_XMLS = BASE_AUTOM / "arquivos_xml"

# Configurações BigQuery
TABELA_BQ = "datalab-pagamentos.DATASET.TABELA_RAW"
PROJECT_ID = "datalab-pagamentos"

# Configurações Web
MATERA_URL = "https://ccs.matera-v2.corp/materaccs/mensagens/leitura.jsf"

# XPaths Absolutos (Conforme solicitado)
XPATH_LOGIN_USER = '/html/body/table/tbody/tr[2]/td/span/form/center/table/tbody/tr[1]/td[2]/input'
XPATH_LOGIN_PASS = '/html/body/table/tbody/tr[2]/td/span/form/center/table/tbody/tr[2]/td[2]/input'
XPATH_LOGIN_OK = '/html/body/table/tbody/tr[2]/td/span/form/center/input[1]'
XPATH_INPUT_ARQUIVO = '//*[@id="importarForm:arquivo"]'
XPATH_BOTAO_PROCESSAR = '/html/body/table/tbody/tr[2]/td/span/form/center/input[1]'
XPATH_BOTAO_VOLTAR = '//*[@id="j_id_jsp_1486882743_12:voltar"]'

# ==============================================================================
# 1. IMPORTS DE MÓDULOS UTILITÁRIOS
# ==============================================================================
sys.path.append(str(MODULES_PATH))

try:
    import pandas_gbq
except ImportError as e:
    print(f"ERRO CRÍTICO: Falha ao importar módulos obrigatórios: {e}")
    
from win32com.client import Dispatch
from datetime import timedelta

# Loader Dollynho
def _carregar_dollynho():
    candidatos = [
        Path(__file__).with_name("dollynho.py"),
        Path(__file__).parent / "dollynho.py",
        MODULES_PATH / "dollynho.py",
    ]
    path = next((p for p in candidatos if p.is_file()), None)
    if path:
        import importlib.util
        spec = importlib.util.spec_from_file_location("dollynho", path)
        if spec and spec.loader:
            mod = importlib.util.module_from_spec(spec)
            sys.modules["dollynho"] = mod
            spec.loader.exec_module(mod)
            return mod
    return None

dollynho = _carregar_dollynho()

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

# ==============================================================================
# 2. CONFIGURAÇÃO DE LOGS
# ==============================================================================
def configurar_logger():
    """Configura o logger para console e arquivo."""
    logger = logging.getLogger(NOME_SCRIPT)
    logger.setLevel(logging.INFO)
    
    # Limpa handlers anteriores
    for h in list(logger.handlers):
        logger.removeHandler(h)
    
    logger.propagate = False
    
    formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    
    timestamp = datetime.now().strftime("%H%M%S")
    log_file = LOG_DIR / f"{NOME_SCRIPT}_{timestamp}.log"
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    
    return logger, log_file

# ==============================================================================
# 3. FUNÇÕES AUXILIARES
# ==============================================================================

def obter_destinatarios(logger):
    try:
        sql = f"""
            SELECT emails_principais, emails_cc 
            FROM `datalab-pagamentos.ADMINISTRACAO_CELULA_PYTHON.Registro_automacoes` 
            WHERE lower(TRIM(metodo_automacao)) = lower(TRIM('{NOME_SCRIPT.lower()}'))
            LIMIT 1
        """
        # Usando pandas_gbq padrao
        df = pandas_gbq.read_gbq(sql, project_id=PROJECT_ID)
        if df.empty: 
            return [], []
        
        def limpar(raw):
            if not raw or str(raw).lower() == 'nan': return []
            return [x.strip() for x in str(raw).replace(';', ',').split(',') if '@' in x]
            
        principais = limpar(df.iloc[0]['emails_principais'])
        cc = limpar(df.iloc[0]['emails_cc'])
        return principais, cc
    except Exception as e:
        logger.error(f"Erro ao buscar destinatários: {e}")
        return ["carlos.lsilva@c6bank.com"], []

def tratar_excel_corrompido(logger, caminho_arquivo):
    if REGRAVEL_EXCEL and caminho_arquivo.exists():
        try:
            import win32com.client as win32
            xl = win32.Dispatch("Excel.Application")
            xl.DisplayAlerts = False
            xl.Visible = False
            wb = xl.Workbooks.Open(str(caminho_arquivo))
            wb.Save()
            wb.Close()
            xl.Quit()
        except Exception as e:
            logger.warning(f"Falha ao regravar Excel via COM: {e}")

def zipar_logs(logger, log_file, arquivos_extras=None):
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        zip_name = LOG_DIR / f"{NOME_SCRIPT}_{timestamp}.zip"
        
        with zipfile.ZipFile(zip_name, 'w', zipfile.ZIP_DEFLATED) as zf:
            if log_file and log_file.exists():
                zf.write(log_file, arcname=log_file.name)
            
            if arquivos_extras:
                for arq in arquivos_extras:
                    p = Path(arq)
                    if p.exists():
                        zf.write(p, arcname=p.name)
        return str(zip_name)
    except Exception as e:
        logger.error(f"Erro ao zipar logs: {e}")
        return None

# ==============================================================================
# 4. CLASSES E LÓGICA DE NEGÓCIO
# ==============================================================================

class MateraAutomacao:
    def __init__(self, logger):
        self.logger = logger
        # Ajuste path sessao para pasta local do automacao
        self.auth_path = BASE_AUTOM / "PLAYWRIGHT_SESSIONS" / f"auth_state_{NOME_SCRIPT}.json"
        self.auth_path.parent.mkdir(parents=True, exist_ok=True)

    def _get_credentials(self):
        try:
            if dollynho:
                return dollynho.get_credencial(NOME_SCRIPT)
            return "user_mock", "pass_mock"
        except Exception as e:
            self.logger.error(f"Falha ao obter credenciais do cofre Dollynho: {e}")
            raise

    def login(self, page):
        # Verifica se já estamos logados (campo de arquivo visível)
        try:
            if page.locator(f"xpath={XPATH_INPUT_ARQUIVO}").is_visible(timeout=2000):
                self.logger.info("Campo de upload detectado. Sessão já ativa. Pulando login.")
                return
        except:
            pass

        # Executa Login Real
        user, password = self._get_credentials()
        self.logger.info("Realizando login no Matera...")
        
        if page.url != MATERA_URL:
            page.goto(MATERA_URL, timeout=60000)

        try:
            page.locator(f"xpath={XPATH_LOGIN_USER}").fill(user)
            page.locator(f"xpath={XPATH_LOGIN_PASS}").fill(password)
            page.locator(f"xpath={XPATH_LOGIN_OK}").click()
            
            self.logger.info("Botão OK clicado. Aguardando...")
            page.wait_for_timeout(5000)
            
            # Verificação final
            try:
                page.wait_for_selector(f"xpath={XPATH_INPUT_ARQUIVO}", timeout=30000, state="visible")
                self.logger.info("Página de processamento carregada com sucesso.")
            except PWTimeoutError:
                # As vezes redireciona para outra URL, força volta
                page.goto(MATERA_URL)
                page.wait_for_selector(f"xpath={XPATH_INPUT_ARQUIVO}", timeout=10000, state="visible")
            
            self.logger.info("Salvando estado de autenticação...")
            context = page.context
            context.storage_state(path=str(self.auth_path))
            
        except Exception as e:
             self.logger.error(f"Erro crítico no login: {e}")
             raise

    def processar_arquivos(self, df_mapa):
        resultados = []
        enviados = 0
        
        with sync_playwright() as p:
            self.logger.info("Iniciando Browser (Chrome)...")
            browser = p.chromium.launch(channel="chrome", headless=HEADLESS, args=["--start-maximized"])
            
            context = None
            if self.auth_path.exists():
                try:
                    self.logger.info("Carregando sessão salva...")
                    context = browser.new_context(storage_state=str(self.auth_path), viewport={"width": 1920, "height": 1080})
                except Exception as e:
                    self.logger.warning(f"Sessão corrompida: {e}")
            
            if not context:
                context = browser.new_context(viewport={"width": 1920, "height": 1080})

            page = context.new_page()

            self.logger.info(f"Navegando para: {MATERA_URL}")
            try:
                page.goto(MATERA_URL, timeout=60000)
                
                # Check login
                precisa_login = False
                try:
                    if not page.locator(f"xpath={XPATH_INPUT_ARQUIVO}").is_visible(timeout=3000):
                        precisa_login = True
                except:
                    precisa_login = True

                if precisa_login:
                    self.logger.info("Sessão não detectada. Iniciando login.")
                    self.login(page)
                
            except Exception as e:
                self.logger.error(f"Erro na inicialização da página: {e}")
                self.login(page)

            # Loop de processamento
            for idx, row in df_mapa.iterrows():
                controle = str(row["controle_ccs"])
                xml_nome = str(row["xml_nome"])
                xml_path = Path(str(row["xml_path"]))
                status_inicial = row["status_inicial"]

                if status_inicial != "OK":
                    resultados.append((controle, xml_nome, status_inicial))
                    continue

                self.logger.info(f"Processando arquivo: {xml_nome}")
                
                try:
                    page.wait_for_selector(f"xpath={XPATH_INPUT_ARQUIVO}", timeout=10000)

                    file_input = page.locator(f"xpath={XPATH_INPUT_ARQUIVO}")
                    file_input.set_input_files(str(xml_path))
                    
                    page.locator(f"xpath={XPATH_BOTAO_PROCESSAR}").click()
                    
                    sucesso = False
                    try:
                        # Tenta validar mensagem de sucesso
                        try:
                            msg = page.locator("css=span.msgWarning").first.inner_text(timeout=5000).strip().lower()
                        except:
                            msg = page.locator("xpath=//*[@class='msgWarning']").first.inner_text(timeout=2000).strip().lower()
                        
                        if xml_nome.lower() in msg and "foi lida com sucesso" in msg:
                            sucesso = True
                    except:
                        pass

                    if sucesso:
                        resultados.append((controle, xml_nome, "SUCESSO"))
                        enviados += 1
                        self.logger.info(f"Arquivo {xml_nome} enviado com SUCESSO.")
                    else:
                        resultados.append((controle, xml_nome, "FALHA_PROCESSAMENTO"))
                        self.logger.error(f"Falha validação: {xml_nome}")

                    try:
                        page.locator(f"xpath={XPATH_BOTAO_VOLTAR}").click(timeout=5000)
                    except:
                        page.goto(MATERA_URL)

                except Exception as e:
                    self.logger.error(f"Erro processamento {xml_nome}: {e}")
                    resultados.append((controle, xml_nome, "FALHA_GERAL"))
                    page.goto(MATERA_URL)

            context.close()
            browser.close()

        return pd.DataFrame(resultados, columns=["controle_ccs", "xml_nome", "status_final"]), enviados

# ==============================================================================
# 5. EXECUÇÃO PRINCIPAL
# ==============================================================================

def main():
    logger, log_path = configurar_logger()
    logger.info("INICIO|Execução Script")
    
    status_final = "FALHA"
    arquivos_gerados = []
    tempo_inicio = time.time()
    
    # Detecção de Modo
    is_servidor = os.getenv("MODO_EXECUCAO") == "AUTO" or "--executado-por-servidor" in sys.argv
    usuario_exec = os.getenv("USUARIO_EXEC", getpass.getuser())

    try:
        dest_principais, dest_cc = obter_destinatarios(logger)
        
        logger.info("Verificando arquivos de input...")
        if not CAMINHO_EXCEL.exists():
            status_final = "SEM DADOS PARA PROCESSAR"
            logger.warning(f"Planilha não encontrada: {CAMINHO_EXCEL}")
            return
            
        tratar_excel_corrompido(logger, CAMINHO_EXCEL)
        
        df_excel = pd.read_excel(CAMINHO_EXCEL, dtype=str)
        if df_excel.empty:
            status_final = "SEM DADOS PARA PROCESSAR"
            logger.warning("Planilha vazia.")
            return

        serie = df_excel.iloc[:, 0].astype(str)
        limpos = [v.strip() for v in serie.tolist() if isinstance(v, str) and v.strip()]
        df_controles = pd.DataFrame({"controle_ccs": limpos})
        
        if df_controles.empty:
            status_final = "SEM DADOS PARA PROCESSAR"
            logger.warning("Nenhum controle válido encontrado.")
            return

        registros = []
        for c in df_controles["controle_ccs"]:
            xml_name = f"{c}.xml"
            xml_full_path = PASTA_XMLS / xml_name
            status_xml = "OK" if xml_full_path.exists() else "XML NAO ENCONTRADO"
            registros.append((c, xml_name, str(xml_full_path), status_xml))
        
        df_mapa = pd.DataFrame(registros, columns=["controle_ccs", "xml_nome", "xml_path", "status_inicial"])
        
        logger.info(f"Total de registros a processar: {len(df_mapa)}")
        
        automacao = MateraAutomacao(logger)
        df_resultados, enviados_count = automacao.processar_arquivos(df_mapa)
        
        resultado_final_path = LOG_DIR / f"RESULTADOS_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        df_resultados.to_excel(resultado_final_path, index=False)
        arquivos_gerados.append(resultado_final_path)
        
        # Backup Input
        try: shutil.copy2(CAMINHO_EXCEL, LOG_DIR / f"INPUT_{CAMINHO_EXCEL.name}")
        except: pass
        
        if enviados_count > 0:
            status_final = "SUCESSO"
        elif "XML NAO ENCONTRADO" in df_resultados["status_final"].values:
             status_final = "SEM DADOS PARA PROCESSAR"
        else:
            status_final = "FALHA"

    except Exception as e:
        status_final = "FALHA"
        logger.error(f"Erro fatal na execução: {traceback.format_exc()}")
        
    finally:
        tempo_total = time.time() - tempo_inicio
        tempo_exec_str = time.strftime('%H:%M:%S', time.gmtime(tempo_total))
        
        zip_path = zipar_logs(logger, log_path, arquivos_gerados)
        anexos = [zip_path] if zip_path and Path(zip_path).exists() else []
        
        destinatarios_finais = dest_principais
        if status_final == "SUCESSO":
            destinatarios_finais += dest_cc
            
            
        # Enviar Email Local
        try:
            pythoncom.CoInitialize()
            outlook = Dispatch("Outlook.Application")
            mail = outlook.CreateItem(0)
            mail.To = ";".join(destinatarios_finais)
            mail.Subject = f"{NOME_AUTOMACAO} - {NOME_SCRIPT} - {status_final}"
            mail.Body = f"Execução finalizada.\nStatus: {status_final}\nTempo: {tempo_exec_str}\nLog em anexo."
            
            if anexos:
                for a in anexos:
                    if Path(a).exists():
                         mail.Attachments.Add(str(a))
            
            mail.Send()
            logger.info("Email enviado via Outlook Local.")
        except Exception as e:
            logger.error(f"Erro ao enviar email local: {e}")
            
        # Upload Métricas BQ
        try:
            df_metric = pd.DataFrame([{
                "nome_automacao": NOME_AUTOMACAO,
                "metodo_automacao": NOME_SCRIPT,
                "status": status_final,
                "tempo_exec": tempo_exec_str,
                "data_exec": datetime.now().strftime("%Y-%m-%d"),
                "hora_exec": datetime.now().strftime("%H:%M:%S"),
                "usuario": usuario_exec,
                "log_path": str(log_path)
            }])
            pandas_gbq.to_gbq(
                df_metric,
                "datalab-pagamentos.ADMINISTRACAO_CELULA_PYTHON.automacoes_exec",
                project_id=PROJECT_ID,
                if_exists="append"
            )
            logger.info("Métricas enviadas ao BigQuery.")
        except Exception as e:
            logger.error(f"Erro ao subir métricas BQ: {e}")

if __name__ == "__main__":
    main()