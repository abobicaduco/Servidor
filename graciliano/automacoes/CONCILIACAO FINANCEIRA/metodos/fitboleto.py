# ==============================================================================
# AUTO-INSTALL DEPENDENCIES & CONFIG
# ==============================================================================
import sys
import os
import logging
import shutil
import time
import zipfile
import traceback
import getpass
from pathlib import Path
from datetime import datetime

# Adiciona diretório raiz ao path (Híbrido: Relativo ou Padrão C6)
current_dir = Path(__file__).resolve().parent
project_root = None

# 1. Tenta achar config_loader.py subindo os níveis
for parent in [current_dir] + list(current_dir.parents)[:5]:
    if (parent / "config_loader.py").exists():
        project_root = parent
        break

# 2. Se não achou relativo, aponta para o caminho padrão da rede
if not project_root:
    standard_root = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "novo_servidor"
    if standard_root.exists():
        project_root = standard_root

if project_root:
    sys.path.insert(0, str(project_root))

try:
    import bootstrap_deps
    
    # ==============================================================================
    # DEPENDÊNCIAS ESPECÍFICAS
    # ==============================================================================
    SCRIPT_DEPS = [
        "pandas",
        "pandas-gbq",
        "pywin32"
    ]
    
    bootstrap_deps.install_requirements(extra_libs=SCRIPT_DEPS)
    from config_loader import Config
    
    # Configuração via Loader (Dinâmica DEV/PROD)
    ROOT_DIR = Config.ROOT_DIR
    PROJECT_ID = Config.PROJECT_ID
    DATASET_ID = Config.DATASET_ID

except ImportError:
    print("⚠️ Config Loader não encontrado. Usando FALLBACK local.")
    
    # Fallback Hardcoded (Padrão C6 Bank - Assume DEV)
    ROOT_DIR = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "Servidor_CELULA_PYTHON"
    PROJECT_ID = 'datalab-pagamentos'
    DATASET_ID = 'ADMINISTRACAO_CELULA_PYTHON'

# Imports após bootstrap
import pandas as pd
import pandas_gbq
from win32com.client import Dispatch

# ==============================================================================
# CONSTANTES DO SCRIPT
# ==============================================================================
SCRIPT_NAME = Path(__file__).stem.upper() # FITBOLETO
AREA_NAME = "CONCILIACAO FINANCEIRA"
START_TIME = datetime.now().replace(microsecond=0)

# Controle de Headless
HEADLESS = False

# Diretórios Locais
TEMP_DIR = Path(os.getenv('TEMP', Path.home())) / "C6_RPA_EXEC" / SCRIPT_NAME
TEMP_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = TEMP_DIR / f"{SCRIPT_NAME}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

# Diretórios de Rede
BASE_PATH_REDE = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "automacoes" / AREA_NAME
PASTA_INPUT = BASE_PATH_REDE / "arquivos input" / SCRIPT_NAME
PASTA_LOGS_REDE = BASE_PATH_REDE / "logs" / SCRIPT_NAME / datetime.now().strftime("%Y-%m-%d")

# Garantir criação de pastas
PASTA_INPUT.mkdir(parents=True, exist_ok=True)

# Configs Globais
GLOBAL_CONFIG = {'area_name': AREA_NAME, 'emails_principal': [], 'emails_cc': [], 'move_file': False}

# Tabela de Negócio
TABELA_NEGOCIO = f"{PROJECT_ID}.conciliacoes_monitoracao.fitbank_2021-07"
ASSUNTO_BUSCA_OUTLOOK = "fit_boleto"

# ==============================================================================
# SETUP LOGGING
# ==============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding='utf-8')
    ]
)
logger = logging.getLogger(SCRIPT_NAME)

# ==============================================================================
# FUNÇÕES DE INFRAESTRUTURA
# ==============================================================================
def get_configs():
    """Carrega configs do BQ (registro_automacoes)"""
    try:
        query = f"""
            SELECT emails_principal, emails_cc, move_file 
            FROM `{PROJECT_ID}.{DATASET_ID}.registro_automacoes` 
            WHERE lower(TRIM(script_name)) = lower('{SCRIPT_NAME}')
            AND (is_active IS NULL OR lower(is_active) = 'true')
            ORDER BY created_at DESC LIMIT 1
        """
        logger.info("Carregando configurações do BigQuery...")
        # use_bqstorage_api=False é válido para READ
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, use_bqstorage_api=False)
        
        if not df.empty:
            def clean_emails(val):
                if not val or str(val).lower() == 'nan': return []
                return [x.strip() for x in str(val).replace(';', ',').split(',') if '@' in x]

            GLOBAL_CONFIG['emails_principal'] = clean_emails(df.iloc[0]['emails_principal'])
            GLOBAL_CONFIG['emails_cc'] = clean_emails(df.iloc[0]['emails_cc'])
            GLOBAL_CONFIG['move_file'] = bool(df.iloc[0]['move_file']) if 'move_file' in df.columns else False
            logger.info(f"Configs carregadas. Move File: {GLOBAL_CONFIG['move_file']}")
        else:
            logger.warning("Configs não encontradas no BQ. Usando padrão.")

    except Exception as e:
        logger.error(f"Erro ao carregar configs: {e}")

def smart_zip(output_files: list) -> Path:
    """Compacta logs e arquivos gerados respeitando limite de 15MB"""
    try:
        PASTA_LOGS_REDE.mkdir(parents=True, exist_ok=True)
        
        zip_name = f"{SCRIPT_NAME}_{datetime.now().strftime('%H%M%S')}.zip"
        zip_path = PASTA_LOGS_REDE / zip_name
        
        max_size = 15 * 1024 * 1024 # 15MB
        current_size = 0
        
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            # 1. Adiciona Log
            if LOG_FILE.exists():
                zf.write(LOG_FILE, LOG_FILE.name)
                current_size += LOG_FILE.stat().st_size
            
            # 2. Adiciona Outputs
            for file in output_files:
                if not file or not file.exists(): continue
                
                f_size = file.stat().st_size
                if (current_size + f_size) < max_size:
                    zf.write(file, file.name)
                    current_size += f_size
                else:
                    logger.warning(f"Arquivo {file.name} ignorado no ZIP (Excede 15MB).")
                    zf.writestr(f"AVISO_ARQUIVO_GRANDE_{file.name}.txt", 
                                "Arquivo ignorado no anexo pois excede o limite de 15MB do Outlook. Consulte a pasta de rede.")
        
        return zip_path
    except Exception as e:
        logger.error(f"Erro ao criar Smart Zip: {e}")
        return None

def log_execution_bq(status: str, exec_mode: str, user: str):
    """Sobe métricas para automacoes_exec"""
    try:
        end_time = datetime.now().replace(microsecond=0)
        duration = round((end_time - START_TIME).total_seconds(), 2)
        
        data = {
            'script_name': [SCRIPT_NAME],
            'area_name': [GLOBAL_CONFIG['area_name']],
            'start_time': [START_TIME],
            'end_time': [end_time],
            'duration_seconds': [duration],
            'status': [status],
            'usuario': [user],
            'modo_exec': [exec_mode]
        }
        
        df_log = pd.DataFrame(data)
        tabela_exec = f"{PROJECT_ID}.{DATASET_ID}.automacoes_exec"
        
        # CORREÇÃO: Removemos use_bqstorage_api=False do to_gbq
        pandas_gbq.to_gbq(df_log, tabela_exec, project_id=PROJECT_ID, if_exists='append')
        logger.info("Métricas de execução registradas com sucesso.")
        
    except Exception as e:
        logger.error(f"Erro ao salvar métricas no BQ: {e}")

def send_email(status: str, zip_path: Path, msg_body: str = ""):
    """Envia email via Outlook"""
    try:
        outlook = Dispatch('Outlook.Application')
        mail = outlook.CreateItem(0)
        
        destinatarios = GLOBAL_CONFIG['emails_principal']
        if status == 'SUCCESS':
            destinatarios += GLOBAL_CONFIG['emails_cc']
            
        if not destinatarios:
            logger.warning("Lista de emails vazia. Abortando envio.")
            return

        # Limpeza e Deduplicação
        lista_final = list(set([e.strip() for e in destinatarios if e.strip()]))
        mail.To = ";".join(lista_final)
        
        mail.Subject = f"CÉLULA PYTHON MONITORAÇÃO - {SCRIPT_NAME} - {status}"
        mail.Body = msg_body
        
        if zip_path and Path(zip_path).exists():
            mail.Attachments.Add(str(zip_path))
            
        mail.Send()
        logger.info(f"Email enviado para: {mail.To}")
        
    except Exception as e:
        logger.error(f"Erro ao enviar email: {e}")

# ==============================================================================
# LÓGICA DE NEGÓCIO
# ==============================================================================
def coletar_input_outlook():
    """Busca anexos no Outlook local"""
    arquivos_baixados = []
    try:
        import pythoncom
        pythoncom.CoInitialize()
        
        outlook = Dispatch("Outlook.Application")
        ns = outlook.GetNamespace("MAPI")
        inbox = ns.GetDefaultFolder(6) 
        
        hoje = datetime.now().date()
        itens = inbox.Items
        itens.Sort("[ReceivedTime]", True)
        
        logger.info(f"Procurando emails com assunto '{ASSUNTO_BUSCA_OUTLOOK}' de hoje ({hoje})...")
        
        for m in itens:
            try:
                if getattr(m, "Class", 0) != 43: continue
                rec_time = getattr(m, "ReceivedTime", None)
                if not rec_time: continue
                
                if rec_time.date() < hoje: break 
                
                if rec_time.date() == hoje and ASSUNTO_BUSCA_OUTLOOK.lower() in str(m.Subject).lower():
                    if m.Attachments.Count > 0:
                        for i in range(1, m.Attachments.Count + 1):
                            att = m.Attachments.Item(i)
                            if str(att.FileName).endswith((".xlsx", ".xls")):
                                destino = PASTA_INPUT / f"{Path(att.FileName).stem}_{datetime.now().strftime('%Y%m%d_%H%M%S')}{Path(att.FileName).suffix}"
                                att.SaveAsFile(str(destino))
                                arquivos_baixados.append(destino)
                                logger.info(f"Anexo salvo do Outlook: {destino.name}")
            except Exception:
                continue
                
        return arquivos_baixados
    except Exception as e:
        logger.error(f"Erro ao ler Outlook: {e}")
        return []

def tratar_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Iniciando tratamento de dados...")
    
    if "PayerTaxNumber" in df.columns:
        df2 = df[df["PayerTaxNumber"].notna()].copy()
    else:
        df2 = df.copy()

    cols_necessarias = [
        "BoletoOutId", "NotaFiscal", "ConcessionariaOuBanco", "ValorTotal",
        "DataDeCriacao", "Status", "UrlDoRecibo", "CodigoDeBarras"
    ]
    
    cols_existentes = [c for c in cols_necessarias if c in df2.columns]
    df2 = df2[cols_existentes]

    df2.insert(2, "Tipo", "")
    df2.insert(4, "Convenio", "")
    df2.insert(10, "Tag", "")
    
    dt_coleta = ""
    if "DataDeCriacao" in df.columns and not df.empty:
        dt_coleta = str(df["DataDeCriacao"].iloc[0])[:10]
    
    df2.insert(11, "dt_coleta", dt_coleta)
    
    novas_colunas = [
        "DocumentNumber", "NumeroDocumento", "Tipo", "ConcessionariaOuBanco",
        "Convenio", "ValorTotal", "DataDeCriacao", "Status", "Url",
        "CodigoDeBarras", "Tag", "dt_coleta"
    ]
    
    if len(df2.columns) == len(novas_colunas):
        df2.columns = novas_colunas
    else:
        logger.warning(f"Numero de colunas diferente do esperado. Ajustando mapeamento seguro.")
    
    if "ValorTotal" in df2.columns:
        df2["ValorTotal"] = pd.to_numeric(df2["ValorTotal"], errors="coerce")
        
    if "NumeroDocumento" in df2.columns:
        df2["NumeroDocumento"] = df2["NumeroDocumento"].apply(
            lambda x: "" if pd.isna(x) else str(int(float(x))) if str(x).replace(".", "", 1).isdigit() else str(x)
        )
        
    for c in df2.columns:
        if c != "NumeroDocumento":
            df2[c] = df2[c].astype(str)
            
    return df2

def dedup_e_carga(df: pd.DataFrame) -> tuple[int, int]:
    if df.empty or "DataDeCriacao" not in df.columns:
        return 0, 0

    datas_input = list(df["DataDeCriacao"].unique())
    if not datas_input:
        return 0, 0
        
    datas_formatadas = "', '".join([str(x) for x in datas_input])
    sql = f"""
        SELECT DISTINCT CAST(DataDeCriacao AS STRING) as data
        FROM `{TABELA_NEGOCIO}`
        WHERE CAST(DataDeCriacao AS STRING) IN ('{datas_formatadas}')
    """
    
    try:
        df_existentes = pandas_gbq.read_gbq(sql, project_id=PROJECT_ID, use_bqstorage_api=False)
        datas_existentes = set(df_existentes['data'].astype(str).tolist()) if not df_existentes.empty else set()
        
        df_final = df[~df["DataDeCriacao"].astype(str).isin(datas_existentes)].copy()
        duplicadas = len(df) - len(df_final)
        
        linhas_subidas = 0
        if not df_final.empty:
            logger.info(f"Subindo {len(df_final)} linhas para {TABELA_NEGOCIO}...")
            # CORREÇÃO: Removemos use_bqstorage_api=False do to_gbq
            pandas_gbq.to_gbq(
                df_final,
                TABELA_NEGOCIO,
                project_id=PROJECT_ID,
                if_exists="append"
            )
            linhas_subidas = len(df_final)
            logger.info("Carga BigQuery realizada com SUCESSO.")
        else:
            logger.info("Todos os dados já existem no BigQuery (Duplicados).")

        return linhas_subidas, duplicadas
        
    except Exception as e:
        logger.error(f"Erro na carga/dedup: {e}")
        raise e

# ==============================================================================
# MAIN FLOW
# ==============================================================================
def main():
    exec_mode = os.environ.get('ENV_EXEC_MODE', 'AGENDAMENTO')
    exec_user = os.environ.get('ENV_EXEC_USER', f"{getpass.getuser()}@c6bank.com")
    
    status = "ERROR"
    output_files = []
    
    total_subidas = 0
    total_duplicadas = 0
    
    logger.info(f"Iniciando {SCRIPT_NAME} | Mode: {exec_mode} | User: {exec_user}")
    
    try:
        get_configs()
        
        arquivos = sorted(list(PASTA_INPUT.glob("*.xlsx")))
        if not arquivos:
            logger.info("Nenhum arquivo local. Tentando Outlook...")
            arquivos = coletar_input_outlook()
            
        if not arquivos:
            status = "NO_DATA"
            logger.warning("Nenhum arquivo encontrado para processamento.")
        else:
            for arq in arquivos:
                logger.info(f"Processando arquivo: {arq.name}")
                try:
                    df_raw = pd.read_excel(arq)
                    if df_raw.empty:
                        logger.warning(f"Arquivo vazio: {arq.name}")
                        continue
                        
                    df_tratado = tratar_dataframe(df_raw)
                    subidas, dups = dedup_e_carga(df_tratado)
                    
                    total_subidas += subidas
                    total_duplicadas += dups
                    
                    output_files.append(arq)
                    
                except Exception as e_file:
                    logger.error(f"Erro ao processar {arq.name}: {e_file}")
            
            if total_subidas > 0:
                status = "SUCCESS"
            elif total_duplicadas > 0 and total_subidas == 0:
                status = "SUCCESS" 
                logger.info("Execução finalizada. Apenas dados duplicados encontrados.")
            elif status != "NO_DATA":
                status = "SUCCESS"

            if status == "SUCCESS" and GLOBAL_CONFIG['move_file']:
                logger.info("Movendo arquivos processados para a rede...")
                PASTA_LOGS_REDE.mkdir(parents=True, exist_ok=True)
                for arq in output_files:
                    try:
                        destino = PASTA_LOGS_REDE / f"PROCESSED_{arq.name}"
                        if arq.exists():
                            shutil.move(str(arq), str(destino))
                            logger.info(f"Movido: {arq.name}")
                    except Exception as e_mv:
                        logger.error(f"Erro ao mover {arq.name}: {e_mv}")

    except Exception as e:
        status = "ERROR"
        logger.error(f"Erro fatal na execução: {traceback.format_exc()}")
    
    finally:
        logger.info("Gerando evidências...")
        zip_path = smart_zip(output_files)
        
        log_execution_bq(status, exec_mode, exec_user)
        
        msg_body = f"""
        Execução: {SCRIPT_NAME}
        Status: {status}
        Linhas Subidas: {total_subidas}
        Linhas Duplicadas: {total_duplicadas}
        Arquivos Processados: {len(output_files)}
        """
        
        send_email(status, zip_path, msg_body)
        
        try:
            shutil.rmtree(TEMP_DIR, ignore_errors=True)
        except:
            pass
            
        logger.info(f"Fim. Status: {status}")

if __name__ == "__main__":
    main()