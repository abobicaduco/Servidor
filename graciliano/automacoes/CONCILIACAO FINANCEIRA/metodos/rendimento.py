# ==============================================================================
# AUTO-INSTALL DEPENDENCIES & CONFIG
# ==============================================================================
import sys
import os
import time
import json
import shutil
import getpass
import logging
import zipfile
import traceback
import pythoncom
import pandas as pd
import pandas_gbq
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor
from google.cloud import bigquery
from win32com.client import Dispatch

# Adiciona diretório raiz ao path (Híbrido: Relativo ou Padrão C6)
CONFIG_LOADER_PATH = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "novo_servidor"
project_root = None

# 1. Tenta achar config_loader.py
if (CONFIG_LOADER_PATH / "config_loader.py").exists():
    project_root = CONFIG_LOADER_PATH
else:
    # 2. Se não achou, tenta subir níveis (Fallback relativo)
    current_dir = Path(__file__).resolve().parent
    for parent in [current_dir] + list(current_dir.parents)[:5]:
        if (parent / "config_loader.py").exists():
            project_root = parent
            break

if not project_root:
    # 3. Fallback Hardcoded Padrão
    project_root = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "automacoes" / ""

if project_root and str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

try:
    import bootstrap_deps
    
    # ==============================================================================
    # DEPENDÊNCIAS ESPECÍFICAS
    # ==============================================================================
    SCRIPT_DEPS = [
        "pandas",
        "pandas-gbq",
        "google-cloud-bigquery",
        "pywin32",
        "openpyxl"
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
    
    class Config:
        TABLE_CONFIG = f"{PROJECT_ID}.{DATASET_ID}.registro_automacoes"
        TABLE_EXEC = f"{PROJECT_ID}.{DATASET_ID}.automacoes_exec"
        COMPANY_DOMAIN = "c6bank.com"

# ==============================================================================
# CONSTANTES DO SCRIPT
# ==============================================================================
SCRIPT_NAME = Path(__file__).stem.upper()
SCRIPT_NAME_LOWER = SCRIPT_NAME.lower()
START_TIME = datetime.now().replace(microsecond=0)
TZ = ZoneInfo("America/Sao_Paulo")

# Controle de Headless
HEADLESS = False

# Diretórios Temp
TEMP_DIR = Path(os.getenv('TEMP', Path.home())) / "C6_RPA_EXEC" / SCRIPT_NAME
TEMP_DIR.mkdir(parents=True, exist_ok=True)

# Configuração de Logger
LOG_FILE = TEMP_DIR / f"{SCRIPT_NAME}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding='utf-8')
    ]
)
LOGGER = logging.getLogger(SCRIPT_NAME)

# Variáveis Globais de Negócio
NOME_AUTOMACAO = "CONCILIACAO FINANCEIRA"
GLOBAL_CONFIG = {
    'area_name': NOME_AUTOMACAO, 
    'emails_principal': [], 
    'emails_cc': [], 
    'move_file': False
}

# Caminhos Originais (Preservados)
BASE_PATH = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "automacoes" / NOME_AUTOMACAO
PASTA_INPUT = BASE_PATH / "arquivos input" / SCRIPT_NAME_LOWER
PASTA_REDE_LOGS = BASE_PATH / "logs" / SCRIPT_NAME / datetime.now(TZ).strftime("%Y-%m-%d")

# Garante pastas
for p in [PASTA_INPUT, PASTA_REDE_LOGS]:
    p.mkdir(parents=True, exist_ok=True)

# ==============================================================================
# 1. FUNÇÕES DE SUPORTE (CONFIG & LOGGING)
# ==============================================================================
def load_bq_configs():
    """Carrega configs do BQ (registro_automacoes)"""
    try:
        query = f"""
            SELECT emails_principal, emails_cc, move_file 
            FROM `{Config.TABLE_CONFIG}`
            WHERE lower(TRIM(script_name)) = lower('{SCRIPT_NAME_LOWER}')
            LIMIT 1
        """
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, use_bqstorage_api=False)
        
        if not df.empty:
            def clean_emails(raw):
                if not raw or str(raw).lower() == 'nan': return []
                return [x.strip() for x in str(raw).replace(';', ',').split(',') if '@' in x]

            GLOBAL_CONFIG['emails_principal'] = clean_emails(df.iloc[0]['emails_principal'])
            GLOBAL_CONFIG['emails_cc'] = clean_emails(df.iloc[0]['emails_cc'])
            mf = df.iloc[0].get('move_file', False)
            GLOBAL_CONFIG['move_file'] = str(mf).lower() == 'true' if isinstance(mf, str) else bool(mf)
            LOGGER.info(f"Configs carregadas. Move File: {GLOBAL_CONFIG['move_file']}")
        else:
            LOGGER.warning("Configs não encontradas no BQ. Usando padrão.")
    except Exception as e:
        LOGGER.error(f"Erro ao carregar configs: {e}")

def upload_execution_log(status, stats_msg="", error_msg=""):
    """Sobe log de execução para o BigQuery (automacoes_exec)"""
    try:
        end_time = datetime.now().replace(microsecond=0)
        duration = round((end_time - START_TIME).total_seconds(), 2)
        modo_exec = os.environ.get("ENV_EXEC_MODE", "AGENDAMENTO")
        usuario = os.environ.get("ENV_EXEC_USER", f"{getpass.getuser().lower()}@{Config.COMPANY_DOMAIN}")

        df_log = pd.DataFrame([{
            "script_name": SCRIPT_NAME,
            "area_name": GLOBAL_CONFIG['area_name'],
            "start_time": START_TIME,
            "end_time": end_time,
            "duration_seconds": duration,
            "status": status,
            "usuario": usuario,
            "modo_exec": modo_exec
        }])
        
        schema = [
            {"name": "script_name", "type": "STRING"},
            {"name": "area_name", "type": "STRING"},
            {"name": "start_time", "type": "TIMESTAMP"},
            {"name": "end_time", "type": "TIMESTAMP"},
            {"name": "duration_seconds", "type": "FLOAT"},
            {"name": "status", "type": "STRING"},
            {"name": "usuario", "type": "STRING"},
            {"name": "modo_exec", "type": "STRING"}
        ]

        pandas_gbq.to_gbq(
            df_log,
            Config.TABLE_EXEC,
            project_id=PROJECT_ID,
            if_exists='append',
            table_schema=schema
            # use_bqstorage_api removido para compatibilidade em insert
        )
        LOGGER.info(f"Log de execução subido. Status: {status}")
    except Exception as e:
        LOGGER.error(f"FALHA CRÍTICA ao subir logs no BQ: {e}")

# ==============================================================================
# 2. FUNÇÕES DO NEGÓCIO (LEGADO REFATORADO)
# ==============================================================================
def regravar_excel_com(caminho: Path) -> Path:
    """Regrava Excel usando COM para corrigir corrupção/formatação."""
    pythoncom.CoInitialize()
    LOGGER.info(f"Regravando Excel via COM: {caminho.name}")
    xl = None
    try:
        xl = Dispatch("Excel.Application")
        xl.DisplayAlerts = False
        xl.Visible = False
        wb = xl.Workbooks.Open(str(caminho))
        caminho_saida = TEMP_DIR / f"{caminho.stem}_regravado.xlsx"
        wb.SaveAs(str(caminho_saida), FileFormat=51)
        wb.Close(SaveChanges=False)
        LOGGER.info(f"Arquivo regravado com sucesso: {caminho_saida.name}")
        return caminho_saida
    except Exception as e:
        LOGGER.warning(f"FALHA ao regravar Excel: {e}. Usando original.")
        return caminho
    finally:
        if xl:
            try: xl.Quit()
            except: pass
        pythoncom.CoUninitialize()

def tratar_dataframe(arquivo: Path) -> pd.DataFrame:
    """Aplica as regras de negócio IMUTÁVEIS e garante TIPAGEM CORRETA."""
    LOGGER.info(f"Tratando dataframe: {arquivo.name}")
    
    df = pd.read_excel(arquivo, engine="openpyxl")
    
    # Limpeza Inicial
    df = df.drop(labels=[0, 1, 2], axis=0, errors="ignore")
    if "Unnamed: 0" not in df.columns:
        raise RuntimeError("Coluna 'Unnamed: 0' nao encontrada (Estrutura Invalida).")
        
    df["Unnamed: 0"] = pd.to_datetime(
        df["Unnamed: 0"],
        format="%d-%m-%Y %H:%M:%S",
        errors="coerce",
    )
    df["Unnamed: 0"] = df["Unnamed: 0"].dt.strftime("%Y-%m-%d")
    
    # Renomeação
    colmap = {
        "Unnamed: 0": "Data",
        "Unnamed: 2": "ID_Lancamento",
        "Unnamed: 3": "Credito",
        "Unnamed: 4": "Debito",
        "Extrato de Conta corrente": "Saldo",
    }
    for origem, destino in colmap.items():
        if origem in df.columns:
            df.rename(columns={origem: destino}, inplace=True)
            
    if "Unnamed: 1" in df.columns:
        df = df[df["Unnamed: 1"].notna()]
    
    # === TRATAMENTO DE STRINGS (LIMPEZA DE CARACTERES) ===
    # Realiza limpeza ANTES da conversão de tipo final
    if "ID_Lancamento" in df.columns:
        df["ID_Lancamento"] = (
            df["ID_Lancamento"]
            .astype(str)
            .str.replace("EST PAG TRIBUTOS ESTADUAIS - Estorno ", "", regex=False)
            .str.replace("ARRECAD. REND_ONLINE - ", "", regex=False)
        )
        
    if "Credito" in df.columns:
        df["Credito"] = (
            df["Credito"].astype(str).str.lstrip("R$").str.replace(",", "", regex=False)
        )
        
    if "Debito" in df.columns:
        temporario = (
            df["Debito"].astype(str).str.lstrip("-R$").str.replace(",", "", regex=False)
        )
        df["Debito"] = "-" + temporario
        
    if "Saldo" in df.columns:
        df["Saldo"] = (
            df["Saldo"].astype(str).str.lstrip("R$").str.replace(",", "", regex=False)
        )
        
    esperadas = ["Data", "Documento", "ID_Lancamento", "Credito", "Debito", "Saldo"]
    if "Documento" not in df.columns and "Unnamed: 1" in df.columns:
        df.rename(columns={"Unnamed: 1": "Documento"}, inplace=True)
        
    df = df[[col for col in esperadas if col in df.columns]]
    
    if len(df) == 0 or df.shape[1] < 4:
        raise RuntimeError("Estrutura inesperada apos tratamento (Colunas insuficientes).")
    
    # Remove primeira linha de cabeçalho residual se houver
    df = df.iloc[1:, :]
    
    # === TIPAGEM ESTRITA PARA O BIGQUERY ===
    # 1. Colunas de Texto (STRING)
    str_cols = ["Data", "Documento", "ID_Lancamento"]
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).replace({"nan": "", "NaT": "", "None": ""})
            
    # 2. Colunas Numéricas (FLOAT)
    float_cols = ["Credito", "Debito", "Saldo"]
    for col in float_cols:
        if col in df.columns:
            # Converte para numérico, erros viram NaN (que o BQ aceita como null)
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Filtro de Negócio (Data vigente)
    df["Data_norm"] = pd.to_datetime(df["Data"], errors="coerce").dt.date
    hoje = datetime.now(TZ).date()
    
    linhas_antes = len(df)
    df = df[df["Data_norm"] != hoje]
    linhas_depois = len(df)
    
    LOGGER.info(f"Linhas removidas (data hoje): {linhas_antes - linhas_depois}")
    LOGGER.info(f"Total linhas para processar: {len(df)}")
    LOGGER.info(f"Tipos finais: \n{df.dtypes}") # Debug
    return df

def subir_bq_deduplicado(client: bigquery.Client, df: pd.DataFrame, tabela_final: str) -> dict:
    """Sobe dados usando padrão Staging e Merge para deduplicação."""
    stats = {"processadas": len(df), "inseridas": 0, "ignoradas": 0, "deletadas": 0}
    
    if df.empty:
        return stats
        
    df_upload = df.drop(columns=["Data_norm"], errors="ignore").copy()
    tabela_staging = f"{tabela_final}_staging_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
    
    try:
        LOGGER.info(f"Subindo Staging: {tabela_staging}")
        
        # Schema Opcional para garantir consistência no Staging
        # Se omitido, o pandas-gbq infere (agora corretamente pois corrigimos o df)
        pandas_gbq.to_gbq(
            df_upload, 
            tabela_staging, 
            project_id=PROJECT_ID, 
            if_exists='replace'
            # Sem use_bqstorage_api
        )
        
        # Merge Deduplicado (TO_JSON_STRING funciona melhor se tipos baterem)
        sql_dedup = f"""
            INSERT INTO `{tabela_final}`
            SELECT * FROM `{tabela_staging}` S
            WHERE NOT EXISTS (
                SELECT 1 FROM `{tabela_final}` F
                WHERE S.Data = F.Data 
                AND S.ID_Lancamento = F.ID_Lancamento
                AND S.Credito = F.Credito
                AND S.Debito = F.Debito
            )
        """
        # Nota: Troquei TO_JSON_STRING por chaves compostas para performance/precisão em floats, 
        # mas se preferir JSON pode voltar. Chaves explícitas evitam problemas de formatação JSON em floats.
        
        LOGGER.info("Executando MERGE Deduplicado...")
        job = client.query(sql_dedup)
        job.result()
        
        stats["inseridas"] = job.num_dml_affected_rows or 0
        stats["ignoradas"] = stats["processadas"] - stats["inseridas"]
        
        # Limpeza do Dia Vigente na tabela final
        hoje_str = datetime.now(TZ).strftime("%Y-%m-%d")
        sql_delete = f"DELETE FROM `{tabela_final}` WHERE Data = '{hoje_str}'"
        
        LOGGER.info(f"Executando Limpeza do Dia Vigente: {hoje_str}")
        job_del = client.query(sql_delete)
        job_del.result()
        stats["deletadas"] = job_del.num_dml_affected_rows or 0
        
    except Exception as e:
        LOGGER.error(f"Erro no processo BQ: {e}")
        raise
    finally:
        try:
            client.query(f"DROP TABLE IF EXISTS `{tabela_staging}`")
        except: pass
        
    return stats

# ==============================================================================
# 3. GERENCIAMENTO DE ARQUIVOS (OUTLOOK, ZIP, MOVE)
# ==============================================================================
def buscar_arquivos_input():
    """Busca no disco ou Outlook."""
    arquivos = []
    
    # 1. Disco
    for p in PASTA_INPUT.glob("*"):
        if p.is_file() and p.suffix.lower() in [".xlsx", ".xls"] and p.stat().st_size > 0:
            arquivos.append(p)
            
    if arquivos:
        return arquivos
        
    # 2. Outlook (Fallback)
    LOGGER.info("Verificando Outlook...")
    try:
        pythoncom.CoInitialize()
        outlook = Dispatch("Outlook.Application")
        ns = outlook.GetNamespace("MAPI")
        
        def _scan_folder(folder):
            hoje_date = datetime.now(TZ).date()
            items = folder.Items
            items.Sort("[ReceivedTime]", True)
            for mail in items:
                try:
                    if mail.ReceivedTime.date() != hoje_date: 
                        break 
                    if "RENDIMENTO - SUBIDA DE BASE" in str(mail.Subject).upper():
                        for att in mail.Attachments:
                            if str(att.FileName).lower().endswith((".xls", ".xlsx")):
                                destino = PASTA_INPUT / att.FileName
                                att.SaveAsFile(str(destino))
                                return destino
                except: continue
            return None

        found = None
        try: found = _scan_folder(ns.GetDefaultFolder(6)) # Inbox
        except: pass
        
        if found:
            arquivos.append(found)
            
    except Exception as e:
        LOGGER.error(f"Erro ao escanear Outlook: {e}")
    finally:
        pythoncom.CoUninitialize()
        
    return arquivos

def create_smart_zip(arquivos_finais):
    """Gera ZIP com logs e arquivos de output (Limite 15MB)."""
    zip_name = f"{SCRIPT_NAME}_{datetime.now().strftime('%H%M%S')}.zip"
    zip_path = TEMP_DIR / zip_name
    max_size = 15 * 1024 * 1024
    
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        if LOG_FILE.exists():
            zf.write(LOG_FILE, arcname=LOG_FILE.name)
            
        current_size = zip_path.stat().st_size
        
        for arq in arquivos_finais:
            if not arq.exists(): continue
            
            file_size = arq.stat().st_size
            if (current_size + file_size) < max_size:
                zf.write(arq, arcname=arq.name)
                current_size += file_size
            else:
                LOGGER.warning(f"Arquivo {arq.name} ignorado no ZIP (Limite 15MB excedido).")
                zf.writestr(
                    f"AVISO_ARQUIVO_GRANDE_{arq.name}.txt",
                    "Arquivo ignorado no anexo pois excede o limite de 15MB. Consulte a pasta de rede."
                )
                
    return zip_path

def enviar_email_outlook(status, zip_path, corpo_extra=""):
    """Envia email via Win32."""
    try:
        pythoncom.CoInitialize()
        outlook = Dispatch("Outlook.Application")
        mail = outlook.CreateItem(0)
        
        destinatarios = GLOBAL_CONFIG['emails_principal']
        if status == 'SUCCESS':
            destinatarios += GLOBAL_CONFIG['emails_cc']
            
        if not destinatarios:
            LOGGER.warning("Sem destinatários configurados. Email não enviado.")
            return

        destinatarios_str = ";".join(destinatarios)
        mail.To = destinatarios_str
        mail.Subject = f"CÉLULA PYTHON MONITORAÇÃO - {SCRIPT_NAME} - [{status}]"
        mail.Body = f"{corpo_extra}\n\nLog execução em anexo."
        
        if zip_path and zip_path.exists():
            mail.Attachments.Add(str(zip_path))
            
        mail.Send()
        LOGGER.info(f"Email enviado para lista configurada (Status: {status})")
    except Exception as e:
        LOGGER.error(f"Erro ao enviar email: {e}")
    finally:
        pythoncom.CoUninitialize()

# ==============================================================================
# 4. MAIN
# ==============================================================================
def main():
    LOGGER.info("=== INICIANDO EXECUÇÃO ===")
    status_final = "SUCCESS"
    observacao_final = ""
    output_files = []
    
    load_bq_configs()
    bq_client = bigquery.Client(project=PROJECT_ID)
    
    try:
        arquivos = buscar_arquivos_input()
        
        if not arquivos:
            status_final = "NO_DATA"
            observacao_final = "Nenhum arquivo encontrado no Input ou Outlook."
            LOGGER.warning(observacao_final)
        else:
            stats_total = {"processadas": 0, "inseridas": 0, "ignoradas": 0, "deletadas": 0}
            
            for arq in arquivos:
                LOGGER.info(f"Processando arquivo: {arq.name}")
                
                arq_tratado = regravar_excel_com(arq)
                df = tratar_dataframe(arq_tratado)
                
                tabela_destino = f"{PROJECT_ID}.conciliacoes_monitoracao.rendimento"
                stats = subir_bq_deduplicado(bq_client, df, tabela_destino)
                
                for k in stats_total:
                    stats_total[k] += stats.get(k, 0)
                
                output_files.append(arq)
                if arq_tratado != arq:
                     output_files.append(arq_tratado)
                
            observacao_final = (
                f"Processadas: {stats_total['processadas']} | "
                f"Inseridas: {stats_total['inseridas']} | "
                f"Ignoradas: {stats_total['ignoradas']} | "
                f"Deletadas: {stats_total['deletadas']}"
            )
            LOGGER.info(f"Resumo: {observacao_final}")

    except Exception as e:
        status_final = "ERROR"
        observacao_final = f"Erro Fatal: {str(e)}"
        LOGGER.error(observacao_final, exc_info=True)
    
    finally:
        zip_evidencia = create_smart_zip(output_files)
        
        if GLOBAL_CONFIG['move_file'] and status_final == 'SUCCESS':
            for f in output_files:
                try:
                    dest = PASTA_REDE_LOGS / f.name
                    if dest.exists(): dest.unlink()
                    shutil.copy2(f, dest)
                    LOGGER.info(f"Copiado para rede: {dest}")
                except Exception as ex:
                    LOGGER.error(f"Erro ao mover arquivo para rede: {ex}")
            shutil.copy2(zip_evidencia, PASTA_REDE_LOGS / zip_evidencia.name)

        upload_execution_log(status_final, stats_msg=observacao_final)
        enviar_email_outlook(status_final, zip_evidencia, corpo_extra=observacao_final)
        LOGGER.info("=== EXECUÇÃO FINALIZADA ===")

if __name__ == "__main__":
    main()