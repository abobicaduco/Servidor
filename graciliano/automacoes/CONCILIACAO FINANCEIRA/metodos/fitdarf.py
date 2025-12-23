# ==============================================================================
# AUTO-INSTALL DEPENDENCIES & CONFIG
# ==============================================================================
import sys
import os
import shutil
import time
import logging
import zipfile
import traceback
import unicodedata
from pathlib import Path
from datetime import datetime
from tempfile import NamedTemporaryFile
from concurrent.futures import ThreadPoolExecutor

# Adiciona diretório raiz ao path (Híbrido: Relativo ou Padrão C6)
CONFIG_LOADER = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "novo_servidor" / "config_loader.py"
project_root = None

# 2. Se não achou relativo, aponta para o caminho padrão da rede
if not project_root:
    standard_root = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "automacoes"
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
        "google-cloud-bigquery",
        "pywin32",
        "pytz",
        "openpyxl"
    ]
    
    bootstrap_deps.install_requirements(extra_libs=SCRIPT_DEPS)
    from config_loader import Config
    
    # Configuração via Loader (Dinâmica DEV/PROD)
    ROOT_DIR = Config.ROOT_DIR
    PROJECT_ID = Config.PROJECT_ID
    DATASET_ID = Config.DATASET_ID

except ImportError:
    # Fallback Hardcoded (Padrão C6 Bank - Assume DEV/Local se loader falhar)
    ROOT_DIR = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "automacoes"
    PROJECT_ID = 'datalab-pagamentos'
    DATASET_ID = 'ADMINISTRACAO_CELULA_PYTHON'

import pandas as pd
import pandas_gbq
from google.cloud import bigquery
import pythoncom
from win32com.client import Dispatch

# ==============================================================================
# CONSTANTES DO SCRIPT
# ==============================================================================
SCRIPT_NAME = "CONCILIACAO FINANCEIRA" # Mantido nome original fixo para compatibilidade
SCRIPT_FILE_NAME = Path(__file__).stem.upper()
START_TIME = datetime.now().replace(microsecond=0)
AREA_NAME = "CONCILIACAO FINANCEIRA" # Baseado no caminho original

# Diretórios
TEMP_DIR = Path(os.getenv('TEMP', Path.home())) / "C6_RPA_EXEC" / SCRIPT_FILE_NAME
TEMP_DIR.mkdir(parents=True, exist_ok=True)

# CORREÇÃO DE CAMINHO: Ajustado para "fitdarf" (sem underscore) conforme evidência
INPUT_DIR = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "automacoes" / "CONCILIACAO FINANCEIRA" / "arquivos input" / "fitdarf"
INPUT_DIR.mkdir(parents=True, exist_ok=True)

# Configs Globais
GLOBAL_CONFIG = {
    'area_name': AREA_NAME, 
    'emails_principal': [], 
    'emails_cc': [], 
    'move_file': False
}

# Logger Setup
LOG_FILE = TEMP_DIR / f"{SCRIPT_FILE_NAME}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
LOGGER = logging.getLogger(SCRIPT_NAME)

# BigQuery Settings
TABELA_ALVO = "conciliacoes_monitoracao.PROC_FITBANK_DARFS"
ASSUNTO_BUSCA = "fit_darf"

# ==============================================================================
# CLASSES DE SERVIÇO (REFATORADAS)
# ==============================================================================

class ConfigManager:
    @staticmethod
    def carregar_configs():
        """Carrega configs do BQ (registro_automacoes)"""
        try:
            LOGGER.info("Carregando configurações do BigQuery...")
            # Tenta buscar pelo nome do arquivo do script ou pelo nome da automação
            query = f"""
                SELECT emails_principal, emails_cc, move_file 
                FROM `{PROJECT_ID}.{DATASET_ID}.registro_automacoes`
                WHERE (upper(TRIM(script_name)) = upper('{SCRIPT_FILE_NAME}') OR upper(TRIM(script_name)) = upper('{SCRIPT_NAME}'))
                AND (is_active IS NULL OR lower(is_active) = 'true')
                ORDER BY created_at DESC LIMIT 1
            """
            # Fallback credentials logic handled by pandas_gbq usually, or ambient
            df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID)
            
            if not df.empty:
                GLOBAL_CONFIG['emails_principal'] = [x.strip() for x in str(df.iloc[0]['emails_principal']).replace(';', ',').split(',') if '@' in x]
                GLOBAL_CONFIG['emails_cc'] = [x.strip() for x in str(df.iloc[0]['emails_cc']).replace(';', ',').split(',') if '@' in x]
                
                # Conversão segura de booleano
                move_val = str(df.iloc[0].get('move_file', '')).lower()
                GLOBAL_CONFIG['move_file'] = move_val in ('true', '1', 's', 'sim', 'yes')
                
                LOGGER.info(f"Configs carregadas: Move={GLOBAL_CONFIG['move_file']}, Dest={len(GLOBAL_CONFIG['emails_principal'])} principais")
            else:
                LOGGER.warning("Configs não encontradas no BQ. Usando valores padrão/fallback.")
                # Fallback conforme código original
                GLOBAL_CONFIG['emails_principal'] = ["carlos.lsilva@c6bank.com", "sofia.fernandes@c6bank.com"]
                
        except Exception as e:
            LOGGER.error(f"Erro ao carregar configs: {e}")
            GLOBAL_CONFIG['emails_principal'] = ["carlos.lsilva@c6bank.com", "sofia.fernandes@c6bank.com"]

class ArquivosService:
    def _garantir_outlook(self):
        try:
            pythoncom.CoInitialize()
            Dispatch("Outlook.Application")
            return True
        except Exception:
            return False

    def procurar(self):
        """Busca arquivos no diretório local e no Outlook"""
        coletados = []
        
        # 1. Busca Local
        try:
            LOGGER.info(f"Buscando arquivos em: {INPUT_DIR}")
            for ext in ("*.xlsx", "*.xls", "*.csv"):
                encontrados = list(INPUT_DIR.glob(ext))
                coletados.extend(encontrados)
            LOGGER.info(f"Arquivos locais encontrados: {len(coletados)}")
        except Exception as e:
            LOGGER.error(f"Erro ao buscar arquivos locais: {e}")

        # 2. Busca Outlook
        if self._garantir_outlook():
            try:
                outlook = Dispatch("Outlook.Application")
                ns = outlook.GetNamespace("MAPI")
                hoje_str = datetime.now().strftime("%m/%d/%Y")
                
                # Pastas para verificar (Inbox Padrão + Pasta Específica Célula Python)
                folders_to_check = []
                
                # Inbox Padrão
                folders_to_check.append(ns.GetDefaultFolder(6))
                
                # Tenta achar pasta específica da Célula
                try:
                    for i in range(1, ns.Folders.Count + 1):
                        pasta = ns.Folders.Item(i)
                        if "Celula Python Monitoracao" in getattr(pasta, "Name", ""):
                            folders_to_check.append(pasta.Folders["Inbox"])
                            break
                except Exception:
                    pass

                count_outlook = 0
                for folder in folders_to_check:
                    try:
                        items = folder.Items.Restrict(f"[ReceivedTime] >= '{hoje_str} 00:00'")
                        items.Sort("[ReceivedTime]", True)
                        
                        for msg in items:
                            try:
                                subj = str(msg.Subject or "")
                                if ASSUNTO_BUSCA in subj and msg.Attachments.Count > 0:
                                    for j in range(1, msg.Attachments.Count + 1):
                                        att = msg.Attachments.Item(j)
                                        fn = (att.FileName or "").lower()
                                        if fn.endswith((".xls", ".xlsx", ".csv")):
                                            temp_file = TEMP_DIR / att.FileName
                                            att.SaveAsFile(str(temp_file))
                                            coletados.append(temp_file)
                                            count_outlook += 1
                            except Exception:
                                continue
                    except Exception:
                        continue
                
                LOGGER.info(f"Arquivos baixados do Outlook: {count_outlook}")
            except Exception as e:
                LOGGER.error(f"Erro na busca do Outlook: {e}")
            finally:
                pythoncom.CoUninitialize()
        
        return sorted(list(set(coletados)), key=lambda x: x.stat().st_mtime)

class TransformacaoService:
    def tratar_dataframe(self, arq: Path) -> pd.DataFrame:
        LOGGER.info(f"Processando arquivo: {arq.name}")
        try:
            if arq.suffix.lower() == ".csv":
                df = pd.read_csv(arq, sep=";", dtype=str, encoding="utf-8", engine="python")
            else:
                df = pd.read_excel(arq, engine="openpyxl")
            
            cols = ["Identifier","ContributorTaxNumber","TotalValue","PaymentDate","DocumentNumber","StatusPagamento","CreationDate","UrlDoAgendamento"]
            
            # Validação simples de colunas
            missing = [c for c in cols if c not in df.columns]
            if missing:
                LOGGER.warning(f"Colunas ausentes em {arq.name}: {missing}")
                # Tenta processar mesmo assim se tiver CreationDate
            
            # Filtra apenas colunas que existem
            cols_existentes = [c for c in cols if c in df.columns]
            df2 = df[cols_existentes].astype(str, errors="ignore")
            
            return df2
        except Exception as e:
            LOGGER.error(f"Erro ao tratar dataframe {arq.name}: {e}")
            return pd.DataFrame()

class BigQueryService:
    def obter_datas_existentes(self) -> set:
        try:
            tabela_full = f"{PROJECT_ID}.{TABELA_ALVO}"
            sql = f"SELECT DISTINCT CAST(CreationDate AS STRING) AS CreationDate FROM `{tabela_full}`"
            df = pandas_gbq.read_gbq(sql, project_id=PROJECT_ID)
            return set(df["CreationDate"].dropna().astype(str).tolist())
        except Exception as e:
            LOGGER.warning(f"Não foi possível obter datas existentes (pode ser primeira carga ou erro): {e}")
            return set()

    def subir(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        
        tabela_full = f"{PROJECT_ID}.{TABELA_ALVO}"
        try:
            # CORREÇÃO: Removido 'use_bqstorage_api' para evitar erro de versão
            pandas_gbq.to_gbq(
                df, 
                tabela_full, 
                project_id=PROJECT_ID, 
                if_exists='append'
            )
            return len(df)
        except Exception as e:
            LOGGER.error(f"Erro ao subir para BQ: {e}")
            raise e

class AutomationUtils:
    @staticmethod
    def get_env_info():
        usuario = os.environ.get("ENV_EXEC_USER", f"{os.getlogin().lower()}@c6bank.com")
        modo = os.environ.get("ENV_EXEC_MODE", "AGENDAMENTO") # Default seguro
        return usuario, modo

    @staticmethod
    def smart_zip(output_files, log_path):
        """Compacta logs e evidências respeitando limite de 15MB"""
        try:
            zip_name = f"{SCRIPT_FILE_NAME}_{datetime.now().strftime('%H%M%S')}.zip"
            # Salva ZIP na pasta de logs da rede
            rede_log_dir = ROOT_DIR / GLOBAL_CONFIG['area_name'] / "logs" / SCRIPT_FILE_NAME / datetime.now().strftime('%Y-%m-%d')
            rede_log_dir.mkdir(parents=True, exist_ok=True)
            zip_path = rede_log_dir / zip_name
            
            limit_bytes = 15 * 1024 * 1024 # 15MB
            current_size = 0
            
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
                # 1. Adiciona Log (Prioridade)
                if log_path.exists():
                    zf.write(log_path, arcname=log_path.name)
                    current_size += log_path.stat().st_size
                
                # 2. Adiciona Arquivos de Output
                for file in output_files:
                    if not file.exists(): continue
                    
                    f_size = file.stat().st_size
                    if (current_size + f_size) < limit_bytes:
                        zf.write(file, arcname=file.name)
                        current_size += f_size
                    else:
                        # Aviso de arquivo grande
                        warning_content = f"O arquivo {file.name} ({f_size/1024/1024:.2f}MB) não foi anexado pois excederia o limite de 15MB."
                        zf.writestr(f"AVISO_ARQUIVO_GRANDE_{file.name}.txt", warning_content)
            
            return zip_path
        except Exception as e:
            LOGGER.error(f"Erro ao gerar Smart Zip: {e}")
            return None

    @staticmethod
    def send_email(status, duration_seconds, lines_processed, lines_uploaded, zip_path):
        """Envia email via Outlook"""
        if not GLOBAL_CONFIG['emails_principal']:
            LOGGER.warning("Sem destinatários configurados. Email não enviado.")
            return

        # Lógica de Destinatários
        recipients = list(GLOBAL_CONFIG['emails_principal'])
        if status == 'SUCCESS' and GLOBAL_CONFIG['emails_cc']:
            recipients_cc = list(GLOBAL_CONFIG['emails_cc'])
        else:
            recipients_cc = [] # Se Erro ou No Data, apenas principal

        try:
            pythoncom.CoInitialize()
            outlook = Dispatch("Outlook.Application")
            mail = outlook.CreateItem(0)
            
            mail.To = ";".join(recipients)
            mail.CC = ";".join(recipients_cc)
            mail.Subject = f"CÉLULA PYTHON MONITORAÇÃO - {SCRIPT_FILE_NAME} - [{status}]"
            mail.Body = "" # Corpo vazio conforme regra
            
            # Anexo
            if zip_path and zip_path.exists():
                mail.Attachments.Add(str(zip_path))
            
            mail.Send()
            LOGGER.info(f"Email enviado para: {recipients} (CC: {recipients_cc}) - Status: {status}")
        except Exception as e:
            LOGGER.error(f"Erro ao enviar email: {e}")
        finally:
            pythoncom.CoUninitialize()

    @staticmethod
    def publish_metrics(start_time, end_time, status, duration, usuario, modo):
        """Publica métricas na tabela automacoes_exec (Schema Rígido)"""
        try:
            row = {
                'script_name': SCRIPT_FILE_NAME,
                'area_name': GLOBAL_CONFIG['area_name'],
                'start_time': start_time,
                'end_time': end_time,
                'duration_seconds': float(duration),
                'status': status,
                'usuario': usuario,
                'modo_exec': modo
            }
            df_metrics = pd.DataFrame([row])
            
            # CORREÇÃO: Removido 'use_bqstorage_api'
            pandas_gbq.to_gbq(
                df_metrics,
                f"{PROJECT_ID}.{DATASET_ID}.automacoes_exec",
                project_id=PROJECT_ID,
                if_exists='append'
            )
            LOGGER.info("Métricas publicadas com sucesso.")
        except Exception as e:
            LOGGER.error(f"Erro ao publicar métricas: {e}")

    @staticmethod
    def cleanup_files(output_files, status):
        """Move arquivos se sucesso e configurado, limpa temp"""
        rede_dir = ROOT_DIR / GLOBAL_CONFIG['area_name'] / "arquivos_processados" / SCRIPT_FILE_NAME / datetime.now().strftime('%Y-%m-%d')
        
        if GLOBAL_CONFIG['move_file'] and status == 'SUCCESS':
            rede_dir.mkdir(parents=True, exist_ok=True)
            for file in output_files:
                try:
                    # Move para rede
                    dest = rede_dir / file.name
                    shutil.move(str(file), str(dest))
                    LOGGER.info(f"Arquivo movido para rede: {dest}")
                except Exception as e:
                    LOGGER.error(f"Erro ao mover arquivo {file.name}: {e}")
        else:
            LOGGER.info("Cleanup: Arquivos mantidos na origem/temp (move_file=False ou Status!=SUCCESS).")

# ==============================================================================
# ORQUESTRADOR PRINCIPAL
# ==============================================================================
def main():
    LOGGER.info("=== INICIANDO EXECUÇÃO ===")
    
    # 1. Configurações e Ambiente
    ConfigManager.carregar_configs()
    usuario, modo_exec = AutomationUtils.get_env_info()
    
    status_exec = "ERROR" # Default
    linhas_lidas = 0
    linhas_subidas = 0
    arquivos_tocados = []
    
    bq_service = BigQueryService()
    arq_service = ArquivosService()
    trf_service = TransformacaoService()
    
    try:
        # 2. Busca Arquivos
        arquivos = arq_service.procurar()
        
        if not arquivos:
            status_exec = "NO_DATA"
            LOGGER.info("Nenhum arquivo encontrado para processamento.")
        else:
            arquivos_tocados.extend(arquivos)
            datas_existentes = bq_service.obter_datas_existentes()
            LOGGER.info(f"Datas já existentes no BQ: {len(datas_existentes)}")
            
            df_final_upload = pd.DataFrame()
            
            # 3. Processamento
            for arq in arquivos:
                try:
                    df = trf_service.tratar_dataframe(arq)
                    linhas_lidas += len(df)
                    
                    if "CreationDate" not in df.columns:
                        LOGGER.error(f"Coluna CreationDate não encontrada em {arq.name}. Pulando.")
                        continue
                    
                    df["CreationDate"] = df["CreationDate"].astype(str)
                    
                    # Filtra duplicados
                    df_novo = df[~df["CreationDate"].isin(datas_existentes)].copy()
                    
                    if not df_novo.empty:
                        cnt = bq_service.subir(df_novo)
                        linhas_subidas += cnt
                        
                        datas_existentes.update(df_novo["CreationDate"].unique().tolist())
                        LOGGER.info(f"Arquivo {arq.name}: {len(df)} lidas, {len(df_novo)} novas subidas.")
                    else:
                        LOGGER.info(f"Arquivo {arq.name}: Dados já existentes no BQ.")
                        
                except Exception as e:
                    LOGGER.error(f"Erro processando item {arq.name}: {e}")
                    status_exec = "ERROR"
            
            # Define Status Final
            if linhas_subidas > 0:
                status_exec = "SUCCESS"
            elif status_exec != "ERROR":
                status_exec = "NO_DATA"

    except Exception as e:
        status_exec = "ERROR"
        LOGGER.critical(f"Erro crítico na execução: {traceback.format_exc()}")
    
    finally:
        end_time = datetime.now().replace(microsecond=0)
        duration = round((end_time - START_TIME).total_seconds(), 2)
        
        LOGGER.info(f"=== FINALIZANDO: Status={status_exec}, Dur={duration}s, Lidas={linhas_lidas}, Subidas={linhas_subidas} ===")
        
        # 4. Gerar Zip
        zip_path = AutomationUtils.smart_zip(arquivos_tocados, LOG_FILE)
        
        # 5. Enviar Email
        AutomationUtils.send_email(status_exec, duration, linhas_lidas, linhas_subidas, zip_path)
        
        # 6. Publicar Métricas
        AutomationUtils.publish_metrics(START_TIME, end_time, status_exec, duration, usuario, modo_exec)
        
        # 7. Limpeza e Movimentação
        AutomationUtils.cleanup_files(arquivos_tocados, status_exec)
        
        logging.shutdown()

if __name__ == "__main__":
    main()