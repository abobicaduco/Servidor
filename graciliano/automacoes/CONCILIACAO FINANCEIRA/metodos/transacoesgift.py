# ==============================================================================
# AUTO-INSTALL DEPENDENCIES & CONFIG
# ==============================================================================
import sys
import os
import time
import shutil
import logging
import traceback
import zipfile
import socket
import json
import tempfile
import re
from datetime import datetime, date
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

# Adiciona diretório raiz ao path (Híbrido: Relativo ou Padrão C6)
# Tenta achar config_loader.py subindo os níveis
current_dir = Path(__file__).resolve().parent
project_root = None

for parent in [current_dir] + list(current_dir.parents)[:5]:
    if (parent / "config_loader.py").exists():
        project_root = parent
        break

# Se não achou relativo, aponta para o caminho padrão da rede
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
        "openpyxl"
    ]
    
    bootstrap_deps.install_requirements(extra_libs=SCRIPT_DEPS)
    from config_loader import Config
    import pandas as pd
    import pandas_gbq
    from google.cloud import bigquery
    import pythoncom
    from win32com.client import Dispatch, GetActiveObject

    # Configuração via Loader (Dinâmica DEV/PROD)
    ROOT_DIR = Config.ROOT_DIR
    PROJECT_ID = Config.PROJECT_ID
    DATASET_ID = Config.DATASET_ID

except ImportError:
    print("⚠️ Config Loader não encontrado. Usando FALLBACK local.")
    import pandas as pd
    import pandas_gbq
    from google.cloud import bigquery
    import pythoncom
    from win32com.client import Dispatch, GetActiveObject
    
    # Fallback Hardcoded
    ROOT_DIR = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "Servidor_CELULA_PYTHON"
    PROJECT_ID = 'datalab-pagamentos'
    DATASET_ID = 'ADMINISTRACAO_CELULA_PYTHON'
    
    class Config:
        PROJECT_ID = PROJECT_ID
        DATASET_ID = DATASET_ID
        TABLE_EXEC = f"{PROJECT_ID}.{DATASET_ID}.automacoes_exec"
        TABLE_CONFIG = f"{PROJECT_ID}.{DATASET_ID}.registro_automacoes"
        COMPANY_DOMAIN = "c6bank.com"

# ==============================================================================
# CONSTANTES DO SCRIPT
# ==============================================================================
SCRIPT_NAME = "transacoesgift" # Mantendo nome original do stem para compatibilidade
AREA_NAME = "CONCILIACAO FINANCEIRA"
START_TIME = datetime.now().replace(microsecond=0)
HEADLESS = False

# Diretórios
TEMP_DIR = Path(os.getenv('TEMP', Path.home())) / "C6_RPA_EXEC" / SCRIPT_NAME
TEMP_DIR.mkdir(parents=True, exist_ok=True)

# Mantendo caminhos originais de negócio
CAMINHO_BASE_NEGOCIO = (
    Path.home()
    / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A"
    / "Mensageria e Cargas Operacionais - 11.CelulaPython"
    / "graciliano"
    / "automacoes"
)
CAMINHO_INPUT = CAMINHO_BASE_NEGOCIO / AREA_NAME / "arquivos input" / SCRIPT_NAME
CAMINHO_ANEXOS = CAMINHO_BASE_NEGOCIO / AREA_NAME / "anexos"

CAMINHO_INPUT.mkdir(parents=True, exist_ok=True)
CAMINHO_ANEXOS.mkdir(parents=True, exist_ok=True)

# Config Globais
GLOBAL_CONFIG = {
    'area_name': AREA_NAME, 
    'emails_principal': ["carlos.lsilva@c6bank.com", "sofia.fernandes@c6bank.com"], # Fallback
    'emails_cc': ["carlos.lsilva@c6bank.com", "sofia.fernandes@c6bank.com"], # Fallback
    'move_file': False
}

# Logger setup
LOG_FILE_PATH = TEMP_DIR / f"{SCRIPT_NAME}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

def setup_logger():
    logger = logging.getLogger(SCRIPT_NAME)
    logger.setLevel(logging.INFO)
    logger.handlers = []
    
    formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
    
    # File Handler
    file_handler = logging.FileHandler(LOG_FILE_PATH, encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Console Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger

LOGGER = setup_logger()

# ==============================================================================
# INFRAESTRUTURA (Configs, Metrics, Email, Zip)
# ==============================================================================

def get_configs():
    """Carrega configs do BQ"""
    try:
        query = f"""
            SELECT emails_principal, emails_cc, move_file 
            FROM `{Config.TABLE_CONFIG}`
            WHERE lower(TRIM(script_name)) = lower('{SCRIPT_NAME}')
            AND (is_active IS NULL OR lower(is_active) = 'true')
            ORDER BY created_at DESC LIMIT 1
        """
        try:
            # REMOVIDO use_bqstorage_api=False DAQUI
            df = pandas_gbq.read_gbq(query, project_id=Config.PROJECT_ID)
        except Exception:
            LOGGER.warning("Falha ao ler configs via pandas-gbq. Tentando client nativo.")
            client = bigquery.Client(project=Config.PROJECT_ID)
            df = client.query(query).to_dataframe()
        
        if not df.empty:
            GLOBAL_CONFIG['emails_principal'] = [x.strip() for x in str(df.iloc[0]['emails_principal']).replace(';', ',').split(',') if '@' in x]
            GLOBAL_CONFIG['emails_cc'] = [x.strip() for x in str(df.iloc[0]['emails_cc']).replace(';', ',').split(',') if '@' in x]
            GLOBAL_CONFIG['move_file'] = bool(df.iloc[0].get('move_file', False))
            LOGGER.info(f"Configs carregadas: Move={GLOBAL_CONFIG['move_file']}, Emails={len(GLOBAL_CONFIG['emails_principal'])}")
        else:
            LOGGER.warning("Configs não encontradas no BQ. Usando padrão fallback.")
    except Exception as e:
        LOGGER.error(f"Erro ao carregar configs: {e}")

def upload_metrics(status, total_rows=0):
    """Sobe métricas de execução no schema padrão"""
    try:
        end_time = datetime.now().replace(microsecond=0)
        duration = round((end_time - START_TIME).total_seconds(), 2)
        
        user_env = os.environ.get('ENV_EXEC_USER')
        usuario = user_env if user_env else f"{os.getlogin().lower()}@{Config.COMPANY_DOMAIN}"
        modo = os.environ.get('ENV_EXEC_MODE', 'AGENDAMENTO')

        df_metric = pd.DataFrame([{
            'script_name': SCRIPT_NAME,
            'area_name': AREA_NAME,
            'start_time': START_TIME,
            'end_time': end_time,
            'duration_seconds': float(duration),
            'status': status,
            'usuario': usuario,
            'modo_exec': modo
        }])

        # REMOVIDO use_bqstorage_api=False DAQUI
        pandas_gbq.to_gbq(
            df_metric,
            Config.TABLE_EXEC,
            project_id=Config.PROJECT_ID,
            if_exists='append'
        )
        LOGGER.info(f"Métricas enviadas: {status} | Duração: {duration}s")
    except Exception as e:
        LOGGER.error(f"Erro ao subir métricas: {e}")

def create_smart_zip(output_files):
    """Cria zip com limite de 15MB"""
    try:
        zip_name = TEMP_DIR / f"{SCRIPT_NAME}_{datetime.now().strftime('%H%M%S')}.zip"
        limit_bytes = 15 * 1024 * 1024  # 15MB
        
        # Garante que o LOG atual esteja na lista
        files_to_zip = output_files.copy()
        if LOG_FILE_PATH.exists() and LOG_FILE_PATH not in files_to_zip:
            files_to_zip.append(LOG_FILE_PATH)
            
        # Força flush do log antes de zipar
        for handler in LOGGER.handlers:
            handler.flush()

        with zipfile.ZipFile(zip_name, 'w', zipfile.ZIP_DEFLATED) as zf:
            current_size = 0
            
            # Prioridade 1: Log file
            if LOG_FILE_PATH.exists():
                zf.write(LOG_FILE_PATH, LOG_FILE_PATH.name)
                current_size += LOG_FILE_PATH.stat().st_size
            
            # Prioridade 2: Outros arquivos
            for file_path in files_to_zip:
                if file_path == LOG_FILE_PATH: continue
                if not file_path.exists(): continue
                
                f_size = file_path.stat().st_size
                if current_size + f_size < limit_bytes:
                    zf.write(file_path, file_path.name)
                    current_size += f_size
                else:
                    LOGGER.warning(f"Arquivo {file_path.name} ignorado no ZIP (Excede 15MB)")
                    zf.writestr(f"AVISO_ARQUIVO_GRANDE_{file_path.name}.txt", 
                                "Arquivo ignorado no anexo pois excede o limite de 15MB. Consulte a pasta de rede.")
        
        return zip_name
    except Exception as e:
        LOGGER.error(f"Erro ao criar Smart Zip: {e}")
        return None

def send_email_outlook(status, subject_suffix, body_html, attachment_path=None):
    """Envia email via Outlook"""
    try:
        pythoncom.CoInitialize()
        try:
            outlook = GetActiveObject("Outlook.Application")
        except Exception:
            outlook = Dispatch("Outlook.Application")
            
        mail = outlook.CreateItem(0)
        
        # Assunto Padrão
        mail.Subject = f"CÉLULA PYTHON MONITORAÇÃO - {SCRIPT_NAME.upper()} - {status} - {subject_suffix}"
        
        # Destinatários
        to_list = GLOBAL_CONFIG['emails_principal']
        cc_list = GLOBAL_CONFIG['emails_cc'] if status == 'SUCCESS' else []
        
        mail.To = ";".join(to_list)
        mail.CC = ";".join(cc_list)
        mail.HTMLBody = body_html
        
        if attachment_path and attachment_path.exists():
            mail.Attachments.Add(str(attachment_path))
            
        mail.Send()
        LOGGER.info(f"Email enviado para: {to_list} (CC: {len(cc_list)})")
        
    except Exception as e:
        LOGGER.error(f"Erro ao enviar email: {e}")
    finally:
        pythoncom.CoUninitialize()

def move_files_to_network(files):
    """Move arquivos para rede se configurado"""
    if not GLOBAL_CONFIG['move_file']:
        return

    try:
        # Caminho de destino padronizado
        dest_dir = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "automacoes" / AREA_NAME / "logs" / SCRIPT_NAME / datetime.now().strftime('%Y-%m-%d')
        dest_dir.mkdir(parents=True, exist_ok=True)
        
        for f in files:
            if f.exists():
                shutil.copy2(f, dest_dir / f.name)
                LOGGER.info(f"Arquivo copiado para rede: {f.name}")
                
    except Exception as e:
        LOGGER.error(f"Erro ao mover arquivos para rede: {e}")

# ==============================================================================
# LÓGICA DE NEGÓCIO (LEGADO REFATORADO)
# ==============================================================================

EMAIL_POR_ARQUIVO = {}

def garantir_outlook_aberto():
    LOGGER.info("VERIFICANDO OUTLOOK")
    try:
        pythoncom.CoInitialize()
    except Exception:
        pass
    try:
        GetActiveObject("Outlook.Application")
        LOGGER.info("OUTLOOK ATIVO")
    except Exception:
        LOGGER.info("OUTLOOK NAO ENCONTRADO, INICIANDO")
        try:
            Dispatch("Outlook.Application")
            LOGGER.info("OUTLOOK INICIADO")
            time.sleep(5)
        except Exception:
            LOGGER.error("FALHA AO INICIAR OUTLOOK", exc_info=True)

def procurar_anexos_email() -> list[Path]:
    garantir_outlook_aberto()
    pythoncom.CoInitialize()
    try:
        outlook = Dispatch("Outlook.Application")
        ns = outlook.GetNamespace("MAPI")
    except Exception:
        return []

    hoje = datetime.now().date()
    salvos = []

    def processar_pasta(folder, nome_pasta: str):
        try:
            items = folder.Items
            items.Sort("[ReceivedTime]", True)
            for msg in items:
                try:
                    if not getattr(msg, "MessageClass", "").startswith("IPM.Note"):
                        continue
                    rec_time = getattr(msg, "ReceivedTime", None)
                    if not rec_time or rec_time.date() != hoje:
                        continue
                    subj = getattr(msg, "Subject", "") or ""
                    if "transacoes_gift | " not in subj.lower():
                        continue
                    for j in range(msg.Attachments.Count, 0, -1):
                        try:
                            att = msg.Attachments.Item(j)
                            fn = (att.FileName or "").lower()
                            if fn.endswith((".xls", ".xlsx")):
                                destino = CAMINHO_ANEXOS / f"{msg.ReceivedTime:%Y%m%d_%H%M%S}_{att.FileName}"
                                if not destino.exists():
                                    att.SaveAsFile(str(destino))
                                    LOGGER.info(f"ANEXO SALVO: {destino.name}")
                                else:
                                    LOGGER.info(f"ANEXO JA EXISTE: {destino.name}")
                                salvos.append(destino)
                                EMAIL_POR_ARQUIVO[destino] = msg
                        except Exception:
                            LOGGER.warning(f"ERRO AO PROCESSAR ANEXO EM {nome_pasta}", exc_info=True)
                except Exception:
                    continue
        except Exception:
            LOGGER.warning(f"ERRO AO ACESSAR PASTA {nome_pasta}", exc_info=True)

    # Varre pastas conforme lógica original
    try:
        for i in range(1, ns.Folders.Count + 1):
            pasta = ns.Folders.Item(i)
            nome = getattr(pasta, "Name", "")
            if "Celula Python Monitoracao" in nome:
                try:
                    inbox = pasta.Folders["Inbox"]
                    processar_pasta(inbox, "COMPARTILHADA")
                except Exception: pass
                break
        
        inbox_pessoal = ns.GetDefaultFolder(6)
        processar_pasta(inbox_pessoal, "PESSOAL")
    except Exception as e:
        LOGGER.error(f"Erro ao varrer Outlook: {e}")

    return salvos

def buscar_arquivos_locais() -> list[Path]:
    encontrados = []
    if CAMINHO_INPUT.exists():
        for padrao in ("*.xlsx", "*.xls"):
            for arquivo in CAMINHO_INPUT.glob(padrao):
                if not arquivo.name.startswith("~$") and arquivo.stat().st_size > 0:
                    encontrados.append(arquivo)
    
    # Ordena por data de modificação
    encontrados = sorted(
        {p.resolve(): p for p in encontrados}.values(),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return encontrados

def regravar_excel(path: Path) -> Path:
    destino_tmp = TEMP_DIR / (path.stem + ".tmp.xlsx")
    xl = None
    try:
        pythoncom.CoInitialize()
        xl = Dispatch("Excel.Application")
        xl.DisplayAlerts = False
        wb = xl.Workbooks.Open(str(path), ReadOnly=False)
        wb.SaveAs(str(destino_tmp), FileFormat=51) # 51 = xlsx
        wb.Close()
        destino_tmp.replace(path)
        LOGGER.info(f"REGRAVADO COM SUCESSO: {path.name}")
        return path
    except Exception as exc:
        LOGGER.warning(f"FALHA AO REGRAVAR {path.name}: {exc}")
        return path
    finally:
        if xl:
            try:
                xl.Quit()
            except: pass

def tratar_dataframe(arq: Path) -> pd.DataFrame:
    try:
        df = pd.read_excel(arq, engine="openpyxl")
        mapping = {
            "Tipo": "Tipo", "Loja": "Loja", "Nome Fantasia": "Nome_Fantasia",
            "CNPJ": "CNPJ", "Data": "Data", "Compra": "Compra",
            "Usuario": "Usuario", "Sistema": "Sistema", "Modelo": "Modelo",
            "Produto": "Produto", "Operadora": "Operadora", "Custo": "Custo",
            "Face": "Face", "Lote": "Lote", "Série PIN": "Serie_PIN",
            "Fone": "Fone", "Vencimento": "Vencimento", "Cod. Cobrança": "Cod_Cobranca",
            "Tipo Cob.": "Tipo_Cob", "Nsu/Referencia": "Nsu_Ref",
            "Nsu Origem": "Nsu_origem", "Usuario POS": "Usuario_POS",
            "Status Transação": "Status_Transacao", "Série Terminal": "Serie_Terminal",
        }
        
        expected = list(mapping.values())
        df_tratado = df[[c for c in df.columns if c in mapping]].rename(columns=mapping)
        df_tratado = df_tratado[expected] # Ordena
        
        # Filtros e conversões
        df_tratado = df_tratado[df_tratado["Modelo"].str.contains("PIN", na=False)].reset_index(drop=True)
        df_tratado["Data"] = pd.to_datetime(df_tratado["Data"], dayfirst=True, errors="coerce").dt.date
        
        for intcol in ["Loja", "Compra", "Lote", "Cod_Cobranca", "Nsu_Ref"]:
            df_tratado[intcol] = pd.to_numeric(df_tratado[intcol], errors="coerce").fillna(0).astype("Int64")
        for floatcol in ["Custo", "Face", "Fone", "Nsu_origem", "Usuario_POS", "Serie_Terminal"]:
            df_tratado[floatcol] = pd.to_numeric(df_tratado[floatcol], errors="coerce").astype(float)
            
        return df_tratado
    except Exception as e:
        LOGGER.error(f"Erro ao tratar dataframe {arq.name}: {e}")
        return pd.DataFrame()

def subir_bq_pandas_gbq(df: pd.DataFrame) -> int:
    """Upload para BQ usando pandas-gbq com schema específico"""
    if df.empty:
        return 0

    df_up = df.copy()
    
    # Tratamentos para BQ (conforme lógica original)
    df_up["Data"] = pd.to_datetime(df_up["Data"], dayfirst=True, errors="coerce").dt.date
    df_up["Dt_coleta"] = df_up["Data"]
    # No pandas-gbq, datas podem subir como objetos date/datetime nativos ou string YYYY-MM-DD
    # A lógica original convertia Data para string, mas Dt_coleta para Date
    df_up["Data"] = pd.to_datetime(df_up["Data"], errors="coerce").dt.strftime("%Y-%m-%d")

    tabela_destino = "conciliacoes_monitoracao.Gift_Card_Relatorio_RV"
    
    # Schema Def
    table_schema = [
        {'name': 'Tipo', 'type': 'STRING'},
        {'name': 'Loja', 'type': 'INTEGER'},
        {'name': 'Nome_Fantasia', 'type': 'STRING'},
        {'name': 'CNPJ', 'type': 'STRING'},
        {'name': 'Data', 'type': 'STRING'},
        {'name': 'Compra', 'type': 'INTEGER'},
        {'name': 'Usuario', 'type': 'STRING'},
        {'name': 'Sistema', 'type': 'STRING'},
        {'name': 'Modelo', 'type': 'STRING'},
        {'name': 'Produto', 'type': 'STRING'},
        {'name': 'Operadora', 'type': 'STRING'},
        {'name': 'Custo', 'type': 'FLOAT'},
        {'name': 'Face', 'type': 'FLOAT'},
        {'name': 'Lote', 'type': 'INTEGER'},
        {'name': 'Serie_PIN', 'type': 'INTEGER'},
        {'name': 'Fone', 'type': 'INTEGER'},
        {'name': 'Vencimento', 'type': 'STRING'},
        {'name': 'Cod_Cobranca', 'type': 'INTEGER'},
        {'name': 'Tipo_Cob', 'type': 'STRING'},
        {'name': 'Nsu_Ref', 'type': 'INTEGER'},
        {'name': 'Nsu_origem', 'type': 'INTEGER'},
        {'name': 'Usuario_POS', 'type': 'STRING'},
        {'name': 'Status_Transacao', 'type': 'STRING'},
        {'name': 'Serie_Terminal', 'type': 'STRING'},
        {'name': 'Dt_coleta', 'type': 'DATE'},
    ]

    LOGGER.info(f"Iniciando upload para {tabela_destino}...")
    
    # REMOVIDO use_bqstorage_api=False DAQUI
    pandas_gbq.to_gbq(
        df_up,
        f"{Config.PROJECT_ID}.{tabela_destino}",
        project_id=Config.PROJECT_ID,
        if_exists='append', # Original logic: MODO_SUBIDA_BQ = "append"
        table_schema=table_schema
    )
    
    rows = len(df_up)
    LOGGER.info(f"Upload concluído: {rows} linhas.")
    return rows

def rodar_procedures():
    try:
        client = bigquery.Client(project=Config.PROJECT_ID)
        # Procedure fixa do código original
        query = f"CALL `{Config.PROJECT_ID}.conciliacoes_monitoracao.GERA_BASE_GIFT_CARD`()"
        client.query(query).result()
        LOGGER.info("Procedure GERA_BASE_GIFT_CARD executada com sucesso.")
    except Exception as e:
        LOGGER.error(f"Erro ao executar Procedure: {e}")

# ==============================================================================
# MAIN
# ==============================================================================
def main():
    LOGGER.info(f"=== INÍCIO EXECUÇÃO: {SCRIPT_NAME} ===")
    get_configs()
    
    output_files = []
    dfs_para_anexo = []
    status_final = "ERROR"
    total_linhas = 0
    linhas_inseridas = 0
    msg_body = ""

    try:
        # 1. Busca Arquivos
        locais = buscar_arquivos_locais()
        emails = procurar_anexos_email()
        
        # Dedup de caminhos
        todos_arquivos = list({str(p.resolve()): p for p in locais + emails}.values())
        
        LOGGER.info(f"Total arquivos a processar: {len(todos_arquivos)}")
        
        if not todos_arquivos:
            status_final = "NO_DATA"
            msg_body = "Nenhum arquivo encontrado para processamento nas pastas ou e-mail."
        else:
            processed_count = 0
            
            for arq in todos_arquivos:
                try:
                    LOGGER.info(f"Processando: {arq.name}")
                    arq = regravar_excel(arq) # Tenta corrigir Excel corrompido
                    
                    df = tratar_dataframe(arq)
                    if df.empty:
                        LOGGER.warning(f"DataFrame vazio para {arq.name}")
                        continue
                    
                    linhas = subir_bq_pandas_gbq(df)
                    total_linhas += linhas
                    linhas_inseridas += linhas # Assumindo append sem dedup prévio conforme legado
                    
                    if linhas > 0:
                        dfs_para_anexo.append(df)
                        processed_count += 1
                        
                        # Remove email se processado com sucesso e tiver vindo de email
                        msg = EMAIL_POR_ARQUIVO.get(arq)
                        if msg:
                            try:
                                msg.Delete()
                                LOGGER.info("Email de origem deletado.")
                            except: pass
                            
                except Exception as e:
                    LOGGER.error(f"Erro ao processar arquivo {arq.name}: {e}")

            if total_linhas > 0:
                status_final = "SUCCESS"
                rodar_procedures()
                
                # Gera anexo consolidado
                try:
                    df_all = pd.concat(dfs_para_anexo, ignore_index=True)
                    anexo_xlsx = TEMP_DIR / f"{SCRIPT_NAME}_CONSOLIDADO_{datetime.now().strftime('%H%M%S')}.xlsx"
                    df_all.to_excel(anexo_xlsx, index=False)
                    output_files.append(anexo_xlsx)
                except Exception as e:
                    LOGGER.error(f"Erro ao gerar Excel consolidado: {e}")

                msg_body = f"""
                <p>Status: <b>SUCESSO</b></p>
                <p>Arquivos Processados: {processed_count}</p>
                <p>Linhas Inseridas: {linhas_inseridas}</p>
                <p>Procedure Executada: SIM</p>
                """
            elif status_final != "NO_DATA":
                status_final = "NO_DATA" # Arquivos existiam mas estavam vazios ou erro no tratamento
                msg_body = "Arquivos encontrados mas nenhum dado válido foi extraído."

    except Exception:
        status_final = "ERROR"
        msg_body = f"<p>Erro Fatal na execução:</p><pre>{traceback.format_exc()}</pre>"
        LOGGER.error("Erro fatal no loop principal", exc_info=True)

    # ==========================================================================
    # FINALIZAÇÃO (Zip, Email, Metrics, Move)
    # ==========================================================================
    
    # 1. Smart Zip
    zip_path = create_smart_zip(output_files)
    
    # 2. Email
    send_email_outlook(
        status=status_final,
        subject_suffix=f"{total_linhas} Linhas",
        body_html=f"<html><body style='font-family:Arial'>{msg_body}</body></html>",
        attachment_path=zip_path
    )
    
    # 3. Métricas
    upload_metrics(status=status_final, total_rows=total_linhas)
    
    # 4. Mover Arquivos (Backup)
    if status_final == "SUCCESS" and GLOBAL_CONFIG['move_file']:
        # Move inputs processados
        move_files_to_network(todos_arquivos)
        # Move outputs gerados
        if zip_path:
            move_files_to_network([zip_path])

    LOGGER.info("=== FIM EXECUÇÃO ===")

if __name__ == "__main__":
    main()