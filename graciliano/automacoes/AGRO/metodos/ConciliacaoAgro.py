import sys
import os
import shutil
import logging
import traceback
import getpass
import time
import zipfile
import re
import warnings
import pandas as pd
import pandas_gbq
import tempfile
from pathlib import Path
from datetime import datetime
from google.cloud import bigquery
from typing import List, Tuple, Optional, Set, Any, Dict
import pytz

# ==================================================================================================
# CONSTANTES E CONFIGURAÇÕES
# ==================================================================================================

SCRIPT_NAME = "ConciliacaoAgro"
AREA_NAME = "AGRO"
NOME_AUTOMACAO = AREA_NAME

# Timezone
TZ = pytz.timezone("America/Sao_Paulo")

ENV_EXEC_USER = os.getenv("ENV_EXEC_USER", getpass.getuser()).lower()
ENV_EXEC_MODE = os.getenv("ENV_EXEC_MODE", "MANUAL").upper()
TEST_MODE = False

# Paths - Robust Detection
POSSIBLE_ROOTS = [
    Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano",
    Path.home() / "Meu Drive/C6 CTVM" / "graciliano",
    Path.home() / "C6 CTVM/graciliano",
]
BASE_DIR = next((p for p in POSSIBLE_ROOTS if p.exists()), None)

if not BASE_DIR:
    # Fallback structure
    BASE_DIR = Path.home() / "graciliano"

# Imports de Modulos Compartilhados
UTIL_PATH = BASE_DIR / "novo_servidor" / "modules"
sys.path.append(str(UTIL_PATH))

try:
    import _utilAutomacoesExec
except ImportError:
    _utilAutomacoesExec = None

AUTOMACOES_DIR = BASE_DIR / "automacoes"
LOG_DIR = AUTOMACOES_DIR / AREA_NAME / "LOGS" / SCRIPT_NAME / datetime.now(TZ).strftime("%Y-%m-%d")
TEMP_DIR = Path(os.getenv('TEMP', Path.home())) / "C6_RPA_EXEC" / SCRIPT_NAME

# Business Paths - INPUT
# Assuming 'Operações C6...' is parallel to 'graciliano' or inside root drive
ROOT_DRIVE = BASE_DIR.parent
INPUT_DIR = ROOT_DRIVE.parent / "Operações C6 - COMPRA, VENDA E ROLAGENS"
if not INPUT_DIR.exists():
    # Attempt typical relative path
    INPUT_DIR = BASE_DIR.parent.parent / "Operações C6 - COMPRA, VENDA E ROLAGENS"

# BigQuery
PROJECT_ID = "datalab-pagamentos"
DATASET_ID = "ADMINISTRACAO_CELULA_PYTHON"

BQ_DATASET = "CELULA_PYTHON_TESTES" if TEST_MODE else "conciliacoes_monitoracao"
BQ_TABELA_AGRO_DESTINO = f"{PROJECT_ID}.{BQ_DATASET}.CONCILIACAO_AGRO_TESTE"
BQ_TABELA_CPR_DESTINO = f"{PROJECT_ID}.{BQ_DATASET}.CONCILIACAO_AGRO_CPR"

REGISTRO_AUTOMACOES_TABLE = f"{PROJECT_ID}.ADMINISTRACAO_CELULA_PYTHON.registro_automacoes"

SECOES_AGRO = ["NOVOS", "RECOMPRAS", "ROLAGENS", "ADITAMENTOS"]

COLUNAS_FINAIS_AGRO = [
    "TIPO", "INICIO", "OPERACAO", "CONTRATO", "CLIENTE", "SACAS", 
    "ARMAZEM_COOPERATIVA", "ATIVO", "VENCIMENTO", "VALOR_VENDA", 
    "PRAZO", "CTR_MATERA", "FUNDING", "CHARGE_USD", "CHARGE_BRL", 
    "SPREAD", "CAFE_D0", "SPOT_D0", "DI", "CUPOM", "DU_TERMO_D0", 
    "DC_TERMO_D0", "DOLFWD", "VALOR_COMPRA", "DIF_ROLAGEM", 
    "dt_coleta", "nome_arquivo"
]

CPR_SCHEMA_ORDER = [
    "INICIO", "OPERACAO", "CONTRATO", "CLIENTE", "SACAS", "PRECO_FATURADO", "ARMAZEM_COOPERATIVA",
    "INICIO2", "VENCIMENTO", "ICF", "VERTICE", "CHARGE_BRL", "CHARGE_USD", "DOLAR_SPOT", "DI_PRE",
    "CUPOM", "FUNDING", "DOLAR_FORWARD", "SPREAD", "CAFE_D0", "SPOT_D0", "CUPOM2", "DI", "DI2",
    "DU_TERMO_D0", "DC_TERMO_D0", "CHG_BRL_D0", "VALOR_VENDA_CPR", "Data_Recebimento_Email", "nome_arquivo",
]

RENAME_MAP_CPR = {
    "Início": "INICIO", "Operação": "OPERACAO", "Contrato": "CONTRATO", "Cliente": "CLIENTE",
    "Sacas": "SACAS", "Preço Faturado": "PRECO_FATURADO", "Armazém | Cooperativa": "ARMAZEM_COOPERATIVA",
    "Inicio": "INICIO2", "Vencimento": "VENCIMENTO", "ICF": "ICF", "Vértice": "VERTICE",
    "Charge BRL": "CHARGE_BRL", "Charge USD": "CHARGE_USD", "Dólar Spot": "DOLAR_SPOT",
    "DI Pré": "DI_PRE", "Cupom": "CUPOM", "Funding": "FUNDING", "Dólar Forward": "DOLAR_FORWARD",
    "Spread a.a.": "SPREAD", "Café D-0": "CAFE_D0", "Spot D-0": "SPOT_D0", "Cupom_2": "CUPOM2",
    "DI": "DI", "DI_2": "DI2", "DU Termo D-0": "DU_TERMO_D0", "DC Termo D-0": "DC_TERMO_D0",
    "Chg BRL D+0": "CHG_BRL_D0", "Valor Venda CPR": "VALOR_VENDA_CPR",
}

# ==================================================================================================
# FUNÇÕES DE SUPORTE
# ==================================================================================================

def setup_logger():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    
    log_filename = f"{SCRIPT_NAME}_{datetime.now(TZ).strftime('%H%M%S')}.log"
    log_path = LOG_DIR / log_filename
    
    logger = logging.getLogger(SCRIPT_NAME)
    logger.setLevel(logging.INFO)
    logger.handlers = []
    logger.propagate = False
    
    formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    
    file_handler = logging.FileHandler(log_path, encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    
    return logger, log_path

def load_config(logger):
    if TEST_MODE:
        return {"emails_principal": ["carlos.lsilva@c6bank.com"], "emails_cc": [], "move_file": False, "is_active": True}
    try:
        # Busca no BQ usando pandas_gbq padrão
        query = f"""
            SELECT emails_principais as emails_principal, emails_cc, move_file, status_automacao
            FROM `{REGISTRO_AUTOMACOES_TABLE}`
            WHERE lower(TRIM(metodo_automacao)) = lower('{SCRIPT_NAME}')
            ORDER BY data_lancamento DESC LIMIT 1
        """
        # Nota: Schema original usa 'metodo_automacao' e 'status_automacao', e 'emails_principais'
        
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID)
        if not df.empty:
            row = df.iloc[0]
            val_move = row.get("move_file", False)
            if isinstance(val_move, str): val_move = val_move.lower() in ('true', '1')
            else: val_move = bool(val_move)
            
            # Status check
            st = row.get("status_automacao", "ATIVO")
            is_active = str(st).upper() not in ["INATIVO", "DESLIGADO", "OFF"]
            
            def _clean(x):
                if not x or str(x).lower() == 'nan': return []
                return [e.strip() for e in str(x).replace(';',',').split(',') if '@' in e]

            return {
                "emails_principal": _clean(row.get("emails_principal")),
                "emails_cc": _clean(row.get("emails_cc")),
                "move_file": val_move,
                "is_active": is_active
            }
    except Exception as e:
        logger.warning(f"Erro config BQ: {e}")
    # Default fallback
    return {"emails_principal": ["carlos.lsilva@c6bank.com"], "emails_cc": [], "move_file": False, "is_active": True}

def smart_zip_logs(output_files: list) -> str:
    zip_filename = f"{SCRIPT_NAME}_{datetime.now(TZ).strftime('%H%M%S')}.zip"
    zip_path = TEMP_DIR / zip_filename
    
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        current_logs = list(LOG_DIR.glob(f"{SCRIPT_NAME}_*.log"))
        if current_logs:
            latest = max(current_logs, key=os.path.getctime)
            zf.write(latest, arcname=latest.name)
            
        for f in output_files:
            if Path(f).exists():
                zf.write(f, arcname=Path(f).name)
                
    return str(zip_path)

# ==================================================================================================
# LÓGICA DE NEGÓCIO - AGRO
# ==================================================================================================

class FileProcessor:
    @staticmethod
    def normalizar_texto(val):
        if pd.isna(val): return None
        return str(val).strip()

    @staticmethod
    def to_int_safe(val):
        try:
            s = str(val).split('.')[0]
            return int(re.sub(r'\D', '', s))
        except: return None

    @classmethod
    def process_agro_general(cls, caminho: Path) -> pd.DataFrame:
        try:
            xls = pd.read_excel(caminho, sheet_name=None, dtype=str, header=None)
        except Exception:
            return pd.DataFrame()
            
        dfs_list = []
        mapa_colunas = {
            "Início": "INICIO", "Início CDAWA": "INICIO", "Operação": "OPERACAO", "Contrato": "CONTRATO", 
            "Cliente": "CLIENTE", "Sacas": "SACAS", "Armazém | Cooperativa": "ARMAZEM_COOPERATIVA", 
            "Ativo": "ATIVO", "Vencimento": "VENCIMENTO", "Valor Venda CDAWA": "VALOR_VENDA", 
            "Prazo": "PRAZO", "CTR Matera": "CTR_MATERA", "Matera": "CTR_MATERA", "Funding": "FUNDING",
            "Charge USD": "CHARGE_USD", "Charge BRL": "CHARGE_BRL", "Spread a.a.": "SPREAD", 
            "Café D-0": "CAFE_D0", "Spot D-0": "SPOT_D0", "DI": "DI", "Cupom": "CUPOM",
            "DU Termo D-0": "DU_TERMO_D0", "DC Termo D-0": "DC_TERMO_D0", "DolFWD": "DOLFWD",
            "Valor da compra": "VALOR_COMPRA", "Dif. Rolagem": "DIF_ROLAGEM"
        }

        for aba_name, df_raw in xls.items():
            df_raw = df_raw.fillna("")
            nlin, ncol = df_raw.shape
            
            indices_secao = []
            for i in range(nlin):
                linha_txt = " ".join([str(x).strip().upper() for x in df_raw.iloc[i] if str(x).strip()])
                for sec in SECOES_AGRO:
                    if re.search(rf"\b{re.escape(sec)}\b", linha_txt):
                        indices_secao.append((i, sec))
                        break
            
            if not indices_secao: continue
            indices_secao.sort(key=lambda x: x[0])
            
            for idx_loop, (start_row, nome_secao) in enumerate(indices_secao):
                header_row = start_row + 1
                if header_row >= nlin: continue
                raw_header = [str(x).strip() for x in df_raw.iloc[header_row]]
                
                col_inicio_idx = -1
                for ic, h in enumerate(raw_header):
                    if "Início" in h or "Inicio" in h:
                        col_inicio_idx = ic
                        break
                if col_inicio_idx == -1: continue
                
                fim_dados = nlin
                if idx_loop + 1 < len(indices_secao):
                    fim_dados = indices_secao[idx_loop+1][0]
                
                df_bloco = df_raw.iloc[header_row+1 : fim_dados].copy()
                df_bloco = df_bloco[df_bloco.iloc[:, col_inicio_idx].astype(str).str.strip() != ""]
                if df_bloco.empty: continue
                
                colunas_bloco = {}
                for ic, h in enumerate(raw_header):
                    for k_map, v_map in mapa_colunas.items():
                        if k_map.lower() == h.lower():
                            colunas_bloco[ic] = v_map
                            break
                    if ic not in colunas_bloco:
                        for k_map, v_map in mapa_colunas.items():
                            if k_map in h:
                                colunas_bloco[ic] = v_map
                                break
                
                dados_validos = {}
                for idx_col, nome_col in colunas_bloco.items():
                    if idx_col < df_bloco.shape[1]:
                        dados_validos[nome_col] = df_bloco.iloc[:, idx_col].values
                
                df_clean = pd.DataFrame(dados_validos)
                df_clean["TIPO"] = nome_secao
                dfs_list.append(df_clean)

        if not dfs_list: return pd.DataFrame()
        
        df_final = pd.concat(dfs_list, ignore_index=True)
        for col in COLUNAS_FINAIS_AGRO:
            if col not in df_final.columns: df_final[col] = pd.NA
        
        df_final["nome_arquivo"] = caminho.name
        df_final["dt_coleta"] = datetime.now()
        
        for col_dt in ["INICIO", "VENCIMENTO"]:
            df_final[col_dt] = pd.to_datetime(df_final[col_dt], dayfirst=True, errors="coerce")
        for col_int in ["SACAS", "PRAZO", "DU_TERMO_D0", "DC_TERMO_D0"]:
            df_final[col_int] = df_final[col_int].apply(cls.to_int_safe).astype("Int64")
            
        ignore_cleanup = ["dt_coleta", "INICIO", "VENCIMENTO", "SACAS", "PRAZO", "DU_TERMO_D0", "DC_TERMO_D0"]
        for c in [x for x in COLUNAS_FINAIS_AGRO if x not in ignore_cleanup]:
            df_final[c] = df_final[c].apply(cls.normalizar_texto).astype("string")
            
        return df_final[COLUNAS_FINAIS_AGRO]

    @classmethod
    def process_cpr(cls, caminho: Path) -> pd.DataFrame:
        try:
            xls = pd.ExcelFile(caminho)
            dfs_validos = []
            colunas_chave = {"Operação", "Contrato", "Cliente", "Sacas"}

            for sheet_name in xls.sheet_names:
                try:
                    df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None).fillna("")
                    cabecalho_idx = -1
                    headers_encontrados = []

                    for idx, row in df_raw.iterrows():
                        row_vals_str = [str(x).strip() for x in row.values]
                        if colunas_chave.issubset(set(row_vals_str)):
                            cabecalho_idx = idx
                            seen = {}
                            for h in row_vals_str:
                                base = h
                                if base in seen:
                                    seen[base] += 1
                                    headers_encontrados.append(f"{base}_{seen[base]}")
                                else:
                                    seen[base] = 1
                                    headers_encontrados.append(base)
                            break
                    
                    if cabecalho_idx != -1:
                        df_aba = df_raw.iloc[cabecalho_idx + 1:].copy()
                        df_aba.columns = headers_encontrados
                        
                        colunas_renomeadas = {c: RENAME_MAP_CPR[c] for c in df_aba.columns if c in RENAME_MAP_CPR}
                        df_aba = df_aba.rename(columns=colunas_renomeadas)
                        
                        if "OPERACAO" in df_aba.columns:
                            df_aba = df_aba[df_aba["OPERACAO"].astype(str).str.strip().str.upper() == "COMPRA DE CPR"]
                            if not df_aba.empty:
                                df_aba = df_aba.loc[:, ~pd.Index(df_aba.columns).duplicated(keep='first')]
                                df_aba = df_aba[~(df_aba.astype(str).apply(lambda r: "".join(r.values), axis=1).str.strip().eq(""))].copy()
                                dfs_validos.append(df_aba)
                except Exception:
                    continue
            
            if not dfs_validos: return pd.DataFrame()
            
            df_final = pd.concat(dfs_validos, ignore_index=True)
            for c in CPR_SCHEMA_ORDER:
                if c not in df_final.columns and c not in ("Data_Recebimento_Email", "nome_arquivo"):
                    df_final[c] = ""
            
            df_final = df_final.astype(str)
            df_final["nome_arquivo"] = caminho.name
            # Use naive now then localize if needed, or straight iso
            df_final["Data_Recebimento_Email"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            for c in CPR_SCHEMA_ORDER:
                if c not in df_final.columns: df_final[c] = ""
                
            return df_final[CPR_SCHEMA_ORDER]

        except Exception:
            return pd.DataFrame()

def load_bq_agro(df, logger):
    # Staging + Merge para evitar duplicatas totais
    client = bigquery.Client(project=PROJECT_ID)
    staging = f"{BQ_TABELA_AGRO_DESTINO}_stg_{int(time.time())}"
    
    try:
        pandas_gbq.to_gbq(df, staging, project_id=PROJECT_ID, if_exists="replace")
        
        # Cria ou Verifica Destino
        try: client.get_table(BQ_TABELA_AGRO_DESTINO)
        except:
             # Create empty
             pandas_gbq.to_gbq(df.head(0), BQ_TABELA_AGRO_DESTINO, project_id=PROJECT_ID)

        # Merge
        stg_ref = client.get_table(staging)
        cols = [f.name for f in stg_ref.schema]
        # Match using all columns as key essentially
        conditions = " AND ".join([f"(T.{c} = S.{c} OR (T.{c} IS NULL AND S.{c} IS NULL))" for c in cols])
        
        sql = f"""
        INSERT INTO `{BQ_TABELA_AGRO_DESTINO}` ({', '.join(cols)})
        SELECT {', '.join(cols)} FROM `{staging}` S
        WHERE NOT EXISTS (SELECT 1 FROM `{BQ_TABELA_AGRO_DESTINO}` T WHERE {conditions})
        """
        client.query(sql).result()
    finally:
        client.delete_table(staging, not_found_ok=True)

def load_bq_cpr(df, logger):
    # Append simple
    pandas_gbq.to_gbq(df, BQ_TABELA_CPR_DESTINO, project_id=PROJECT_ID, if_exists="append")

def main():
    start_time = datetime.now(TZ)
    logger, log_path = setup_logger()
    logger.info(f"Inicio {SCRIPT_NAME}")
    
    status = "SUCESSO"
    processed_count = 0
    arquivos_proc = []
    
    try:
        config = load_config(logger)
        if not config["is_active"]:
            logger.info("Automacao INATIVA.")
            return

        if not INPUT_DIR.exists():
            status = "ERRO_DIR"
            logger.error(f"Input dir nao existe: {INPUT_DIR}")
        else:
            files = sorted([f for f in INPUT_DIR.glob("*") if f.is_file()])
            logger.info(f"Arquivos no diretorio: {len(files)}")

            for f in files:
                # Otimizacao: Pular arquivos temp ou nao excel
                if f.name.startswith("~") or not f.suffix.lower() in [".xlsx", ".xls"]: continue
                
                # 1. AGRO
                df_agro = FileProcessor.process_agro_general(f)
                if not df_agro.empty:
                    load_bq_agro(df_agro, logger)
                    processed_count += 1
                    if f not in arquivos_proc: arquivos_proc.append(f)
                    logger.info(f"Agro Processed: {f.name}")
                    
                # 2. CPR
                df_cpr = FileProcessor.process_cpr(f)
                if not df_cpr.empty:
                    load_bq_cpr(df_cpr, logger)
                    processed_count += 1
                    if f not in arquivos_proc: arquivos_proc.append(f)
                    logger.info(f"CPR Processed: {f.name}")

            if processed_count == 0:
                status = "NO_DATA"

    except Exception as e:
        status = "FALHA"
        logger.error(f"Fatal: {traceback.format_exc()}")
        
    end_time = datetime.now(TZ)
    tempo_exec = str(end_time - start_time).split('.')[0]
    
    zip_path = smart_zip_logs([str(p) for p in arquivos_proc])
    
    # Publicacao Metricas
    if _utilAutomacoesExec:
        try:
            client_exec = _utilAutomacoesExec.AutomacoesExecClient(logger)
            dest = config["emails_principal"]
            if status == "SUCESSO": dest += config["emails_cc"]
            
            client_exec.publicar(
                nome_automacao=NOME_AUTOMACAO,
                metodo_automacao=SCRIPT_NAME,
                status=status,
                tempo_exec=tempo_exec,
                data_exec=start_time.strftime("%Y-%m-%d"),
                hora_exec=start_time.strftime("%H:%M:%S"),
                usuario=ENV_EXEC_USER,
                log_path=str(log_path),
                destinatarios=dest,
                send_email=True,
                anexos=[zip_path] if Path(zip_path).exists() else []
            )
        except Exception as e:
            logger.error(f"Erro publicacao util: {e}")

if __name__ == "__main__":
    if "--test" in sys.argv: TEST_MODE = True
    main()
