import sys
import os
import shutil
import logging
import traceback
import getpass
import time
import zipfile
import re
import pandas as pd
import pandas_gbq
import win32com.client as win32
import pythoncom
import tempfile
import pytz
from pathlib import Path
from datetime import datetime
from google.cloud import bigquery
from typing import List, Tuple, Optional, Dict, Any

# ==================================================================================================
# CONSTANTES E CONFIGURAÇÕES
# ==================================================================================================

SCRIPT_NAME = "arquivosmastercard"
# Original code had NOME_SCRIPT as ARQUIVOSMASTERCARD derived from file stem.upper()
# Adhering to prompt which usually implies lower handling in config but matches filename
AREA_NAME = "BO CARTOES"

TZ = pytz.timezone("America/Sao_Paulo")
ENV_EXEC_USER = os.getenv("ENV_EXEC_USER", getpass.getuser()).lower()
ENV_EXEC_MODE = os.getenv("ENV_EXEC_MODE", "MANUAL").upper()
TEST_MODE = False

# Paths - Robust Detection
HOME = Path.home()
POSSIBLE_ROOTS = [
    HOME / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A",
    HOME / "Meu Drive/C6 CTVM",
    HOME / "C6 CTVM",
    HOME / "SharePoint",
    HOME / "OneDrive - C6 Bank S.A",
    HOME,
]
ROOT_DRIVE = next((p for p in POSSIBLE_ROOTS if p.exists() and (p / "Mensageria e Cargas Operacionais - 11.CelulaPython").exists()), POSSIBLE_ROOTS[0])

# Specific Paths
BASE_DIR = ROOT_DRIVE / "Mensageria e Cargas Operacionais - 11.CelulaPython/graciliano"
if not BASE_DIR.exists():
    BASE_DIR = ROOT_DRIVE / "graciliano" # Fallback

AUTOMACOES_DIR = BASE_DIR / "automacoes"
LOG_DIR = AUTOMACOES_DIR / AREA_NAME / "LOGS" / SCRIPT_NAME / datetime.now(TZ).strftime("%Y-%m-%d")
TEMP_DIR = Path(os.getenv('TEMP', Path.home())) / "C6_RPA_EXEC" / SCRIPT_NAME

# Business Paths
INPUT_DIR = AUTOMACOES_DIR / AREA_NAME / "arquivos input"
JA_FEITOS_DIR = INPUT_DIR / "ja_feitos"

# BigQuery
PROJECT_ID = "datalab-pagamentos"
DATASET_ID = "ADMINISTRACAO_CELULA_PYTHON"

REGISTRO_AUTOMACOES_TABLE = f"{PROJECT_ID}.ADMINISTRACAO_CELULA_PYTHON.registro_automacoes"
AUTOMACOES_EXEC_TABLE = f"{PROJECT_ID}.ADMINISTRACAO_CELULA_PYTHON.automacoes_exec"

DESTINO_MAP = {
    "1SWCHD53": "datalab-pagamentos.ARQUIVOS_MASTERCARD.SWCHD53",
    "1SWCHD53_IND": "datalab-pagamentos.ARQUIVOS_MASTERCARD.SWCHD53_INDICE",
    "1SWCHD363": "datalab-pagamentos.ARQUIVOS_MASTERCARD.SWCHD363_NOVO",
    "1SWCHD353": "datalab-pagamentos.ARQUIVOS_MASTERCARD.SWCHD353",
    "1SWCHD353_IND": "datalab-pagamentos.ARQUIVOS_MASTERCARD.SWCHD353_INDICE",
}
EXTRA_TABELAS_DISTINCT = ["monitoracao_shared.SWCHD363_NOVO"]
SUBIDA_BQ = "append"

# ==================================================================================================
# FUNÇÕES DE SUPORTE
# ==================================================================================================

def setup_logger():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    JA_FEITOS_DIR.mkdir(parents=True, exist_ok=True)
    
    log_filename = f"{SCRIPT_NAME}_{datetime.now(TZ).strftime('%H%M%S')}.log"
    log_path = LOG_DIR / log_filename
    
    logger = logging.getLogger(SCRIPT_NAME)
    logger.setLevel(logging.INFO)
    logger.handlers = []
    
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
        query = f"""
            SELECT emails_principal, emails_cc, move_file, is_active
            FROM `{REGISTRO_AUTOMACOES_TABLE}`
            WHERE lower(TRIM(script_name)) = lower('{SCRIPT_NAME}')
            ORDER BY created_at DESC LIMIT 1
        """
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, use_bqstorage_api=False)
        if not df.empty:
            row = df.iloc[0]
            val_move = row.get("move_file", False)
            if isinstance(val_move, str): val_move = val_move.lower() in ('true', '1')
            else: val_move = bool(val_move)
            
            val_active = row.get("is_active", "true")
            if isinstance(val_active, str): val_active = val_active.lower() in ('true', '1', 'ativo')
            else: val_active = bool(val_active)

            return {
                "emails_principal": [e.strip() for e in row.get("emails_principal", "").split(";") if "@" in e],
                "emails_cc": [e.strip() for e in row.get("emails_cc", "").split(";") if "@" in e],
                "move_file": val_move,
                "is_active": val_active
            }
    except Exception as e:
        logger.warning(f"Erro config BQ: {e}")
    return {"emails_principal": ["carlos.lsilva@c6bank.com"], "emails_cc": [], "move_file": False, "is_active": True}

def record_metrics(logger, start_time, end_time, status, error_msg=""):
    if TEST_MODE: return
    try:
        duration = (end_time - start_time).total_seconds()
        metrics = {
            "script_name": SCRIPT_NAME,
            "area_name": AREA_NAME,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": duration,
            "status": status,
            "usuario": ENV_EXEC_USER,
            "modo_exec": ENV_EXEC_MODE,
        }
        pandas_gbq.to_gbq(pd.DataFrame([metrics]), AUTOMACOES_EXEC_TABLE, project_id=PROJECT_ID, if_exists="append", use_bqstorage_api=False)
    except Exception as e:
        logger.error(f"Erro metrics: {e}")

def send_email_outlook(logger, subject, body, to_list, cc_list=None, attachments=None):
    if not to_list or TEST_MODE: return
    try:
        pythoncom.CoInitialize()
        outlook = win32.Dispatch("Outlook.Application")
        mail = outlook.CreateItem(0)
        mail.Subject = subject
        if body: mail.HTMLBody = body
        mail.To = ";".join(to_list)
        if cc_list: mail.CC = ";".join(cc_list)
        if attachments:
            for att in attachments:
                if Path(att).exists(): mail.Attachments.Add(str(att))
        mail.Send()
    except Exception as e:
        logger.error(f"Erro email: {e}")
    finally:
         try: pythoncom.CoUninitialize()
         except: pass

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
# LÓGICA DE NEGÓCIO - PARSER
# ==================================================================================================

class ParserMaster:
    @staticmethod
    def preprocessar_arquivo(arquivo: bytes) -> Tuple[Dict[str, str], Dict[str, List[Tuple[int, int]]]]:
        texto = arquivo.decode("ISO-8859-1", errors="ignore")
        linhas = texto.replace("\r\n", "\n").split("\n")
        cabecalhos = [
            (lin.split()[0], idx)
            for (idx, lin) in enumerate(linhas)
            if lin.replace(" ", "")[-21:] == "MASTERCARDDEBITSWITCH"
        ]
        cods = ["1SWCHD53", "1SWCHD363", "1SWCHD353"]
        arquivos: Dict[str, str] = {}
        indices: Dict[str, List[Tuple[int, int]]] = {}
        for cod in cods:
            ini = 0
            fim = 0
            for idx, cab in enumerate(cabecalhos):
                if cab[0] == cod:
                    ini = idx
                    break
            for jdx in range(ini, len(cabecalhos)):
                cab = cabecalhos[jdx]
                if cab[0] != cod:
                    fim = jdx
                    break
            primeira = cabecalhos[ini][1] if ini < len(cabecalhos) else 0
            ultima = cabecalhos[fim][1] if fim and fim < (len(cabecalhos) - 1) else len(linhas)
            arquivos[cod] = "\n".join(linhas[primeira:ultima])
            indices[cod] = [(cab[1] - primeira, cab[1]) for cab in (cabecalhos[ini:fim] if fim else cabecalhos[ini:])]
        return arquivos, indices

    @staticmethod
    def processar_53(linhas: List[str], cabecalhos: List[Tuple[int, int]]) -> Tuple[pd.DataFrame, pd.DataFrame]:
        df_rows = []
        pat = (
            r"000(\d{16}).*?ADV REASON\s{4}(\d{3})\s(\d{4}).*?ORG SWCH SER\s+(\d+).*?SWCH SERIAL\s+(\d+).*?"
            r"TRACE NO\s+(\d+)\s+ORG DATE\s+(\d{2}-\d{2}).*?ORG AMT\s+(\S+\d{2}).*?REFERENCE NO\s+(\d+).*?"
            r"ACQ INST NAME\s(.{23}).*?NEW AMT\s+(\S+\d{2}).*?RESP CODE-A\s\d{2}\s\d(\d{6}).*?TERMINAL ID\s+(\S+).*?"
            r"ACQ LOC\s(.{23}).*?REV DR AMT\s+(\S+\d{2}).*?ACQ INST ID\s+(\d+)\s+REV CR AMT\s+(\S+\d{2}).*?"
            r"STLMT AMT\s+(\S+\d{2})(\w).*?DOC IND\s+(\d)"
        )
        pat = re.compile(pat, re.S)
        movimentos, paginas = [], []
        for i in range(len(cabecalhos)):
            ini = cabecalhos[i][0] + 5
            fim = cabecalhos[i + 1][0] if i < len(cabecalhos) - 1 else len(linhas)
            bloco_cab = linhas[max(0, ini - 6) : ini + 1]
            mdata = None
            for hdr in bloco_cab:
                mdata = re.search(r"WORK OF[: ]\s*(\d{2}\/\d{2}\/\d{2})", hdr)
                if mdata: break
            movimentos.append(mdata.group(1) if mdata else datetime.now().strftime("%m/%d/%y"))
            paginas.append("\n".join(linhas[ini:fim]))
        
        if not movimentos: return pd.DataFrame(), pd.DataFrame()
        movimento = pd.Series(movimentos).iloc[0]
        dados = [(pag.replace("\n", "").split("0PAN")) for pag in paginas if "0PAN" in pag]
        dados = ["0PAN" + l.replace("\n", "") for lin in dados for l in lin if l.strip()]
        cols = [
            "PAN", "ADV", "REASON", "ORG_SWCH_SER", "SWCH_SERIAL", "NU_NSU_ORIGEM", "DATA_TRANSACAO",
            "VALOR_ORIGINAL", "REFERENCE_NUMBER", "ESTABELECIMENTO", "VALOR_ATUALIZADO", "AUTORIZACAO",
            "TERMINAL_ID", "LOCALIZACAO", "DEBITO", "ADQUIRENTE_ID", "CREDITO", "PROCESSAMENTO",
            "TIPO_PROCESSAMENTO", "IND_DOC",
        ]
        for lin in dados:
            m = pat.search(lin)
            if not m: continue
            df_rows.append(m.groups())
        df = pd.DataFrame(df_rows, columns=cols)
        df["DT_COLETA"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df["MOVIMENTO"] = datetime.strptime(movimento, "%m/%d/%y").strftime("%Y-%m-%d")
        df["RELATORIO"] = "SWCHD53"
        
        cols_ind = ["AUTO_REVERSALS", "ADJUSTMENTS", "CHARGEBACKS", "REVERSED_CHARGEBACKS", "REPRESENTMENTS", "REPRESENTMENT_REVERSALS", "TOTAL"]
        pat_ind = re.compile(r"AUTO REVERSALS\s+ADJUSTMENTS\s+CHARGEBACKS\s+CHARGEBACKS\s+REPRESENTMENTS\s+REVERSALS\s+TOTAL\s+.*?"
                             r"MAESTRO\s\s\s\s\s\s\s\s\s\s\s(.{15})\s(.{15})\s(.{15})\s(.{15})\s(.{18})\s(.{16})\s(.{11})", re.S)
        ind_total = {k: "0" for k in cols_ind}
        if paginas:
            mind = pat_ind.search(paginas[-1])
            if mind:
                vals = [v.replace(",", "").strip() for v in mind.groups()]
                ind_total = dict(zip(cols_ind, vals))
        df_ind = pd.DataFrame([ind_total], columns=cols_ind)
        df_ind["MOVIMENTO"] = datetime.strptime(movimento, "%m/%d/%y").strftime("%Y-%m-%d")
        for c in cols_ind:
            df_ind[c] = pd.to_numeric(df_ind[c], errors="coerce").fillna(0)
        return df, df_ind

    @staticmethod
    def processar_353(linhas: List[str], cabecalhos: List[Tuple[int, int]]) -> Tuple[pd.DataFrame, pd.DataFrame]:
        df_rows = []
        pat = re.compile(
            r".*?(\d{3})\s(.{4})\s+(\d+)\s+(\d+)\s+(\d{2}-\d{2})\s+(\d+)\s+(\d+)\s+(\w+)\s+(\d{3})\s(.{15})\s(.{13})(\w)\s+"
            r"(\d{2})\s(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s(\w{3})\s+(\d+)\s+(\d+)\s(.{15})\s(.{13})(\w)"
        )
        movimentos, paginas = [], []
        for i in range(len(cabecalhos)):
            ini = cabecalhos[i][0] + 5
            fim = cabecalhos[i + 1][0] if i < len(cabecalhos) - 1 else len(linhas)
            bloco_cab = linhas[max(0, ini - 6) : ini + 1]
            mdata = None
            for hdr in bloco_cab:
                mdata = re.search(r"WORK OF[: ]\s*(\d{2}\/\d{2}\/\d{2})", hdr)
                if mdata: break
            if not mdata:
                mdata = re.search(r"WORK OF\s*(\d{2}\/\d{2}\/\d{2})", " ".join(bloco_cab))
            movimentos.append(mdata.group(1) if mdata else datetime.now().strftime("%m/%d/%y"))
            paginas.append("\n".join(linhas[ini:fim]))
        
        if not movimentos: return pd.DataFrame(), pd.DataFrame()
        movimento = pd.Series(movimentos).iloc[0]
        dados = [(pag.split("PAN ")) for pag in paginas if "PAN" in pag]
        dados = ["PAN " + l for lin in dados for l in lin]
        dados = [re.split(r"\n\s*\n", d) for d in dados]
        registros = [rec for d in dados for rec in d[1:] if rec.strip()]
        cols = ["ADV", "REASON", "SWCH_SERIAL", "NEW_TRACE_NO", "LOCAL_DATE", "INST_ID", "PROC_CODE", "REFERENCE_NO",
                "TRANS_CURR", "ORG_AMT", "REV_AMT", "REV_AMT_CD", "RESP", "CODE", "ORG_SWCH_SERIAL", "ORG_TRACE_NO",
                "LOCAL_TIME", "PROC_ID", "BRAND", "CONV_RATE", "STLMT_CURR", "NEW_AMT", "STLMT_AMT", "STLMT_AMT_CD"]
        for lin in registros:
            m = re.search(pat, lin)
            if not m: continue
            df_rows.append(m.groups())
        df = pd.DataFrame(df_rows, columns=cols)
        df["DT_COLETA"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df["MOVIMENTO"] = datetime.strptime(movimento, "%m/%d/%y").strftime("%Y-%m-%d")
        df["RELATORIO"] = "SWCHD353"
        cols_ind = ["AUTO_REVERSALS", "ADJUSTMENTS", "CHARGEBACKS", "REPRESENTMENTS", "REPRESENTMENT_REVERSALS", "PREAUTH_NONFIN", "TOTAL"]
        pat_ind = re.compile(r"AUTO REVERSALS\s+ADJUSTMENTS\s+CHARGEBACKS\s+REPRESENTMENTS\s+REVERSALS\s+NON-FIN\s+TOTAL\s+MAESTRO\s\s\s\s\s\s\s\s\s\s\s\s(.{15})\s(.{15})\s(.{15})\s\s\s\s\s\s(.{15})\s\s(.{15})\s(.{12})\s(.{12})")
        ind_total = {k: "0" for k in cols_ind}
        if paginas:
            m_ind = re.search(pat_ind, paginas[-1])
            if m_ind:
                vals = [v.replace(",", "").strip() for v in list(m_ind.groups())]
                ind_total = dict(zip(cols_ind, vals))
        df_ind = pd.DataFrame([ind_total], columns=cols_ind)
        df_ind["MOVIMENTO"] = datetime.strptime(movimento, "%m/%d/%y").strftime("%Y-%m-%d")
        for c in cols_ind:
            df_ind[c] = pd.to_numeric(df_ind[c], errors="coerce").fillna(0)
        return df, df_ind

    @staticmethod
    def processar_363(linhas: List[str], cabecalhos: List[Tuple[int, int]]) -> Dict[str, List[pd.DataFrame]]:
        paginas, paginas_info = [], []
        linhas = [li[:5].replace("0", " ").replace("-", " ") + li[5:] for li in linhas]
        for i in range(len(cabecalhos)):
            borda = cabecalhos[i][0]
            fim = cabecalhos[i + 1][0] if i < len(cabecalhos) - 1 else len(linhas)
            ini = min((b for b in range(borda, fim) if linhas[b].strip()[:4] in ["DESC", "ACQU"] and ":" not in linhas[b]), default=borda+5)
            info = {}
            try:
                info["MOVIMENTO"] = re.search(r"WORK OF: (\d{2}\/\d{2}\/\d{2})", linhas[borda + 1]).group(1)
                titulo = linhas[borda + 2]
                info["TITULO_PAGINA"] = titulo[: titulo.index("PAGE")].strip()
                info["NUMERO_PAGINA"] = int(re.search(r"PAGE:\s*(\d+)", titulo).group(1))
                info["TIPO_PAGINA"] = "C" if info["TITULO_PAGINA"] == "NET SETTLEMENT SUMMARY" else "A"
                if info["TIPO_PAGINA"] == "A":
                    hdr = linhas[ini].split()
                    info["TIPO_PAGINA"] = "B" if hdr and hdr[0] in ["DESCRIPTION", "DEBITS", "CREDITS"] else "A"
                paginas_info.append(info)
                paginas.append("\n".join(linhas[ini:fim]))
            except: continue

        colunas = {
            "A": ["DESCRIPTION", "DEBITS_NUMBER", "DEBITS_AMOUNT", "CREDITS_NUMBER", "CREDITS_AMOUNT"],
            "B": ["DESCRIPTION", "TRANSACTIONS_NUMBER_APPROVED", "TRANSACTIONS_NUMBER_DENIALS", "FINANCIAL_AMOUNT_TRANS", "INTERCHANGE_COUNTS_FINANCIAL", "INTERCHANGE_COUNTS_PCT_BASED", "INTERCHANGE_COUNTS_NONFIN", "INTERCHANGE_COUNTS_NONBILL", "INTERCHANGE_AMOUNT"],
            "C1": ["DESCRIPTION", "DEBIT", "CREDIT", "NET"],
            "C2": ["DESCRIPTION", "NETACQ", "FEEACQ", "NETISS", "FEEISS"],
        }
        quebras = {"A": [0, 32, 44, 64, 83, 103], "B": [0, 22, 32, 42, 63, 73, 83, 93, 103, 130], "C1": [0, 35, 53, 84, 120], "C2": [0, 35, 45, 64, 84, 104]}
        
        def numeros_espacos(lin: str) -> int: return min((i for i in range(len(lin)) if lin[i] != " "), default=0)

        def tratar_generico(pag, info, tipo, k_quebra, k_col):
            dados = []
            linhas_pag = [li for li in pag.split("\n")[2:] if li.strip()]
            prefixo = ""
            for lin in linhas_pag:
                if tipo in ["B", "C"] and sum(1 for c in lin.strip() if c != "-") == 0: continue
                num_esp = numeros_espacos(lin)
                campos = [lin[i:j].strip() for i, j in zip(quebras[k_quebra], quebras[k_quebra][1:])]
                if num_esp <= 2:
                    if sum(1 for c in campos if c and (c not in colunas[k_col] if tipo=="C" else True)) == 1:
                        prefixo = lin.strip().replace(":", "") if tipo=="B" or tipo=="C" else lin.strip()
                        if tipo=="C": prefixo = lin.split()[0].strip().replace(":", "")
                        continue
                    else: prefixo = ""
                if prefixo: campos[0] = prefixo + "_" + campos[0]
                row = {c: v for c, v in zip(colunas[k_col], campos)}
                row.update(info)
                dados.append(row)
            return dados

        dfs = {"A": [], "B": [], "C1": [], "C2": []}
        for pag, info in zip(paginas, paginas_info):
            if info["TIPO_PAGINA"] == "A": dfs["A"].append(pd.DataFrame(tratar_generico(pag, info, "A", "A", "A")))
            elif info["TIPO_PAGINA"] == "B": dfs["B"].append(pd.DataFrame(tratar_generico(pag, info, "B", "B", "B")))
        return dfs

    @staticmethod
    def bytes_candidato_zip(zf: zipfile.ZipFile) -> Optional[bytes]:
        candidatos = []
        for info in zf.infolist():
            if info.is_dir(): continue
            try:
                with zf.open(info) as f: data = f.read()
                texto = data.decode("ISO-8859-1", errors="ignore")
                score = 0
                if "MASTERCARDDEBITSWITCH" in texto.replace(" ", ""): score += 3
                for tag in ("1SWCHD53", "1SWCHD363", "1SWCHD353"):
                    if tag in texto: score += (2 if tag == "1SWCHD353" else 1)
                candidatos.append((score, len(data), data, info.filename))
            except: pass
        if not candidatos: return None
        candidatos.sort(key=lambda x: (x[0], x[1]), reverse=True)
        return candidatos[0][2]

    @staticmethod
    def tratar_dataframe(arquivo_bytes: bytes) -> Dict[str, pd.DataFrame]:
        arquivos, indices = ParserMaster.preprocessar_arquivo(arquivo_bytes)
        out = {k: pd.DataFrame() for k in ["1SWCHD53", "1SWCHD53_IND", "1SWCHD363", "1SWCHD353", "1SWCHD353_IND"]}
        if "1SWCHD53" in arquivos:
            df53, df53ind = ParserMaster.processar_53(arquivos["1SWCHD53"].split("\n"), indices["1SWCHD53"])
            out["1SWCHD53"], out["1SWCHD53_IND"] = df53, df53ind
        if "1SWCHD363" in arquivos:
            dfs363 = ParserMaster.processar_363(arquivos["1SWCHD363"].split("\n"), indices["1SWCHD363"])
            df363B = pd.concat(dfs363["B"], ignore_index=True) if dfs363["B"] else pd.DataFrame()
            if not df363B.empty and "MOVIMENTO" in df363B.columns:
                df363B["MOVIMENTO"] = pd.to_datetime(df363B["MOVIMENTO"], format="%m/%d/%y", errors="coerce").dt.strftime("%Y-%m-%d")
            out["1SWCHD363"] = df363B
        if "1SWCHD353" in arquivos:
            df353, df353ind = ParserMaster.processar_353(arquivos["1SWCHD353"].split("\n"), indices["1SWCHD353"])
            out["1SWCHD353"], out["1SWCHD353_IND"] = df353, df353ind
        return out

    @staticmethod
    def procurar_arquivos(base_dir: Path) -> List[Path]:
        if not base_dir.exists(): return []
        return sorted([p for p in base_dir.iterdir() if p.is_file() and p.suffix.lower() in (".zip", ".txt")])

class BQOps:
    @staticmethod
    def subir_bq(dfs: Dict[str, pd.DataFrame], logger) -> None:
        for nome, df in dfs.items():
            if nome not in DESTINO_MAP or df is None or df.empty: continue
            pandas_gbq.to_gbq(df.copy(), destination_table=DESTINO_MAP[nome], project_id=PROJECT_ID, if_exists=SUBIDA_BQ, use_bqstorage_api=False)
            logger.info(f"Subido {DESTINO_MAP[nome]}: {len(df)} linhas")

    @staticmethod
    def aplicar_distinct_em_todas(logger) -> None:
        client = bigquery.Client(project=PROJECT_ID)
        tabelas = sorted(set(list(DESTINO_MAP.values())))
        for table_fqn in tabelas:
            try:
                tbl = client.get_table(table_fqn)
                # Keep logic simple without credentials param reuse
                sql = f"CREATE OR REPLACE TABLE `{table_fqn}` AS SELECT DISTINCT * FROM `{table_fqn}`"
                client.query(sql).result()
                logger.info(f"Distinct aplicado: {table_fqn}")
            except Exception as e:
                logger.error(f"Erro distinct {table_fqn}: {e}")

    @staticmethod
    def rodar_procedures(logger) -> None:
        client = bigquery.Client(project=PROJECT_ID)
        try:
            sql = "CALL `datalab-pagamentos.ARQUIVOS_MASTERCARD.SWCHD_PROCEDURE`()"
            logger.info(f"Chamando Procedure: {sql}")
            client.query(sql).result()
            logger.info("Procedure Final Executada.")
        except Exception as e:
            logger.error(f"Erro Procedure Final: {e}")

# ==================================================================================================
# MAIN
# ==================================================================================================

def main():
    start_time = datetime.now(TZ)
    logger, log_path = setup_logger()
    logger.info(f"Inicio {SCRIPT_NAME}")
    
    status = "SUCESSO"
    error_msg = ""
    arquivos_anexos = []
    
    try:
        config = load_config(logger)
        
        # Files search
        arquivos = ParserMaster.procurar_arquivos(INPUT_DIR)
        
        if not arquivos:
            status = "NO_DATA"
            logger.info("Nenhum arquivo encontrado.")
        else:
            agregados = {k: [] for k in DESTINO_MAP.keys()}
            precisa_procedure_363 = False
            total_ok = 0
            
            for f in arquivos:
                try:
                    logger.info(f"Processando: {f.name}")
                    conteudo = None
                    if f.suffix.lower() == ".zip":
                        try:
                            with zipfile.ZipFile(str(f), "r") as zf:
                                conteudo = ParserMaster.bytes_candidato_zip(zf)
                        except: pass
                    if conteudo is None: conteudo = f.read_bytes()
                    
                    dfs_all = ParserMaster.tratar_dataframe(conteudo)
                    
                    tem_dados = False
                    for k, df in dfs_all.items():
                        if not df.empty:
                            agregados[k].append(df)
                            tem_dados = True
                    
                    if not dfs_all["1SWCHD363"].empty:
                        precisa_procedure_363 = True
                        
                    if tem_dados:
                        total_ok += 1
                        # Move to Done
                        try:
                            shutil.move(str(f), str(JA_FEITOS_DIR / f.name))
                        except: pass
                    else:
                        logger.warning(f"Arquivo {f.name} vazio ou nao reconhecido.")
                        
                except Exception as ex:
                    logger.error(f"Erro arquivo {f.name}: {ex}")

            if total_ok > 0:
                dfs_upload = {k: pd.concat(v, ignore_index=True) for k, v in agregados.items() if v}
                if dfs_upload:
                    BQOps.subir_bq(dfs_upload, logger)
                    if precisa_procedure_363:
                        BQOps.rodar_procedures(logger)
                    BQOps.aplicar_distinct_em_todas(logger)
            else:
                status = "NO_DATA"

    except Exception as e:
        status = "ERRO"
        error_msg = str(e)
        logger.error(f"Fatal: {traceback.format_exc()}")
        
    end_time = datetime.now(TZ)
    
    # Simple email body
    zip_path = smart_zip_logs(arquivos_anexos)
    body = f"""
    <html><body>
    <h2>Execução {SCRIPT_NAME}</h2>
    <p>Status: {status}</p>
    </body></html>
    """
    send_email_outlook(logger, f"{SCRIPT_NAME} - {status}", body, config["emails_principal"], config["emails_cc"], [zip_path])
    record_metrics(logger, start_time, end_time, status, error_msg)

if __name__ == "__main__":
    if "--test" in sys.argv: TEST_MODE = True
    main()
