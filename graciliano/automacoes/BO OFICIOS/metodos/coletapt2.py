# ==============================================================================
# AUTO-INSTALL DEPENDENCIES & CONFIG
# ==============================================================================
import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
import time
import shutil
import traceback
import logging
import zipfile
import re
import getpass
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

# Define Root Path (approximated)
HOME = Path.home()
POSSIBLE_ROOTS = [
    HOME / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A",
    HOME / "Meu Drive/C6 CTVM",
    HOME / "C6 CTVM",
]
ROOT_DRIVE = next((p for p in POSSIBLE_ROOTS if p.exists()), HOME / "C6 CTVM")

try:
    import bootstrap_deps
    SCRIPT_DEPS = [
        "pandas",
        "pandas-gbq",
        "pywin32",
        "google-cloud-bigquery",
        "pydata-google-auth",
        "openpyxl",
        "python-docx"
    ]
    bootstrap_deps.install_requirements(extra_libs=SCRIPT_DEPS)
except: pass

import pandas as pd
import pandas_gbq
import win32com.client as win32
from google.cloud import bigquery
from pydata_google_auth import cache, get_user_credentials

# ==============================================================================
# CONSTANTES DO SCRIPT
# ==============================================================================
SCRIPT_NAME = Path(__file__).stem.upper()
SCRIPT_NAME_LOWER = SCRIPT_NAME.lower()
START_TIME = datetime.now().replace(microsecond=0)
AREA_NAME = "BO OFICIOS"

PROJECT_ID = "datalab-pagamentos"
TABLE_CONFIG = f"{PROJECT_ID}.ADMINISTRACAO_CELULA_PYTHON.registro_automacoes"
TABLE_EXEC = f"{PROJECT_ID}.ADMINISTRACAO_CELULA_PYTHON.automacoes_exec"

# Imports de Módulos Legados (preservando caminhos)
try:
    NOVO_SERVIDOR_DIR = ROOT_DRIVE / "Mensageria e Cargas Operacionais - 11.CelulaPython/graciliano/novo_servidor"
    if not NOVO_SERVIDOR_DIR.exists(): NOVO_SERVIDOR_DIR = ROOT_DRIVE / "graciliano/novo_servidor"
    
    sys.path.append(str(NOVO_SERVIDOR_DIR))
    sys.path.append(str(NOVO_SERVIDOR_DIR / "modules"))
    
    NCPLIB_DIR = ROOT_DRIVE / "Mensageria e Cargas Operacionais - 11.CelulaPython/graciliano/ncplib"
    if not NCPLIB_DIR.exists(): NCPLIB_DIR = ROOT_DRIVE / "graciliano/ncplib"
    sys.path.append(str(NCPLIB_DIR))

    from modules.TratamentoDados1 import TratamentoDados
    from modules.GerarExtratos import Extrato
    from modules.PAC import PAC
    from modules.Machadao1 import Machadao
except ImportError as e:
    print(f"Erro importando modulos legados: {e}")

# Diretórios
TEMP_DIR = Path(os.getenv('TEMP', Path.home())) / "C6_RPA_EXEC" / SCRIPT_NAME
TEMP_DIR.mkdir(parents=True, exist_ok=True)

PASTA_GERACAO_TEMP = Path.home() / "Downloads" / "PASTAS_OFICIOS_TMP"
SEGREGADAS_ROOT = Path.home() / "Downloads" / "PASTA_SEGREGADAS"
COPIA_XLSX = TEMP_DIR / f"base_processada_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

NETWORK_ROOT = ROOT_DRIVE / "Catarina Cristina Bernardes De Freitas - Célula Python - Relatórios de Execução/Wall.B/PASTAS OFICIOS"
if not NETWORK_ROOT.exists(): NETWORK_ROOT = ROOT_DRIVE 

GLOBAL_CONFIG = {'area_name': AREA_NAME, 'emails_principal': [], 'emails_cc': [], 'move_file': False}

# Contadores Globais
COUNTERS = {
    'PASTAS': 0, 'CARTAS': 0, 'EXTRATOS': 0, 'MACHADAO': 0, 'PACS': 0
}

# ==============================================================================
# SETUP LOGGING
# ==============================================================================
def setup_logger():
    logger = logging.getLogger(SCRIPT_NAME)
    logger.setLevel(logging.INFO)
    logger.handlers = []
    
    fmt = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
    
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    
    log_file = TEMP_DIR / f"{SCRIPT_NAME}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    fh = logging.FileHandler(log_file, encoding='utf-8')
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    
    return logger, log_file

LOGGER, LOG_FILE = setup_logger()

# ==============================================================================
# CREDENCIAIS & BIGQUERY
# ==============================================================================
SCOPES = ["https://www.googleapis.com/auth/bigquery"]
CREDENTIALS = None

if not CREDENTIALS:
    try:
        TOKENS_DIR = Path.home() / "AppData" / "Roaming" / "CELPY" / "tokens"
        CREDENTIALS = get_user_credentials(SCOPES, credentials_cache=cache.ReadWriteCredentialsCache(str(TOKENS_DIR)), auth_local_webserver=True)
        pandas_gbq.context.credentials = CREDENTIALS
        pandas_gbq.context.project = PROJECT_ID
    except: pass

# ==============================================================================
# CLASSE PRINCIPAL
# ==============================================================================
class AutomationTask:
    def __init__(self):
        self.output_files = []

    def get_configs(self):
        try:
            query = f"""
                SELECT emails_principal, emails_cc, move_file 
                FROM `{TABLE_CONFIG}`
                WHERE lower(TRIM(script_name)) = lower('{SCRIPT_NAME_LOWER}')
                ORDER BY created_at DESC LIMIT 1
            """
            try:
                df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID)
            except:
                query = query.replace(f"lower('{SCRIPT_NAME_LOWER}')", f"lower('{AREA_NAME.lower()}')")
                df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID)

            if not df.empty:
                GLOBAL_CONFIG['emails_principal'] = [x.strip() for x in str(df.iloc[0]['emails_principal']).replace(';', ',').split(',') if '@' in x]
                GLOBAL_CONFIG['emails_cc'] = [x.strip() for x in str(df.iloc[0]['emails_cc']).replace(';', ',').split(',') if '@' in x]
            else:
                GLOBAL_CONFIG['emails_principal'] = ["carlos.lsilva@c6bank.com"]
        except Exception as e:
            LOGGER.error(f"Erro configs: {e}")

    def run(self):
        self.get_configs()
        modo_exec = os.environ.get("ENV_EXEC_MODE", "AGENDAMENTO")
        usuario_exec = os.environ.get("ENV_EXEC_USER", f"{os.getlogin().lower()}@c6bank.com")
        status = "ERROR"
        
        args_cartas = "--cartas-only" in sys.argv
        args_machadao = "--machadao" in sys.argv

        try:
            LOGGER.info(">>> INICIO <<<")
            
            df = self.carregar_base()
            if df.empty:
                status = "NO_DATA"
                LOGGER.info("Sem dados WallB.")
            else:
                PASTA_GERACAO_TEMP.mkdir(parents=True, exist_ok=True)
                
                # Instancia TratamentoDados (Legado)
                # IMPORTANTE: Presume que TratamentoDados nao depende de config_loader
                tratador = TratamentoDados(PASTA_GERACAO_TEMP, substituir_tudo=True)
                
                ctrls = df['numero_controle_envio'].unique()
                LOGGER.info(f"Controles a processar: {len(ctrls)}")
                
                with ThreadPoolExecutor(max_workers=min(2, len(ctrls))) as exe:
                    futures = [
                        exe.submit(self.processar_controle, c, df[df['numero_controle_envio']==c].copy(), tratador, args_cartas, args_machadao) 
                        for c in ctrls
                    ]
                    for f in as_completed(futures):
                        f.result()
                
                msg = f"Fim. Cartas: {COUNTERS['CARTAS']}, Extratos: {COUNTERS['EXTRATOS']}, Mach: {COUNTERS['MACHADAO']}"
                LOGGER.info(msg)
                status = "SUCCESS"

        except Exception as e:
            status = "ERROR"
            LOGGER.critical(f"Erro fatal: {e}", exc_info=True)
            
        finally:
            end_time = datetime.now().replace(microsecond=0)
            duration = round((end_time - START_TIME).total_seconds(), 2)
            zip_path = self._create_smart_zip()
            self._upload_metrics(status, usuario_exec, modo_exec, end_time, duration)
            self._send_email(status, zip_path)

    def carregar_base(self):
        # Lógica original de filtro
        coleta_mes = "DEZEMBRO_2025" # Hardcoded no original, mantendo mas deveria ser dinâmico? Original: "DEZEMBRO_2025"
        # Vou tentar ver se tem ENV var, senao deixa default
        coleta_mes = os.environ.get("COLETA_BANKING", coleta_mes)
        
        filt = "TRUE"
        if "_" in coleta_mes:
            try:
                ms, ys = coleta_mes.split("_")
                mm = {"JANEIRO":1,"FEVEREIRO":2,"MARCO":3,"MARÇO":3,"ABRIL":4,"MAIO":5,"JUNHO":6,"JULHO":7,"AGOSTO":8,"SETEMBRO":9,"OUTUBRO":10,"NOVEMBRO":11,"DEZEMBRO":12}
                filt = f"EXTRACT(YEAR FROM dt_bacen)={ys} AND EXTRACT(MONTH FROM dt_bacen)={mm.get(ms.upper(),0)}"
            except: pass
            
        sql = f"""
        WITH base AS (
          SELECT *, COALESCE(SAFE.PARSE_DATE('%d/%m/%Y', SUBSTR(CAST(data_bacen AS STRING), 1, 10)), SAFE.PARSE_DATE('%Y-%m-%d', SUBSTR(CAST(data_bacen AS STRING), 1, 10))) AS dt_bacen
          FROM `datalab-pagamentos.ADMINISTRACAO_CELULA_PYTHON.WallB_casos`
        )
        SELECT * FROM base WHERE dt_bacen IS NOT NULL AND status_caso = 'ABERTO' AND {filt}
        """
        LOGGER.info(f"Carregando base com filtro: {filt}")
        df = pandas_gbq.read_gbq(sql, project_id=PROJECT_ID)
        
        if not df.empty:
            df["numero_controle_envio"] = df["numero_controle_envio"].astype(str).str.split(".").str[0].str.strip()
            df["numero_conta_limpa"] = pd.to_numeric(df["numero_conta"], errors='coerce').fillna(0).astype(int)
            try: df.to_excel(COPIA_XLSX, index=False)
            except: pass
            self.output_files.append(COPIA_XLSX)
            
        return df

    def processar_controle(self, ctrl, subdf, tratador, cartas_only, mach_only):
        try:
            pasta_ctrl = PASTA_GERACAO_TEMP / ctrl
            if not pasta_ctrl.exists():
                pasta_ctrl.mkdir(parents=True, exist_ok=True)
                COUNTERS['PASTAS'] += 1

            LOGGER.info(f"Processando controle: {ctrl}")

            if not mach_only:
                try:
                    tratador.criarCartas(subdf, pasta_ctrl)
                    COUNTERS['CARTAS'] += len(subdf)
                except Exception as e: LOGGER.error(f"Erro Cartas {ctrl}: {e}")

            if not cartas_only and not mach_only:
                df_ext = subdf[subdf['ordem_oficio'].astype(str).str.lower().str.contains('extrato', na=False)].copy()
                valid_idxs = [i for i, c in enumerate(df_ext['numero_conta_limpa'].tolist()) if c > 0]
                if valid_idxs:
                    try:
                        contas = [int(df_ext.iloc[i]['numero_conta_limpa']) for i in valid_idxs]
                        ini = [str(df_ext.iloc[i]['data_inicio_oficio']) for i in valid_idxs]
                        fim = [str(df_ext.iloc[i]['data_fim_oficio']) for i in valid_idxs]
                        ctrls_list = [ctrl] * len(contas)
                        tratador.criarExtratos(contas, ini, fim, ctrls_list, str(pasta_ctrl))
                        COUNTERS['EXTRATOS'] += len(contas)
                    except Exception as e: LOGGER.error(f"Erro Extratos {ctrl}: {e}")

            if not cartas_only:
                df_mac = subdf[subdf['ordem_oficio'].astype(str).str.lower().str.contains('cartacircular3454', na=False) & (subdf['caso_outros']=='NAO') & (subdf['possui_relacionamento']=='SIM') & (subdf['numero_conta_limpa']>0)].copy()
                if not df_mac.empty:
                    try:
                        contas = df_mac['numero_conta_limpa'].astype(int).tolist()
                        mc = Machadao(contas, df_mac['data_inicio_oficio'].astype(str).tolist(), df_mac['data_fim_oficio'].astype(str).tolist(), pasta_ctrl, [ctrl]*len(contas), substituir_tudo=True)
                        mc.assis(oficio_ccs=False)
                        COUNTERS['MACHADAO'] += 1
                    except Exception as e: LOGGER.error(f"Erro Machadao {ctrl}: {e}")

            if not cartas_only and not mach_only:
                df_pac = subdf[subdf['ordem_oficio'].astype(str).str.lower().str.contains('propostaaberturaconta', na=False)]
                for row in df_pac.itertuples():
                    try:
                        cpf = str(row.cnpj_cpf_cliente).strip().replace('.0','')
                        if cpf:
                            PAC([cpf], endereco_salvar=str(pasta_ctrl)).criar_pac()
                            COUNTERS['PACS'] += 1
                    except: pass

            self._backup_pastas(subdf, pasta_ctrl, ctrl)
            
        except Exception as e:
            LOGGER.error(f"Erro Fatal no controle {ctrl}: {e}")

    def _meses_do_subdf(self, df):
        m = set()
        col = next((c for c in ["dt_bacen", "data_bacen", "data_movimento_oficio"] if c in df.columns), None)
        if not col: return m
        for d in pd.to_datetime(df[col], errors='coerce').dropna():
            mn = {1:"JANEIRO",2:"FEVEREIRO",3:"MARÇO",4:"ABRIL",5:"MAIO",6:"JUNHO",7:"JULHO",8:"AGOSTO",9:"SETEMBRO",10:"OUTUBRO",11:"NOVEMBRO",12:"DEZEMBRO"}.get(d.month, "DESCONHECIDO")
            m.add(f"CASOS_{mn}_{d.year}")
        return m

    def _backup_pastas(self, subdf, pasta_ctrl, ctrl):
        for mp in self._meses_do_subdf(subdf):
            try:
                d_loc = SEGREGADAS_ROOT / mp / ctrl
                self._cp_tree(pasta_ctrl, d_loc)
                if NETWORK_ROOT.exists(): 
                   self._cp_tree(d_loc, NETWORK_ROOT / mp / ctrl)
            except: pass

    def _cp_tree(self, s, d):
        if not s.exists(): return
        d.mkdir(parents=True, exist_ok=True)
        for i in s.iterdir():
            try:
                if i.is_file(): shutil.copy2(i, d / i.name)
                elif i.is_dir(): self._cp_tree(i, d / i.name)
            except: pass

    def _create_smart_zip(self):
        zip_path = TEMP_DIR / f"{SCRIPT_NAME}_{datetime.now().strftime('%H%M%S')}.zip"
        try:
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
                if LOG_FILE.exists(): zf.write(LOG_FILE, LOG_FILE.name)
                for f in self.output_files:
                    if f.exists(): zf.write(f, f.name)
        except: pass
        return zip_path

    def _upload_metrics(self, status, user, mode, end, duration):
        try:
            df = pd.DataFrame([{
                "script_name": SCRIPT_NAME,
                "area_name": GLOBAL_CONFIG['area_name'],
                "start_time": START_TIME,
                "end_time": end,
                "duration_seconds": duration,
                "status": status,
                "usuario": user,
                "modo_exec": mode
            }])
            pandas_gbq.to_gbq(df, TABLE_EXEC, project_id=PROJECT_ID, if_exists='append')
        except: pass

    def _send_email(self, status, zip_path):
        try:
            to = GLOBAL_CONFIG['emails_principal']
            if status=="SUCCESS": to += GLOBAL_CONFIG['emails_cc']
            if not to: return

            import pythoncom
            pythoncom.CoInitialize()
            outlook = win32.Dispatch("Outlook.Application")
            mail = outlook.CreateItem(0)
            mail.To = ";".join(set(to))
            mail.Subject = f"CÉLULA PYTHON MONITORAÇÃO - {SCRIPT_NAME} - {status}"
            mail.Body = f"Status: {status}\nCartas: {COUNTERS['CARTAS']}\nExtratos: {COUNTERS['EXTRATOS']}"
            if zip_path.exists(): mail.Attachments.Add(str(zip_path))
            mail.Send()
        except: pass

if __name__ == "__main__":
    AutomationTask().run()