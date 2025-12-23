# ==============================================================================
# AUTO-INSTALL DEPENDENCIES & CONFIG
# ==============================================================================
import sys
import os
from pathlib import Path
from datetime import datetime
import time
import shutil
import traceback
import logging
import zipfile
import re
import unicodedata

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
        "xlsxwriter"
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
SCRIPT_NAME = Path(__file__).stem.lower()
START_TIME = datetime.now().replace(microsecond=0)
AREA_NAME = "BO INVESTIMENTOS"

# Project Configs
PROJECT_ID = "datalab-pagamentos"
DATASET_ID = "bo_investimentos"
# This script processes files and outputs TXT, does not seem to upload data content to BQ, just metrics.

TABLE_CONFIG = f"{PROJECT_ID}.ADMINISTRACAO_CELULA_PYTHON.registro_automacoes"
TABLE_EXEC = f"{PROJECT_ID}.ADMINISTRACAO_CELULA_PYTHON.automacoes_exec"

# Diretórios
TEMP_DIR = Path(os.getenv('TEMP', Path.home())) / "C6_RPA_EXEC" / SCRIPT_NAME
TEMP_DIR.mkdir(parents=True, exist_ok=True)

INPUT_DIR = ROOT_DRIVE / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "automacoes" / AREA_NAME / "arquivos input" / "MOVIMENTACOES"
if not INPUT_DIR.exists():
    INPUT_DIR = ROOT_DRIVE / "graciliano" / "automacoes" / AREA_NAME / "arquivos input" / "MOVIMENTACOES"

OUTPUT_DIR = Path.home() / "Downloads" / "ARQUIVOS_SAIDA"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

GLOBAL_CONFIG = {'area_name': AREA_NAME, 'emails_principal': [], 'emails_cc': [], 'move_file': False}

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
                WHERE lower(TRIM(script_name)) = lower('{SCRIPT_NAME}')
                ORDER BY created_at DESC LIMIT 1
            """
            try:
                df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID)
            except:
                query = query.replace(f"lower('{SCRIPT_NAME}')", f"lower('{AREA_NAME.lower()}')")
                df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID)

            if not df.empty:
                GLOBAL_CONFIG['emails_principal'] = [x.strip() for x in str(df.iloc[0]['emails_principal']).replace(';', ',').split(',') if '@' in x]
                GLOBAL_CONFIG['emails_cc'] = [x.strip() for x in str(df.iloc[0]['emails_cc']).replace(';', ',').split(',') if '@' in x]
                GLOBAL_CONFIG['move_file'] = bool(df.iloc[0].get('move_file', False))
            else:
                GLOBAL_CONFIG['emails_principal'] = ["carlos.lsilva@c6bank.com"]
        except Exception as e:
            LOGGER.error(f"Erro configs: {e}")

    def run(self):
        self.get_configs()
        modo_exec = os.environ.get("ENV_EXEC_MODE", "AGENDAMENTO")
        usuario_exec = os.environ.get("ENV_EXEC_USER", f"{os.getlogin().lower()}@c6bank.com")
        
        status = "ERROR"
        
        try:
            LOGGER.info(">>> INICIO <<<")
            LOGGER.info(f"Input: {INPUT_DIR}")
            LOGGER.info(f"Output: {OUTPUT_DIR}")
            
            # Arquivos Modelo
            arquivo_input_xlsx = INPUT_DIR / "MOVIMENTACOES.xlsx"
            arquivo_layout_btg = INPUT_DIR / "LAYOUT_BTG.txt"
            arquivo_layout_mellon = INPUT_DIR / "LAYOUT_MELLON.txt"
            
            if not arquivo_input_xlsx.exists():
                raise FileNotFoundError(f"Arquivo não encontrado: {arquivo_input_xlsx}")
            
            # Leitura
            header_btg = self._ler_arquivo_header(arquivo_layout_btg)
            header_mellon = self._ler_arquivo_header(arquivo_layout_mellon)
            
            if not header_btg or not header_mellon:
                raise ValueError("Falha ao ler headers dos layouts.")
            
            try:
                df_dados = pd.read_excel(arquivo_input_xlsx)
            except PermissionError:
                raise PermissionError("Arquivo Excel aberto. Feche-o e tente novamente.")
                
            # Processamento
            arquivos_gerados = self._processar_dados(df_dados, header_btg, header_mellon)
            
            if arquivos_gerados:
                status = "SUCCESS"
                LOGGER.info(f"Arquivos gerados: {arquivos_gerados}")
                try: os.startfile(OUTPUT_DIR)
                except: pass
            else:
                status = "NO_DATA"

        except Exception as e:
            status = "ERROR"
            LOGGER.critical(f"Erro fatal: {e}", exc_info=True)
            
        finally:
            end_time = datetime.now().replace(microsecond=0)
            duration = round((end_time - START_TIME).total_seconds(), 2)
            
            zip_path = self._create_smart_zip()
            self._upload_metrics(status, usuario_exec, modo_exec, end_time, duration)
            self._send_email(status, zip_path)

    def _ler_arquivo_header(self, caminho):
        try:
            try:
                with open(caminho, 'r', encoding='utf-8') as f: return f.readline().strip()
            except UnicodeDecodeError:
                with open(caminho, 'r', encoding='latin-1') as f: return f.readline().strip()
        except Exception as e:
            LOGGER.error(f"Erro header {caminho}: {e}")
            return None

    def _sanitizar_nome(self, nome):
        return re.sub(r'[<>:"/\\|?*]', '', str(nome)).strip()

    def _formatar_valor(self, valor, is_negativo=False):
        try:
            if pd.isna(valor) or valor == '': return "0"
            val_float = float(str(valor).replace('R$', '').replace(' ', '').replace('.', '').replace(',', '.')) if isinstance(valor, str) else float(valor)
            val_abs = abs(val_float)
            val_str = f"{val_abs:.8f}".rstrip('0').rstrip('.').replace('.', ',')
            if val_str == "": val_str = "0"
            if is_negativo and val_abs > 0: return "-" + val_str
            return val_str
        except: return "0"

    def _processar_dados(self, df, header_btg, header_mellon):
        df = df.dropna(subset=['Gestores', 'Ação'])
        gestores = df['Gestores'].unique()
        count = 0
        
        mapa_acoes = {
            "APLICAÇÃO": "A", "APLICACAO": "A", "RESGATE PARCIAL": "RI",
            "RESGATE TOTAL": "RI", "RETIRADA COME COTAS": "RI",
            "RETIRADA DE COTAS": "RI", "RESGATE": "RI"
        }
        
        for gestor in gestores:
            s_gestor = self._sanitizar_nome(gestor)
            df_gestor = df[df['Gestores'] == gestor]
            acoes = df_gestor['Ação'].unique()
            
            for acao in acoes:
                df_final = df_gestor[df_gestor['Ação'] == acao].copy()
                if df_final.empty: continue
                
                adm = str(df_final.iloc[0].get('ADM', '')).strip().upper()
                s_adm = self._sanitizar_nome(adm)
                
                path_dest = OUTPUT_DIR / s_adm / s_gestor
                path_dest.mkdir(parents=True, exist_ok=True)
                
                is_mellon = 'MELLON' in adm
                header_use = header_mellon if is_mellon else header_btg
                
                dados_out = []
                idx = 1
                for _, row in df_final.iterrows():
                    acao_raw = str(row['Ação']).strip()
                    acao_lower = acao_raw.lower()
                    
                    if "resgate" in acao_lower:
                        tipo_mov = "RP" if "BTG" in adm else "RI"
                    else:
                        tipo_mov = mapa_acoes.get(acao_raw.upper(), acao_raw)
                        
                    valor_txt = "0,00"
                    
                    if tipo_mov == "A":
                        # Aplicação 2 casas
                        val = float(str(row['diferenca_real']).replace('R$', '').replace('.', '').replace(',', '.')) if isinstance(row['diferenca_real'], str) else float(row['diferenca_real'])
                        valor_txt = f"{val:.2f}".replace('.', ',')
                    else:
                        val_raw = row['Total R$']
                        is_neg = tipo_mov in ["RI", "RP"]
                        valor_txt = self._formatar_valor(val_raw, is_neg)

                    # CNPJ Class
                    cnpj_prod = re.sub(r'\D', '', str(row.get('cnpj_classe', ''))).zfill(14)
                    if cnpj_prod:
                         cnpj_prod = f"{cnpj_prod[:2]}.{cnpj_prod[2:5]}.{cnpj_prod[5:8]}/{cnpj_prod[8:12]}-{cnpj_prod[12:]}"

                    # Helper safely get
                    def _get(k): return str(row.get(k, '')).strip().replace('nan', '')

                    dados_out.append({
                        "ID": idx, "CODIGO": "", "NOME": _get('codigo_pco'),
                        "PRODUTO": cnpj_prod, "TIPO_MOV": tipo_mov, "COTAS": "",
                        "VALOR": valor_txt, "NUM_NOTA": "", "FORMA": "TED",
                        "NUMERO_BANCO": "336", "NUMERO_CONTA": "6017363", "AGENCIA": "",
                        "DIGITO": "3", "TIPO_CONTA": "", "ORDEM_ADM": "0", "PENALTY": "",
                        "DIAS_PENALTY": "0", "PROD_DEST": "", "ORDEM_ORIG": "", "LIMITES": "0",
                        "CPF_CNPJ": "31.872.495/0001-72", "COD_CVM": _get('codigo_cvm_subclasse')
                    })
                    idx += 1

                # Build TXT
                lines = [header_use]
                for d in dados_out:
                    base = [
                         str(d["ID"]), str(d["CODIGO"]), str(d["NOME"]), str(d["PRODUTO"]),
                         str(d["TIPO_MOV"]), str(d["COTAS"]), str(d["VALOR"]), str(d["NUM_NOTA"]),
                         str(d["FORMA"]), str(d["NUMERO_BANCO"]), str(d["AGENCIA"]), str(d["NUMERO_CONTA"]),
                         str(d["DIGITO"]), str(d["TIPO_CONTA"]), str(d["ORDEM_ADM"]), str(d["PENALTY"]),
                         str(d["DIAS_PENALTY"]), str(d["PROD_DEST"]), str(d["ORDEM_ORIG"]), str(d["LIMITES"]),
                         str(d["CPF_CNPJ"])
                    ]
                    if is_mellon:
                        extra = ["", "", "", "", "", "", "", str(d["COD_CVM"])]
                    else:
                        extra = [str(d["COD_CVM"])]
                    
                    lines.append(";".join(base + extra))

                s_acao = self._sanitizar_nome(acao)
                fname = f"{s_gestor} {s_acao}.txt"
                out_path = path_dest / fname
                
                with open(out_path, 'w', encoding='utf-8') as f:
                    f.write("\n".join(lines))
                
                count += 1
                self.output_files.append(out_path)
                
        return count

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
            mail.Body = f"Status: {status}"
            if zip_path.exists(): mail.Attachments.Add(str(zip_path))
            mail.Send()
        except: pass

if __name__ == "__main__":
    AutomationTask().run()