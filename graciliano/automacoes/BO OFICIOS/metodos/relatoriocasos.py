import sys
import os
import shutil
import traceback
import logging
import getpass
import time
import zipfile
import pandas as pd
import pandas_gbq
import pythoncom
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from win32com.client import Dispatch

# ==============================================================================
# CONFIGURAÇÃO DE AMBIENTE
# ==============================================================================
NOME_AUTOMACAO = "BO OFICIOS" 
NOME_SCRIPT = Path(__file__).stem.upper()
TZ = ZoneInfo("America/Sao_Paulo")

# Robust Path Detection
POSSIBLE_ROOTS = [
    Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano",
    Path.home() / "Meu Drive/C6 CTVM" / "graciliano",
    Path.home() / "C6 CTVM/graciliano",
]
BASE_DIR = next((p for p in POSSIBLE_ROOTS if p.exists()), Path.home() / "graciliano")
MODULES_PATH = BASE_DIR / "novo_servidor" / "modules"

if str(MODULES_PATH) not in sys.path:
    sys.path.append(str(MODULES_PATH))

try:
    import dollynho
except ImportError:
    dollynho = None

# --- CONFIGURAÇÕES GLOBAIS ---
REGRAVEL_EXCEL = False
SUBIDA_BQ = "append" 
HEADLESS = True
PROJECT_ID = "datalab-pagamentos"

# --- LOGGING ---
def configurar_logs(log_dir):
    log_file = log_dir / f"{NOME_SCRIPT}_{datetime.now(TZ).strftime('%H%M%S')}.log"
    
    logger = logging.getLogger(NOME_SCRIPT)
    logger.setLevel(logging.INFO)
    logger.handlers = [] 
    
    fh = logging.FileHandler(log_file, encoding='utf-8')
    fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    
    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger, log_file

def _exibir_dialogo_inicial():
    """Manual fallback."""
    if os.environ.get("HEADLESS") == "true":
         return "AUTO", f"{getpass.getuser()}@c6bank.com"
    try:
        from PyQt5.QtWidgets import QApplication, QDialog, QVBoxLayout, QLabel, QComboBox, QLineEdit, QPushButton, QMessageBox
        app = QApplication.instance() or QApplication([])
        dialog = QDialog()
        dialog.setWindowTitle(f"Config: {NOME_SCRIPT}")
        dialog.resize(300, 200)
        
        layout = QVBoxLayout(dialog)
        layout.addWidget(QLabel("Modo de Execução:"))
        cmb_modo = QComboBox()
        cmb_modo.addItems(["AUTO", "SOLICITACAO"])
        layout.addWidget(cmb_modo)
        
        layout.addWidget(QLabel("Usuário:"))
        txt_user = QLineEdit(f"{getpass.getuser()}@c6bank.com")
        layout.addWidget(txt_user)
        
        btn_run = QPushButton("EXECUTAR")
        btn_run.clicked.connect(lambda: dialog.accept() if txt_user.text().strip() else QMessageBox.warning(dialog, "Erro", "Usuário obrigatório."))
        layout.addWidget(btn_run)
        
        if dialog.exec_() == QDialog.Accepted:
            return cmb_modo.currentText(), txt_user.text().strip()
    except: pass
    return "AUTO", f"{getpass.getuser()}@c6bank.com"

def obter_destinatarios(logger):
    try:
        sql = f"SELECT emails_principais, emails_cc FROM `datalab-pagamentos.ADMINISTRACAO_CELULA_PYTHON.Registro_automacoes` WHERE lower(TRIM(metodo_automacao)) = lower(TRIM('{NOME_SCRIPT.lower()}')) LIMIT 1"
        df = pandas_gbq.read_gbq(sql, project_id=PROJECT_ID)
        if df.empty: return [], []
        
        def limpar(raw):
            if not raw or str(raw).lower() == 'nan': return []
            return [x.strip() for x in str(raw).replace(';', ',').split(',') if '@' in x]
            
        return limpar(df.iloc[0]['emails_principais']), limpar(df.iloc[0]['emails_cc'])
    except Exception as e:
        logger.error(f"Erro ao obter destinatarios: {e}")
        return ["carlos.lsilva@c6bank.com"], []

def main():
    # 1. DETECÇÃO DE AMBIENTE
    modo = os.environ.get("MODO_EXECUCAO")
    usuario = os.environ.get("USUARIO_EXEC")
    start_time_counter = time.time()
    dt_inicio = datetime.now(TZ)

    if not modo:
        modo, usuario = _exibir_dialogo_inicial()
    
    if usuario and "@" not in usuario:
        usuario = f"{usuario}@c6bank.com"

    # Setup de Pastas (Standardized)
    base_log_dir = BASE_DIR / "automacoes" / NOME_AUTOMACAO / "logs" / NOME_SCRIPT / dt_inicio.strftime('%Y-%m-%d')
    base_log_dir.mkdir(parents=True, exist_ok=True)
    
    logger, log_file = configurar_logs(base_log_dir)

    logger.info(f"=== INICIANDO {NOME_SCRIPT} ===")
    logger.info(f"Modo: {modo} | Usuário: {usuario}")

    status_final = "FALHA"
    arquivos_anexos = []
    
    try:
        # === INICIO LÓGICA DO USUÁRIO ===
        logger.info("Conectando ao BigQuery para extrair dados...")
        
        query = "SELECT * FROM `datalab-pagamentos.CELULA_PYTHON_TESTES.materaOpenCASES`"
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID)
        
        if df.empty:
            logger.warning("Query retornou vazio. Sem dados para processar.")
            status_final = "SEM DADOS PARA PROCESSAR"
        else:
            logger.info(f"Dados obtidos: {len(df)} linhas. Convertendo tudo para texto bruto.")
            df = df.astype(str)
            timestamp_file = datetime.now(TZ).strftime('%Y%m%d_%H%M%S')
            file_name = f"MateraOpenCases_{timestamp_file}.xlsx"
            output_path = base_log_dir / file_name
            
            logger.info(f"Salvando arquivo Excel em: {output_path}")
            df.to_excel(output_path, index=False)
            
            if output_path.exists():
                arquivos_anexos.append(output_path)
                logger.info("Arquivo gerado com sucesso.")
                status_final = "SUCESSO"
            else:
                raise FileNotFoundError("Erro ao salvar o arquivo Excel.")

        # === FIM LÓGICA DO USUÁRIO ===
        
    except Exception as e:
        logger.error(f"Erro: {traceback.format_exc()}")
        status_final = "FALHA"
    finally:
        # Zipar Logs
        try:
            zip_name = base_log_dir / f"{NOME_SCRIPT}_{datetime.now(TZ).strftime('%H%M%S')}.zip"
            with zipfile.ZipFile(zip_name, 'w', zipfile.ZIP_DEFLATED) as zf:
                if log_file.exists(): zf.write(log_file, arcname=log_file.name)
            arquivos_anexos.append(zip_name)
        except: pass

        # Enviar Email & Métricas (Modularized locally)
        duration = time.time() - start_time_counter
        tempo_exec_str = str(timedelta(seconds=int(duration)))
        
        dest_to, dest_cc = obter_destinatarios(logger)
        
        # 1. Email via Outlook
        try:
            pythoncom.CoInitialize()
            outlook = Dispatch("Outlook.Application")
            mail = outlook.CreateItem(0)
            mail.To = ";".join(dest_to)
            if status_final == "SUCESSO" and dest_cc:
                mail.CC = ";".join(dest_cc)
            
            mail.Subject = f"{NOME_AUTOMACAO} - {NOME_SCRIPT} - {status_final}"
            mail.Body = f"Execução finalizada.\nStatus: {status_final}\nTempo: {tempo_exec_str}\nUsuário: {usuario}\nLog em anexo."
            
            for att in arquivos_anexos:
                if Path(att).exists(): mail.Attachments.Add(str(att))
            
            mail.Send()
            logger.info("Email enviado via Outlook.")
        except Exception as e:
            logger.error(f"Falha envio email: {e}")

        # 2. Métricas BigQuery
        try:
            df_m = pd.DataFrame([{
                "nome_automacao": NOME_AUTOMACAO,
                "metodo_automacao": NOME_SCRIPT,
                "status": status_final,
                "tempo_exec": tempo_exec_str,
                "data_exec": dt_inicio.strftime("%Y-%m-%d"),
                "hora_exec": dt_inicio.strftime("%H:%M:%S"),
                "usuario": usuario,
                "log_path": str(base_log_dir)
            }])
            pandas_gbq.to_gbq(df_m, "datalab-pagamentos.ADMINISTRACAO_CELULA_PYTHON.automacoes_exec", project_id=PROJECT_ID, if_exists="append")
            logger.info("Métricas enviadas para BigQuery.")
        except Exception as e:
            logger.error(f"Falha envio métricas BQ: {e}")

    # RETCODES
    if status_final == "SUCESSO": sys.exit(0)
    elif status_final == "SEM DADOS PARA PROCESSAR": sys.exit(2)
    else: sys.exit(1)

if __name__ == "__main__":
    main()