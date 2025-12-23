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
import win32com.client as win32
from pathlib import Path
from datetime import datetime
from google.cloud import bigquery

# --- CONFIGURAÇÃO DE AMBIENTE ---
# Navega para encontrar a pasta 'novo_servidor'
BASE_DIR = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "novo_servidor"
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

try:
    from novo_servidor.modules import _utilAutomacoesExec
    from novo_servidor.modules import dollynho
except ImportError:
    try:
        from modules import _utilAutomacoesExec
        from modules import dollynho
    except:
        print("CRITICAL: Modulos 'dollynho' e '_utilAutomacoesExec' nao encontrados.")
        sys.exit(1)


# --- CONFIGURAÇÕES GLOBAIS ---
REGRAVEL_EXCEL = True
SUBIDA_BQ = "replace" 
NOME_SERVIDOR = "Servidor.py"
HEADLESS = False
NOME_AUTOMACAO = "MENSAGERIA"
NOME_SCRIPT = Path(__file__).stem.upper()

# Configurações Específicas do Projeto
PROJETO_BQ = "datalab-pagamentos"
BQ_TABELA_DESTINO = f"{PROJETO_BQ}.00_temp.mensageria2_campanha_padronizada"
PROC_POS_CARGA = f"{PROJETO_BQ}.05_procs_mensageria.PR_EM2_CAMPANHA_PADRONIZADA"

# --- LOGGING ---
logger = logging.getLogger(NOME_SCRIPT)
logger.setLevel(logging.INFO)

def configurar_logs(log_dir):
    log_file = log_dir / f"{NOME_SCRIPT}_{datetime.now().strftime('%H%M%S')}.log"
    fh = logging.FileHandler(log_file, encoding='utf-8')
    fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.handlers = []
    logger.addHandler(fh)
    logger.addHandler(sh)
    return log_file


class Execucao:
    @staticmethod
    def is_servidor() -> bool:
        try:
            if os.getenv("SERVIDOR_ORIGEM", "").strip().lower() == NOME_SERVIDOR.lower():
                return True
            args = " ".join(sys.argv).lower()
            if "--executado-por-servidor" in args:
                return True
            if NOME_SERVIDOR.lower() in args:
                return True
        except Exception:
            return False
        return False

    @staticmethod
    def abrir_gui() -> dict:
        import sys as _sys
        import getpass as _getpass
        from pathlib import Path as _Path
        try:
            from PySide6.QtWidgets import (
                QApplication, QWidget, QLabel, QLineEdit, QPushButton, QVBoxLayout, QComboBox
            )
            from PySide6.QtGui import QFont
            from PySide6.QtCore import Qt
            app = QApplication.instance() or QApplication(_sys.argv)
        except ImportError:
            try:
                from PyQt5.QtWidgets import (
                    QApplication, QWidget, QLabel, QLineEdit, QPushButton, QVBoxLayout, QComboBox
                )
                from PyQt5.QtGui import QFont
                from PyQt5.QtCore import Qt
                app = QApplication.instance() or QApplication([])
            except ImportError:
                return {}

        nome_titulo = _Path(__file__).stem.lower()
        janela = QWidget()
        janela.setWindowTitle(nome_titulo)
        janela.setWindowFlags(
            Qt.WindowStaysOnTopHint | Qt.WindowTitleHint | Qt.WindowCloseButtonHint
        )
        janela.setFixedSize(420, 260)

        fonte_titulo = QFont("Segoe UI", 12, QFont.Bold)
        fonte_normal = QFont("Segoe UI", 10)

        lbl_titulo = QLabel("Selecione o modo de execução")
        lbl_titulo.setFont(fonte_titulo)
        lbl_titulo.setAlignment(Qt.AlignCenter)

        lbl_usuario = QLabel("E-mail do usuário:")
        lbl_usuario.setFont(fonte_normal)

        campo_usuario = QLineEdit()
        campo_usuario.setPlaceholderText("nome.sobrenome@c6bank.com")
        campo_usuario.setFont(fonte_normal)

        lbl_modo = QLabel("Modo de execução:")
        lbl_modo.setFont(fonte_normal)

        combo_modo = QComboBox()
        combo_modo.addItems(["AUTO", "SOLICITACAO"])
        combo_modo.setFont(fonte_normal)

        btn_confirmar = QPushButton("Confirmar")
        btn_confirmar.setFont(QFont("Segoe UI", 10, QFont.Bold))
        btn_confirmar.setStyleSheet(
            "QPushButton {background-color: #007AFF; color: white; border-radius: 6px; padding: 7px;}"
            "QPushButton:hover {background-color: #005BBB;}"
        )

        resultado = {}

        def confirmar():
            email = campo_usuario.text().strip()
            if not email:
                email = f"{_getpass.getuser()}@c6bank.com"
            resultado.update(
                {
                    "modo_execucao": combo_modo.currentText(),
                    "observacao": "null",
                    "usuario": email,
                    "is_server": "0",
                }
            )
            janela.close()

        btn_confirmar.clicked.connect(confirmar)

        layout = QVBoxLayout()
        layout.addWidget(lbl_titulo)
        layout.addSpacing(10)
        layout.addWidget(lbl_usuario)
        layout.addWidget(campo_usuario)
        layout.addSpacing(10)
        layout.addWidget(lbl_modo)
        layout.addWidget(combo_modo)
        layout.addStretch()
        layout.addWidget(btn_confirmar)

        janela.setLayout(layout)
        janela.show()
        app.exec()

        if not resultado:
            email = f"{_getpass.getuser()}@c6bank.com"
            resultado.update(
                {
                    "modo_execucao": "AUTO",
                    "observacao": "null",
                    "usuario": email,
                    "is_server": "0",
                }
            )
        return resultado

    @staticmethod
    def detectar() -> dict:
        if Execucao.is_servidor():
            return {
                "modo_execucao": "AUTO",
                "observacao": "null",
                "usuario": f"{getpass.getuser()}@c6bank.com",
                "is_server": "1",
            }
        try:
            return Execucao.abrir_gui()
        except Exception:
            return {
                "modo_execucao": "AUTO",
                "observacao": "null",
                "usuario": f"{getpass.getuser()}@c6bank.com",
                "is_server": "0",
            }


def _exibir_dialogo_inicial():
    """Fallback para dialogo inicial."""
    res = Execucao.detectar()
    return res.get('modo_execucao'), res.get('usuario')

def obter_destinatarios():
    try:
        sql = f"SELECT emails_principais, emails_cc FROM `datalab-pagamentos.ADMINISTRACAO_CELULA_PYTHON.Registro_automacoes` WHERE lower(TRIM(metodo_automacao)) = lower(TRIM('{NOME_SCRIPT}')) LIMIT 1"
        df = pandas_gbq.read_gbq(sql, project_id=PROJETO_BQ)
        if df.empty: return [], []
        def limpar(raw):
            if not raw or str(raw).lower() in ('nan', 'none'): return []
            return [x.strip() for x in str(raw).replace(';', ',').split(',') if '@' in x]
        return limpar(df.iloc[0]['emails_principais']), limpar(df.iloc[0]['emails_cc'])
    except:
        return [], []

def regravar_excel(arquivo_path):
    """Regrava o Excel usando COM para evitar erros de leitura."""
    if not REGRAVEL_EXCEL or arquivo_path.suffix.lower() not in ['.xls', '.xlsx']:
        return
    try:
        xl = win32.Dispatch("Excel.Application")
        xl.DisplayAlerts = False
        wb = xl.Workbooks.Open(str(arquivo_path))
        wb.Save()
        wb.Close()
        xl.Quit()
        logger.info(f"Arquivo regravado via COM: {arquivo_path.name}")
    except Exception as e:
        logger.warning(f"Falha ao regravar Excel {arquivo_path.name}: {e}")

def processar_dataframe(arquivo_path):
    """Lê o arquivo, ajusta colunas e adiciona auditoria."""
    logger.info(f"Lendo arquivo: {arquivo_path.name}")
    try:
        if arquivo_path.suffix.lower() in ['.xls', '.xlsx']:
            df = pd.read_excel(arquivo_path, dtype=str)
        elif arquivo_path.suffix.lower() == '.csv':
            # Tenta detecção de encoding/separador
            df = None
            for enc in ["utf-8", "utf-8-sig", "cp1252", "latin1"]:
                for sep in [";", ",", "\t", "|"]:
                    try:
                        df = pd.read_csv(arquivo_path, dtype=str, engine="python", sep=sep, encoding=enc)
                        break
                    except: continue
                if df is not None: break
        else:
            logger.warning(f"Formato não suportado: {arquivo_path.suffix}")
            return None

        if df is None or df.empty:
            logger.warning(f"Dataframe vazio ou erro de leitura: {arquivo_path.name}")
            return None

        df = df.fillna("")
        df.columns = [str(c).strip() for c in df.columns]

        # Mapeamento de Colunas (Lógica Original)
        alias_map = {
            "MOTIVO": "MOTIVO2__C",
            "SUBMOTIVO": "SUBMOTIVO2__C",
            "CATEGORIA": "CATEGORIA__C",
        }
        for src, dst in alias_map.items():
            if src in df.columns and dst not in df.columns:
                df[dst] = df[src]

        # Auditoria
        data_exec = datetime.now()
        df['dt_carga'] = data_exec.strftime('%Y-%m-%d')
        df['hr_carga'] = data_exec.strftime('%H:%M:%S')

        return df
    except Exception as e:
        logger.error(f"Erro ao processar dataframe: {e}")
        return None

def subir_com_staging(df):
    """
    Sobe dados para tabela staging e depois substitui ou insere na tabela final.
    """
    if df.empty: return False
    
    tabela_staging = f"{BQ_TABELA_DESTINO}_STAGING"
    client = bigquery.Client(project=PROJETO_BQ)
    
    try:
        # 1. Subir para Staging (Replace)
        logger.info(f"Subindo {len(df)} linhas para Staging: {tabela_staging}")
        pandas_gbq.to_gbq(
            df, 
            tabela_staging, 
            project_id=PROJETO_BQ, 
            if_exists='replace',
            progress_bar=False
        )
        
        # 2. Operação na Tabela Final
        if SUBIDA_BQ == 'replace':
            # Modo Replace: Substitui a tabela final pela staging
            query = f"""
            CREATE OR REPLACE TABLE `{BQ_TABELA_DESTINO}` AS
            SELECT * FROM `{tabela_staging}`
            """
            logger.info(f"Substituindo tabela final: {BQ_TABELA_DESTINO}")
        else:
            # Modo Append (Dedup): Insere apenas novos registros
            query = f"""
            INSERT INTO `{BQ_TABELA_DESTINO}`
            SELECT * FROM `{tabela_staging}` S
            WHERE NOT EXISTS (
                SELECT 1 FROM `{BQ_TABELA_DESTINO}` T
                WHERE TO_JSON_STRING(T) = TO_JSON_STRING(S)
            )
            """
            logger.info(f"Inserindo novos registros em: {BQ_TABELA_DESTINO}")

        client.query(query).result()
        
        # 3. Limpeza
        client.delete_table(tabela_staging, not_found_ok=True)
        return True

    except Exception as e:
        logger.error(f"Erro ao subir para BigQuery: {e}")
        # Tenta limpar staging
        try: client.delete_table(tabela_staging, not_found_ok=True)
        except: pass
        raise e

def main():
    # 1. DETECÇÃO DE AMBIENTE
    modo = os.environ.get("MODO_EXECUCAO")
    usuario = os.environ.get("USUARIO_EXEC")

    if not modo:
        modo, usuario = _exibir_dialogo_inicial()
        if not modo: return
    
    if usuario and "@" not in usuario:
        usuario = f"{usuario}@c6bank.com"

    # Setup de Pastas
    base_automacao = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "automacoes" / NOME_AUTOMACAO
    pasta_input = base_automacao / "arquivos input" / NOME_SCRIPT
    pasta_logs = base_automacao / "logs" / NOME_SCRIPT / datetime.now().strftime('%Y-%m-%d')
    
    pasta_input.mkdir(parents=True, exist_ok=True)
    pasta_logs.mkdir(parents=True, exist_ok=True)
    
    log_file = configurar_logs(pasta_logs)

    logger.info(f"=== INICIANDO {NOME_SCRIPT} ===")
    logger.info(f"Modo: {modo} | Usuário: {usuario}")

    status_final = "FALHA"
    arquivos_anexos = []
    arquivos_movidos = []
    
    try:
        # Busca Arquivos
        arquivos = list(pasta_input.glob("*.xlsx")) + list(pasta_input.glob("*.xls")) + list(pasta_input.glob("*.csv"))
        
        if not arquivos:
            status_final = "SEM DADOS PARA PROCESSAR"
            logger.info("Nenhum arquivo encontrado.")
        else:
            arquivos_processados = 0
            for arq in arquivos:
                # Regravar se necessario (Excel antigo/corrompido)
                regravar_excel(arq)
                
                # Processar
                df = processar_dataframe(arq)
                if df is not None:
                    # Subir BQ (Com Staging)
                    if subir_com_staging(df):
                        arquivos_processados += 1
                        
                        # Mover arquivo
                        destino = pasta_logs / f"{arq.stem}_{datetime.now().strftime('%H%M%S')}{arq.suffix}"
                        shutil.move(str(arq), str(destino))
                        arquivos_movidos.append(destino)
                        logger.info(f"Arquivo movido para: {destino.name}")

            if arquivos_processados > 0:
                # Executar Procedure
                logger.info(f"Executando procedure: {PROC_POS_CARGA}")
                client = bigquery.Client(project=PROJETO_BQ)
                query = f"CALL `{PROC_POS_CARGA}`()"
                client.query(query).result()
                logger.info("Procedure executada com sucesso.")
                status_final = "SUCESSO"
            else:
                status_final = "SEM DADOS PARA PROCESSAR"

    except Exception as e:
        logger.error(f"Erro: {traceback.format_exc()}")
        status_final = "FALHA"
    finally:
        # Zipar Logs
        try:
            zip_name = pasta_logs / f"{NOME_SCRIPT}_{datetime.now().strftime('%H%M%S')}.zip"
            with zipfile.ZipFile(zip_name, 'w', zipfile.ZIP_DEFLATED) as zf:
                zf.write(log_file, arcname=log_file.name)
                for mov in arquivos_movidos:
                    try: zf.write(mov, arcname=mov.name)
                    except: pass
            arquivos_anexos.append(zip_name)
        except: pass

        # Enviar Email/Metricas
        try:
            dest_to, dest_cc = obter_destinatarios()
            client = _utilAutomacoesExec.AutomacoesExecClient(logger)
            client.publicar(
                nome_automacao=NOME_AUTOMACAO,
                metodo_automacao=NOME_SCRIPT,
                status=status_final,
                tempo_exec="00:00:00", 
                usuario=usuario,
                log_path=str(pasta_logs),
                destinatarios=dest_to + (dest_cc if status_final == "SUCESSO" else []),
                send_email=True,
                anexos=[str(x) for x in arquivos_anexos]
            )
        except Exception as e_pub:
            logger.error(f"Erro ao publicar: {e_pub}")

    # RETCODES
    if status_final == "SUCESSO":
        sys.exit(0)
    elif status_final == "SEM DADOS PARA PROCESSAR":
        sys.exit(2)
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()
