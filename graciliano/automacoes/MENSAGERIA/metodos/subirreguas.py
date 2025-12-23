import sys
import os
import shutil
import traceback
import logging
import getpass
import time
import zipfile
import uuid
import pandas as pd
import pandas_gbq
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
REGRAVEL_EXCEL = False
SUBIDA_BQ = "replace"
NOME_SERVIDOR = "Servidor.py"
HEADLESS = False
NOME_AUTOMACAO = "MENSAGERIA"
NOME_SCRIPT = Path(__file__).stem.upper()

# --- CONFIGURAÇÕES ESPECÍFICAS DE NEGÓCIO ---
BQ_TABELA_DESTINO = "datalab-pagamentos.01_aux_apoio.TAUX_EM_CAMPANHAS"
# Emails padrão caso não encontre no BQ
EMAILS_FALLBACK = ["carlos.lsilva@c6bank.com", "sofia.fernandes@c6bank.com", "antonio.marcoss@c6bank.com"]

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
            # Fallback simples se PySide6 não estiver instalado
            try:
                from PyQt5.QtWidgets import (
                    QApplication, QWidget, QLabel, QLineEdit, QPushButton, QVBoxLayout, QComboBox
                )
                from PyQt5.QtGui import QFont
                from PyQt5.QtCore import Qt
                app = QApplication.instance() or QApplication([])
            except:
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
    """Fallback para ambiente manual sem GUI complexa."""
    res = Execucao.detectar()
    return res.get("modo_execucao"), res.get("usuario")

def obter_destinatarios():
    try:
        sql = f"SELECT emails_principais, emails_cc FROM `datalab-pagamentos.ADMINISTRACAO_CELULA_PYTHON.Registro_automacoes` WHERE lower(TRIM(metodo_automacao)) = lower(TRIM('{NOME_SCRIPT}')) LIMIT 1"
        df = pandas_gbq.read_gbq(sql, project_id="datalab-pagamentos")
        if df.empty: return EMAILS_FALLBACK, []
        def limpar(raw):
            if not raw or str(raw).lower() == 'nan': return []
            return [x.strip() for x in str(raw).replace(';', ',').split(',') if '@' in x]
        return limpar(df.iloc[0]['emails_principais']), limpar(df.iloc[0]['emails_cc'])
    except:
        return EMAILS_FALLBACK, []

def get_bq_client():
    # Helper para autenticação manual se necessário (Regra 2/4)
    if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        cred_path = Path.home() / "AppData" / "Roaming" / "CELPY"
        if cred_path.exists():
            for f in cred_path.glob("*.json"):
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(f)
                break
    return bigquery.Client()

def ler_dataframe(caminho: Path) -> pd.DataFrame:
    """Lê o arquivo como string para garantir integridade antes do cast no BQ."""
    try:
        df = pd.read_excel(caminho, dtype=str)
        df = df.astype("string")
        return df
    except Exception as e:
        logger.error(f"Erro ao ler arquivo {caminho.name}: {e}")
        return pd.DataFrame()

def processar_bigquery_replace_string(df: pd.DataFrame):
    """
    Implementa a lógica específica de REPLACE com CAST explícito conforme script original.
    """
    if df.empty: return 0

    client = get_bq_client()
    # Nome da tabela de staging
    projeto, dataset, tabela = BQ_TABELA_DESTINO.split(".")
    tabela_staging = f"{projeto}.{dataset}.{tabela}_staging_{NOME_SCRIPT}_{uuid.uuid4().hex[:8]}"

    try:
        # 1. Subir para Staging (Tudo STRING)
        schema_staging = [bigquery.SchemaField(col, "STRING") for col in df.columns]
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=schema_staging,
        )

        logger.info(f"Carregando staging {tabela_staging} (Modo STRING).")
        job = client.load_table_from_dataframe(df, tabela_staging, job_config=job_config)
        job.result()
        logger.info(f"Staging carregada. Job ID: {job.job_id}")

        # 2. Executar CREATE OR REPLACE com CAST explícito (Lógica de Negócio Preservada)
        query = f"""
        CREATE OR REPLACE TABLE `{BQ_TABELA_DESTINO}` (
          CAMPANHA STRING,
          CLASSIFICACAO_CAMPANHA STRING,
          TIPO_ROTINA STRING,
          AREA_SOLICITANTE STRING,
          CANAL_EMAIL BOOL,
          CANAL_SMS BOOL,
          CANAL_PUSH BOOL,
          CANAL_NOTIFICATION BOOL,
          ASSUNTO_PUSH STRING,
          TEXTO_PUSH STRING,
          DEEPLINK STRING,
          DEEPLINK_LOGADO STRING,
          MODELO INT64,
          ASSUNTO_EMAIL STRING,
          TEXTO_EMAIL STRING,
          PH STRING,
          TEXTO_SMS STRING,
          NM_TB_TEMP STRING,
          DS_CAMPOS_TEMP JSON,
          NM_PROC_UTILIZADA STRING,
          NM_TB_FINAL STRING,
          NM_FLUXO_DESTINO STRING,
          TIPO_FILA_DESTINO STRING,
          TB_FILA_DESTINO STRING,
          TEMPLATE_S3_URL STRING,
          TEMPLATE_S3_VARIAVEIS STRING,
          TEMPLATE_S3_BATCH BOOL,
          FL_ENCERRA_CASO BOOL,
          USER_INCLUSAO STRING,
          UUID_INCLUSAO STRING,
          DT_INCLUSAO TIMESTAMP,
          DT_INATIVACAO TIMESTAMP,
          DS_MOTIVO_INATIVACAO STRING,
          USER_INATIVACAO STRING,
          INFO_EXTRA JSON,
          FL_VALIDA_CONTA_ENCERRADA BOOL,
          FL_QUEBRAR_DISPARO BOOL,
          FL_PRIORIDADE_DISPARO STRING
        )
        AS
        SELECT
          SAFE_CAST(`CAMPANHA` AS STRING) AS CAMPANHA,
          SAFE_CAST(`CLASSIFICACAO_CAMPANHA` AS STRING) AS CLASSIFICACAO_CAMPANHA,
          SAFE_CAST(`TIPO_ROTINA` AS STRING) AS TIPO_ROTINA,
          SAFE_CAST(`AREA_SOLICITANTE` AS STRING) AS AREA_SOLICITANTE,
          SAFE_CAST(`CANAL_EMAIL` AS BOOL) AS CANAL_EMAIL,
          SAFE_CAST(`CANAL_SMS` AS BOOL) AS CANAL_SMS,
          SAFE_CAST(`CANAL_PUSH` AS BOOL) AS CANAL_PUSH,
          SAFE_CAST(`CANAL_NOTIFICATION` AS BOOL) AS CANAL_NOTIFICATION,
          SAFE_CAST(`ASSUNTO_PUSH` AS STRING) AS ASSUNTO_PUSH,
          SAFE_CAST(`TEXTO_PUSH` AS STRING) AS TEXTO_PUSH,
          SAFE_CAST(`DEEPLINK` AS STRING) AS DEEPLINK,
          SAFE_CAST(`DEEPLINK_LOGADO` AS STRING) AS DEEPLINK_LOGADO,
          SAFE_CAST(`MODELO` AS INT64) AS MODELO,
          SAFE_CAST(`ASSUNTO_EMAIL` AS STRING) AS ASSUNTO_EMAIL,
          SAFE_CAST(`TEXTO_EMAIL` AS STRING) AS TEXTO_EMAIL,
          SAFE_CAST(`PH` AS STRING) AS PH,
          SAFE_CAST(`TEXTO_SMS` AS STRING) AS TEXTO_SMS,
          SAFE_CAST(`NM_TB_TEMP` AS STRING) AS NM_TB_TEMP,
          SAFE.PARSE_JSON(`DS_CAMPOS_TEMP`) AS DS_CAMPOS_TEMP,
          SAFE_CAST(`NM_PROC_UTILIZADA` AS STRING) AS NM_PROC_UTILIZADA,
          SAFE_CAST(`NM_TB_FINAL` AS STRING) AS NM_TB_FINAL,
          SAFE_CAST(`NM_FLUXO_DESTINO` AS STRING) AS NM_FLUXO_DESTINO,
          SAFE_CAST(`TIPO_FILA_DESTINO` AS STRING) AS TIPO_FILA_DESTINO,
          SAFE_CAST(`TB_FILA_DESTINO` AS STRING) AS TB_FILA_DESTINO,
          SAFE_CAST(`TEMPLATE_S3_URL` AS STRING) AS TEMPLATE_S3_URL,
          SAFE_CAST(`TEMPLATE_S3_VARIAVEIS` AS STRING) AS TEMPLATE_S3_VARIAVEIS,
          SAFE_CAST(`TEMPLATE_S3_BATCH` AS BOOL) AS TEMPLATE_S3_BATCH,
          SAFE_CAST(`FL_ENCERRA_CASO` AS BOOL) AS FL_ENCERRA_CASO,
          SAFE_CAST(`USER_INCLUSAO` AS STRING) AS USER_INCLUSAO,
          SAFE_CAST(`UUID_INCLUSAO` AS STRING) AS UUID_INCLUSAO,
          SAFE_CAST(`DT_INCLUSAO` AS TIMESTAMP) AS DT_INCLUSAO,
          SAFE_CAST(`DT_INATIVACAO` AS TIMESTAMP) AS DT_INATIVACAO,
          SAFE_CAST(`DS_MOTIVO_INATIVACAO` AS STRING) AS DS_MOTIVO_INATIVACAO,
          SAFE_CAST(`USER_INATIVACAO` AS STRING) AS USER_INATIVACAO,
          SAFE.PARSE_JSON(`INFO_EXTRA`) AS INFO_EXTRA,
          SAFE_CAST(`FL_VALIDA_CONTA_ENCERRADA` AS BOOL) AS FL_VALIDA_CONTA_ENCERRADA,
          SAFE_CAST(`FL_QUEBRAR_DISPARO` AS BOOL) AS FL_QUEBRAR_DISPARO,
          SAFE_CAST(`FL_PRIORIDADE_DISPARO` AS STRING) AS FL_PRIORIDADE_DISPARO
        FROM `{tabela_staging}`
        """
        
        logger.info(f"Executando transformação e replace em {BQ_TABELA_DESTINO}...")
        job_final = client.query(query)
        job_final.result()
        logger.info(f"Job final concluído. Tabela atualizada.")

        return len(df)

    except Exception as e:
        logger.error(f"Erro no processamento BigQuery: {e}")
        raise e
    finally:
        # Limpeza
        try:
            client.delete_table(tabela_staging, not_found_ok=True)
            logger.info("Tabela de staging removida.")
        except: pass

def main():
    # 1. DETECÇÃO DE AMBIENTE (AUTO/SOLICITACAO vs MANUAL)
    modo = os.environ.get("MODO_EXECUCAO")
    usuario = os.environ.get("USUARIO_EXEC")

    if not modo:
        # Se nao tem env var, estamos rodando manual (VSCode/CMD) -> Abrir GUI
        modo, usuario = _exibir_dialogo_inicial()
        if not modo: return # Cancelado
    
    # Garantir sufixo @c6bank.com
    if usuario and "@" not in usuario:
        usuario = f"{usuario}@c6bank.com"

    # Setup de Pastas
    base_path = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "automacoes" / NOME_AUTOMACAO
    pasta_input = base_path / "arquivos input" / NOME_SCRIPT
    pasta_logs = base_path / "logs" / NOME_SCRIPT / datetime.now().strftime('%Y-%m-%d')
    
    pasta_input.mkdir(parents=True, exist_ok=True)
    pasta_logs.mkdir(parents=True, exist_ok=True)
    
    log_file = configurar_logs(pasta_logs)

    logger.info(f"=== INICIANDO {NOME_SCRIPT} ===")
    logger.info(f"Modo: {modo} | Usuário: {usuario}")

    status_final = "FALHA"
    arquivos_anexos = []
    detalhe_msg = None
    
    try:
        # Busca Arquivos
        arquivos = list(pasta_input.glob("*.xlsx"))
        
        if not arquivos:
            status_final = "SEM DADOS PARA PROCESSAR"
            detalhe_msg = "Nenhum arquivo encontrado na pasta de input."
            logger.info(detalhe_msg)
        else:
            arquivos_processados_count = 0
            
            for arq in arquivos:
                logger.info(f"Processando: {arq.name}")
                
                # 1. Leitura (Regra 1: Preservar lógica de leitura como string)
                df = ler_dataframe(arq)
                
                if df.empty:
                    logger.warning(f"Arquivo vazio ou erro de leitura: {arq.name}")
                    continue
                
                # 2. Upload e Transformação (Regra 2: Staging -> Final com Schema Específico)
                lines_uploaded = processar_bigquery_replace_string(df)
                
                if lines_uploaded > 0:
                    arquivos_processados_count += 1
                
                # 3. Mover para Logs
                try:
                    destino = pasta_logs / arq.name
                    if destino.exists():
                        timestamp = datetime.now().strftime("%H%M%S")
                        destino = pasta_logs / f"{arq.stem}_{timestamp}{arq.suffix}"
                    shutil.copy2(str(arq), str(destino))
                    logger.info(f"Arquivo movido para logs: {destino.name}")
                except Exception as e_mv:
                    logger.error(f"Erro ao mover arquivo: {e_mv}")

            if arquivos_processados_count > 0:
                status_final = "SUCESSO"
            else:
                status_final = "SEM DADOS PARA PROCESSAR"
                detalhe_msg = "Arquivos encontrados mas nenhum dataframe válido processado."

    except Exception as e:
        logger.error(f"Erro Crítico: {traceback.format_exc()}")
        status_final = "FALHA"
        
    finally:
        # Zipar Logs
        try:
            zip_name = pasta_logs / f"{NOME_SCRIPT}_{datetime.now().strftime('%H%M%S')}.zip"
            with zipfile.ZipFile(zip_name, 'w', zipfile.ZIP_DEFLATED) as zf:
                zf.write(log_file, arcname=log_file.name)
            arquivos_anexos.append(zip_name)
        except: pass

        # Enviar Email/Metricas via _utilAutomacoesExec
        try:
            dest_to, dest_cc = obter_destinatarios()
            
            # Adicionar destinatários hardcoded como fallback/complemento se necessário
            # (Mantendo lógica original de mesclar se sucesso)
            if status_final == "SUCESSO":
                dest_final = list(set(dest_to + dest_cc + EMAILS_FALLBACK))
            else:
                dest_final = list(set(dest_to + EMAILS_FALLBACK))

            client = _utilAutomacoesExec.AutomacoesExecClient(logger)
            client.publicar(
                nome_automacao=NOME_AUTOMACAO,
                metodo_automacao=NOME_SCRIPT,
                status=status_final,
                tempo_exec="00:00:00", # TODO: Implementar delta
                usuario=usuario,
                log_path=str(pasta_logs),
                destinatarios=dest_final,
                send_email=True,
                anexos=[str(x) for x in arquivos_anexos],
                observacao=detalhe_msg
            )
        except Exception as e_pub:
            logger.error(f"Erro ao publicar métricas/email: {e_pub}")

    # RETCODES
    if status_final == "SUCESSO":
        sys.exit(0)
    elif status_final == "SEM DADOS PARA PROCESSAR":
        sys.exit(2)
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()
