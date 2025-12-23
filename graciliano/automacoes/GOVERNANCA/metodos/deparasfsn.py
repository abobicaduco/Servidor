import sys
import os
import shutil
import traceback
import logging
import getpass
import time
import zipfile
import re
import pandas as pd
import pandas_gbq
from pathlib import Path
from datetime import datetime, timedelta
from google.cloud import bigquery

# --- CONFIGURAÇÃO DE AMBIENTE ---
# Navega para encontrar a pasta 'novo_servidor'
BASE_DIR = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "novo_servidor"
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

# Diretório específico dos dados do usuário (extraído do código original)
DATA_DIR = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Rafael Brito Peixoto - Subida de Base (1)"

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
SUBIDA_BQ = "append" 
NOME_SERVIDOR = "Servidor.py"
HEADLESS = False 
NOME_AUTOMACAO = "GOVERNANCA" 
NOME_SCRIPT = Path(__file__).stem.upper() 
PROJECT_ID = "datalab-pagamentos"

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


# --- FUNÇÕES DE LIMPEZA E TRANSFORMAÇÃO (MIGRADO DO LEGADO) ---
def ascii_clean(s):
    if not isinstance(s, str): return str(s)
    import unicodedata
    s = s.replace("\r", " ").replace("\n", " ").replace("\t", " ").replace("\xa0", " ").strip()
    s_nfkd = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s_nfkd if not unicodedata.combining(ch))

def canon(s):
    if s is None: return ""
    s = str(s).lower().strip().replace("\xa0", " ")
    return ascii_clean(s)

def clean_object_df(df):
    out = df.copy()
    for c in out.columns:
        out[c] = out[c].astype(str).replace({'NaT': None, 'nan': None, '<NA>': None, 'None': None})
        out[c] = out[c].apply(lambda x: None if x == 'None' or x == '' else x)
    return out

def normalize_turno_v2(df, target_order):
    df = df.copy()
    alias = {
        "id": "ID", "email": "Email", "nome": "Nome", "hora de inicio": "Hora_de_in_cio",
        "hora de conclusao": "Hora_de_conclus_o", "selecione o turno": "Selecione_o_turno",
        "t1- qual tarefa foi executada?": "T1__Qual_tarefa_foi_executada_",
        "t2 - qual tarefa foi executada?": "T2___Qual_tarefa_foi_executada_",
        "t3 - qual tarefa foi executada ?": "T3___Qual_tarefa_foi_executada__",
        "ocorreu algum problema na execucao da tarefa ?": "Ocorreu_algum_problema_na_execu__o_da_tarefa__",
        "rotina automatizada": "Rotina_Automatizada", "qual atuacao necessaria ?": "Qual_atua__o_necess_ria__",
        "descricao do inc: exemplo: [boleto] - alto indice de boletos sem registro na cip": "Descri__o_do_INC__Exemplo___BOLETO____Alto__ndice_de_boletos_sem_registro_na_CIP",
        "possivel impacto: exemplo: nao liquidacao de boletos": "Poss_vel_impacto__Exemplo__N_o_liquida__o_de_boletos",
        "horario do primeiro impacto:": "Hor_rio_do_primeiro_impacto_",
        "deseja adicionar anexos a essa observacao?": "Deseja_adicionar_anexos_a_essa_observa__o_",
        "detalhes da nova tarefa para inclusao no forms": "Detalhes_da_nova_tarefa_para_inclus_o_no_forms",
    }
    mapping = {c: alias[canon(c)] for c in df.columns if canon(c) in alias}
    mapping.update({c: c for c in df.columns if c in target_order and c not in mapping})
    df = df.rename(columns=mapping)
    for c in target_order:
        if c not in df.columns: df[c] = None
    return clean_object_df(df[target_order])

def normalize_v6(df, target_order):
    df = df.copy()
    df.columns = [ascii_clean(c) for c in df.columns]
    alias = {
        "id": "ID", "hora de inicio": "Hora_inicio", "hora de conclusao": "Hora_conclusao",
        "email": "Email", "tempo acao": "Tempo_Acao",
        "qual painel ou alerta apresentou inconsistencias?": "painel_alerta_inconsistencia",
        "bloqueio 22 (solicitacao escalonada de bloqueio) - quem solicitou o bloqueio da conta?": "Solicitante_bloqueio_conta",
        "id alerta:": "ID_Alerta", "execucao comunicacao critica": "Execucao_Comunicacao_Critica",
        "qual foi horario disparado?": "Qual foi Horario disparado",
        "procedimento resolvido pelo time?": "Procedimento_resolvido_pelo_time",
        "solucao feita:": "Solucao_feita", "nivel criticidade incidente (em teste)": "Nivel_Criticidade_Incidente_Teste",
        "incidente": "Incidente", "descricao do inc: exemplo: [boleto] - alto indice de boletos sem registro na cip": "Descricao_INC",
        "o cliente esta sendo impactado?": "Cliente_sendo_impactado",
        "possivel impacto: exemplo: nao liquidacao de boletos": "Possivel_impacto",
        "horario do first impacto:": "Horario_primeiro_impacto", "horario do primeiro impacto": "Horario_primeiro_impacto",
        "volume de clientes impactados ? exemplo: 250 transacoes nos ultimos 15 minutos": "Volume_clientes_impactados",
        "justificativa": "Justificativa", "comunicacao de": "Comunicacao", "acao aplicavel": "Acao_Aplicavel",
        "alerta criado": "Alerta_criado", "coluna1": "Coluna1"
    }
    # Tratamento para chaves que variam pouco
    mapping = {}
    for c in df.columns:
        k = canon(c)
        if "alerta" in k and "id" in k: mapping[c] = "ID_Alerta"
        elif k in alias: mapping[c] = alias[k]
        else:
            found = False
            for ak, av in alias.items():
                if canon(ak) == k:
                    mapping[c] = av
                    found = True
                    break
            if not found and c in target_order: mapping[c] = c
            
    df = df.rename(columns=mapping)
    for c in target_order:
        if c not in df.columns: df[c] = None
        
    df = df[target_order]
    if "Hora_inicio" in df.columns:
        df["Hora_inicio"] = df["Hora_inicio"].astype(str).str[:19].replace({'nan': None, 'None': None})
    return clean_object_df(df)

def normalize_alertas_v2(df, target_order):
    df = df.copy()
    alias = {
        "id": "ID", "nome do alerta": "Nome do alerta", "owner": "Owner",
        "data de envio": "Data de Envio", "hora de envio": "Hora de envio", "hora de conclusao": "Hora de conclusão"
    }
    mapping = {c: alias[canon(c)] for c in df.columns if canon(c) in alias}
    mapping.update({c: c for c in df.columns if c in target_order and c not in mapping})
    df = df.rename(columns=mapping)
    for c in target_order:
        if c not in df.columns: df[c] = None
    return clean_object_df(df[target_order])

def normalize_massivo(df, target_order):
    df = df.copy()
    alias = {
        "id": "ID", "email": "Email", "nome": "Nome", "preditivo": "Preditivo", "tabulacao": "Tabulacao",
        "query": "Query", "hora de inicio": "Hora_de_in__cio", "hora de conclusao": "Hora_de_conclus__o",
        "turno:": "Turno_", "qual acao massiva foi executada?": "Qual_a_____o_massiva_foi_executada_",
        "ha casos a serem escalonado ou encerrados?": "H___casos_a_serem_escalonado_ou_encerrados_",
        "incidente": "incidente", "quantidade de clientes impactados (splunk)": "Quantidade_de_clientes_impactados_splunk",
        "chats totais": "chats_totais", "% retencao preditivo cliente.": "retencao_preditivo_cliente",
        "casos fechados pelo n1": "Casos_fechados_t1", "acao executada": "Acao_executada",
        "quantos casos foram escalonados?": "Quantos_casos_foram_escalonados_",
        "quantos casos foram encerrados?": "Quantos_casos_foram_encerrados_"
    }
    mapping = {c: alias[canon(c)] for c in df.columns if canon(c) in alias}
    mapping.update({c: c for c in df.columns if c in target_order and c not in mapping})
    df = df.rename(columns=mapping)
    for c in target_order:
        if c not in df.columns: df[c] = None
    return clean_object_df(df[target_order])

def normalize_table_guarda(df, target_order):
    df = df.copy()
    alias = {
        "id": "ID", "hora de inicio": "Hora_de_in__cio", "hora de conclusao": "Hora_de_conclus__o",
        "email": "Email", "tempo acao": "Tempo_A_____o",
        "qual painel ou alerta apresentou inconsistencias?": "Qual_painel_ou_alerta_apresentou_inconsist__ncia_",
        "id alerta:": "ID_Alerta_", "procedimento resolvido pelo time?": "_Procedimento_resolvido_pelo_time_",
        "solucao feita:": "Solu_____o_feita_", "nivel criticidade incidente (em teste)": "N__vel_Criticidade_Incidente__Em_Teste_",
        "incidente": "Incidente", "descricao do inc: exemplo: [boleto] - alto indice de boletos sem registro na cip": "Descri_____o_do_INC__Exemplo___BOLETO____Alto___ndice_de_boletos_sem_registro_na_CIP",
        "o cliente esta sendo impactado?": "O_cliente_est___sendo_impactado__",
        "possivel impacto: exemplo: nao liquidacao de boletos": "_Poss__vel_impacto__Exemplo__N__o_liquida_____o_de_boletos",
        "horario do primeiro impacto:": "Hor__rio_do_primeiro_impacto_",
        "volume de clientes impactados ? exemplo: 250 transacoes nos ultimos 15 minutos": "Volume_de_clientes_impactados___Exemplo__250_transa_____es_nos___ltimos_15_minutos__",
        "comunicacao de": "Comunica_____o_de"
    }
    mapping = {}
    for c in df.columns:
        k = canon(c)
        if "alerta" in k and "id" in k: mapping[c] = "ID_Alerta_"
        elif k in alias: mapping[c] = alias[k]
        else: mapping[c] = c
            
    df = df.rename(columns=mapping)
    for c in target_order:
        if c not in df.columns: df[c] = None
    return clean_object_df(df[target_order])

FILES_CONFIG = {
    "Monitoracao24x7.Rotina_de_TurnoV2": {
        "filename": "Rotinas de TurnoV2.xlsx", "mode": "replace", "func": normalize_turno_v2,
        "cols": ["ID", "Email", "Nome", "Hora_de_in_cio", "Hora_de_conclus_o", "Selecione_o_turno", "T1__Qual_tarefa_foi_executada_", "T2___Qual_tarefa_foi_executada_", "T3___Qual_tarefa_foi_executada__", "Ocorreu_algum_problema_na_execu__o_da_tarefa__", "Rotina_Automatizada", "Qual_atua__o_necess_ria__", "Descri__o_do_INC__Exemplo___BOLETO____Alto__ndice_de_boletos_sem_registro_na_CIP", "Poss_vel_impacto__Exemplo__N_o_liquida__o_de_boletos", "Hor_rio_do_primeiro_impacto_", "Deseja_adicionar_anexos_a_essa_observa__o_", "Detalhes_da_nova_tarefa_para_inclus_o_no_forms"]
    },
    "Monitoracao24x7.ID_AlertasV6": {
        "filename": "Indicador - V6 - AlertasComunicações.xlsx", "mode": "replace", "func": normalize_v6,
        "cols": ["ID", "Hora_inicio", "Hora_conclusao", "Email", "Tempo_Acao", "painel_alerta_inconsistencia", "Solicitante_bloqueio_conta", "ID_Alerta", "Execucao_Comunicacao_Critica", "Qual foi Horario disparado", "Procedimento_resolvido_pelo_time", "Solucao_feita", "Nivel_Criticidade_Incidente_Teste", "Incidente", "Descricao_INC", "Cliente_sendo_impactado", "Possivel_impacto", "Horario_primeiro_impacto", "Volume_clientes_impactados", "Justificativa", "Comunicacao", "Acao_Aplicavel", "Alerta_criado", "Coluna1"]
    },
    "Monitoracao24x7.AlertasV2": {
        "filename": "Canal alertas criticos - V2.xlsx", "mode": "replace", "func": normalize_alertas_v2,
        "cols": ["ID", "Nome do alerta", "Owner", "Data de Envio", "Hora de envio", "Hora de conclusão"]
    },
    "Monitoracao24x7.Execucao_massiva": {
        "filename": "Indicador Massivos.xlsx", "mode": "replace", "func": normalize_massivo,
        "cols": ["ID", "Email", "Nome", "Preditivo", "Tabulacao", "Query", "Hora_de_in__cio", "Hora_de_conclus__o", "Turno_", "Qual_a_____o_massiva_foi_executada_", "H___casos_a_serem_escalonado_ou_encerrados_", "incidente", "Quantidade_de_clientes_impactados_splunk", "chats_totais", "retencao_preditivo_cliente", "Casos_fechados_t1", "Acao_executada", "Quantos_casos_foram_escalonados_", "Quantos_casos_foram_encerrados_"]
    },
    "Monitoracao24x7.TableGuarda_INC": {
        "filename": "Table Guarda INCs 24x7.xlsx", "mode": "append", "func": normalize_table_guarda,
        "cols": ["ID", "Hora_de_in__cio", "Hora_de_conclus__o", "Email", "Tempo_A_____o", "Qual_painel_ou_alerta_apresentou_inconsist__ncia_", "ID_Alerta_", "_Procedimento_resolvido_pelo_time_", "Solu_____o_feita_", "N__vel_Criticidade_Incidente__Em_Teste_", "Incidente", "Descri_____o_do_INC__Exemplo___BOLETO____Alto___ndice_de_boletos_sem_registro_na_CIP", "O_cliente_est___sendo_impactado__", "_Poss__vel_impacto__Exemplo__N__o_liquida_____o_de_boletos", "Hor__rio_do_primeiro_impacto_", "Volume_de_clientes_impactados___Exemplo__250_transa_____es_nos___ltimos_15_minutos__", "Comunica_____o_de"]
    },
    "Monitoracao24x7.TableGuarda_INC_atual": {
        "filename": "Table Guarda INCs 24x7.xlsx", "mode": "replace", "func": normalize_table_guarda,
        "cols": ["ID", "Hora_de_in__cio", "Hora_de_conclus__o", "Email", "Tempo_A_____o", "Qual_painel_ou_alerta_apresentou_inconsist__ncia_", "ID_Alerta_", "_Procedimento_resolvido_pelo_time_", "Solu_____o_feita_", "N__vel_Criticidade_Incidente__Em_Teste_", "Incidente", "Descri_____o_do_INC__Exemplo___BOLETO____Alto___ndice_de_boletos_sem_registro_na_CIP", "O_cliente_est___sendo_impactado__", "_Poss__vel_impacto__Exemplo__N__o_liquida_____o_de_boletos", "Hor__rio_do_primeiro_impacto_", "Volume_de_clientes_impactados___Exemplo__250_transa_____es_nos___ltimos_15_minutos__", "Comunica_____o_de"]
    }
}


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
        from PySide6.QtWidgets import (
            QApplication, QWidget, QLabel, QLineEdit, QPushButton, QVBoxLayout, QComboBox
        )
        from PySide6.QtGui import QFont
        from PySide6.QtCore import Qt

        app = QApplication(_sys.argv)
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
    """Fallback PyQt5 para ambiente sem PySide6."""
    try:
        from PyQt5.QtWidgets import QApplication, QDialog, QVBoxLayout, QLabel, QComboBox, QLineEdit, QPushButton, QMessageBox
    except ImportError:
        return "AUTO", f"{getpass.getuser()}@c6bank.com"

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
    return None, None

def obter_destinatarios():
    try:
        sql = f"SELECT emails_principais, emails_cc FROM `datalab-pagamentos.ADMINISTRACAO_CELULA_PYTHON.Registro_automacoes` WHERE lower(TRIM(metodo_automacao)) = lower(TRIM('{NOME_SCRIPT}')) LIMIT 1"
        df = pandas_gbq.read_gbq(sql, project_id=PROJECT_ID)
        if df.empty: return [], []
        def limpar(raw):
            if not raw or str(raw).lower() == 'nan': return []
            return [x.strip() for x in str(raw).replace(';', ',').split(',') if '@' in x]
        return limpar(df.iloc[0]['emails_principais']), limpar(df.iloc[0]['emails_cc'])
    except:
        return [], []

def subir_com_staging(dataset_table, df, mode):
    """
    REGRA 2: Cria Staging, sobe DF, e faz Merge/Replace na final.
    """
    bq_client = bigquery.Client(project=PROJECT_ID)
    table_staging = f"{dataset_table}_STAGING"
    
    # 1. Copiar Schema (Implicitamente feito pelo pandas_gbq com schema da tabela final se existir, ou inferido)
    # Garante que staging seja limpa antes
    pandas_gbq.to_gbq(
        df, 
        table_staging, 
        project_id=PROJECT_ID, 
        if_exists='replace',
        table_schema=[{'name': c, 'type': 'STRING'} for c in df.columns] # Força string como no original
    )
    
    # 2. Operação Final
    if mode == 'replace':
        query = f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.{dataset_table}` AS
        SELECT * FROM `{PROJECT_ID}.{table_staging}`
        """
        bq_client.query(query).result()
        logger.info(f"Tabela {dataset_table} substituída via STAGING.")
        
    elif mode == 'append':
        # Dedup: Inserir apenas o que não existe (baseado em todas as colunas)
        query = f"""
        INSERT INTO `{PROJECT_ID}.{dataset_table}` 
        SELECT * FROM `{PROJECT_ID}.{table_staging}` AS S
        WHERE NOT EXISTS (
            SELECT 1 FROM `{PROJECT_ID}.{dataset_table}` AS T
            WHERE TO_JSON_STRING(T) = TO_JSON_STRING(S)
        )
        """
        # Nota: TO_JSON_STRING é um hack eficiente para comparar todas as colunas sem listar uma a uma dinamicamente
        bq_client.query(query).result()
        logger.info(f"Dados inseridos em {dataset_table} com deduplicação via STAGING.")

    # Limpeza Staging (Opcional, mas boa prática)
    bq_client.delete_table(f"{PROJECT_ID}.{table_staging}", not_found_ok=True)


def main():
    # 1. DETECÇÃO DE AMBIENTE
    modo = os.environ.get("MODO_EXECUCAO")
    usuario = os.environ.get("USUARIO_EXEC")

    if not modo:
        # Tenta detectar via GUI do template (Execucao) ou Fallback
        try:
            res_gui = Execucao.detectar()
            modo = res_gui.get("modo_execucao")
            usuario = res_gui.get("usuario")
        except:
            modo, usuario = _exibir_dialogo_inicial()
            if not modo: return
    
    if usuario and "@" not in usuario:
        usuario = f"{usuario}@c6bank.com"

    # Setup Logs
    base_log_dir = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "automacoes" / NOME_AUTOMACAO / "logs" / NOME_SCRIPT / datetime.now().strftime('%Y-%m-%d')
    base_log_dir.mkdir(parents=True, exist_ok=True)
    log_file = configurar_logs(base_log_dir)

    logger.info(f"=== INICIANDO {NOME_SCRIPT} ===")
    logger.info(f"Modo: {modo} | Usuário: {usuario}")

    status_final = "FALHA"
    arquivos_anexos = []
    tabelas_tocadas = []
    
    try:
        # Credenciais (se necessário via Dollynho, aqui usamos o padrão do ambiente/BQ)
        # dollynho.get_credential(...)
        
        count_processados = 0
        
        for table_key, config in FILES_CONFIG.items():
            fpath = DATA_DIR / config["filename"]
            
            logger.info(f"Processando: {table_key} | Arquivo: {fpath}")
            
            if not fpath.exists():
                logger.warning(f"Arquivo não encontrado: {fpath}")
                continue
                
            try:
                # Leitura
                df_raw = pd.read_excel(fpath, engine="openpyxl") # sheet_name padrão 0/primeira
                
                # Transformação
                df_clean = config["func"](df_raw, config["cols"])
                
                if df_clean.empty:
                    logger.info(f"DataFrame vazio para {table_key}")
                    continue
                    
                # Upload com Regra de Staging
                subir_com_staging(table_key, df_clean, config["mode"])
                
                tabelas_tocadas.append(table_key)
                count_processados += 1
                
            except Exception as e_file:
                logger.error(f"Erro ao processar {table_key}: {traceback.format_exc()}")
                # Não para o loop, tenta os próximos
                continue

        if count_processados > 0:
            status_final = "SUCESSO"
        elif not tabelas_tocadas:
            status_final = "SEM DADOS PARA PROCESSAR"
        else:
            status_final = "FALHA"
        
    except Exception as e:
        logger.error(f"Erro Crítico: {traceback.format_exc()}")
        status_final = "FALHA"
        
    finally:
        # Zipar Logs
        try:
            zip_name = base_log_dir / f"{NOME_SCRIPT}_{datetime.now().strftime('%H%M%S')}.zip"
            with zipfile.ZipFile(zip_name, 'w', zipfile.ZIP_DEFLATED) as zf:
                zf.write(log_file, arcname=log_file.name)
            arquivos_anexos.append(zip_name)
        except: pass

        # Enviar Email/Metricas
        try:
            dest_to, dest_cc = obter_destinatarios()
            if status_final != "SUCESSO":
                # Adiciona hardcoded de falha se necessario, ou confia na tabela BQ
                dest_to.extend(["carlos.lsilva@c6bank.com", "sofia.fernandes@c6bank.com"])
                
            client = _utilAutomacoesExec.AutomacoesExecClient(logger)
            client.publicar(
                nome_automacao=NOME_AUTOMACAO,
                metodo_automacao=NOME_SCRIPT,
                status=status_final,
                tempo_exec="00:00:00", 
                usuario=usuario,
                log_path=str(base_log_dir),
                destinatarios=list(set(dest_to + (dest_cc if status_final == "SUCESSO" else []))),
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
