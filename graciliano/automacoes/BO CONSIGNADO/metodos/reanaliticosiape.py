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
SUBIDA_BQ = "append"
NOME_SERVIDOR = "Servidor.py"
HEADLESS = False
NOME_AUTOMACAO = "BO CONSIGNADO"
NOME_SCRIPT = Path(__file__).stem.upper()

# Configurações Específicas
BQ_TABELA_DESTINO = "datalab-pagamentos.conciliacoes_monitoracao.RE_ANALITICO_SIAPE"
INPUT_DIR_ROOT = Path.home() / "graciliano" / "automacoes" / NOME_AUTOMACAO / "arquivos input" / Path(__file__).stem.lower()

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
                QApplication,
                QWidget,
                QLabel,
                QLineEdit,
                QPushButton,
                QVBoxLayout,
                QComboBox,
            )
            from PySide6.QtGui import QFont
            from PySide6.QtCore import Qt
        except ImportError:
            return {"modo_execucao": "AUTO", "usuario": f"{_getpass.getuser()}@c6bank.com"}

        app = QApplication.instance() or QApplication(_sys.argv)
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
    """Exibe diálogo PyQt5 para configurar execução manual."""
    res = Execucao.detectar()
    return res.get("modo_execucao"), res.get("usuario")

def obter_destinatarios():
    try:
        sql = f"SELECT emails_principais, emails_cc FROM `datalab-pagamentos.ADMINISTRACAO_CELULA_PYTHON.Registro_automacoes` WHERE lower(TRIM(metodo_automacao)) = lower(TRIM('{NOME_SCRIPT}')) LIMIT 1"
        df = pandas_gbq.read_gbq(sql, project_id="datalab-pagamentos")
        if df.empty: return ["carlos.lsilva@c6bank.com", "sofia.fernandes@c6bank.com", "antonio.marcoss@c6bank.com"], []
        def limpar(raw):
            if not raw or str(raw).lower() == 'nan': return []
            return [x.strip() for x in str(raw).replace(';', ',').split(',') if '@' in x]
        return limpar(df.iloc[0]['emails_principais']), limpar(df.iloc[0]['emails_cc'])
    except:
        return ["carlos.lsilva@c6bank.com", "sofia.fernandes@c6bank.com", "antonio.marcoss@c6bank.com"], []

# --- CLASSES DE LOGICA DE NEGOCIO ---

class LayoutDefinitions:
    @staticmethod
    def posicoes_layout() -> dict:
        return {
            "EXCL_SERV": [5, 12, 21, 23, 73, 84, 89, 90, 101, 104, 164, 176, 196],
            "EXCL_PENS": [5, 13, 22, 29, 31, 76, 87, 92, 93, 104, 107, 167, 179, 199],
            "SERV": [5, 12, 21, 23, 73, 84, 89, 90, 101, 104, 110, 122, 142],
            "PENS": [5, 13, 22, 29, 31, 76, 87, 92, 93, 104, 107, 113, 125, 145],
        }

    @staticmethod
    def nomes_colunas() -> dict:
        return {
            "EXCL_SERV": ["cod_orgao", "matricula", "upag", "uf", "cliente", "cpf_cnpj", "rubrica", "dig_rubrica", "valor", "prazo", "motivo_exclusao", "tipo", "cto_proposta", "dig_interno"],
            "EXCL_PENS": ["cod_orgao", "cod_interno", "matricula", "upag", "uf", "cliente", "cpf_cnpj", "rubrica", "dig_rubrica", "valor", "prazo", "motivo_exclusao", "tipo", "cto_proposta", "dig_interno"],
            "SERV": ["cod_orgao", "matricula", "upag", "uf", "cliente", "cpf_cnpj", "rubrica", "dig_rubrica", "valor", "prazo", "cod_ug_siape", "tipo", "cto_proposta", "dig_interno"],
            "PENS": ["cod_orgao", "cod_interno", "matricula", "upag", "uf", "cliente", "cpf_cnpj", "rubrica", "dig_rubrica", "valor", "prazo", "cod_ug_siape", "tipo", "cto_proposta", "dig_interno"],
        }

    @staticmethod
    def classificar_arquivo(p: Path) -> str:
        n = p.name.lower()
        if "EXCLU" in n and "PENS" in n: return "EXCL_PENS"
        if "EXCLU" in n and "SERV" in n: return "EXCL_SERV"
        if "PENS" in n: return "PENS"
        if "SERV" in n: return "SERV"
        return "SERV" # Default

    @staticmethod
    def split_por_posicoes(linha: str, pos: list) -> list:
        cortes = [0] + pos + [len(linha)]
        vals = []
        for i in range(len(cortes) - 1):
            a = cortes[i]
            b = cortes[i + 1]
            vals.append(linha[a:b])
        return vals

    @staticmethod
    def ler_arquivo(arquivo: Path, tipo: str) -> pd.DataFrame:
        pos = LayoutDefinitions.posicoes_layout()[tipo]
        cols = LayoutDefinitions.nomes_colunas()[tipo]
        linhas = []
        try:
            with open(arquivo, "r", encoding="latin-1", errors="ignore") as fd:
                for raw in fd:
                    ln = raw.rstrip("\r\n")
                    if not ln: continue
                    partes = LayoutDefinitions.split_por_posicoes(ln, pos)
                    if len(partes) < len(cols):
                        partes += [""] * (len(cols) - len(partes))
                    d = {cols[i]: partes[i].strip() for i in range(len(cols))}
                    d["tipo_arquivo"] = tipo
                    d["arquivo_nome"] = arquivo.name
                    d["dt_coleta_utc"] = datetime.utcnow().isoformat()
                    linhas.append(d)
        except Exception as e:
            logger.error(f"Erro ao ler arquivo {arquivo}: {e}")
            return pd.DataFrame()
        
        if not linhas:
            return pd.DataFrame(columns=cols + ["tipo_arquivo", "arquivo_nome", "dt_coleta_utc"])
        
        df = pd.DataFrame(linhas)
        for c in df.columns:
            df[c] = df[c].astype(str)
        return df

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
    base_log_dir = Path.home() / "graciliano" / "automacoes" / NOME_AUTOMACAO / "logs" / NOME_SCRIPT / datetime.now().strftime('%Y-%m-%d')
    base_log_dir.mkdir(parents=True, exist_ok=True)
    log_file = configurar_logs(base_log_dir)

    logger.info(f"=== INICIANDO {NOME_SCRIPT} ===")
    logger.info(f"Modo: {modo} | Usuário: {usuario}")
    logger.info(f"Input Dir: {INPUT_DIR_ROOT}")

    status_final = "FALHA"
    arquivos_anexos = []
    
    try:
        # === PROCURAR ARQUIVOS ===
        achados = []
        if INPUT_DIR_ROOT.exists():
            aceitar = {"", ".txt", ".csv", ".ok"}
            pular = {".xlsx", ".xls", ".xlsm", ".pdf", ".png", ".jpg", ".jpeg", ".gif", ".zip", ".rar", ".7z", ".log", ".tmp"}
            for p in INPUT_DIR_ROOT.rglob("*"):
                if p.is_file() and p.suffix.lower() not in pular:
                    # Logica legado: aceita extensao vazia ou numerica ou lista aceita
                    if p.suffix.lower() in aceitar or (p.suffix.startswith(".") and p.suffix[1:].isdigit()):
                        if p.stat().st_size > 0:
                            achados.append(p)
        
        achados = sorted(achados)
        logger.info(f"Arquivos encontrados: {len(achados)}")

        if not achados:
            status_final = "SEM DADOS PARA PROCESSAR"
        else:
            # === PROCESSAMENTO ===
            dfs = []
            
            # Copiar para log (comportamento legado preservado)
            pares = []
            for arq in achados:
                destino = base_log_dir / arq.name
                shutil.copy2(str(arq), str(destino))
                pares.append((arq, destino))
                arquivos_anexos.append(destino)

            # Ler Dataframes
            for _, destino in pares:
                try:
                    tipo = LayoutDefinitions.classificar_arquivo(destino)
                    df = LayoutDefinitions.ler_arquivo(destino, tipo)
                    if not df.empty:
                        dfs.append(df)
                except Exception as e:
                    logger.error(f"Erro ao processar {destino.name}: {e}")

            if not dfs:
                status_final = "SEM DADOS PARA PROCESSAR"
            else:
                # Concatenar
                cols_all = sorted({c for d in dfs for c in d.columns})
                for i in range(len(dfs)):
                    for c in cols_all:
                        if c not in dfs[i].columns:
                            dfs[i][c] = "" if c != "dt_coleta_utc" else datetime.utcnow().isoformat()
                    dfs[i] = dfs[i][cols_all] # Reorder
                
                df_final = pd.concat(dfs, ignore_index=True)
                
                # === SUBIDA BIGQUERY (STAGING -> MERGE) ===
                client = bigquery.Client()
                project_id = BQ_TABELA_DESTINO.split('.')[0]
                dataset_id = BQ_TABELA_DESTINO.split('.')[1]
                table_id = BQ_TABELA_DESTINO.split('.')[2]
                
                # Nome tabelas
                table_ref_final = f"{project_id}.{dataset_id}.{table_id}"
                table_ref_stg = f"{project_id}.{dataset_id}.{table_id}_STAGING"

                # Schema
                schema = []
                for c in df_final.columns:
                    tipo = "TIMESTAMP" if c == "dt_coleta_utc" else "STRING"
                    schema.append(bigquery.SchemaField(c, tipo))

                # 1. Garantir Tabela Final Existe
                try:
                    client.get_table(table_ref_final)
                except:
                    logger.info("Criando tabela final...")
                    t = bigquery.Table(table_ref_final, schema=schema)
                    t.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY, field="dt_coleta_utc")
                    client.create_table(t)

                # 2. Subir Staging (Replace)
                logger.info("Subindo para Staging...")
                pandas_gbq.to_gbq(
                    df_final,
                    destination_table=f"{dataset_id}.{table_id}_STAGING",
                    project_id=project_id,
                    if_exists="replace",
                    table_schema=[{'name': c, 'type': 'TIMESTAMP' if c == 'dt_coleta_utc' else 'STRING'} for c in df_final.columns]
                )

                # 3. Merge / Dedup (Insert New)
                logger.info("Executando Merge...")
                
                # Construir Query Dinamica
                cols_sql = ", ".join([f"`{c}`" for c in df_final.columns])
                # Dedup usando todas as colunas como chave
                conditions = " AND ".join([f"T.`{c}` = S.`{c}`" for c in df_final.columns])
                
                query = f"""
                INSERT INTO `{table_ref_final}` ({cols_sql})
                SELECT {cols_sql}
                FROM `{table_ref_stg}` S
                WHERE NOT EXISTS (
                    SELECT 1 FROM `{table_ref_final}` T
                    WHERE {conditions}
                )
                """
                job = client.query(query)
                job.result() # Wait
                
                # Limpar Staging
                client.delete_table(table_ref_stg, not_found_ok=True)
                
                status_final = "SUCESSO"

    except Exception as e:
        logger.error(f"Erro: {traceback.format_exc()}")
        status_final = "FALHA"
    finally:
        # Zipar Logs
        try:
            zip_name = base_log_dir / f"{NOME_SCRIPT}_{datetime.now().strftime('%H%M%S')}.zip"
            with zipfile.ZipFile(zip_name, 'w', zipfile.ZIP_DEFLATED) as zf:
                zf.write(log_file, arcname=log_file.name)
            arquivos_anexos.append(zip_name)
        except: pass

        # Enviar Email/Metricas via _utilAutomacoesExec
        try:
            dest_to, dest_cc = obter_destinatarios()
            client_exec = _utilAutomacoesExec.AutomacoesExecClient(logger)
            client_exec.publicar(
                nome_automacao=NOME_AUTOMACAO,
                metodo_automacao=NOME_SCRIPT,
                status=status_final,
                tempo_exec="00:00:00", 
                usuario=usuario,
                log_path=str(base_log_dir),
                destinatarios=dest_to + (dest_cc if status_final == "SUCESSO" else []),
                send_email=True,
                anexos=[str(x) for x in arquivos_anexos],
                tabela_referencia=BQ_TABELA_DESTINO if status_final == "SUCESSO" else ""
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
