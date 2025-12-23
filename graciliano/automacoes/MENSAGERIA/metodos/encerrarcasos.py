from __future__ import annotations

from pathlib import Path
from datetime import datetime
import logging
import sys
import csv
import io

from typing import List

from cardutil.mciipm import block_1014_check, unblock_1014  # pip install cardutil

# ============================================================
# CONFIGURAÇÕES FIXAS (CAMINHOS)
# ============================================================

PASTA_IPM = (
    Path.home()
    / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A"
    / "Mensageria e Cargas Operacionais - 11.CelulaPython"
    / "graciliano"
    / "automacoes"
    / "BO CARTOES"
    / "arquivos input"
    / "mastercard_464"
)

PASTA_CSV = PASTA_IPM / "csv"
PASTA_PROCESSADOS = PASTA_IPM / "ja_processados"

EBCDIC_CANDIDATES = ["cp500", "cp1047", "cp037", "cp1140", "cp273", "cp1147", "cp875"]

# tamanho típico de registro T464 em bytes (fallback)
T464_RECORD_LENGTH = 250


# ============================================================
# LOG
# ============================================================

def configurar_logger() -> logging.Logger:
    nome_script = Path(__file__).stem.lower()
    base_logs = (
        Path.home()
        / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A"
        / "Mensageria e Cargas Operacionais - 11.CelulaPython"
        / "graciliano"
        / "automacoes"
        / "BO CARTOES"
        / "logs"
        / nome_script
    )
    data_dir = datetime.now().strftime("%d.%m.%Y")
    dir_dia = base_logs / data_dir
    dir_dia.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = dir_dia / f"{nome_script}_{ts}.log"

    logger = logging.getLogger(nome_script)
    logger.setLevel(logging.INFO)
    logger.handlers = []

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    logger.propagate = False
    logger.info("logger_inicializado log_file=%s", log_file)
    return logger


# ============================================================
# HEURÍSTICAS DE ENCODING
# ============================================================

def is_printable_ratio(s: str) -> float:
    return sum(1 for c in s if c.isprintable() or c in "\r\n\t") / len(s) if s else 0.0


def score_text(s: str) -> float:
    if not s:
        return 0.0
    r = is_printable_ratio(s)
    at_ratio = s.count("@") / len(s)
    nul_ratio = s.count("\x00") / len(s)
    digit_ratio = sum(ch.isdigit() for ch in s) / len(s)
    return (r * 1.0) + (digit_ratio * 0.2) - (at_ratio * 0.5) - (nul_ratio * 0.8)


def detect_best_ebcdic(raw: bytes) -> str:
    sample = raw[:16384]
    best_enc = None
    best_score = float("-inf")
    for enc in EBCDIC_CANDIDATES:
        try:
            text = sample.decode(enc, errors="ignore")
            s = score_text(text)
            if s > best_score:
                best_score = s
                best_enc = enc
        except Exception:
            continue
    return best_enc or "cp500"


def decode_bytes(raw: bytes, encoding: str) -> str:
    return raw.decode(encoding, errors="ignore")


def limpar_texto(txt: str) -> str:
    # mesma limpeza que você já usava
    while "@@@@@@" in txt:
        txt = txt.replace("@@@@@@", "@@@ @@@")
    txt = txt.replace("@", " ")
    return txt


# ============================================================
# LEITURA E QUEBRA EM REGISTROS (GARANTINDO >1 LINHA)
# ============================================================

def ler_linhas_ipm(caminho_ipm: Path, logger: logging.Logger) -> List[str]:
    """
    Lê o IPM, trata 1014-block se necessário, detecta encoding,
    limpa caracteres e devolve lista de registros (linhas).

    Se não houver quebras de linha, faz fallback quebrando em blocos
    fixos de T464_RECORD_LENGTH bytes, pra evitar "só uma linha".
    """
    logger.info("lendo_arquivo_ipm caminho=%s", caminho_ipm)

    with caminho_ipm.open("rb") as f:
        sample = f.read(2500)
        f.seek(0)

        if block_1014_check(sample):
            logger.info("arquivo_detectado_1014_blocked=True -> usando_unblock_1014(cardutil)")
            tmp = io.BytesIO()
            unblock_1014(f, tmp)
            raw = tmp.getvalue()
        else:
            logger.info("arquivo_detectado_1014_blocked=False -> leitura_direta")
            raw = f.read()

    logger.info("tamanho_bytes_lido=%d", len(raw))

    encoding = detect_best_ebcdic(raw)
    logger.info("encoding_detectado=%s", encoding)

    txt = decode_bytes(raw, encoding)
    txt = limpar_texto(txt)

    # tentativa 1: usar quebras de linha existentes
    linhas = [linha.rstrip("\r\n") for linha in txt.splitlines() if linha.strip()]
    logger.info("qtde_linhas_splitlines=%d", len(linhas))

    # se só veio 0 ou 1 linha, forçamos quebra em blocos fixos
    if len(linhas) <= 1 and len(raw) > T464_RECORD_LENGTH:
        logger.warning(
            "poucas_linhas_detectadas (%d). aplicando_fallback_blocos_fixos_%d_bytes",
            len(linhas),
            T464_RECORD_LENGTH,
        )
        linhas = []
        for i in range(0, len(raw), T464_RECORD_LENGTH):
            chunk = raw[i : i + T464_RECORD_LENGTH]
            if not chunk:
                continue
            rec_txt = limpar_texto(decode_bytes(chunk, encoding)).rstrip("\r\n")
            if rec_txt.strip():
                linhas.append(rec_txt)

        logger.info("qtde_linhas_apos_fallback=%d", len(linhas))

    return linhas


# ============================================================
# IPM -> CSV (1 REGISTRO POR LINHA)
# ============================================================

def converter_ipm_para_csv(caminho_ipm: Path, caminho_csv: Path, logger: logging.Logger) -> None:
    """
    Converte IPM para CSV com duas colunas:
    - numero_linha (1, 2, 3, ...)
    - conteudo_arquivo (texto do registro)
    """
    logger.info("inicio_conversao_ipm_para_csv ipm=%s csv=%s", caminho_ipm, caminho_csv)

    linhas = ler_linhas_ipm(caminho_ipm, logger)
    if not linhas:
        logger.warning("nenhuma_linha_valida_para_gerar_csv ipm=%s", caminho_ipm)
        return

    caminho_csv.parent.mkdir(parents=True, exist_ok=True)

    total_linhas = 0
    with caminho_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["numero_linha", "conteudo_arquivo"])

        for idx, linha in enumerate(linhas, start=1):
            writer.writerow([idx, linha])
            total_linhas = idx
            if idx % 100_000 == 0:
                logger.info("linhas_csv_gravadas=%d", idx)

    logger.info(
        "csv_gerado caminho=%s total_linhas=%d",
        caminho_csv,
        total_linhas,
    )


# ============================================================
# MAIN: PROCESSAR TODOS OS .IPM DA PASTA FIXA
# ============================================================

def main() -> int:
    logger = configurar_logger()

    logger.info("inicio_execucao mastercard464 PASTA_IPM=%s", PASTA_IPM)

    PASTA_IPM.mkdir(parents=True, exist_ok=True)
    PASTA_CSV.mkdir(parents=True, exist_ok=True)
    PASTA_PROCESSADOS.mkdir(parents=True, exist_ok=True)

    arquivos_ipm = sorted(PASTA_IPM.glob("*.ipm"))

    if not arquivos_ipm:
        logger.info("nenhum_arquivo_ipm_encontrado pasta=%s", PASTA_IPM)
        return 0

    for arq in arquivos_ipm:
        logger.info("processando_arquivo_ipm nome=%s", arq.name)
        try:
            csv_destino = PASTA_CSV / f"{arq.stem}.csv"
            converter_ipm_para_csv(arq, csv_destino, logger)

            destino_processado = PASTA_PROCESSADOS / arq.name
            arq.rename(destino_processado)
            logger.info(
                "arquivo_processado_e_movido nome=%s destino=%s",
                arq.name,
                destino_processado,
            )
        except Exception:
            logger.exception("falha_ao_processar_arquivo_ipm nome=%s", arq.name)

    logger.info("execucao_concluida_sucesso")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())