import logging
import threading
from datetime import datetime

import pandas as pd

import Servidor  # noqa: E402


def _build_agendador():
    ag = Servidor.AgendadorMetodos.__new__(Servidor.AgendadorMetodos)
    ag.logger = logging.getLogger("test_agendador")
    ag.obter_mapeamento = lambda: {}
    ag.obter_exec_df = lambda: pd.DataFrame()
    ag.enfileirar_callback = lambda *_, **__: None
    ag.intervalo_segundos = 60
    ag.tz = Servidor.TZ
    ag.lock = threading.Lock()
    return ag


def _build_sincronizador():
    sync = Servidor.SincronizadorPlanilhas.__new__(Servidor.SincronizadorPlanilhas)
    sync.logger = logging.getLogger("test_sincronizador")
    return sync


def test_norm_key_normaliza_texto():
    assert Servidor.NormalizadorDF.norm_key("Ábç 123.py") == "abc123"
    assert Servidor.NormalizadorDF.norm_key(None) == ""


def test_normalizar_horarios():
    ag = _build_agendador()
    horarios = ag._normalizar_horarios("08:00; 9:30 /10:45 teste")
    assert horarios == ["08:00", "09:30", "10:45"]
    assert ag._normalizar_horarios("sem") == []


def test_normalizar_dias_semana():
    ag = _build_agendador()
    dias = ag._normalizar_dias_semana("segunda, quarta")
    assert dias == {0, 2}
    assert ag._normalizar_dias_semana("sem") == set()
    assert ag._normalizar_dias_semana("invalido") == set(range(0, 7))


def test_calcular_proxima_execucao():
    ag = _build_agendador()
    base = datetime(2024, 1, 1, 9, 0, tzinfo=Servidor.TZ)
    horarios = ["08:00", "10:00"]
    dias = {0, 1}
    proxima = ag._calcular_proxima_execucao(horarios, dias, base)
    assert proxima == datetime(2024, 1, 1, 10, 0, tzinfo=Servidor.TZ)
    base_tarde = datetime(2024, 1, 1, 11, 0, tzinfo=Servidor.TZ)
    proxima_tarde = ag._calcular_proxima_execucao(horarios, dias, base_tarde)
    assert proxima_tarde == datetime(2024, 1, 2, 8, 0, tzinfo=Servidor.TZ)


def test_safe_date_convert_local():
    ag = _build_agendador()
    df = pd.DataFrame({"data_exec": ["01/02/2024", "02/02/2024"], "hora_exec": ["10:00:00", "11:00:00"]})
    convertido = ag._safe_date_convert_local(df)
    assert "dt_full" in convertido.columns
    assert pd.to_datetime("2024-02-01 10:00:00") == convertido.loc[0, "dt_full"]


def test_preparar_exec_df():
    sync = _build_sincronizador()
    df = pd.DataFrame(
        {
            "data_exec": ["01/02/2024", "03/02/2024"],
            "hora_exec": ["10:00", "09:00"],
        }
    )
    preparado = sync._preparar_exec_df(df)
    assert "dt_full" in preparado.columns
    assert list(preparado["dt_full"].dt.strftime("%Y-%m-%d %H:%M")) == [
        "2024-02-03 09:00",
        "2024-02-01 10:00",
    ]

