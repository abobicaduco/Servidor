from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path


class ConfigServidor(BaseSettings):
    # Paths
    DIRETORIO_AUTOMACOES: Path
    PLANILHA_REGISTRO: Path
    PLANILHA_WORKFLOWS: Path
    DIRETORIO_FRONTEND_BUILD: Path

    # Business rules
    MAX_PROCESSOS_SIMULTANEOS: int = 3
    RELOAD_INTERVAL_MINUTES: int = 30
    RELOAD_COOLDOWN_SECONDS: int = 60

    # Server
    FRONTEND: bool = True
    HOST: str = "127.0.0.1"
    PORT: int = 5000
    TIMEZONE: str = "America/Sao_Paulo"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


config = ConfigServidor()
