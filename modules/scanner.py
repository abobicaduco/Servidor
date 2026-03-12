import os
from pathlib import Path
from modules.config import config


def normalize_name(raw: str) -> str:
    """Strip whitespace, lowercase, remove .py extension."""
    s = str(raw).strip().lower()
    return s[:-3] if s.endswith(".py") else s


def _is_under_metodos(path: Path) -> bool:
    return any(part.lower() == "metodos" for part in path.parts)


def buscar_arquivos_locais() -> dict[str, Path]:
    """
    Walk DIRETORIO_AUTOMACOES recursively.
    Returns {normalized_name: full_path} for all .py files under metodos/ folders.
    Files starting with '_' are skipped.
    Duplicate names: first found wins, warning printed.
    """
    found: dict[str, Path] = {}

    if not config.DIRETORIO_AUTOMACOES.exists():
        print(f"[WARN] DIRETORIO_AUTOMACOES does not exist: {config.DIRETORIO_AUTOMACOES}")
        return found

    for root, _dirs, files in os.walk(config.DIRETORIO_AUTOMACOES):
        root_path = Path(root)
        if not _is_under_metodos(root_path):
            continue
        for filename in files:
            if not filename.endswith(".py") or filename.startswith("_"):
                continue
            name = normalize_name(filename)
            full_path = root_path / filename
            if name in found:
                print(f"[WARN] Duplicate script name '{name}': keeping {found[name]}, ignoring {full_path}")
                continue
            found[name] = full_path

    print(f"[BOOT] Disk scan complete: {len(found)} .py files found under metodos/ folders.")
    return found
