import pandas as pd
import sys
from modules.config import config
from modules.scanner import buscar_arquivos_locais, normalize_name


def _safe_str(val) -> str:
    return "" if pd.isna(val) else str(val).strip()


def _parse_hours(raw: str) -> list[int]:
    hours = []
    for part in raw.split(","):
        part = part.strip()
        if part.isdigit():
            h = int(part)
            if 0 <= h <= 23:
                hours.append(h)
    return sorted(set(hours))


def obter_todos_scripts_planilha() -> list[dict]:
    """
    Returns ALL scripts from the registry spreadsheet (active and inactive).
    Used by /api/scripts and /api/areas endpoints.
    Does NOT filter by availability on disk.
    """
    local_files = buscar_arquivos_locais()
    try:
        df = pd.read_excel(config.PLANILHA_REGISTRO, engine="openpyxl")
    except Exception as e:
        print(f"[ERR] Failed to read registry spreadsheet: {e}")
        return []

    result = []
    for row in df.itertuples(index=False):
        raw_name = _safe_str(getattr(row, "script_name", ""))
        if not raw_name:
            continue
        name = normalize_name(raw_name)
        area = _safe_str(getattr(row, "area_name", "sem area")).lower()
        is_active = _safe_str(getattr(row, "is_active", "false")).lower() == "true"
        hours = _parse_hours(_safe_str(getattr(row, "cron_schedule", "")))
        result.append({
            "script_name": name,
            "area_name": area,
            "is_active": is_active,
            "cron_schedule": hours,
            "available_locally": name in local_files,
            "path": str(local_files[name]) if name in local_files else None,
            "emails_principal": _safe_str(getattr(row, "emails_principal", "")),
            "emails_cc": _safe_str(getattr(row, "emails_cc", "")),
            "move_file": _safe_str(getattr(row, "move_file", "false")).lower() == "true",
            "movimentacao_financeira": _safe_str(getattr(row, "movimentacao_financeira", "nao")).lower(),
            "interacao_cliente": _safe_str(getattr(row, "interacao_cliente", "nao")).lower(),
            "tempo_manual": int(getattr(row, "tempo_manual", 0) if not pd.isna(getattr(row, "tempo_manual", 0)) else 0),
        })
    return result


def obter_scripts_agendaveis() -> list[dict]:
    """
    Returns only scripts that: is_active=True AND have hours AND exist on disk.
    Used by the scheduler to register cron jobs.
    """
    all_scripts = obter_todos_scripts_planilha()
    local_files = buscar_arquivos_locais()
    schedulable = [
        s for s in all_scripts
        if s["is_active"]
        and s["cron_schedule"]
        and s["available_locally"]
    ]
    # Attach full Path object for subprocess use
    for s in schedulable:
        s["path_obj"] = local_files[s["script_name"]]
    print(f"[RELOAD OK] {len(schedulable)} schedulable scripts out of {len(all_scripts)} total.")
    return schedulable


def obter_workflows() -> list[dict]:
    """Returns workflow definitions from workflows.xlsx. Returns [] if file missing."""
    if not config.PLANILHA_WORKFLOWS.exists():
        return []
    try:
        df = pd.read_excel(config.PLANILHA_WORKFLOWS, engine="openpyxl")
    except Exception as e:
        print(f"[WARN] Failed to read workflows spreadsheet: {e}")
        return []

    workflows = []
    for _, row in df.iterrows():
        name = _safe_str(row.get("Workflow_name", ""))
        if not name:
            continue
        scripts = [s.strip().lower() for s in str(row.get("script_name", "")).split(",") if s.strip()]
        hours = _parse_hours(_safe_str(row.get("horario", "")))
        if scripts and hours:
            workflows.append({"workflow_name": name, "scripts": scripts, "horarios": hours})
    return workflows
