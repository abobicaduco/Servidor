import threading
import pytz
import time
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from modules.config import config
from modules.registry import obter_scripts_agendaveis, obter_workflows
from modules import executor
from modules import workflow_manager

_tz = pytz.timezone(config.TIMEZONE)
scheduler = BackgroundScheduler(timezone=_tz)


def _run_workflow_async(workflow_name: str, scripts: list[str]) -> None:
    """Wrapper: run workflow in a daemon thread so the scheduler does not block."""
    t = threading.Thread(
        target=workflow_manager.iniciar_workflow,
        args=(workflow_name, scripts),
        daemon=True,
        name=f"workflow-cron-{workflow_name}",
    )
    t.start()


def _job_wrapper(script_name: str, script_path: str, area_name: str) -> None:
    executor.enqueue_script(
        script_name, script_path, area_name,
        scheduled_timestamp=time.time(),
        trigger_reason="scheduled",
    )


def _apply_catchup(scripts: list[dict]) -> None:
    """
    For each active script that had scheduled hours earlier today,
    enqueue it ONCE with the priority of its oldest missed hour.
    """
    now = datetime.now(_tz)
    for s in scripts:
        past_hours = [h for h in s["cron_schedule"] if h <= now.hour]
        if not past_hours:
            continue
        oldest_hour = min(past_hours)
        catchup_ts = now.replace(hour=oldest_hour, minute=0, second=0, microsecond=0).timestamp()
        print(f"[CATCHUP] {s['script_name']} missed {oldest_hour:02d}:00 → enqueuing now")
        executor.enqueue_script(
            s["script_name"],
            str(s["path_obj"]),
            s["area_name"],
            scheduled_timestamp=catchup_ts,
            trigger_reason="catchup",
        )


def recarregar_agendamentos() -> tuple[list, list]:
    """Remove all non-reload jobs and re-register from spreadsheets."""
    print("[RELOAD] Hot-reloading schedules...")
    for job in scheduler.get_jobs():
        if job.id != "hot_reload_job":
            job.remove()

    scripts = obter_scripts_agendaveis()
    for s in scripts:
        for hora in s["cron_schedule"]:
            job_id = f"{s['script_name']}_{hora:02d}h"
            scheduler.add_job(
                _job_wrapper,
                CronTrigger(hour=hora, minute=0, timezone=_tz),
                id=job_id,
                name=f"{s['script_name']} @ {hora:02d}:00",
                args=[s["script_name"], str(s["path_obj"]), s["area_name"]],
                replace_existing=True,
                misfire_grace_time=86400,
                coalesce=True,
            )

    workflows = obter_workflows()
    for w in workflows:
        for hora in w["horarios"]:
            job_id = f"flow_{w['workflow_name']}_{hora:02d}h"
            scheduler.add_job(
                _run_workflow_async,
                CronTrigger(hour=hora, minute=0, timezone=_tz),
                id=job_id,
                name=f"WORKFLOW:{w['workflow_name']} @ {hora:02d}:00",
                args=[w["workflow_name"], w["scripts"]],
                replace_existing=True,
                misfire_grace_time=86400,
                coalesce=True,
            )

    print(f"[RELOAD OK] {len(scripts)} scripts | {len(workflows)} workflows registered.")
    return scripts, workflows


def iniciar_scheduler() -> None:
    scripts, _ = recarregar_agendamentos()
    _apply_catchup(scripts)
    scheduler.add_job(
        recarregar_agendamentos,
        "interval",
        minutes=config.RELOAD_INTERVAL_MINUTES,
        id="hot_reload_job",
        name="Hot-Reload (auto)",
    )
    scheduler.start()
    print(f"[BOOT] APScheduler started (timezone: {config.TIMEZONE}, CPU: ~0%).")


def pausar_tudo() -> None:
    scheduler.pause()


def retomar_tudo() -> None:
    scheduler.resume()


def obter_jobs() -> list[dict]:
    jobs = []
    for job in scheduler.get_jobs():
        next_run = job.next_run_time
        jobs.append({
            "id": job.id,
            "name": job.name or job.id,
            "next_run_br": next_run.astimezone(_tz).isoformat() if next_run else None,
            "trigger": "cron" if "cron" in job.id or "flow_" in job.id else "interval",
        })
    return sorted(jobs, key=lambda j: j["next_run_br"] or "")
