import subprocess
import sys
import time
import threading
from modules import executor
from modules import scheduler_engine
from modules.scanner import buscar_arquivos_locais

_state: dict = {
    "active": False,
    "name": None,
    "current_script": None,
    "progress": None,
    "log": [],
}
_lock = threading.Lock()


def get_state() -> dict:
    with _lock:
        return {k: v for k, v in _state.items()}


def is_active() -> bool:
    return _state["active"]


def iniciar_workflow(workflow_name: str, script_names: list[str]) -> None:
    """
    Runs workflow sequentially in the calling thread.
    Must be called from a daemon thread (started by api.py endpoint).
    """
    print(f"\n[WORKFLOW] Starting: {workflow_name} ({len(script_names)} scripts)")

    with _lock:
        _state.update({"active": True, "name": workflow_name, "log": [], "current_script": None, "progress": None})

    scheduler_engine.pausar_tudo()
    executor.set_workflow_state(True)
    killed = executor.kill_all_regular_processes()
    if killed:
        print(f"[WORKFLOW] Terminated {len(killed)} regular processes: {killed}")
    time.sleep(0.5)  # grace period

    local_files = buscar_arquivos_locais()
    total = len(script_names)

    for i, script_name in enumerate(script_names, 1):
        progress_str = f"{i}/{total}"
        with _lock:
            _state["current_script"] = script_name
            _state["progress"] = progress_str

        print(f"[WORKFLOW] Step {progress_str}: {script_name}")
        step_log = {"script": script_name, "step": progress_str, "status": "not_found"}

        path = local_files.get(script_name)
        if path is None:
            print(f"[WARN] Workflow step '{script_name}' not found on disk — skipping.")
            with _lock:
                _state["log"].append(step_log)
            continue

        proc = None
        try:
            proc = subprocess.Popen(
                [sys.executable, str(path)],
                shell=False,
                cwd=str(path.parent),
            )
            with executor._running_lock:
                executor.running_processes[proc.pid] = {
                    "pid": proc.pid,
                    "proc_obj": proc,
                    "script_name": f"[FLOW] {script_name}",
                    "area_name": workflow_name.upper(),
                    "start_time": time.time(),
                    "is_workflow_item": True,
                    "trigger_reason": "workflow",
                }
            proc.wait()
            status = "success" if proc.returncode == 0 else f"error (exit {proc.returncode})"
        except Exception as exc:
            status = f"exception: {exc}"
            print(f"[CRIT] Workflow step {script_name}: {exc}")
        finally:
            if proc:
                with executor._running_lock:
                    executor.running_processes.pop(proc.pid, None)

        step_log["status"] = status
        with _lock:
            _state["log"].append(step_log)
        print(f"[WORKFLOW] Step {progress_str} done: {status}")

    print(f"[WORKFLOW] Completed: {workflow_name}\n")
    with _lock:
        _state.update({"active": False, "name": workflow_name, "current_script": None, "progress": None})

    executor.set_workflow_state(False)
    scheduler_engine.retomar_tudo()
