import subprocess
import sys
import time
import threading
from pathlib import Path
from queue import PriorityQueue
import psutil
from modules.config import config

# ── Shared state (all protected by locks or atomic Python GIL semantics) ──────
execution_semaphore = threading.Semaphore(config.MAX_PROCESSOS_SIMULTANEOS)
task_queue: PriorityQueue = PriorityQueue()
running_processes: dict[int, dict] = {}   # {pid: process_info}
is_workflow_active: bool = False
_start_time = time.time()

_running_lock = threading.Lock()


def set_workflow_state(active: bool) -> None:
    global is_workflow_active
    is_workflow_active = active
    print(f"[WORKFLOW] Queue {'FROZEN' if active else 'RESUMED'}.")


def enqueue_script(
    script_name: str,
    script_path: str,
    area_name: str,
    scheduled_timestamp: float,
    is_workflow_item: bool = False,
    trigger_reason: str = "scheduled",
) -> bool:
    """
    Thread-safe enqueue with deduplication.
    Returns True if enqueued, False if duplicate.
    """
    # Check running
    with _running_lock:
        if any(d["script_name"] == script_name for d in running_processes.values()):
            print(f"[DUP] Already running: {script_name}")
            return False

    # Check queue (snapshot iteration — PriorityQueue.queue is a list)
    for _, _, task in list(task_queue.queue):
        if task["script_name"] == script_name:
            print(f"[DUP] Already queued: {script_name}")
            return False

    task_queue.put((scheduled_timestamp, time.time(), {
        "script_name": script_name,
        "path": script_path,
        "area_name": area_name,
        "scheduled_timestamp": scheduled_timestamp,
        "is_workflow_item": is_workflow_item,
        "trigger_reason": trigger_reason,
    }))
    print(f"[QUEUE] Enqueued: {script_name} | priority={scheduled_timestamp:.0f} | reason={trigger_reason}")
    return True


def _run_process(task_data: dict) -> None:
    """Worker thread: starts subprocess, waits for it, cleans up."""
    script_name = task_data["script_name"]
    raw_path = task_data["path"]
    script_path = Path(raw_path) if not hasattr(raw_path, "parent") else raw_path

    print(f"[>] Starting: {script_name}")
    proc = None
    try:
        proc = subprocess.Popen(
            [sys.executable, str(script_path)],
            shell=False,
            cwd=str(script_path.parent),
        )
        with _running_lock:
            running_processes[proc.pid] = {
                "pid": proc.pid,
                "proc_obj": proc,
                "script_name": script_name,
                "area_name": task_data["area_name"],
                "start_time": time.time(),
                "is_workflow_item": task_data["is_workflow_item"],
                "trigger_reason": task_data["trigger_reason"],
            }

        proc.wait()
        tag = "[OK]" if proc.returncode == 0 else "[ERR]"
        elapsed = round(time.time() - running_processes.get(proc.pid, {}).get("start_time", time.time()), 1)
        print(f"{tag} {script_name} | exit={proc.returncode} | elapsed={elapsed}s")

    except Exception as exc:
        print(f"[CRIT] Failed to start {script_name}: {exc}")
    finally:
        if proc and proc.pid in running_processes:
            with _running_lock:
                running_processes.pop(proc.pid, None)
        execution_semaphore.release()
        print(f"[-] Slot released. (from: {script_name})")


def _queue_processor() -> None:
    """Daemon thread: drains the PriorityQueue respecting the semaphore."""
    while True:
        if is_workflow_active:
            time.sleep(0.5)
            continue
        _, _, task_data = task_queue.get()
        execution_semaphore.acquire()   # blocks until a slot is free
        t = threading.Thread(target=_run_process, args=(task_data,), daemon=True)
        t.start()
        task_queue.task_done()


def kill_process(pid: int) -> bool:
    """Kill a specific PID and all its child processes."""
    with _running_lock:
        info = running_processes.get(pid)
    if not info:
        return False
    try:
        parent = psutil.Process(pid)
        children = parent.children(recursive=True)
        for child in children:
            try: child.kill()
            except psutil.NoSuchProcess: pass
        parent.kill()
        print(f"[KILL] {info['script_name']} (PID {pid}) terminated.")
    except psutil.NoSuchProcess:
        pass
    except Exception as exc:
        print(f"[WARN] Error killing PID {pid}: {exc}")
    finally:
        with _running_lock:
            running_processes.pop(pid, None)
        execution_semaphore.release()
    return True


def kill_all_regular_processes() -> list[str]:
    """Kill all non-workflow processes. Returns list of killed script names."""
    with _running_lock:
        targets = [(pid, info) for pid, info in running_processes.items()
                   if not info["is_workflow_item"]]
    killed = []
    for pid, info in targets:
        if kill_process(pid):
            killed.append(info["script_name"])
    return killed


def graceful_shutdown() -> None:
    print("[SHUTDOWN] Killing all child processes...")
    with _running_lock:
        all_pids = list(running_processes.keys())
    for pid in all_pids:
        kill_process(pid)
    print("[SHUTDOWN] Done.")


def get_uptime_seconds() -> float:
    return round(time.time() - _start_time, 1)


# Start the queue processor daemon thread at module import time
_queue_thread = threading.Thread(target=_queue_processor, daemon=True, name="queue-processor")
_queue_thread.start()
