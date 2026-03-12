import time
import threading
from datetime import datetime
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from modules.config import config
from modules.scheduler_engine import _tz
from modules import executor, scheduler_engine, workflow_manager
from modules.registry import obter_todos_scripts_planilha, obter_workflows, obter_scripts_agendaveis
from modules.scanner import buscar_arquivos_locais

app = Flask(
    __name__,
    static_folder=str(config.DIRETORIO_FRONTEND_BUILD.resolve()),
    static_url_path="/",
)
CORS(app)

_last_reload_time = 0.0
_reload_lock = threading.Lock()


# ── Frontend serving ──────────────────────────────────────────────────────────

@app.route("/")
def serve_root():
    if config.FRONTEND:
        return send_from_directory(app.static_folder, "index.html")
    return jsonify({"status": "backend-only", "frontend": False})


@app.route("/<path:path>")
def serve_static(path):
    if config.FRONTEND:
        try:
            return send_from_directory(app.static_folder, path)
        except Exception:
            return send_from_directory(app.static_folder, "index.html")
    return jsonify({"error": "Frontend disabled"}), 404


# ── Core status ───────────────────────────────────────────────────────────────

@app.route("/api/status")
def api_status():
    now = time.time()
    with executor._running_lock:
        running = [
            {
                "pid": info["pid"],
                "script_name": info["script_name"],
                "area_name": info["area_name"],
                "running_time_seconds": int(now - info["start_time"]),
                "is_workflow": info["is_workflow_item"],
                "trigger_reason": info.get("trigger_reason", "scheduled"),
            }
            for info in executor.running_processes.values()
        ]
    running.sort(key=lambda x: x["running_time_seconds"], reverse=True)

    queued = []
    for i, (priority, _, task) in enumerate(list(executor.task_queue.queue)):
        try:
            dt = datetime.fromtimestamp(priority, tz=_tz)
            priority_iso = dt.isoformat()
        except Exception:
            priority_iso = str(priority)
        queued.append({
            "script_name": task["script_name"],
            "area_name": task["area_name"],
            "priority_timestamp": priority_iso,
            "status": "waiting",
            "position": i + 1,
        })

    wf = workflow_manager.get_state()
    return jsonify({
        "running_processes": running,
        "queued_processes": queued,
        "workflow_active": wf["active"],
        "workflow_name": wf.get("name"),
        "workflow_current_script": wf.get("current_script"),
        "workflow_progress": wf.get("progress"),
        "workflow_log": wf.get("log", []),
        "max_concurrent": config.MAX_PROCESSOS_SIMULTANEOS,
        "running_count": len(running),
        "queued_count": len(queued),
    })


@app.route("/api/health")
def api_health():
    return jsonify({
        "status": "ok",
        "uptime_seconds": executor.get_uptime_seconds(),
        "running": len(executor.running_processes),
        "queued": executor.task_queue.qsize(),
    })


# ── Script info ───────────────────────────────────────────────────────────────

@app.route("/api/scripts")
def api_scripts():
    running_names = {
        d["script_name"].replace("[FLOW] ", "")
        for d in executor.running_processes.values()
    }
    scripts = obter_todos_scripts_planilha()
    for s in scripts:
        s["is_running"] = s["script_name"] in running_names
    return jsonify(scripts)


@app.route("/api/areas")
def api_areas():
    running_names = {
        d["script_name"].replace("[FLOW] ", "")
        for d in executor.running_processes.values()
    }
    scripts = obter_todos_scripts_planilha()
    areas: dict = {}
    for s in scripts:
        s["is_running"] = s["script_name"] in running_names
        areas.setdefault(s["area_name"], []).append(s)
    return jsonify(areas)


# ── Process control ───────────────────────────────────────────────────────────

@app.route("/api/run/<script_name>", methods=["POST"])
def api_run(script_name: str):
    local_files = buscar_arquivos_locais()
    path = local_files.get(script_name.lower())
    if not path:
        return jsonify({"status": "error", "message": f"Script '{script_name}' not found on disk."}), 404

    enqueued = executor.enqueue_script(
        script_name.lower(), str(path), "manual",
        scheduled_timestamp=time.time(),
        trigger_reason="manual",
    )
    if enqueued:
        return jsonify({"status": "success", "message": f"'{script_name}' enqueued for manual execution."})
    return jsonify({"status": "duplicate", "message": f"'{script_name}' is already running or queued."})


@app.route("/api/kill/<int:pid>", methods=["POST"])
def api_kill(pid: int):
    success = executor.kill_process(pid)
    if success:
        return jsonify({"status": "success", "message": f"PID {pid} terminated."})
    return jsonify({"status": "error", "message": "PID not found or already dead."}), 404


# ── Reload ────────────────────────────────────────────────────────────────────

@app.route("/api/reload", methods=["POST"])
def api_reload():
    global _last_reload_time
    now = time.time()
    with _reload_lock:
        elapsed = now - _last_reload_time
        if elapsed < config.RELOAD_COOLDOWN_SECONDS:
            remaining = int(config.RELOAD_COOLDOWN_SECONDS - elapsed)
            return jsonify({"status": "cooldown", "wait_seconds": remaining}), 429
        _last_reload_time = now

    scripts, workflows = scheduler_engine.recarregar_agendamentos()
    return jsonify({
        "status": "success",
        "message": "Configuration reloaded.",
        "script_count": len(scripts),
        "workflow_count": len(workflows),
    })


# ── Jobs & Workflows ──────────────────────────────────────────────────────────

@app.route("/api/jobs")
def api_jobs():
    return jsonify(scheduler_engine.obter_jobs())


@app.route("/api/workflows")
def api_workflows():
    return jsonify({
        "workflows": obter_workflows(),
        "state": workflow_manager.get_state(),
    })


@app.route("/api/workflows/run/<workflow_name>", methods=["POST"])
def api_run_workflow(workflow_name: str):
    if workflow_manager.is_active():
        return jsonify({"status": "error", "message": "A workflow is already active."}), 409
    workflows = obter_workflows()
    wf = next((w for w in workflows if w["workflow_name"] == workflow_name), None)
    if not wf:
        return jsonify({"status": "error", "message": f"Workflow '{workflow_name}' not found."}), 404
    t = threading.Thread(
        target=workflow_manager.iniciar_workflow,
        args=[wf["workflow_name"], wf["scripts"]],
        daemon=True,
        name=f"workflow-{workflow_name}",
    )
    t.start()
    return jsonify({"status": "success", "message": f"Workflow '{workflow_name}' started."})
