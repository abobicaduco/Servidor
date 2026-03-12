#!/usr/bin/env python3
"""
Script de teste do servidor: sobe o Flask em thread, testa todos os endpoints
e variáveis, depois encerra. Não abre navegador.
"""
import os
import sys
import time
import threading
import urllib.request
import urllib.error
import json

# Evita abrir o browser
os.environ["FRONTEND"] = "true"

BASE = "http://127.0.0.1:5000"
FAILED = []


def req(method, path, data=None):
    url = BASE + path
    try:
        r = urllib.request.Request(url, data=data, method=method)
        if data and method == "POST":
            r.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(r, timeout=10) as f:
            return f.getcode(), f.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()
    except Exception as e:
        return None, str(e)


def get(path):
    code, body = req("GET", path)
    try:
        return code, json.loads(body) if body and body.strip().startswith("{") or body.strip().startswith("[") else body
    except json.JSONDecodeError:
        return code, body


def post(path):
    code, body = req("POST", path, data=b"")
    try:
        return code, json.loads(body) if body and (body.strip().startswith("{") or body.strip().startswith("[")) else body
    except json.JSONDecodeError:
        return code, body


def assert_ok(code, msg=""):
    if code != 200:
        FAILED.append(f"Expected 200 got {code} {msg}")
        return False
    return True


def assert_key(obj, key, msg=""):
    if not isinstance(obj, dict) or key not in obj:
        FAILED.append(f"Missing key '{key}' {msg}")
        return False
    return True


def main():
    # Importa e sobe o app em thread (sem webbrowser)
    from modules.config import config
    from modules.scheduler_engine import iniciar_scheduler
    from modules.api import app

    print("=== Variáveis de ambiente / config ===")
    print(f"  DIRETORIO_AUTOMACOES: {config.DIRETORIO_AUTOMACOES}")
    print(f"  PLANILHA_REGISTRO: {config.PLANILHA_REGISTRO}")
    print(f"  PLANILHA_WORKFLOWS: {config.PLANILHA_WORKFLOWS}")
    print(f"  DIRETORIO_FRONTEND_BUILD: {config.DIRETORIO_FRONTEND_BUILD}")
    print(f"  MAX_PROCESSOS_SIMULTANEOS: {config.MAX_PROCESSOS_SIMULTANEOS}")
    print(f"  RELOAD_INTERVAL_MINUTES: {config.RELOAD_INTERVAL_MINUTES}")
    print(f"  RELOAD_COOLDOWN_SECONDS: {config.RELOAD_COOLDOWN_SECONDS}")
    print(f"  FRONTEND: {config.FRONTEND}")
    print(f"  HOST: {config.HOST}  PORT: {config.PORT}")
    print(f"  TIMEZONE: {config.TIMEZONE}")
    assert config.MAX_PROCESSOS_SIMULTANEOS == 3, "MAX_PROCESSOS_SIMULTANEOS"
    assert config.PORT == 5000, "PORT"
    assert config.TIMEZONE == "America/Sao_Paulo", "TIMEZONE"
    print("  OK config\n")

    iniciar_scheduler()
    server_thread = threading.Thread(
        target=lambda: app.run(host=config.HOST, port=config.PORT, debug=False, threaded=True, use_reloader=False),
        daemon=True,
    )
    server_thread.start()

    # Espera o servidor subir
    for _ in range(30):
        try:
            code, _ = get("/api/health")
            if code == 200:
                break
        except Exception:
            pass
        time.sleep(0.2)
    else:
        print("ERRO: Servidor não respondeu em 6s")
        sys.exit(1)
    print("Servidor no ar.\n")

    # --- GET /api/health ---
    print("=== GET /api/health ===")
    code, body = get("/api/health")
    assert_ok(code, "/api/health")
    assert_key(body, "status", "health")
    assert_key(body, "uptime_seconds", "health")
    assert_key(body, "running", "health")
    assert_key(body, "queued", "health")
    print("  ", body)
    print("  OK\n")

    # --- GET /api/status ---
    print("=== GET /api/status ===")
    code, body = get("/api/status")
    assert_ok(code, "/api/status")
    assert_key(body, "running_processes", "status")
    assert_key(body, "queued_processes", "status")
    assert_key(body, "workflow_active", "status")
    assert_key(body, "max_concurrent", "status")
    assert_key(body, "running_count", "status")
    assert_key(body, "queued_count", "status")
    if body.get("queued_processes"):
        q = body["queued_processes"][0]
        if "priority_timestamp" not in q:
            FAILED.append("status: queued_processes[].priority_timestamp missing")
        if "status" not in q:
            FAILED.append("status: queued_processes[].status missing")
    print("  running_count:", body["running_count"], "queued_count:", body["queued_count"], "max_concurrent:", body["max_concurrent"])
    print("  OK\n")

    # --- GET /api/scripts ---
    print("=== GET /api/scripts ===")
    code, body = get("/api/scripts")
    assert_ok(code, "/api/scripts")
    if not isinstance(body, list):
        FAILED.append("/api/scripts should return list")
    else:
        print(f"  {len(body)} script(s)")
        if body:
            s = body[0]
            for k in ["script_name", "area_name", "is_active", "cron_schedule", "available_locally"]:
                assert_key(s, k, "scripts[0]")
    print("  OK\n")

    # --- GET /api/areas ---
    print("=== GET /api/areas ===")
    code, body = get("/api/areas")
    assert_ok(code, "/api/areas")
    assert isinstance(body, dict), "areas should be dict"
    print(f"  áreas: {list(body.keys())}")
    print("  OK\n")

    # --- GET /api/jobs ---
    print("=== GET /api/jobs ===")
    code, body = get("/api/jobs")
    assert_ok(code, "/api/jobs")
    if not isinstance(body, list):
        FAILED.append("/api/jobs should return list")
    else:
        print(f"  {len(body)} job(s)")
        for j in body[:3]:
            assert_key(j, "id", "job"); assert_key(j, "name", "job"); assert_key(j, "trigger", "job")
    print("  OK\n")

    # --- GET /api/workflows ---
    print("=== GET /api/workflows ===")
    code, body = get("/api/workflows")
    assert_ok(code, "/api/workflows")
    assert_key(body, "workflows", "workflows")
    assert_key(body, "state", "workflows")
    print(f"  workflows: {len(body.get('workflows', []))}  state.active: {body.get('state', {}).get('active')}")
    print("  OK\n")

    # --- GET / (frontend) ---
    print("=== GET / (frontend) ===")
    code, raw = req("GET", "/")
    assert_ok(code, "/")
    assert "root" in raw or "html" in raw.lower(), "Frontend HTML should contain root or html"
    print("  HTML length:", len(raw))
    print("  OK\n")

    # --- GET /assets (estático) ---
    code_assets, _ = req("GET", "/assets/index-DgWeRo9L.js")
    if code_assets != 200:
        # Pode ter hash diferente
        code_alt, _ = req("GET", "/assets/")
        print("  /assets/... (hash pode variar):", code_assets, "ou", code_alt)
    else:
        print("  GET /assets/... OK")

    # --- POST /api/reload ---
    print("=== POST /api/reload ===")
    code, body = post("/api/reload")
    assert_ok(code, "/api/reload")
    assert_key(body, "status", "reload")
    assert body.get("status") in ("success", "cooldown"), "reload status"
    print("  ", body)
    print("  OK\n")

    # --- POST /api/reload cooldown (deve retornar 429 ou success após 60s; testamos só 429 se chamar de novo logo)
    print("=== POST /api/reload (cooldown) ===")
    code2, body2 = post("/api/reload")
    if code2 == 429:
        assert_key(body2, "wait_seconds", "reload cooldown")
        print("  Cooldown ativo:", body2.get("wait_seconds"), "s")
    else:
        print("  (cooldown já passou ou não aplicado)")
    print("  OK\n")

    # --- POST /api/run/<script> ---
    print("=== POST /api/run/teste ===")
    code, body = post("/api/run/teste")
    assert_ok(code, "/api/run/teste")
    assert_key(body, "status", "run")
    print("  ", body)
    time.sleep(0.5)
    code2, st = get("/api/status")
    running = st.get("running_processes") or []
    queued = st.get("queued_processes") or []
    print(f"  status após run: running={len(running)} queued={len(queued)}")
    pid_to_kill = running[0]["pid"] if running else None
    print("  OK\n")

    # --- POST /api/kill/<pid> (se temos um processo rodando) ---
    if pid_to_kill:
        print("=== POST /api/kill/<pid> ===")
        code, body = post(f"/api/kill/{pid_to_kill}")
        assert_ok(code, f"/api/kill/{pid_to_kill}")
        assert_key(body, "status", "kill")
        print("  ", body)
        print("  OK\n")
    else:
        # Script pode ter terminado rápido (teste.py é instantâneo)
        print("=== POST /api/kill (skip: nenhum processo rodando) ===\n")

    # --- POST /api/workflows/run (apenas se não bloquear; workflow pode demorar)
    print("=== POST /api/workflows/run/<name> ===")
    code, body = post("/api/workflows/run/test_flow")
    if code == 200:
        assert_key(body, "status", "workflow run")
        print("  ", body)
    elif code == 409:
        print("  Workflow já ativo (409)")
    else:
        print("  ", code, body)
    print("  OK\n")

    # --- Resumo ---
    print("=" * 50)
    if FAILED:
        print("FALHAS:")
        for f in FAILED:
            print("  -", f)
        sys.exit(1)
    print("Todos os testes passaram.")
    sys.exit(0)


if __name__ == "__main__":
    main()
