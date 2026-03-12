import sys
import signal
import time
import threading
import webbrowser

from modules.config import config
from modules.scheduler_engine import iniciar_scheduler
from modules.executor import graceful_shutdown
from modules.api import app


def handle_exit(sig, frame):
    print("\n[SHUTDOWN] Signal received. Shutting down safely...")
    graceful_shutdown()
    print("[SHUTDOWN] Goodbye.")
    sys.exit(0)


signal.signal(signal.SIGINT, handle_exit)
signal.signal(signal.SIGTERM, handle_exit)


if __name__ == "__main__":
    print("=" * 60)
    print("  ABOBI CRON SERVER — Python Workflow Orchestrator")
    print(f"  Frontend : {f'ENABLED → http://{config.HOST}:{config.PORT}' if config.FRONTEND else 'DISABLED (backend-only mode)'}")
    print(f"  Timezone : {config.TIMEZONE}")
    print(f"  Concurrent limit: {config.MAX_PROCESSOS_SIMULTANEOS}")
    print("=" * 60)

    # 1. Start APScheduler (reads xlsx, registers jobs, runs catch-up)
    iniciar_scheduler()

    # 2. Start Flask in daemon thread
    flask_thread = threading.Thread(
        target=lambda: app.run(
            host=config.HOST,
            port=config.PORT,
            debug=False,
            threaded=True,
            use_reloader=False,
        ),
        daemon=True,
        name="flask-server",
    )
    flask_thread.start()
    print(f"[BOOT] API running at http://{config.HOST}:{config.PORT}/api/")

    # 3. Open browser if frontend enabled
    if config.FRONTEND:
        time.sleep(1.5)  # Wait for Flask to fully start
        webbrowser.open(f"http://{config.HOST}:{config.PORT}")
        print(f"[BOOT] Browser opened: http://{config.HOST}:{config.PORT}")

    print("[BOOT] Server ready. Press Ctrl+C to stop.\n")

    # 4. Block main thread forever (daemon threads keep running)
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        handle_exit(None, None)
