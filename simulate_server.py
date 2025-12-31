import http.server
import socketserver
import json
import os
from pathlib import Path

PORT = 8000
BASE_DIR = Path(__file__).parent / "frontend"

class MockHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/':
            self.path = '/index.html'
        
        if self.path == '/api/status':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            
            # Mock Data
            scripts = []
            for i in range(1, 10):
                scripts.append({
                    "id": f"script_{i}",
                    "script_name": f"mock_script_{i}",
                    "area_name": "TEST_AREA",
                    "status": "IDLE",
                    "active": True,
                    "daily_runs": 5,
                    "target_runs": 10,
                    "next_run_text": "14:00",
                    "last_execution": {"status": "SUCCESS", "timestamp": "2023-01-01T12:00:00", "duration": "5s"},
                    "cron_type": "CALCULATED",
                    "path": "/tmp/test.py"
                })
            
            response = {
                "scripts": scripts,
                "stats": {
                    "queueSize": 2,
                    "concurrentRunning": 1,
                    "bqVerified": True,
                    "nextDiscovery": 10, # Seconds
                    "nextBqSync": 100,
                    "paused": False
                }
            }
            self.wfile.write(json.dumps(response).encode())
            return

        return super().do_GET()

    def translate_path(self, path):
        # Serve from frontend directory
        path = super().translate_path(path)
        rel_path = os.path.relpath(path, os.getcwd())
        return str(BASE_DIR / rel_path)

# Ensure frontend dir exists logic not needed if running from correct cwd
# But let's be safe and change dir
os.chdir(Path(__file__).parent)

with socketserver.TCPServer(("", PORT), MockHandler) as httpd:
    print(f"Serving at port {PORT}")
    httpd.serve_forever()
