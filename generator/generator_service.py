#!/usr/bin/env python3
import json
import os
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from synthetic_generator import DEFAULT_FAULT_PARAMS, run_generator

CONFIG_DIR = os.environ.get("CONFIG_DIR", "/workspace/config")
PORT = int(os.environ.get("GENERATOR_SERVICE_PORT", "5001"))
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
PRESETS_DIR = os.path.join(CONFIG_DIR, "presets")

GRAPH_PRESETS = {
    "small": os.path.join(PRESETS_DIR, "warehouse_small.json"),
    "medium": os.path.join(CONFIG_DIR, "warehouse_graph.json"),
    "large": os.path.join(PRESETS_DIR, "warehouse_large.json"),
}

_lock = threading.Lock()
_thread = None
_stop_event = None
_status = {"running": False}
_log_lines = []
_fault_injectors = {}
MAX_LOG_LINES = 200

def _log(line):
    print(line, flush=True)
    _log_lines.append(line)
    if len(_log_lines) > MAX_LOG_LINES:
        del _log_lines[: len(_log_lines) - MAX_LOG_LINES]

def _run_in_thread(config, run_id):
    global _status
    graph_file = GRAPH_PRESETS.get(config.get("graph_preset", "medium"), GRAPH_PRESETS["medium"])
    try:
        run_generator(
            config_dir=CONFIG_DIR,
            graph_file=graph_file,
            num_robots=int(config["num_robots"]),
            hz=float(config["hz"]),
            speed_mps=float(config.get("speed_mps", 0.2)),
            duration_s=float(config["duration_s"]),
            robot_id_prefix=config.get("robot_id_prefix", "SIM"),
            kafka_bootstrap=KAFKA_BOOTSTRAP,
            faults=config.get("faults", []),
            stop_event=_stop_event,
            status=_status,
            log=_log,
            run_id=run_id,
            fault_injectors_out=_fault_injectors,
        )
    except Exception as exc:
        _log(f"ERRORE nel generatore: {exc}")
        _status.update(running=False, error=str(exc))

class Handler(BaseHTTPRequestHandler):
    def _send_json(self, status, payload):
        body = json.dumps(payload, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/health":
            self._send_json(200, {"status": "ok", "presets": list(GRAPH_PRESETS)})
        elif self.path == "/status":
            self._send_json(200, {**_status, "log_tail": _log_lines[-20:]})
        else:
            self._send_json(404, {"error": "not found"})

    def do_POST(self):
        global _thread, _stop_event

        if self.path == "/start":
            length = int(self.headers.get("Content-Length", 0))
            try:
                config = json.loads(self.rfile.read(length) or b"{}")
            except json.JSONDecodeError as exc:
                self._send_json(400, {"error": f"JSON non valido: {exc}"})
                return

            with _lock:
                if _status.get("running"):
                    self._send_json(409, {"error": "un run e' gia' in corso, fermalo prima di avviarne un altro"})
                    return
                if config.get("graph_preset", "medium") not in GRAPH_PRESETS:
                    self._send_json(400, {"error": f"graph_preset sconosciuto: {config.get('graph_preset')}"})
                    return
                run_id = uuid.uuid4().hex[:8]
                _stop_event = threading.Event()
                _status.clear()
                _status.update(running=True, config=config, run_id=run_id)
                _log_lines.clear()
                _thread = threading.Thread(target=_run_in_thread, args=(config, run_id), daemon=True)
                _thread.start()
            self._send_json(200, {"started": True, "config": config, "run_id": run_id})

        elif self.path == "/stop":
            with _lock:
                if not _status.get("running") or _stop_event is None:
                    self._send_json(409, {"error": "nessun run in corso"})
                    return
                _stop_event.set()
            self._send_json(200, {"stopping": True})

        elif self.path == "/fault/inject":
            length = int(self.headers.get("Content-Length", 0))
            try:
                body = json.loads(self.rfile.read(length) or b"{}")
            except json.JSONDecodeError as exc:
                self._send_json(400, {"error": f"JSON non valido: {exc}"})
                return
            if not _status.get("running"):
                self._send_json(409, {"error": "nessun run in corso"})
                return
            robot_id = body.get("robot_id")
            fault_type = body.get("fault_type")
            duration_s = float(body.get("duration_s", 30))
            injector = _fault_injectors.get(robot_id)
            if injector is None:
                self._send_json(404, {"error": f"robot sconosciuto nel run attivo: {robot_id}"})
                return
            if fault_type not in DEFAULT_FAULT_PARAMS:
                self._send_json(400, {"error": f"tipo di guasto sconosciuto: {fault_type}"})
                return
            fault_id = injector.inject_live(fault_type, duration_s, params=body.get("params"))
            self._send_json(200, {"ok": True, "fault_id": fault_id})

        else:
            self._send_json(404, {"error": "not found"})

    def log_message(self, fmt, *args):
        print(f"[generator_service] {self.address_string()} - {fmt % args}")

def main():
    print(f"generator_service in ascolto su :{PORT}, CONFIG_DIR={CONFIG_DIR}")
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()

if __name__ == "__main__":
    main()
