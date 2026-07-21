#!/usr/bin/env python3
"""Servizio HTTP di controllo per il generatore sintetico (estensione del
Passo 11/12): processo persistente nel container `ros` che la dashboard
puo' avviare/fermare/interrogare, invece di dover lanciare
synthetic_generator.py a mano da CLI. Un solo run alla volta (un secondo
POST /start mentre uno e' attivo viene rifiutato con 409).

Stesso stile di streaming/query_service.py (Passo 10): http.server nativo,
niente Flask per poche route.
"""
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from synthetic_generator import run_generator

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
MAX_LOG_LINES = 200


def _log(line):
    print(line, flush=True)
    _log_lines.append(line)
    if len(_log_lines) > MAX_LOG_LINES:
        del _log_lines[: len(_log_lines) - MAX_LOG_LINES]


def _run_in_thread(config):
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
        )
    except Exception as exc:  # noqa: BLE001 -- riportato via /status, non deve morire silenziosamente
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
                _stop_event = threading.Event()
                _status.clear()
                _status.update(running=True, config=config)
                _log_lines.clear()
                _thread = threading.Thread(target=_run_in_thread, args=(config,), daemon=True)
                _thread.start()
            self._send_json(200, {"started": True, "config": config})

        elif self.path == "/stop":
            with _lock:
                if not _status.get("running") or _stop_event is None:
                    self._send_json(409, {"error": "nessun run in corso"})
                    return
                _stop_event.set()
            self._send_json(200, {"stopping": True})

        else:
            self._send_json(404, {"error": "not found"})

    def log_message(self, fmt, *args):
        print(f"[generator_service] {self.address_string()} - {fmt % args}")


def main():
    print(f"generator_service in ascolto su :{PORT}, CONFIG_DIR={CONFIG_DIR}")
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
