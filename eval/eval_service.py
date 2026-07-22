#!/usr/bin/env python3
"""Servizio HTTP di controllo per i run di valutazione sperimentale: la
dashboard avvia un run con `POST /run` e ne legge l'avanzamento con
`GET /status`, invece di aspettare un PNG pre-generato aggiornato ogni 30s.
Un solo run alla volta (un secondo POST /run mentre uno e' attivo viene
rifiutato con 409) -- stesso vincolo e stesso stile di
generator_service.py: http.server nativo, niente Flask per poche route.

A differenza di `python3 run_effectiveness.py` da riga di comando (che resta
disponibile, produce solo CSV/JSON dentro /data/eval/<run_id>/, niente piu'
PNG), qui i sotto-esperimenti (detection/prediction/tag oppure
throughput/latency) vengono eseguiti in sequenza nello stesso thread e ogni
risultato viene pubblicato in _status["results"] non appena pronto: la
dashboard, facendo polling di /status, mostra ogni risultato appena arriva
invece di aspettare la fine dell'intero run.
"""
import json
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import json_safe, new_run_dir, update_index  # noqa: E402
import run_effectiveness  # noqa: E402
import run_efficiency  # noqa: E402

PORT = int(os.environ.get("EVAL_SERVICE_PORT", "5003"))

_lock = threading.Lock()
_status = {"running": False, "run_type": None, "run_id": None, "stage": None, "results": {}, "error": None}

STAGES = {
    "effectiveness": [
        ("detection", run_effectiveness.run_detection_experiment),
        ("prediction", run_effectiveness.run_prediction_experiment),
        ("live_prediction", run_effectiveness.run_live_prediction_experiment),
        ("tag", run_effectiveness.run_tag_experiment),
    ],
    "efficiency": [
        ("throughput", run_efficiency.run_throughput_sweep),
        ("latency", run_efficiency.run_latency_trials),
        ("scalability", run_efficiency.run_scalability_experiment),
        ("selfhealing", run_efficiency.run_selfhealing_latency_experiment),
    ],
}


def _run_in_thread(run_type, run_id, run_dir):
    try:
        for stage_name, stage_fn in STAGES[run_type]:
            with _lock:
                _status["stage"] = stage_name
            result = stage_fn(run_dir)
            with _lock:
                _status["results"][stage_name] = result
        with _lock:
            update_index(run_type, run_id, dict(_status["results"]))
    except Exception as exc:  # noqa: BLE001 -- riportato via /status, non deve morire silenziosamente
        print(f"[eval_service] ERRORE nel run {run_type}: {exc}", flush=True)
        with _lock:
            _status["error"] = str(exc)
    finally:
        with _lock:
            _status["running"] = False
            _status["stage"] = None


class Handler(BaseHTTPRequestHandler):
    def _send_json(self, status, payload):
        body = json.dumps(json_safe(payload), default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/health":
            self._send_json(200, {"status": "ok", "run_types": list(STAGES)})
        elif self.path == "/status":
            with _lock:
                self._send_json(200, dict(_status))
        else:
            self._send_json(404, {"error": "not found"})

    def do_POST(self):
        if self.path != "/run":
            self._send_json(404, {"error": "not found"})
            return

        length = int(self.headers.get("Content-Length", 0))
        try:
            body = json.loads(self.rfile.read(length) or b"{}")
        except json.JSONDecodeError as exc:
            self._send_json(400, {"error": f"JSON non valido: {exc}"})
            return

        run_type = body.get("run_type")
        if run_type not in STAGES:
            self._send_json(400, {"error": f"run_type deve essere uno fra {list(STAGES)}"})
            return

        with _lock:
            if _status["running"]:
                self._send_json(409, {"error": f"un run ({_status['run_type']}) e' gia' in corso"})
                return
            run_id, run_dir = new_run_dir(run_type)
            _status.update(running=True, run_type=run_type, run_id=run_id, stage=None, results={}, error=None)
            thread = threading.Thread(target=_run_in_thread, args=(run_type, run_id, run_dir), daemon=True)
            thread.start()
        self._send_json(200, {"started": run_type, "run_id": run_id})

    def log_message(self, fmt, *args):
        print(f"[eval_service] {self.address_string()} - {fmt % args}")


def main():
    print(f"eval_service in ascolto su :{PORT}")
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
