#!/usr/bin/env python3
"""Servizio di controllo HTTP per la flotta reale: ponte fra il backend Node
(dashboard/anello automatico) e i topic ROS di controllo di kafka_bridge.py
(iniezione guasto live, ~fault_inject) e graph_navigator.py (assegnazione
missione, ~nav_control). Stesso stile di generator_service.py: http.server
nativo, un processo persistente nel container `ros`, niente Flask per poche
route -- l'unica differenza e' che questo e' anche un nodo rospy (deve
pubblicare su topic ROS), non un processo Python puro.

Espone anche /sim/start, /sim/stop, /sim/status: la simulazione ROS/Gazebo
(`sim_multi_robot`, programma supervisord) non parte in automatico all'avvio
del container -- viene avviata/fermata da qui su richiesta della dashboard,
con la scala (small/large) scelta dall'utente.
"""
import json
import os
import subprocess
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import rospy
from std_msgs.msg import String

PORT = int(os.environ.get("FLEET_CONTROL_SERVICE_PORT", "5002"))
CONFIG_DIR = os.environ.get("CONFIG_DIR", "/workspace/config")
SCALE_FILE = "/tmp/shf_scale"
# Stesso pattern del marker della scala: sim_multi_robot.launch non puo'
# ricevere un argomento dinamico ad ogni `supervisorctl start` (il comando e'
# statico), quindi si scrive qui un file letto dallo script di lancio.
# run_id isola i dati di run diversi della flotta reale (stesso motivo del
# run_id del generatore sintetico: senza, telemetria/previsioni di avvii
# diversi si mischierebbero nello storico).
RUN_ID_FILE = "/tmp/shf_run_id"
SIM_PROGRAM = "sim_multi_robot"
VALID_SCALES = ("small", "large")

_pub_lock = threading.Lock()
_publishers = {}  # topic -> rospy.Publisher


def _publish(topic, payload):
    """Pubblica su un topic std_msgs/String, creando il Publisher al primo
    utilizzo. Un Publisher appena creato impiega un momento a connettersi ai
    subscriber gia' attivi (negoziazione XML-RPC ROS) -- una breve pausa solo
    alla primissima pubblicazione su quel topic evita di perdere il comando."""
    with _pub_lock:
        pub = _publishers.get(topic)
        just_created = pub is None
        if just_created:
            pub = rospy.Publisher(topic, String, queue_size=5)
            _publishers[topic] = pub
    if just_created:
        rospy.sleep(0.3)
    pub.publish(String(json.dumps(payload)))


def _nav_control(robot_id, nodes):
    _publish(f"/{robot_id}/graph_navigator/nav_control", {"nodes": nodes})


def _nav_freeze(robot_id):
    _publish(f"/{robot_id}/graph_navigator/nav_control", {"cmd": "freeze"})


def _fault_inject(robot_id, fault_type, duration_s, params=None):
    payload = {"fault_type": fault_type, "duration_s": duration_s}
    if params:
        payload["params"] = params
    _publish(f"/{robot_id}/kafka_bridge/fault_inject", payload)


def _load_experiment():
    with open(os.path.join(CONFIG_DIR, "experiment.json")) as f:
        return json.load(f)


def _supervisorctl(*args, timeout=30):
    result = subprocess.run(
        ["supervisorctl", *args], capture_output=True, text=True, timeout=timeout
    )
    return result.returncode, (result.stdout + result.stderr).strip()


def _sim_status():
    """Stato di sim_multi_robot via `supervisorctl status` (stesso meccanismo
    usato da test/conftest.py per pausare/riprendere la simulazione durante
    i test) + l'ultima scala scelta (marker file, "small" se mai scelta)."""
    _, out = _supervisorctl("status", SIM_PROGRAM, timeout=10)
    running = " RUNNING " in f" {out} "
    try:
        with open(SCALE_FILE) as f:
            scale = f.read().strip() or "small"
    except FileNotFoundError:
        scale = "small"
    try:
        with open(RUN_ID_FILE) as f:
            run_id = f.read().strip() or None
    except FileNotFoundError:
        run_id = None
    return {"running": running, "scale": scale, "run_id": run_id, "raw_status": out}


class Handler(BaseHTTPRequestHandler):
    def _send_json(self, status, payload):
        body = json.dumps(payload, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json_body(self):
        length = int(self.headers.get("Content-Length", 0))
        return json.loads(self.rfile.read(length) or b"{}")

    def do_GET(self):
        if self.path == "/health":
            self._send_json(200, {"status": "ok"})
        elif self.path == "/sim/status":
            self._send_json(200, _sim_status())
        else:
            self._send_json(404, {"error": "not found"})

    def do_POST(self):
        try:
            body = self._read_json_body()

            if self.path == "/fault/inject":
                robot_id = body["robot_id"]
                fault_type = body["fault_type"]
                duration_s = float(body.get("duration_s", 30))
                _fault_inject(robot_id, fault_type, duration_s, params=body.get("params"))
                self._send_json(200, {"ok": True})

            elif self.path == "/robot/goto":
                robot_id = body["robot_id"]
                nodes = body["nodes"]
                _nav_control(robot_id, nodes)
                self._send_json(200, {"ok": True})

            elif self.path == "/robot/repair":
                # Anche innescato automaticamente dal backend
                # (fleetStateStore.js) su health_anomaly=true.
                robot_id = body["robot_id"]
                repair_node = _load_experiment()["repair_node"]
                _nav_control(robot_id, [repair_node])
                self._send_json(200, {"ok": True, "repair_node": repair_node})

            elif self.path == "/robot/freeze":
                # Guasto persistente reale (soglia dura confermata) -- il
                # robot si ferma dov'e' invece di essere mandato in
                # riparazione automaticamente, in attesa che l'operatore lo
                # veda in dashboard e decida (di solito: decommissionarlo).
                robot_id = body["robot_id"]
                _nav_freeze(robot_id)
                self._send_json(200, {"ok": True})

            elif self.path == "/robot/return-to-service":
                robot_id = body["robot_id"]
                reserve_node = _load_experiment()["reserve_node"]
                _nav_control(robot_id, [reserve_node])
                self._send_json(200, {"ok": True, "reserve_node": reserve_node})

            elif self.path == "/robot/dispatch-mission":
                # Manda a robot_id (tipicamente il robot di riserva) la
                # goal_sequence originale di source_robot_id (quello guasto):
                # "prende in carico" la missione.
                robot_id = body["robot_id"]
                source_robot_id = body["source_robot_id"]
                task = next(
                    (t for t in _load_experiment()["tasks"] if t["robot_id"] == source_robot_id), None
                )
                if task is None:
                    self._send_json(404, {"error": f"nessun task per {source_robot_id}"})
                    return
                _nav_control(robot_id, task["goal_sequence"])
                self._send_json(200, {"ok": True, "nodes": task["goal_sequence"]})

            elif self.path == "/sim/start":
                scale = body.get("scale", "small")
                if scale not in VALID_SCALES:
                    self._send_json(400, {"error": f"scale deve essere una fra {VALID_SCALES}"})
                    return
                if _sim_status()["running"]:
                    self._send_json(409, {"error": "simulazione gia' in corso, fermala prima di cambiare scala"})
                    return
                run_id = uuid.uuid4().hex[:8]
                with open(SCALE_FILE, "w") as f:
                    f.write(scale)
                with open(RUN_ID_FILE, "w") as f:
                    f.write(run_id)
                code, out = _supervisorctl("start", SIM_PROGRAM, timeout=30)
                if code != 0 or "ERROR" in out:
                    self._send_json(502, {"error": out or "avvio fallito"})
                    return
                self._send_json(200, {"ok": True, "scale": scale, "run_id": run_id})

            elif self.path == "/sim/stop":
                if not _sim_status()["running"]:
                    self._send_json(409, {"error": "nessuna simulazione in corso"})
                    return
                code, out = _supervisorctl("stop", SIM_PROGRAM, timeout=30)
                if code != 0 or "ERROR" in out:
                    self._send_json(502, {"error": out or "arresto fallito"})
                    return
                self._send_json(200, {"ok": True})

            else:
                self._send_json(404, {"error": "not found"})
        except (KeyError, ValueError) as exc:
            self._send_json(400, {"error": str(exc)})

    def log_message(self, fmt, *args):
        print(f"[fleet_control_service] {self.address_string()} - {fmt % args}")


def main():
    rospy.init_node("fleet_control_service", anonymous=False)
    print(f"fleet_control_service in ascolto su :{PORT}, CONFIG_DIR={CONFIG_DIR}")
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
