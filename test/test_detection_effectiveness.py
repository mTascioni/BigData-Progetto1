import json
import time

from conftest import collect_messages, kafka_producer, produce_json, start_consumer, start_generator, wait_generator_done

with open("/workspace/config/warehouse_graph.json") as f:
    _graph = json.load(f)
NODE_POS = {n["id"]: (n["x"], n["y"]) for n in _graph["nodes"]}

def _telemetry(robot_id, x, y, task_state, current_edge, goal_node, ts_ms,
                motor_temp=35.0, motor_current=1.5, battery_pct=90.0):
    return {
        "ts": ts_ms, "robot_id": robot_id, "x": x, "y": y, "theta": 0.0,
        "v_lin": 0.2 if task_state == "moving" else 0.0, "v_ang": 0.0,
        "cmd_v_lin": 0.2, "cmd_v_ang": 0.0,
        "battery_pct": battery_pct, "motor_current": motor_current, "motor_temp": motor_temp,
        "min_obstacle_dist": 3.5, "task_state": task_state,
        "current_edge": current_edge, "goal_node": goal_node,
    }

def test_salute_vero_positivo_su_temperatura_oltre_soglia(kafka_producer):
    robot_id = "TESTHEALTH_HOT"
    consumer = start_consumer("anomalies")
    now = int(time.time() * 1000)
    for i in range(4):
        produce_json(kafka_producer, "telemetry", _telemetry(
            robot_id, 5, 0, "moving", "A-B", "C", now + i * 500, motor_temp=92.0,
        ), key=robot_id)
    kafka_producer.flush(5)

    events = collect_messages(
        consumer, timeout_s=25,
        predicate=lambda e: e.get("type") == "salute" and e.get("robot_id") == robot_id,
    )
    assert events, "nessuna anomalia di salute rilevata per motor_temp=92 (soglia 55)"
    assert "motor_temp" in events[0]["threshold_reasons"]

def test_salute_nessun_falso_positivo_su_valori_nominali(kafka_producer):
    robot_id = "TESTHEALTH_NOMINAL"
    consumer = start_consumer("anomalies")
    now = int(time.time() * 1000)
    for i in range(6):
        produce_json(kafka_producer, "telemetry", _telemetry(
            robot_id, 5, 0, "moving", "A-B", "C", now + i * 500,
            motor_temp=35.0 + (i % 2) * 0.5, motor_current=1.5, battery_pct=90.0,
        ), key=robot_id)
    kafka_producer.flush(5)

    events = collect_messages(
        consumer, timeout_s=8,
        predicate=lambda e: e.get("type") == "salute" and e.get("robot_id") == robot_id,
    )
    assert not events, f"falso positivo di salute su valori nominali: {events}"

def test_livelock_vero_positivo_robot_fermo_ma_task_state_moving(kafka_producer):
    robot_id = "TESTLIVELOCK1"
    x, y = 5.0, 0.0
    consumer = start_consumer("anomalies")
    now = time.time()
    for i in range(45):
        ts_ms = int((now + i * 3.0) * 1000)
        produce_json(kafka_producer, "telemetry", _telemetry(
            robot_id, x, y, "moving", "A-B", "H", ts_ms,
        ), key=robot_id)
        kafka_producer.poll(0)
        time.sleep(0.1)
    kafka_producer.flush(5)

    events = collect_messages(
        consumer, timeout_s=45,
        predicate=lambda e: e.get("type") == "livelock" and e.get("robot_id") == robot_id,
    )
    assert events, "livelock non rilevato su un robot fermo con task_state=moving"
    assert events[0]["min_dist"] == events[0]["max_dist"], "un robot davvero fermo deve avere dist_to_goal costante"
    assert events[0]["stall_duration_s"] >= 60, "la conferma deve basarsi su almeno 60s consecutivi senza progresso"

def test_deadlock_vero_positivo_due_robot_blocked_stesso_arco(kafka_producer):
    edge = "C-F"
    consumer = start_consumer("anomalies")
    now = time.time()
    for i in range(16):
        ts_ms = int((now + i * 3.0) * 1000)
        produce_json(kafka_producer, "telemetry", _telemetry(
            "TESTDEADLOCK1", 20, 5, "blocked", edge, "F", ts_ms,
        ), key="TESTDEADLOCK1")
        produce_json(kafka_producer, "telemetry", _telemetry(
            "TESTDEADLOCK2", 20, 6, "blocked", edge, "C", ts_ms,
        ), key="TESTDEADLOCK2")
        kafka_producer.poll(0)
        time.sleep(0.1)
    kafka_producer.flush(5)

    events = collect_messages(
        consumer, timeout_s=30,
        predicate=lambda e: e.get("type") == "deadlock" and e.get("current_edge") == edge
        and {"TESTDEADLOCK1", "TESTDEADLOCK2"}.issubset(set(e.get("robots", []))),
    )
    assert events, "deadlock non rilevato con 2 robot blocked sullo stesso arco"

def test_nessun_falso_positivo_livelock_su_robot_in_movimento():
    consumer = start_consumer("anomalies")
    start_generator({"num_robots": 4, "hz": 3, "duration_s": 60, "graph_preset": "medium"})

    events = collect_messages(
        consumer, timeout_s=120,
        predicate=lambda e: e.get("type") == "livelock" and str(e.get("robot_id", "")).startswith("SIM"),
    )
    status = wait_generator_done(10)
    assert status["sent"] > 0
    assert not events, f"falsi positivi di livelock su robot senza guasti in movimento normale: {events}"
