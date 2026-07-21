"""Effectiveness della detection (Passo 7): veri/falsi positivi sui tre
meccanismi (salute, livelock, deadlock). Due stili di test:

- **messaggi sintetici diretti su Kafka** (stesso approccio della verifica
  originale del Passo 7): controllo pieno su valori/timing, veloce,
  isola la logica di detection da tutto il resto.
- **generatore sintetico con guasti** (Passo 12): copre l'integrazione
  reale end-to-end (generatore -> Kafka -> Spark -> anomalies).

Il test piu' importante di questo file e' `test_nessun_falso_positivo_livelock_su_robot_in_movimento`:
regression test diretto del bug di falsi positivi corretto il 2026-07-21
(vedi docs/passi/07-detection-streaming.md) -- prima del fix, questo stesso
scenario (robot che percorre normalmente un arco lungo) faceva scattare
livelock quasi sempre.

Nota d'ordine importante in ogni test: ci si iscrive SEMPRE al topic
(`start_consumer`) prima di scatenare l'azione (produrre messaggi, avviare
il generatore) -- il join del gruppo Kafka richiede alcuni secondi, e un
evento che scatta subito andrebbe perso iscrivendosi dopo.
"""
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

    # timeout piu' largo di quanto servirebbe alla query di salute da sola
    # (trigger 2s): il livelock come stato per-robot (fix del 2026-07-21)
    # e' piu' pesante della vecchia aggregazione a finestre e condivide gli
    # stessi core, quindi in pratica la query di salute puo' restare
    # indietro di qualche secondo in piu' sotto carico concorrente.
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
    """Livelock come stato esplicito per-robot (fix del 2026-07-21, vedi
    sopra): l'anomalia scatta solo dopo LIVELOCK_CONFIRM_DURATION_S (60s)
    *consecutivi* di event time senza progresso, ricampionato ogni
    LIVELOCK_CHECK_INTERVAL_S (10s). Come nel test precedente, cio' che
    conta e' il `ts` dei messaggi (event time), non il tempo reale di
    invio: si spedisce in fretta con i `ts` gia' distanziati di
    conseguenza."""
    robot_id = "TESTLIVELOCK1"
    x, y = 5.0, 0.0  # a meta' dell'arco A-B, mai aggiornato: nessun progresso reale
    consumer = start_consumer("anomalies")
    now = time.time()
    for i in range(45):
        ts_ms = int((now + i * 3.0) * 1000)  # copre ~132s di event time, ben oltre i 60s richiesti
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
    """Stessa nota di timing del test del livelock sopra: finestra 20s +
    watermark 20s, serve event time per ~45-50s."""
    edge = "C-F"
    consumer = start_consumer("anomalies")
    now = time.time()
    for i in range(16):
        ts_ms = int((now + i * 3.0) * 1000)  # copre ~48s di event time
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
    """Regression test del fix del 2026-07-21 (docs/passi/07-detection-streaming.md):
    robot-token del generatore che percorrono normalmente il grafo (nessun
    guasto) non devono MAI far scattare livelock, indipendentemente da
    quanto a lungo restano "piu' vicini" a un nodo che a un altro durante
    l'attraversamento di un arco. Il generatore non simula mai
    task_state=blocked, quindi il deadlock non e' strutturalmente possibile
    qui: non serve controllarlo in questo test."""
    # eventuali robot ROS reali (R1/R2/R3, sim multi-robot del Passo 5,
    # sempre attiva) restano nel flusso: contano solo i falsi positivi sui
    # robot-token SIM* del generatore, senza guasti.
    # livelock come stato per-robot (fix del 2026-07-21): un robot in
    # movimento normale non accumula mai 60s consecutivi senza progresso,
    # quindi non dovrebbe MAI scattare qui, indipendentemente da quanto si
    # allunga la raccolta -- il margine e' comunque generoso per coprire la
    # durata del run PIU' l'eventuale ritardo dei micro-batch sotto carico.
    consumer = start_consumer("anomalies")
    start_generator({"num_robots": 4, "hz": 3, "duration_s": 60, "graph_preset": "medium"})

    events = collect_messages(
        consumer, timeout_s=120,
        predicate=lambda e: e.get("type") == "livelock" and str(e.get("robot_id", "")).startswith("SIM"),
    )
    status = wait_generator_done(10)
    assert status["sent"] > 0
    assert not events, f"falsi positivi di livelock su robot senza guasti in movimento normale: {events}"
