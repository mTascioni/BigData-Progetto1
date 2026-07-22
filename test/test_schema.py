"""Conformita' allo schema condiviso dei messaggi: verificato sui dati che
circolano DAVVERO sui topic Kafka in questo momento, non su fixture
statiche."""
from conftest import collect_messages, consume_topic, query_sql, start_consumer, start_generator, wait_generator_done

TELEMETRY_FIELDS = {
    "ts": int, "robot_id": str, "x": (int, float), "y": (int, float), "theta": (int, float),
    "v_lin": (int, float), "v_ang": (int, float), "cmd_v_lin": (int, float), "cmd_v_ang": (int, float),
    "battery_pct": (int, float), "motor_current": (int, float), "motor_temp": (int, float),
    "task_state": str, "current_edge": (str, type(None)), "goal_node": (str, type(None)),
}
TASK_STATES = {"idle", "moving", "blocked", "charging"}

FLEET_STATE_FIELDS = {
    "ts", "robot_id", "x", "y", "theta", "v_lin", "v_ang", "battery_pct", "motor_current",
    "motor_temp", "min_obstacle_dist", "task_state", "current_edge", "goal_node", "health_anomaly",
}

INJECTED_FAULT_FIELDS = {
    "fault_id", "robot_id", "fault_type", "start_time_s", "end_time_s", "params", "start_ts", "end_ts",
}

PREDICTIONS_FIELDS = {
    "robot_id", "channel", "predicted_at_ts", "current_value", "slope_per_min",
    "critical_threshold", "predicted_crossing_ts", "lead_time_s", "model", "n_points",
}


def test_telemetry_schema_conforme_al_contratto_condiviso():
    # la simulazione ROS reale e' in pausa per tutta la sessione di test
    # (vedi conftest.py, _pause_real_simulation): si usa il generatore per
    # avere comunque telemetria live da verificare.
    consumer = start_consumer("telemetry")
    start_generator({"num_robots": 2, "hz": 3, "duration_s": 6})
    messages = collect_messages(consumer, timeout_s=10)
    wait_generator_done(10)
    assert messages, "nessun messaggio su telemetry nonostante il generatore attivo"

    for m in messages:
        for field, expected_type in TELEMETRY_FIELDS.items():
            assert field in m, f"campo mancante '{field}' in {m}"
            assert isinstance(m[field], expected_type), (
                f"campo '{field}' di tipo {type(m[field])}, atteso {expected_type} ({m})"
            )
        assert m["task_state"] in TASK_STATES, f"task_state non valido: {m['task_state']}"


def test_fleet_state_schema():
    consumer = start_consumer("fleet_state")
    start_generator({"num_robots": 2, "hz": 3, "duration_s": 6})
    messages = collect_messages(consumer, timeout_s=10)
    wait_generator_done(10)
    assert messages, "nessun messaggio su fleet_state: detection_job.py e' attivo?"
    for m in messages:
        missing = FLEET_STATE_FIELDS - m.keys()
        assert not missing, f"campi mancanti in fleet_state: {missing} ({m})"
        assert isinstance(m["health_anomaly"], bool)


def test_injected_faults_schema():
    consumer = start_consumer("injected_faults")  # iscritto PRIMA di scatenare il guasto
    # genera un guasto controllato invece di aspettare che ne capiti uno reale.
    # "random" con num_robots=1 si risolve deterministicamente sull'unico
    # robot esistente (un robot_id inventato verrebbe ignorato e il guasto
    # ripiegherebbe comunque su un robot a caso, con nome imprevedibile).
    start_generator({
        "num_robots": 1, "hz": 2, "duration_s": 8, "robot_id_prefix": "SCHEMA",
        "faults": [{"fault_type": "spike_corrente", "robot_id": "random", "start_time_s": 1, "duration_s": 4}],
    })
    events = collect_messages(consumer, timeout_s=15, predicate=lambda e: e.get("fault_type") == "spike_corrente" and str(e.get("robot_id", "")).startswith("SCHEMA"))
    wait_generator_done(20)
    assert events, "nessun evento su injected_faults per il guasto appena iniettato"

    event = events[0]
    missing = INJECTED_FAULT_FIELDS - event.keys()
    assert not missing, f"campi mancanti in injected_faults: {missing} ({event})"
    assert event["fault_type"] == "spike_corrente"
    assert isinstance(event["params"], dict) and event["params"], "params vuoto o non e' un dict"
    assert event["end_ts"] > event["start_ts"]


def test_predictions_schema():
    result = query_sql("SELECT * FROM predictions LIMIT 5")
    assert result["rows"], "tabella predictions vuota: nessuna previsione mai scritta in /data/predictions"
    missing = PREDICTIONS_FIELDS - set(result["columns"])
    assert not missing, f"colonne mancanti in predictions: {missing}"
