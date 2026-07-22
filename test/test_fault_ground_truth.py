import time

from conftest import collect_messages, start_consumer, start_generator, wait_generator_done

def test_spike_corrente_ground_truth_precisa():
    consumer = start_consumer("injected_faults")
    start_ts_before = int(time.time() * 1000)
    start_generator({
        "num_robots": 1, "hz": 4, "duration_s": 12, "robot_id_prefix": "GT",
        "faults": [{"fault_type": "spike_corrente", "robot_id": "random", "start_time_s": 2, "duration_s": 5}],
    })

    events = collect_messages(consumer, timeout_s=18, predicate=lambda e: str(e.get("robot_id", "")).startswith("GT"))
    status = wait_generator_done(15)

    assert status["sent"] > 0, "il run non ha inviato nessun messaggio"
    assert len(events) == 1, f"attesa 1 istanza di guasto, trovate {len(events)}: {events}"

    event = events[0]
    assert event["fault_type"] == "spike_corrente"
    assert event["start_time_s"] == 2
    assert event["end_time_s"] == 7

    real_duration_s = (event["end_ts"] - event["start_ts"]) / 1000
    assert 3.0 <= real_duration_s <= 7.0, f"durata reale del guasto fuori tolleranza: {real_duration_s}s"
    assert event["start_ts"] >= start_ts_before

    assert event["params"]["peak_a"] == 4.5
    assert event["params"]["rise_time_s"] == 5

def test_guasto_su_robot_casuale_finisce_su_un_robot_esistente():
    consumer = start_consumer("injected_faults")
    start_generator({
        "num_robots": 3, "hz": 2, "duration_s": 8,
        "faults": [{"fault_type": "batteria_collasso", "robot_id": "random", "start_time_s": 1, "duration_s": 3}],
    })
    events = collect_messages(consumer, timeout_s=12, predicate=lambda e: e.get("fault_type") == "batteria_collasso")
    wait_generator_done(15)

    assert len(events) == 1
    assert events[0]["robot_id"] in {"SIM00000", "SIM00001", "SIM00002"}
