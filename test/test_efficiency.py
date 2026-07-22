import time

from conftest import collect_messages, start_consumer, start_generator, wait_generator_done

def test_throughput_carico_leggero():
    start_generator({"num_robots": 200, "hz": 5, "duration_s": 10})
    status = wait_generator_done(20)
    target = status["target_rate_msgs_s"]
    achieved = status["achieved_rate_msgs_s"]
    print(f"\n[efficiency] carico leggero: target={target:.0f} msg/s, raggiunto={achieved:.0f} msg/s")

    assert status["errors"] == 0
    assert achieved >= target * 0.85, (
        f"a carico leggero ({target:.0f} msg/s) raggiunto solo {achieved:.0f} msg/s: "
        "un collo di bottiglia qui indicherebbe una regressione, non un limite fisiologico"
    )

def test_throughput_carico_alto_riporta_il_punto_di_rottura():
    start_generator({"num_robots": 3000, "hz": 10, "duration_s": 12})
    status = wait_generator_done(25)
    target = status["target_rate_msgs_s"]
    achieved = status["achieved_rate_msgs_s"]
    ratio = achieved / target if target else 0
    print(f"\n[efficiency] carico alto: target={target:.0f} msg/s, raggiunto={achieved:.0f} msg/s ({ratio:.0%})")

    assert achieved > target * 0.2, f"throughput crollato quasi a zero sotto carico ({achieved:.0f} msg/s)"
    assert status["errors"] == 0, "BufferError del producer: il buffer configurato non basta piu'"

def test_latenza_onset_alert():
    time.sleep(15)
    fault_consumer = start_consumer("injected_faults")
    alert_consumer = start_consumer("anomalies")
    start_generator({
        "num_robots": 1, "hz": 4, "duration_s": 15, "robot_id_prefix": "LATENCY",
        "faults": [{"fault_type": "spike_corrente", "robot_id": "random", "start_time_s": 2, "duration_s": 8}],
    })

    fault_events = collect_messages(
        fault_consumer, timeout_s=12, predicate=lambda e: str(e.get("robot_id", "")).startswith("LATENCY")
    )
    assert fault_events, "il guasto di test non e' mai stato loggato su injected_faults"
    onset_ts_ms = fault_events[0]["start_ts"]

    alert_events = collect_messages(
        alert_consumer, timeout_s=20,
        predicate=lambda e: e.get("type") == "salute" and str(e.get("robot_id", "")).startswith("LATENCY"),
    )
    t_alert_received = time.time()
    wait_generator_done(15)

    assert alert_events, "nessun alert di salute ricevuto per il guasto iniettato"
    latency_s = t_alert_received - (onset_ts_ms / 1000)
    print(f"\n[efficiency] latenza onset->alert: {latency_s:.1f}s (dall'attivazione del guasto alla ricezione dell'alert)")

    assert 0 <= latency_s < 35, f"latenza onset->alert fuori scala: {latency_s:.1f}s"
