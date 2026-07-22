#!/usr/bin/env python3
"""Valutazione sperimentale — efficiency.

Quattro esperimenti:
1. Sweep di carico crescente col generatore sintetico: throughput
   raggiunto vs target, fino a trovare il punto di rottura (dove il
   generatore smette di reggere il ritmo richiesto).
2. Latenza onset->alert: quanto tempo passa fra l'attivazione reale di un
   guasto e la comparsa del primo alert di salute, su piu' prove.
3. Scalabilita': non solo throughput puro (1), ma se la detection resta
   corretta ENTRE il carico sale (le due cose non sono mai state misurate
   insieme finora).
4. Reattivita' del loop di auto-riparazione (flotta reale): latenza fra la
   previsione rilevata in streaming e la riparazione preventiva
   effettivamente dispacciata (fleetStateStore.js). Richiede la flotta
   reale gia' avviata -- se non lo e', l'esperimento viene saltato invece
   di far fallire l'intero run di efficiency.

Uso: `python3 run_efficiency.py` (dentro il container ros).
"""
import csv
import os
import sys
import time

import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (  # noqa: E402
    BACKEND_URL, collect_messages, load_experiment, new_run_dir,
    start_consumer, start_generator, update_index, wait_generator_done,
)

BREAKING_POINT_RATIO = 0.8  # sotto questa frazione del target, si considera "rotto"


def run_throughput_sweep(run_dir):
    print("== Efficiency: sweep di throughput a carico crescente ==")
    hz = 10.0
    robot_counts = [50, 100, 250, 500, 1000, 2000, 4000, 6000]
    rows = []
    breaking_point = None

    for n in robot_counts:
        start_generator({"num_robots": n, "hz": hz, "duration_s": 10})
        status = wait_generator_done(30)
        target = status["target_rate_msgs_s"]
        achieved = status["achieved_rate_msgs_s"]
        ratio = achieved / target if target else 0.0
        rows.append({"num_robots": n, "target_msgs_s": target, "achieved_msgs_s": achieved,
                     "ratio": ratio, "errors": status["errors"]})
        print(f"  {n} robot x {hz}Hz -> target={target:.0f} msg/s, raggiunto={achieved:.0f} msg/s ({ratio:.0%})")
        if breaking_point is None and ratio < BREAKING_POINT_RATIO:
            breaking_point = target

    with open(os.path.join(run_dir, "throughput_sweep.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["num_robots", "target_msgs_s", "achieved_msgs_s", "ratio", "errors"])
        w.writeheader()
        w.writerows(rows)

    achieved = [r["achieved_msgs_s"] for r in rows]
    return {"breaking_point_msgs_s": breaking_point, "max_achieved_msgs_s": max(achieved) if achieved else None}


def run_latency_trials(run_dir, n_trials=5):
    print(f"== Efficiency: latenza onset->alert ({n_trials} prove) ==")
    rows = []
    for trial in range(n_trials):
        fault_consumer = start_consumer("injected_faults")
        alert_consumer = start_consumer("anomalies")
        prefix = f"LAT{trial}"
        start_generator({
            "num_robots": 1, "hz": 4, "duration_s": 15, "robot_id_prefix": prefix,
            "faults": [{"fault_type": "spike_corrente", "robot_id": "random", "start_time_s": 2, "duration_s": 8}],
        })
        fault_events = collect_messages(fault_consumer, timeout_s=12, predicate=lambda e: str(e.get("robot_id", "")).startswith(prefix))
        if not fault_events:
            rows.append({"trial": trial, "latency_s": None})
            wait_generator_done(15)
            continue
        onset_ts_ms = fault_events[0]["start_ts"]

        alert_events = collect_messages(
            alert_consumer, timeout_s=20,
            predicate=lambda e: e.get("type") == "salute" and str(e.get("robot_id", "")).startswith(prefix),
        )
        t_received = time.time()
        wait_generator_done(15)
        latency_s = (t_received - onset_ts_ms / 1000) if alert_events else None
        rows.append({"trial": trial, "latency_s": latency_s})
        print(f"  prova {trial}: latenza {latency_s}")

    with open(os.path.join(run_dir, "latency_onset_alert.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["trial", "latency_s"])
        w.writeheader()
        w.writerows(rows)

    valid = [r["latency_s"] for r in rows if r["latency_s"] is not None]
    return {"trials": n_trials, "successful": len(valid),
            "avg_latency_s": (sum(valid) / len(valid)) if valid else None,
            "max_latency_s": max(valid) if valid else None}


def run_scalability_experiment(run_dir):
    print("== Scalabilita': detection sotto carico crescente ==")
    hz = 3.0
    robot_counts = [100, 1500, 5000]
    n_faulty = 3
    prefix = "SCALE"
    rows = []

    for n in robot_counts:
        faulty_idx = set(range(min(n_faulty, n)))
        robot_ids = [f"{prefix}{i:05d}" for i in range(n)]
        faults = [
            {"fault_type": "spike_corrente", "robot_id": rid, "start_time_s": 3, "duration_s": 15}
            for i, rid in enumerate(robot_ids) if i in faulty_idx
        ]

        consumer = start_consumer("anomalies")
        start_generator({"num_robots": n, "hz": hz, "duration_s": 20, "robot_id_prefix": prefix, "faults": faults})
        events = collect_messages(
            consumer, timeout_s=30,
            predicate=lambda e: e.get("type") == "salute" and str(e.get("robot_id", "")).startswith(prefix),
        )
        status = wait_generator_done(15)
        target = status["target_rate_msgs_s"]
        achieved = status["achieved_rate_msgs_s"]

        flagged = {e["robot_id"] for e in events}
        tp = sum(1 for i in faulty_idx if robot_ids[i] in flagged)
        fn = len(faulty_idx) - tp
        fp = sum(1 for i, rid in enumerate(robot_ids) if i not in faulty_idx and rid in flagged)
        precision = tp / (tp + fp) if (tp + fp) else float("nan")
        recall = tp / (tp + fn) if (tp + fn) else float("nan")

        rows.append({
            "robot_count": n, "target_msgs_s": target, "achieved_msgs_s": achieved,
            "precision": precision, "recall": recall, "tp": tp, "fp": fp, "fn": fn,
        })
        print(f"  {n} robot: throughput {achieved:.0f}/{target:.0f} msg/s, precision={precision:.2f} recall={recall:.2f}")

    with open(os.path.join(run_dir, "scalability.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["robot_count", "target_msgs_s", "achieved_msgs_s", "precision", "recall", "tp", "fp", "fn"])
        w.writeheader()
        w.writerows(rows)

    last = rows[-1]
    return {
        "levels": len(rows), "max_robot_count": robot_counts[-1],
        "precision_at_max_load": last["precision"], "recall_at_max_load": last["recall"],
        "achieved_at_max_load": last["achieved_msgs_s"],
    }


def run_selfhealing_latency_experiment(run_dir, n_trials=2):
    print("== Reattivita': latenza previsione -> riparazione dispacciata (flotta reale) ==")
    try:
        sim_status = requests.get(f"{BACKEND_URL}/api/fleet-control/sim/status", timeout=5).json()
    except requests.RequestException as exc:
        print(f"  saltato: flotta reale non raggiungibile ({exc})")
        return {"skipped": True, "reason": "flotta reale non raggiungibile"}
    if not sim_status.get("running"):
        print("  saltato: nessuna simulazione della flotta reale in corso")
        return {"skipped": True, "reason": "flotta reale non attiva"}

    experiment = load_experiment()
    repair_node = experiment["repair_node"]
    task_robot_ids = [t["robot_id"] for t in experiment["tasks"]]
    n_trials = min(n_trials, len(task_robot_ids))
    if n_trials == 0:
        return {"skipped": True, "reason": "nessun robot con missione in experiment.json"}

    rows = []
    for trial in range(n_trials):
        robot_id = task_robot_ids[trial]
        anomaly_consumer = start_consumer("anomalies")
        state_consumer = start_consumer("fleet_state")

        requests.post(
            f"{BACKEND_URL}/api/fleet-control/fault",
            json={"robot_id": robot_id, "fault_type": "preavviso_intermittente", "duration_s": 60},
            timeout=10,
        )

        previsione_events = collect_messages(
            anomaly_consumer, timeout_s=65,
            predicate=lambda e: e.get("type") == "previsione" and e.get("robot_id") == robot_id,
        )
        t_previsione = time.time()

        latency_s = None
        if previsione_events:
            dispatch_events = collect_messages(
                state_consumer, timeout_s=20,
                predicate=lambda e: e.get("robot_id") == robot_id and e.get("goal_node") == repair_node,
            )
            if dispatch_events:
                latency_s = time.time() - t_previsione

        rows.append({"trial": trial, "robot_id": robot_id, "latency_s": latency_s})
        print(f"  prova {trial} ({robot_id}): latenza previsione->riparazione {latency_s}")

        try:
            requests.post(f"{BACKEND_URL}/api/fleet-control/return-to-service", json={"robot_id": robot_id}, timeout=10)
        except requests.RequestException:
            pass

    with open(os.path.join(run_dir, "selfhealing_latency.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["trial", "robot_id", "latency_s"])
        w.writeheader()
        w.writerows(rows)

    valid = [r["latency_s"] for r in rows if r["latency_s"] is not None]
    return {
        "skipped": False, "trials": len(rows), "successful": len(valid),
        "avg_latency_s": (sum(valid) / len(valid)) if valid else None,
    }


def main():
    run_id, run_dir = new_run_dir("efficiency")
    print(f"Run: {run_id} -> {run_dir}")

    throughput_summary = run_throughput_sweep(run_dir)
    latency_summary = run_latency_trials(run_dir)
    scalability_summary = run_scalability_experiment(run_dir)
    selfhealing_summary = run_selfhealing_latency_experiment(run_dir)

    summary = {
        "throughput": throughput_summary, "latency": latency_summary,
        "scalability": scalability_summary, "selfhealing": selfhealing_summary,
    }
    update_index("efficiency", run_id, summary)
    print(f"\nFatto. Risultati in {run_dir}")


if __name__ == "__main__":
    main()
