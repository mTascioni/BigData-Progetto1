#!/usr/bin/env python3
"""Valutazione sperimentale — efficiency (Passo 13).

Due esperimenti:
1. Sweep di carico crescente col generatore sintetico (Passo 12): throughput
   raggiunto vs target, fino a trovare il punto di rottura (dove il
   generatore smette di reggere il ritmo richiesto).
2. Latenza onset->alert: quanto tempo passa fra l'attivazione reale di un
   guasto e la comparsa del primo alert di salute, su piu' prove.

Uso: `python3 run_efficiency.py` (dentro il container ros).
"""
import csv
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (  # noqa: E402
    collect_messages, new_run_dir, start_consumer,
    start_generator, update_index, wait_generator_done,
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


def main():
    run_id, run_dir = new_run_dir("efficiency")
    print(f"Run: {run_id} -> {run_dir}")

    throughput_summary = run_throughput_sweep(run_dir)
    latency_summary = run_latency_trials(run_dir)

    summary = {"throughput": throughput_summary, "latency": latency_summary}
    update_index("efficiency", run_id, summary)
    print(f"\nFatto. Risultati in {run_dir}")


if __name__ == "__main__":
    main()
