#!/usr/bin/env python3
import csv
import glob
import os
import subprocess
import sys
import tempfile
import time

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (
    ask_tag, collect_messages, new_run_dir, query_sql,
    start_consumer, start_generator, update_index, wait_generator_done,
)
from reference_questions import REFERENCE_QUESTIONS

FORECAST_SCRIPT = "/opt/shf/predictive/forecast_failures.py"

def run_detection_experiment(run_dir):
    print("== Detection: precision/recall/F1 (anomalie di salute) ==")
    n_robots = 8
    faulty_idx = {0, 2, 4}
    prefix = "EVAL"
    robot_ids = [f"{prefix}{i:05d}" for i in range(n_robots)]
    faults = [
        {"fault_type": "spike_corrente", "robot_id": rid, "start_time_s": 3, "duration_s": 15}
        for i, rid in enumerate(robot_ids) if i in faulty_idx
    ]

    consumer = start_consumer("anomalies")
    start_generator({"num_robots": n_robots, "hz": 3, "duration_s": 25, "robot_id_prefix": prefix, "faults": faults})
    events = collect_messages(
        consumer, timeout_s=35,
        predicate=lambda e: e.get("type") == "salute" and str(e.get("robot_id", "")).startswith(prefix),
    )
    wait_generator_done(15)

    flagged = {e["robot_id"] for e in events}
    rows = []
    tp = fp = fn = tn = 0
    for i, rid in enumerate(robot_ids):
        has_fault = i in faulty_idx
        detected = rid in flagged
        if has_fault and detected:
            tp += 1
        elif has_fault and not detected:
            fn += 1
        elif not has_fault and detected:
            fp += 1
        else:
            tn += 1
        rows.append({"robot_id": rid, "has_fault": has_fault, "detected": detected})

    precision = tp / (tp + fp) if (tp + fp) else float("nan")
    recall = tp / (tp + fn) if (tp + fn) else float("nan")
    f1 = 2 * precision * recall / (precision + recall) if precision and recall and (precision + recall) else float("nan")

    with open(os.path.join(run_dir, "detection_robots.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["robot_id", "has_fault", "detected"])
        w.writeheader()
        w.writerows(rows)

    with open(os.path.join(run_dir, "detection_summary.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["metric", "value"])
        for k, v in [("precision", precision), ("recall", recall), ("f1", f1),
                     ("true_positive", tp), ("false_positive", fp), ("false_negative", fn), ("true_negative", tn)]:
            w.writerow([k, v])

    print(f"  precision={precision:.2f} recall={recall:.2f} f1={f1:.2f} (TP={tp} FP={fp} FN={fn} TN={tn})")
    return {"precision": precision, "recall": recall, "f1": f1, "tp": tp, "fp": fp, "fn": fn, "tn": tn}

def _write_ramp(data_dir, robot_id, channel, start_ts_ms, start_value, rate_per_s, observed_duration_s, step_s=2):
    rows = []
    t = 0
    others = {"motor_temp": 35.0, "motor_current": 1.5, "battery_pct": 90.0}
    others.pop(channel, None)
    while t <= observed_duration_s:
        row = {"ts": start_ts_ms + t * 1000, "robot_id": robot_id, channel: start_value + rate_per_s * t}
        row.update(others)
        rows.append(row)
        t += step_s
    df = pd.DataFrame(rows)
    telemetry_dir = os.path.join(data_dir, "telemetry")
    os.makedirs(telemetry_dir, exist_ok=True)
    df.to_parquet(os.path.join(telemetry_dir, "part-0.parquet"), index=False)

def _run_forecast(data_dir, now_ts_ms, lookback_s=300):
    out_dir = os.path.join(data_dir, "predictions")
    subprocess.run(
        ["python3", FORECAST_SCRIPT, "--data-dir", data_dir, "--out", out_dir,
         "--now-ts", str(now_ts_ms), "--lookback-s", str(lookback_s)],
        check=True, capture_output=True, text=True, timeout=60,
    )
    files = glob.glob(os.path.join(out_dir, "*.parquet"))
    return pd.read_parquet(out_dir) if files else pd.DataFrame()

def run_prediction_experiment(run_dir):
    print("== Previsione: errore lead time su trend sintetici noti ==")
    start_ts_ms = 1_800_000_000_000
    scenarios = [
        ("motor_temp", 60.0, 0.05, 85.0, 400),
        ("motor_current", 3.0, 0.01, 4.5, 100),
        ("battery_pct", 50.0, -0.05, 10.0, 300),
    ]
    rows = []
    for channel, start_value, rate, threshold, obs_duration in scenarios:
        expected_crossing_s = (threshold - start_value) / rate
        expected_lead_time_s = expected_crossing_s - obs_duration
        with tempfile.TemporaryDirectory() as tmp:
            _write_ramp(tmp, "PREDEVAL", channel, start_ts_ms, start_value, rate, obs_duration)
            now_ts_ms = start_ts_ms + obs_duration * 1000
            predictions = _run_forecast(tmp, now_ts_ms)
        match = predictions[(predictions.robot_id == "PREDEVAL") & (predictions.channel == channel)] if not predictions.empty else predictions
        if len(match) == 1:
            predicted_lead_time_s = float(match.iloc[0].lead_time_s)
            error_s = predicted_lead_time_s - expected_lead_time_s
        else:
            predicted_lead_time_s = None
            error_s = None
        rows.append({
            "channel": channel, "expected_lead_time_s": expected_lead_time_s,
            "predicted_lead_time_s": predicted_lead_time_s, "error_s": error_s,
        })
        print(f"  {channel}: atteso {expected_lead_time_s:.1f}s, previsto {predicted_lead_time_s}, errore {error_s}")

    with open(os.path.join(run_dir, "prediction_accuracy.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["channel", "expected_lead_time_s", "predicted_lead_time_s", "error_s"])
        w.writeheader()
        w.writerows(rows)

    valid_errors = [abs(r["error_s"]) for r in rows if r["error_s"] is not None]
    mae = sum(valid_errors) / len(valid_errors) if valid_errors else None
    return {"mae_lead_time_s": mae, "scenarios": len(rows), "missing": sum(1 for r in rows if r["error_s"] is None)}

def run_live_prediction_experiment(run_dir):
    print("== Previsione live: detection streaming + latenza onset->previsione ==")
    n_robots = 6
    faulty_idx = {0, 2, 4}
    prefix = "EVALPREV"
    start_time_s = 3
    robot_ids = [f"{prefix}{i:05d}" for i in range(n_robots)]
    faults = [
        {"fault_type": "preavviso_intermittente", "robot_id": rid, "start_time_s": start_time_s, "duration_s": 60}
        for i, rid in enumerate(robot_ids) if i in faulty_idx
    ]

    anomaly_consumer = start_consumer("anomalies")
    t_launch = time.time()
    start_generator({"num_robots": n_robots, "hz": 3, "duration_s": 70, "robot_id_prefix": prefix, "faults": faults})
    onset_ts = t_launch + start_time_s

    previsione_events = collect_messages(
        anomaly_consumer, timeout_s=65,
        predicate=lambda e: e.get("type") == "previsione" and str(e.get("robot_id", "")).startswith(prefix),
    )
    t_received = time.time()
    wait_generator_done(15)

    flagged = {e["robot_id"] for e in previsione_events}
    rows = []
    tp = fp = fn = tn = 0
    for i, rid in enumerate(robot_ids):
        has_fault = i in faulty_idx
        detected = rid in flagged
        if has_fault and detected:
            tp += 1
        elif has_fault and not detected:
            fn += 1
        elif not has_fault and detected:
            fp += 1
        else:
            tn += 1
        latency_s = (t_received - onset_ts) if (has_fault and detected) else None
        rows.append({"robot_id": rid, "has_fault": has_fault, "detected": detected, "latency_s": latency_s})

    precision = tp / (tp + fp) if (tp + fp) else float("nan")
    recall = tp / (tp + fn) if (tp + fn) else float("nan")
    f1 = 2 * precision * recall / (precision + recall) if precision and recall and (precision + recall) else float("nan")
    latencies = [r["latency_s"] for r in rows if r["latency_s"] is not None]
    avg_latency = sum(latencies) / len(latencies) if latencies else None

    with open(os.path.join(run_dir, "live_prediction_robots.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["robot_id", "has_fault", "detected", "latency_s"])
        w.writeheader()
        w.writerows(rows)

    print(f"  precision={precision:.2f} recall={recall:.2f} f1={f1:.2f} (TP={tp} FP={fp} FN={fn} TN={tn}), latenza media onset->previsione {avg_latency}")
    return {"precision": precision, "recall": recall, "f1": f1, "tp": tp, "fp": fp, "fn": fn, "tn": tn, "avg_latency_s": avg_latency}

def row_matches(tag_row, gt_values, tol=0.5):
    tag_values = list(tag_row.values())
    for gv in gt_values:
        if isinstance(gv, (int, float)) and not isinstance(gv, bool):
            if not any(isinstance(tv, (int, float)) and not isinstance(tv, bool) and abs(tv - gv) <= tol for tv in tag_values):
                return False
        else:
            if not any(str(tv) == str(gv) for tv in tag_values):
                return False
    return True

def question_correct(ground_truth_rows, tag_rows):
    if not ground_truth_rows:
        return True
    if not tag_rows:
        return False
    return all(any(row_matches(tr, list(gr.values())) for tr in tag_rows) for gr in ground_truth_rows)

def run_tag_experiment(run_dir):
    print(f"== TAG: execution accuracy su {len(REFERENCE_QUESTIONS)} domande di riferimento ==")
    rows = []
    correct_count = 0
    for question, sql in REFERENCE_QUESTIONS:
        try:
            ground_truth = query_sql(sql)
        except Exception as exc:
            rows.append({"question": question, "reference_sql": sql, "correct": False, "error": f"verita' diretta fallita: {exc}"})
            continue

        tag_result = ask_tag(question)
        if "error" in tag_result:
            rows.append({"question": question, "reference_sql": sql, "tag_sql": tag_result.get("sql"),
                         "correct": False, "error": tag_result["error"]})
            continue

        correct = question_correct(ground_truth.get("rows", []), tag_result.get("rows", []))
        correct_count += int(correct)
        rows.append({
            "question": question, "reference_sql": sql, "tag_sql": tag_result.get("sql"),
            "attempts": tag_result.get("attempts"), "correct": correct, "error": "",
        })
        print(f"  [{'OK' if correct else 'X '}] {question}")
        time.sleep(1)

    with open(os.path.join(run_dir, "tag_accuracy.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["question", "reference_sql", "tag_sql", "attempts", "correct", "error"])
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in w.fieldnames})

    accuracy = correct_count / len(REFERENCE_QUESTIONS) if REFERENCE_QUESTIONS else 0.0
    print(f"  accuracy: {correct_count}/{len(REFERENCE_QUESTIONS)} = {accuracy:.0%}")
    return {"accuracy": accuracy, "correct": correct_count, "total": len(REFERENCE_QUESTIONS)}

def main():
    run_id, run_dir = new_run_dir("effectiveness")
    print(f"Run: {run_id} -> {run_dir}")

    detection_summary = run_detection_experiment(run_dir)
    prediction_summary = run_prediction_experiment(run_dir)
    live_prediction_summary = run_live_prediction_experiment(run_dir)
    tag_summary = run_tag_experiment(run_dir)

    summary = {
        "detection": detection_summary, "prediction": prediction_summary,
        "live_prediction": live_prediction_summary, "tag": tag_summary,
    }
    update_index("effectiveness", run_id, summary)
    print(f"\nFatto. Risultati in {run_dir}")

if __name__ == "__main__":
    main()
