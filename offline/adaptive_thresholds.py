#!/usr/bin/env python3
"""Calibra le soglie di salute (Passo 7: motor_temp/motor_current/battery_pct)
sullo storico reale, per ridurre i falsi positivi -- il "job che tara
soglie adattive" del Passo 8.

Usa `injected_faults` (Passo 6) come ground truth per distinguere, fra le
anomalie di tipo "salute" gia' scritte da detection_job.py, quelle vere
(cadono dentro la finestra di un guasto reale) da quelle false. Per ogni
canale con troppi falsi positivi, allarga la soglia fino a un percentile
alto (o basso, per la batteria) dei valori osservati nei periodi NOMINALI
(fuori da ogni finestra di guasto) -- cosi' il rumore nominale non la fa
piu' scattare, senza bisogno di sapere a priori quanto rumore ci sia.

Scrive /data/adaptive_thresholds.json. detection_job.py lo rilegge al
prossimo avvio: e' il "feedback verso lo streaming" richiesto dal piano --
non hot-reload a caldo (fuori scopo per un job Spark gia' in esecuzione),
ma calibrazione fra una run e la successiva.
"""
import argparse
import glob
import json
import os
from datetime import datetime, timezone

import pandas as pd

DEFAULT_THRESHOLDS = {
    "motor_temp_threshold_c": 55.0,
    "motor_current_threshold_a": 2.5,
    "battery_low_threshold_pct": 20.0,
}

CHANNEL_TO_THRESHOLD_KEY = {
    "motor_temp": "motor_temp_threshold_c",
    "motor_current": "motor_current_threshold_a",
    "battery_pct": "battery_low_threshold_pct",
}
LOWER_BOUND_CHANNELS = {"battery_pct"}  # soglia "sotto" invece che "sopra"

PERCENTILE_HIGH = 0.995
PERCENTILE_LOW = 0.005


def load_parquet_dir(path):
    # pd.read_parquet su una directory (non sui singoli file) usa il
    # dataset API di pyarrow e ricostruisce le colonne di partizione
    # Hive-style (es. anomalies/type=salute/...) -- leggendo i file uno per
    # uno quella colonna andrebbe persa, perche' Spark non la scrive dentro
    # ai file quando partiziona.
    if not glob.glob(os.path.join(path, "**", "*.parquet"), recursive=True):
        return pd.DataFrame()
    return pd.read_parquet(path)


def build_fault_windows(injected_faults):
    windows = {}
    for _, f in injected_faults.iterrows():
        windows.setdefault(f["robot_id"], []).append((f["start_ts"], f["end_ts"]))
    return windows


def in_any_window(robot_id, ts, windows_by_robot):
    return any(start <= ts <= end for start, end in windows_by_robot.get(robot_id, []))


def calibrate(telemetry, anomalies, injected_faults, min_false_positives):
    thresholds = dict(DEFAULT_THRESHOLDS)
    report = {}

    if telemetry.empty:
        return thresholds, {"_global": "nessuna telemetria persistita: nessuna calibrazione, restano i default"}

    windows_by_robot = build_fault_windows(injected_faults) if not injected_faults.empty else {}
    telemetry = telemetry.copy()
    telemetry["in_fault_window"] = telemetry.apply(
        lambda r: in_any_window(r["robot_id"], r["ts"], windows_by_robot), axis=1
    )
    nominal = telemetry[~telemetry["in_fault_window"]]

    salute = anomalies[anomalies["type"] == "salute"].copy() if not anomalies.empty else pd.DataFrame()
    if not salute.empty:
        salute["true_positive"] = salute.apply(
            lambda r: in_any_window(r["robot_id"], r["ts"], windows_by_robot), axis=1
        )

    for channel, key in CHANNEL_TO_THRESHOLD_KEY.items():
        if salute.empty:
            report[channel] = "nessuna anomalia 'salute' nello storico: soglia invariata"
            continue

        false_positives = salute[
            salute["threshold_reasons"].apply(lambda reasons: channel in reasons if reasons is not None else False)
            & (~salute["true_positive"])
        ]
        if len(false_positives) < min_false_positives:
            report[channel] = (
                f"{len(false_positives)} falsi positivi (< {min_false_positives}): soglia invariata"
            )
            continue

        nominal_values = nominal[channel].dropna()
        if nominal_values.empty:
            report[channel] = f"{len(false_positives)} falsi positivi ma nessun dato nominale per ricalibrare"
            continue

        if channel in LOWER_BOUND_CHANNELS:
            candidate = float(nominal_values.quantile(PERCENTILE_LOW))
            improves = candidate < thresholds[key]
        else:
            candidate = float(nominal_values.quantile(PERCENTILE_HIGH))
            improves = candidate > thresholds[key]

        if improves:
            report[channel] = f"{len(false_positives)} falsi positivi -> soglia {thresholds[key]} -> {round(candidate, 2)}"
            thresholds[key] = round(candidate, 2)
        else:
            report[channel] = (
                f"{len(false_positives)} falsi positivi ma il percentile nominale ({round(candidate, 2)}) "
                f"non e' meno sensibile della soglia attuale: invariata"
            )

    return thresholds, report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="/data")
    parser.add_argument("--out", default="/data/adaptive_thresholds.json")
    parser.add_argument("--min-false-positives", type=int, default=3)
    args = parser.parse_args()

    telemetry = load_parquet_dir(os.path.join(args.data_dir, "telemetry"))
    anomalies = load_parquet_dir(os.path.join(args.data_dir, "anomalies"))
    injected_faults = load_parquet_dir(os.path.join(args.data_dir, "injected_faults"))

    thresholds, report = calibrate(telemetry, anomalies, injected_faults, args.min_false_positives)

    payload = dict(thresholds)
    payload["computed_at"] = datetime.now(timezone.utc).isoformat()
    payload["report"] = report
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    print(f"Soglie scritte in {args.out}:")
    print(json.dumps(payload, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
