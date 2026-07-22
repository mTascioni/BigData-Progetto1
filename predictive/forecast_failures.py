#!/usr/bin/env python3
"""Previsione offline dei guasti sui canali di salute.

Per ogni robot e ciascun canale di salute (motor_temp, motor_current,
battery_pct), guarda la finestra recente dello storico persistito e, solo
se c'e' gia' un trend abbastanza marcato da non essere rumore nominale,
allena una regressione lineare sulla serie ricampionata ed estrapola la
retta in avanti fino a trovare l'istante previsto di superamento della
soglia critica -> "lead time" (tempo di vita utile residuo). Scrive
`/data/predictions`.

Regressione lineare invece di ARIMA/Prophet/LSTM: i segnali che vogliamo
prevedere (rampe di guasto quasi lineari, es. deriva_termica) non hanno
bisogno di un modello piu' complesso, ed e' molto piu' semplice da
spiegare e verificare. Le soglie critiche non sono arbitrarie: coincidono
coi valori-obiettivo delle firme di guasto fissate in config/experiment.json
(`plateau_temp_c` di deriva_termica, `peak_a` di spike_corrente) -- e' li'
che un guasto di quel tipo, se non corretto, porterebbe la metrica.
"""
import argparse
import glob
import os
from datetime import datetime, timezone

import numpy as np
import pandas as pd

CRITICAL_THRESHOLDS = {
    "motor_temp": {"direction": "above", "value": 85.0},    # = plateau_temp_c di deriva_termica
    "motor_current": {"direction": "above", "value": 4.5},  # = peak_a di spike_corrente
    "battery_pct": {"direction": "below", "value": 10.0},
}

# sotto questa pendenza (per canale, per minuto) non e' un trend verso il
# guasto, e' rumore nominale -- calibrate sui rate nominali di
# config/experiment.json: drain nominale batteria -0.5%/min in movimento,
# quindi la soglia va ben oltre per non scattare sempre.
MIN_SLOPE_PER_MIN = {
    "motor_temp": 0.5,
    "motor_current": 0.05,
    "battery_pct": -2.0,
}

RESAMPLE_S = 5
MIN_POINTS = 12  # almeno un minuto di storia ricampionata per tentare un fit
FORECAST_HORIZON_S = 1800  # non si prevede oltre 30 minuti nel futuro


def load_parquet_dir(path):
    files = glob.glob(os.path.join(path, "**", "*.parquet"), recursive=True)
    # un file Parquet da 0 byte capita se lo streaming writer viene ucciso a
    # meta' batch (es. pkill su persistence_job.py): pandas/pyarrow, leggendo
    # la cartella direttamente (senza passare dal commit log Spark
    # _spark_metadata, che lo escluderebbe), altrimenti crasha. Il file e'
    # comunque vuoto per definizione: cancellarlo non perde dati veri, e
    # lasciare a pd.read_parquet() la cartella intera (non i singoli file)
    # preserva l'inferenza delle colonne di partizione Hive-style.
    empty = [f for f in files if os.path.getsize(f) == 0]
    for f in empty:
        os.remove(f)
    if empty:
        print(f"  rimossi {len(empty)} file .parquet da 0 byte (scrittura interrotta) in {path}")
    if not [f for f in files if f not in empty]:
        return pd.DataFrame()
    return pd.read_parquet(path)


def resample_channel(telemetry, robot_id, channel, lookback_s, now_ts, run_id=None):
    mask = (
        (telemetry["robot_id"] == robot_id)
        & (telemetry["ts"] >= now_ts - lookback_s * 1000)
        & (telemetry["ts"] <= now_ts)
    )
    # Isolamento fra run diversi: robot_id e' riusato ad ogni run del
    # generatore sintetico (es. SIM00000), quindi senza filtrare per run_id
    # il trend calcolato mischierebbe run diversi. run_id=None (storico
    # pre-esistente senza questo campo) mantiene il comportamento
    # precedente, nessun filtro.
    if run_id is not None and "run_id" in telemetry.columns:
        mask &= telemetry["run_id"] == run_id
    sub = telemetry[mask][["ts", channel]].dropna().sort_values("ts")
    if sub.empty:
        return None
    sub = sub.assign(t=pd.to_datetime(sub["ts"], unit="ms")).set_index("t")[channel]
    return sub.resample(f"{RESAMPLE_S}s").mean().interpolate()


def fit_linear_trend(series):
    """Regressione lineare ai minimi quadrati: valore = intercept + slope * t,
    con t in secondi dall'inizio della finestra ricampionata. Ritorna
    (intercept, slope_per_s)."""
    t = (series.index - series.index[0]).total_seconds().values
    slope, intercept = np.polyfit(t, series.values, 1)
    return intercept, slope


def find_crossing_s(intercept, slope, direction, critical_value, max_horizon_s):
    """Istante (in secondi dall'inizio della finestra) in cui la retta
    incrocia la soglia critica, o None se non succede entro l'orizzonte
    massimo o se il trend va nella direzione sbagliata."""
    if slope == 0:
        return None
    reaches_threshold = (slope > 0 and direction == "above") or (slope < 0 and direction == "below")
    if not reaches_threshold:
        return None
    t_cross = (critical_value - intercept) / slope
    if t_cross < 0 or t_cross > max_horizon_s:
        return None
    return t_cross


def analyze(telemetry, robot_id, channel, lookback_s, now_ts, run_id=None):
    series = resample_channel(telemetry, robot_id, channel, lookback_s, now_ts, run_id=run_id)
    if series is None or len(series) < MIN_POINTS:
        return None

    minutes = (series.index[-1] - series.index[0]).total_seconds() / 60.0
    if minutes <= 0:
        return None
    slope_per_min = (series.iloc[-1] - series.iloc[0]) / minutes
    min_slope = MIN_SLOPE_PER_MIN[channel]
    trending = (slope_per_min >= min_slope) if min_slope > 0 else (slope_per_min <= min_slope)
    if not trending:
        return None

    intercept, slope_per_s = fit_linear_trend(series)
    crit = CRITICAL_THRESHOLDS[channel]
    t_cross_s = find_crossing_s(intercept, slope_per_s, crit["direction"], crit["value"], FORECAST_HORIZON_S)
    if t_cross_s is None:
        return None

    window_start_ms = int(series.index[0].value // 10**6)
    crossing_ts_ms = window_start_ms + int(t_cross_s * 1000)

    return {
        "robot_id": robot_id,
        "run_id": run_id,
        "channel": channel,
        "predicted_at_ts": now_ts,
        "current_value": float(series.iloc[-1]),
        "slope_per_min": float(slope_per_min),
        "critical_threshold": crit["value"],
        "predicted_crossing_ts": crossing_ts_ms,
        "lead_time_s": (crossing_ts_ms - now_ts) / 1000.0,
        "model": "regressione lineare (OLS)",
        "n_points": int(len(series)),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="/data")
    parser.add_argument("--out", default="/data/predictions")
    parser.add_argument("--lookback-s", type=int, default=300)
    parser.add_argument("--now-ts", type=int, default=None,
                         help="epoch ms da cui guardare indietro (default: l'ultimo timestamp disponibile). "
                              "Utile per rieseguire l'analisi 'come se fosse' un istante passato.")
    parser.add_argument("--run-id", default=None,
                         help="isola l'analisi a un run_id specifico (default: l'ultimo presente nello "
                              "storico, o nessun filtro se lo storico non ha affatto la colonna run_id)")
    args = parser.parse_args()

    telemetry = load_parquet_dir(os.path.join(args.data_dir, "telemetry"))
    if telemetry.empty:
        print("Nessuna telemetria persistita: nessuna previsione possibile.")
        return

    now_ts = args.now_ts if args.now_ts is not None else int(telemetry["ts"].max())

    # Isolamento fra run: di default si analizza solo l'ULTIMO run_id
    # presente nello storico (quello a cui appartiene now_ts), non tutto lo
    # storico assieme -- altrimenti robot_id riusati fra run diversi (es.
    # il generatore sintetico) mischierebbero trend di run differenti in
    # un'unica retta.
    run_id = args.run_id if "run_id" in telemetry.columns else None
    if run_id is None and "run_id" in telemetry.columns and telemetry["run_id"].notna().any():
        run_id = telemetry.loc[telemetry["ts"] == telemetry["ts"].max(), "run_id"].iloc[0]

    scope = telemetry if run_id is None else telemetry[telemetry["run_id"] == run_id]
    robots = sorted(scope["robot_id"].dropna().unique())
    print(f"Analizzo {len(robots)} robot (run_id={run_id or 'nessun filtro, storico senza run_id'}) "
          f"su una finestra di {args.lookback_s}s (now_ts={now_ts})")

    predictions = []
    for robot_id in robots:
        for channel in CRITICAL_THRESHOLDS:
            pred = analyze(telemetry, robot_id, channel, args.lookback_s, now_ts, run_id=run_id)
            if pred:
                predictions.append(pred)

    if not predictions:
        print("Nessun robot con un trend verso una soglia critica in questa finestra.")
        return

    out_df = pd.DataFrame(predictions)
    ts_tag = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    os.makedirs(args.out, exist_ok=True)
    out_path = os.path.join(args.out, f"predictions_{ts_tag}.parquet")
    out_df.to_parquet(out_path, index=False)

    print(f"\n{len(predictions)} previsioni scritte in {out_path}:")
    print(out_df.to_string(index=False))


if __name__ == "__main__":
    main()
