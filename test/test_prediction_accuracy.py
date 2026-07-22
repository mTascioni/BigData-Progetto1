"""Accuratezza della previsione (regressione lineare): dati sintetici con
un trend NOTO analiticamente (non serve aspettare uno storico
reale), scritti in una cartella temporanea isolata (mai in /data, per non
sporcare lo storico reale persistito da persistence_job.py), e confrontati
con l'istante di superamento soglia calcolato a mano.

predictive/forecast_failures.py non ha dipendenze Spark (e' pandas puro,
vedi il suo stesso docstring), gira benissimo dentro il container `ros`.
"""
import glob
import os
import subprocess
import tempfile

import pandas as pd
import pytest

FORECAST_SCRIPT = "/opt/shf/predictive/forecast_failures.py"


def _write_synthetic_ramp(data_dir, robot_id, start_ts_ms, duration_s, step_s,
                           start_temp, rate_c_per_s):
    rows = []
    t = 0
    while t <= duration_s:
        rows.append({
            "ts": start_ts_ms + t * 1000,
            "robot_id": robot_id,
            "motor_temp": start_temp + rate_c_per_s * t,
            "motor_current": 1.5,  # nominale, piatto: nessun trend spurio
            "battery_pct": 90.0,   # nominale, piatto
        })
        t += step_s
    df = pd.DataFrame(rows)
    telemetry_dir = os.path.join(data_dir, "telemetry")
    os.makedirs(telemetry_dir, exist_ok=True)
    df.to_parquet(os.path.join(telemetry_dir, "part-0.parquet"), index=False)
    return rows


def _run_forecast(data_dir, now_ts_ms, lookback_s=300):
    out_dir = os.path.join(data_dir, "predictions")
    subprocess.run(
        [
            "python3", FORECAST_SCRIPT,
            "--data-dir", data_dir, "--out", out_dir,
            "--now-ts", str(now_ts_ms), "--lookback-s", str(lookback_s),
        ],
        check=True, capture_output=True, text=True, timeout=60,
    )
    files = glob.glob(os.path.join(out_dir, "*.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.read_parquet(out_dir)


def test_lead_time_previsto_vicino_al_valore_analitico():
    start_ts_ms = 1_800_000_000_000  # fisso e arbitrario: e' un test offline, non serve "adesso"
    rate = 0.05  # C/s = 3 C/min, ben oltre MIN_SLOPE_PER_MIN (0.5 C/min)
    start_temp = 60.0
    observed_duration_s = 400  # ultimo dato osservato: temp = 60 + 0.05*400 = 80 C

    with tempfile.TemporaryDirectory() as tmp:
        _write_synthetic_ramp(tmp, "TESTPRED1", start_ts_ms, observed_duration_s, step_s=2,
                               start_temp=start_temp, rate_c_per_s=rate)
        now_ts_ms = start_ts_ms + observed_duration_s * 1000
        predictions = _run_forecast(tmp, now_ts_ms)

    assert not predictions.empty, "nessuna previsione generata per un trend lineare inequivocabile"
    row = predictions[(predictions.robot_id == "TESTPRED1") & (predictions.channel == "motor_temp")]
    assert len(row) == 1, f"attesa 1 previsione motor_temp per TESTPRED1, trovate {len(row)}"
    row = row.iloc[0]

    # verita' analitica: temp(t) = 60 + 0.05*t incrocia 85 C a t=500s,
    # cioe' 100s dopo l'ultimo dato osservato (t=400s) -> lead_time=100s
    expected_crossing_ts = start_ts_ms + 500_000
    expected_lead_time_s = 100.0

    assert abs(row.predicted_crossing_ts - expected_crossing_ts) < 10_000, (
        f"crossing previsto {row.predicted_crossing_ts}, atteso {expected_crossing_ts} (tolleranza 10s)"
    )
    assert abs(row.lead_time_s - expected_lead_time_s) < 10, (
        f"lead_time previsto {row.lead_time_s}s, atteso {expected_lead_time_s}s (tolleranza 10s)"
    )


def test_nessuna_previsione_su_canale_stabile_nominale():
    start_ts_ms = 1_800_000_000_000
    with tempfile.TemporaryDirectory() as tmp:
        # motor_temp con rumore nominale (+-0.5C), nessun trend reale
        rows = []
        t = 0
        while t <= 300:
            rows.append({
                "ts": start_ts_ms + t * 1000, "robot_id": "TESTPRED2",
                "motor_temp": 35.0 + (0.5 if t % 20 < 10 else -0.5),
                "motor_current": 1.5, "battery_pct": 90.0,
            })
            t += 2
        df = pd.DataFrame(rows)
        telemetry_dir = os.path.join(tmp, "telemetry")
        os.makedirs(telemetry_dir, exist_ok=True)
        df.to_parquet(os.path.join(telemetry_dir, "part-0.parquet"), index=False)

        predictions = _run_forecast(tmp, start_ts_ms + 300_000)

    if not predictions.empty:
        assert not ((predictions.robot_id == "TESTPRED2") & (predictions.channel == "motor_temp")).any(), (
            "previsione spuria generata su un canale senza trend reale (solo rumore nominale)"
        )


def test_trend_nella_direzione_sbagliata_non_genera_previsione():
    """motor_temp che SCENDE non deve mai generare una previsione di
    superamento della soglia critica 'above' (85 C)."""
    start_ts_ms = 1_800_000_000_000
    with tempfile.TemporaryDirectory() as tmp:
        _write_synthetic_ramp(tmp, "TESTPRED3", start_ts_ms, 300, step_s=2,
                               start_temp=70.0, rate_c_per_s=-0.05)
        predictions = _run_forecast(tmp, start_ts_ms + 300_000)

    if not predictions.empty:
        assert not ((predictions.robot_id == "TESTPRED3") & (predictions.channel == "motor_temp")).any()
