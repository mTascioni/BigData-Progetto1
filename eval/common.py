"""Helper condivisi dagli script di valutazione sperimentale.

A differenza di test/conftest.py (suite pytest pass/fail, verifica di
correttezza) questi script producono i numeri per la tesina, letti anche
dal pannello "Risultati sperimentazioni" della dashboard, che lancia i run
on-demand via eval_service.py e ne disegna i risultati live in JS/canvas,
non da PNG pre-generati (quelli, se servono per il PDF, vanno prodotti a
parte). Ogni run scrive in una cartella propria sotto /data/eval/ (sul
volume Docker condiviso `shf-data`, cosi' backend/dashboard possono
leggerli) e aggiorna un indice condiviso `/data/eval/index.json`.
"""
import json
import math
import os
import time
import uuid
from datetime import datetime, timezone

import requests
from confluent_kafka import Consumer, Producer


def json_safe(obj):
    """NaN/Infinity sono float Python legali (precision/f1 diventano NaN
    quando TP+FP=0, caso normale, non un errore) ma non JSON valido: sia il
    fetch() della dashboard sia JSON.parse in Node vanno in eccezione su un
    letterale NaN nel body, silenziosamente (interrompe il polling live di
    eval_service.py proprio quando arriva il primo risultato). Si convertono
    in null prima di scrivere index.json o rispondere via HTTP."""
    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    if isinstance(obj, dict):
        return {k: json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [json_safe(v) for v in obj]
    return obj

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
BACKEND_URL = os.environ.get("BACKEND_URL", "http://backend:3000")
QUERY_SERVICE_URL = os.environ.get("QUERY_SERVICE_URL", "http://spark-master:5000")
GENERATOR_SERVICE_URL = os.environ.get("GENERATOR_SERVICE_URL", "http://localhost:5001")
CONFIG_DIR = os.environ.get("CONFIG_DIR", "/workspace/config")
EVAL_DIR = os.environ.get("EVAL_DIR", "/data/eval")


def load_experiment():
    with open(os.path.join(CONFIG_DIR, "experiment.json")) as f:
        return json.load(f)


# ------------------------------------------------------------------ Kafka
def start_consumer(topic):
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": f"eval-{uuid.uuid4()}",
        "auto.offset.reset": "latest",
    })
    consumer.subscribe([topic])
    consumer.poll(2.0)
    return consumer


def collect_messages(consumer, timeout_s, predicate=None):
    results = []
    end = time.time() + timeout_s
    try:
        while time.time() < end:
            msg = consumer.poll(0.5)
            if msg is None or msg.error():
                continue
            try:
                data = json.loads(msg.value())
            except (json.JSONDecodeError, TypeError):
                continue
            if predicate is None or predicate(data):
                results.append(data)
    finally:
        consumer.close()
    return results


def get_producer():
    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})


def produce_json(producer, topic, payload, key=None):
    producer.produce(topic, key=key.encode("utf-8") if key else None, value=json.dumps(payload).encode("utf-8"))
    producer.poll(0)


# --------------------------------------------------------------- servizi
def query_sql(sql):
    res = requests.post(f"{QUERY_SERVICE_URL}/query", json={"sql": sql}, timeout=30)
    res.raise_for_status()
    return res.json()


def ask_tag(question):
    res = requests.post(f"{BACKEND_URL}/api/tag", json={"question": question}, timeout=90)
    try:
        return res.json()
    except ValueError:
        return {"error": f"risposta non JSON (status {res.status_code})"}


def stop_generator():
    try:
        requests.post(f"{GENERATOR_SERVICE_URL}/stop", timeout=10)
    except requests.RequestException:
        pass
    for _ in range(20):
        if not requests.get(f"{GENERATOR_SERVICE_URL}/status", timeout=10).json().get("running"):
            return
        time.sleep(0.5)


def start_generator(config):
    stop_generator()
    res = requests.post(f"{GENERATOR_SERVICE_URL}/start", json=config, timeout=10)
    res.raise_for_status()
    return res.json()


def wait_generator_done(timeout_s):
    end = time.time() + timeout_s
    status = {}
    while time.time() < end:
        status = requests.get(f"{GENERATOR_SERVICE_URL}/status", timeout=10).json()
        if not status.get("running"):
            return status
        time.sleep(1)
    raise TimeoutError(f"generatore non terminato entro {timeout_s}s (ultimo status: {status})")


# ------------------------------------------------------------- risultati
def new_run_dir(run_type):
    """Crea /data/eval/<run_type>_<timestamp>/ e ritorna (run_id, path)."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_id = f"{run_type}_{ts}"
    path = os.path.join(EVAL_DIR, run_id)
    os.makedirs(path, exist_ok=True)
    return run_id, path


def update_index(run_type, run_id, summary):
    """Aggiunge/aggiorna una entry nell'indice condiviso letto dal backend
    (GET /api/eval/results): un run per volta, il piu' recente per tipo e'
    quello che la dashboard mostra di default."""
    index_path = os.path.join(EVAL_DIR, "index.json")
    os.makedirs(EVAL_DIR, exist_ok=True)
    try:
        with open(index_path) as f:
            index = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        index = []

    index = [e for e in index if e["run_id"] != run_id]
    index.append({
        "run_type": run_type,
        "run_id": run_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "summary": summary,
    })
    index.sort(key=lambda e: e["timestamp"])

    with open(index_path, "w") as f:
        json.dump(json_safe(index), f, indent=2, default=str)
    return index_path
