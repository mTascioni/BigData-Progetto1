import json
import os
import subprocess
import time
import uuid

import pytest
import requests
from confluent_kafka import Consumer, Producer

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
BACKEND_URL = os.environ.get("BACKEND_URL", "http://backend:3000")
QUERY_SERVICE_URL = os.environ.get("QUERY_SERVICE_URL", "http://spark-master:5000")
GENERATOR_SERVICE_URL = os.environ.get("GENERATOR_SERVICE_URL", "http://localhost:5001")

@pytest.fixture(scope="session")
def kafka_producer():
    p = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    yield p
    p.flush(5)

def produce_json(producer, topic, payload, key=None):
    producer.produce(
        topic,
        key=key.encode("utf-8") if key else None,
        value=json.dumps(payload).encode("utf-8"),
    )
    producer.poll(0)

def start_consumer(topic):
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": f"test-{uuid.uuid4()}",
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

def consume_topic(topic, timeout_s, predicate=None):
    return collect_messages(start_consumer(topic), timeout_s, predicate)

def query_sql(sql):
    res = requests.post(f"{QUERY_SERVICE_URL}/query", json={"sql": sql}, timeout=30)
    res.raise_for_status()
    return res.json()

def ask_tag(question):
    res = requests.post(f"{BACKEND_URL}/api/tag", json={"question": question}, timeout=90)
    try:
        return res.json()
    except ValueError:
        return {"error": f"risposta non JSON (status {res.status_code}): {res.text[:200]}"}

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

@pytest.fixture(scope="session", autouse=True)
def _pause_real_simulation():
    was_running = False
    try:
        status = subprocess.run(
            ["supervisorctl", "status", "sim_multi_robot"],
            capture_output=True, timeout=15, text=True,
        )
        was_running = "RUNNING" in status.stdout
        if was_running:
            subprocess.run(["supervisorctl", "stop", "sim_multi_robot"], check=True, capture_output=True, timeout=15)
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
        was_running = False
    yield
    if was_running:
        subprocess.run(["supervisorctl", "start", "sim_multi_robot"], capture_output=True, timeout=30)

@pytest.fixture(autouse=True)
def _ensure_generator_idle():
    stop_generator()
    yield
    stop_generator()
