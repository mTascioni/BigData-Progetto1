"""Fixture e helper condivisi dalla suite di test (eseguita dentro il
container `ros`, che ha gia' confluent-kafka/pandas/numpy/pyarrow e vede
tutti gli altri servizi sulla rete Docker interna del progetto).

Non e' il layer di valutazione sperimentale (quello vive in `eval/`, con
piu' domande di riferimento e output per la tesina): questa suite verifica
che il sistema funzioni correttamente, con soglie di correttezza
(pass/fail), non produce numeri per un report.
"""
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
    """Crea un consumer, lo sottoscrive e forza il join del gruppo PRIMA di
    tornare (auto.offset.reset='latest': si vede solo cosa arriva da questo
    momento in poi). Va sempre chiamata prima di scatenare l'azione che si
    vuole osservare (avviare il generatore, produrre un messaggio): il join
    del gruppo puo' richiedere alcuni secondi, e in quel lasso di tempo un
    evento che scatta subito (es. un guasto con start_time_s basso) andrebbe
    perso se ci si iscrivesse dopo."""
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": f"test-{uuid.uuid4()}",
        "auto.offset.reset": "latest",
    })
    consumer.subscribe([topic])
    consumer.poll(2.0)
    return consumer


def collect_messages(consumer, timeout_s, predicate=None):
    """Raccoglie per `timeout_s` secondi i messaggi JSON-decodificati che
    soddisfano `predicate` (tutti se None), poi chiude il consumer."""
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
    """Scorciatoia per quando non serve controllare l'ordine sottoscrizione/
    azione (es. si osserva un topic gia' in continuo movimento)."""
    return collect_messages(start_consumer(topic), timeout_s, predicate)


def query_sql(sql):
    res = requests.post(f"{QUERY_SERVICE_URL}/query", json={"sql": sql}, timeout=30)
    res.raise_for_status()
    return res.json()


def ask_tag(question):
    # niente raise_for_status(): un 500/503 arriva comunque con un body JSON
    # {"error": ...} significativo (layer non configurato, LLM fallito dopo
    # il retry, ...) che i test vogliono ispezionare, non un'eccezione che
    # lo nasconde.
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
    # /stop e' un segnale, non e' istantaneo: aspetta che il thread finisca
    # davvero prima di lasciare che il prossimo test riparta.
    for _ in range(20):
        if not requests.get(f"{GENERATOR_SERVICE_URL}/status", timeout=10).json().get("running"):
            return
        time.sleep(0.5)


def start_generator(config):
    stop_generator()  # un solo run alla volta e' ammesso dal servizio
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
    """La simulazione ROS/Gazebo reale e' pesante in CPU e non serve a
    questa suite (usa il generatore sintetico per avere robot controllati).
    Lasciarla accesa satura la CPU della macchina di sviluppo insieme a
    Spark, causando batch Spark in ritardo e falsi segnali nei test.
    Messa in pausa per tutta la sessione di test, rimessa su alla fine --
    ma solo se era davvero gia' in esecuzione prima: `supervisorctl stop`
    ritorna comunque successo anche se il programma era gia' fermo, quindi
    un controllo basato solo su "il comando stop e' andato a buon fine" la
    riavvierebbe sempre in teardown, anche quando nessuno l'aveva mai
    avviata. Se supervisorctl non e' raggiungibile (es. suite lanciata
    fuori dal container ros) si prosegue comunque, solo piu' lenti."""
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
        # supervisord ha startsecs=15 per questo programma (aspetta che resti
        # su 15s prima di confermare l'avvio): il timeout qui deve essere
        # piu' largo di quello, altrimenti scade sempre per pochi istanti.
        subprocess.run(["supervisorctl", "start", "sim_multi_robot"], capture_output=True, timeout=30)


@pytest.fixture(autouse=True)
def _ensure_generator_idle():
    """Ogni test parte e finisce con il generatore fermo: evita che un test
    fallito a meta' lasci un run appeso che fa fallire i successivi con 409."""
    stop_generator()
    yield
    stop_generator()
