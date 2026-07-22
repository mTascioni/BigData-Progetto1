#!/usr/bin/env python3
"""Job PySpark Structured Streaming di detection real-time.

Consuma il topic Kafka `telemetry`. Tre meccanismi di detection, tre
query streaming indipendenti sulla stessa sorgente:

- **Salute** (per messaggio): soglie statiche + Isolation Forest
  (streaming/isolation_forest_model.py) sul vettore
  (motor_temp, motor_current, battery_pct, v_lin, min_obstacle_dist).
  Scrive ogni tick su `fleet_state` (stato live per la dashboard) e, se
  anomalo, anche su `anomalies`.
- **Livelock** (stato esplicito per robot_id, `applyInPandasWithState`): il
  robot e' `moving` ma la distanza sul grafo dal `goal_node` non cala per
  almeno LIVELOCK_CONFIRM_DURATION_S secondi *consecutivi* -- non un
  singolo campionamento, per non scambiare una sosta transitoria (es. un
  sorpasso in un corridoio a corsia singola) per uno stallo prolungato.
- **Deadlock** (finestra scorrevole per current_edge): >=2 robot distinti
  `blocked` sullo stesso arco nella stessa finestra.

Le finestre livelock/deadlock riusano `task_state`/`current_edge` gia'
calcolati dal nodo-ponte; la "distanza sul grafo dal goal_node" e'
calcolata qui con Floyd-Warshall sul piccolo grafo del magazzino
(config/warehouse_graph.json), broadcastata una volta sola.
"""
import json
import math
import os
import sys

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupStateTimeout
from pyspark.sql.types import (
    ArrayType, BooleanType, DoubleType, IntegerType, LongType, StringType,
    StructField, StructType, TimestampType,
)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from isolation_forest_model import FEATURES, load_or_train_model  # noqa: E402
from schemas import TELEMETRY_SCHEMA  # noqa: E402

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
CONFIG_DIR = os.environ.get("CONFIG_DIR", "/workspace/config")
MODEL_PATH = os.environ.get("MODEL_PATH", os.path.join(os.path.dirname(__file__), "models", "isolation_forest.pkl"))
CHECKPOINT_DIR = os.environ.get("CHECKPOINT_DIR", "/tmp/shf-checkpoints")
# Se presente, sovrascrive le soglie di default -- prodotto da
# offline/adaptive_thresholds.py, il feedback verso lo streaming.
ADAPTIVE_THRESHOLDS_PATH = os.environ.get("ADAPTIVE_THRESHOLDS_PATH", "/data/adaptive_thresholds.json")

DEFAULT_THRESHOLDS = {
    "motor_temp_threshold_c": 55.0,
    "motor_current_threshold_a": 2.5,
    "battery_low_threshold_pct": 20.0,
}


def load_thresholds(path):
    """Soglie di salute: quelle adattive se disponibili (calibrate sullo
    storico reale per ridurre i falsi positivi), altrimenti i default
    fissati a mano qui sopra."""
    try:
        with open(path) as f:
            adaptive = json.load(f)
        thresholds = dict(DEFAULT_THRESHOLDS)
        thresholds.update({k: v for k, v in adaptive.items() if k in DEFAULT_THRESHOLDS})
        return thresholds, True
    except (FileNotFoundError, json.JSONDecodeError):
        return dict(DEFAULT_THRESHOLDS), False


# Un singolo campionamento senza progresso puo' essere una sosta del tutto
# normale -- es. un robot che si accoda dietro un altro su un corridoio a
# corsia singola e lo supera correttamente poco dopo -- non un vero
# livelock (che per definizione e' uno stallo *prolungato*, non un singolo
# istante). Si traccia quindi lo stato per-robot (vedi make_livelock_state_func
# piu' sotto): ogni LIVELOCK_CHECK_INTERVAL_S secondi di event time si
# ricampiona dist_to_goal rispetto all'ultimo checkpoint; se il progresso e'
# sotto soglia si accumula lo stallo, altrimenti si azzera. L'anomalia
# scatta solo quando lo stallo continua per almeno
# LIVELOCK_CONFIRM_DURATION_S secondi *consecutivi* (costo: fino a un
# minuto di latenza in piu' prima dell'alert, a fronte di molti meno falsi
# positivi su sorpassi/code transitorie).
#
# (Nota tecnica: la prima versione di questo fix incatenava due
# aggregazioni a finestra con watermark separati -- si e' rivelata un bug/
# limite di Spark: il watermark del secondo stadio restava bloccato
# all'epoch anche con dati reali in arrivo, quindi la conferma non
# scattava mai. `applyInPandasWithState` usa un solo operatore stateful e
# non ha questo problema.)
LIVELOCK_MIN_PROGRESS_M = 0.5  # sotto questa soglia, "la distanza non e' calata"
LIVELOCK_CHECK_INTERVAL_S = 10.0
LIVELOCK_CONFIRM_DURATION_S = 60.0
LIVELOCK_STATE_TIMEOUT_S = 90.0  # oltre questo senza messaggi, lo stato del robot si scarta

LIVELOCK_STATE_SCHEMA = StructType([
    StructField("ref_dist", DoubleType()),
    StructField("ref_time_ms", LongType()),
    StructField("stall_start_ms", LongType()),  # -1 = nessuno stallo in corso
    StructField("alerted", BooleanType()),
    # goal_node del campionamento di riferimento (fix falsi positivi, vedi
    # sotto): senza, un cambio di goal fra due campionamenti fa sembrare
    # "nessun progresso" (o un arretramento) anche se il robot si e' mosso
    # normalmente -- osservato soprattutto sul generatore sintetico, che
    # assegna un nuovo goal_node a caso ogni volta che un robot arriva,
    # molto piu' spesso di un robot ROS reale (sequenza fissa e finita).
    StructField("ref_goal_node", StringType()),
])

LIVELOCK_OUTPUT_SCHEMA = StructType([
    StructField("type", StringType()),
    StructField("robot_id", StringType()),
    StructField("run_id", StringType()),
    StructField("window_start", TimestampType()),
    StructField("window_end", TimestampType()),
    StructField("min_dist", DoubleType()),
    StructField("max_dist", DoubleType()),
    StructField("stall_duration_s", DoubleType()),
    StructField("n_msgs", IntegerType()),
])


def _livelock_state_func(key, pdf_iter, state):
    """Una chiamata per robot_id per micro-batch: pdf_iter sono i nuovi
    messaggi di telemetria arrivati per quel robot in questo batch. Lo
    stato persiste fra i batch (vedi LIVELOCK_STATE_SCHEMA)."""
    (robot_id,) = key
    pdf = pd.concat(pdf_iter, ignore_index=True)

    if state.hasTimedOut:
        state.remove()
        return iter([pd.DataFrame(columns=[f.name for f in LIVELOCK_OUTPUT_SCHEMA.fields])])

    pdf = pdf.sort_values("event_time")

    if state.exists:
        ref_dist, ref_time_ms, stall_start_ms, alerted, ref_goal_node = state.get
    else:
        ref_dist, ref_time_ms, stall_start_ms, alerted, ref_goal_node = None, None, -1, False, None

    out_rows = []
    min_d = max_d = None
    n_msgs = 0
    last_event_ms = ref_time_ms or 0
    run_id = None

    for row in pdf.itertuples():
        t_ms = int(row.event_time.timestamp() * 1000)
        last_event_ms = max(last_event_ms, t_ms)
        d = row.dist_to_goal
        moving = row.task_state == "moving"
        run_id = getattr(row, "run_id", None) or run_id
        if d is None:
            continue
        # Il goal e' cambiato dal campionamento di riferimento: la distanza
        # verso un nuovo obiettivo non e' confrontabile con quella verso il
        # vecchio (quasi raggiunto), sembrerebbe un arretramento anche se il
        # robot si muove normalmente. Si tratta come un nuovo riferimento,
        # nessun calcolo di progresso in questo giro -- stesso trattamento
        # del primissimo messaggio mai visto per questo robot.
        if ref_time_ms is None or row.goal_node != ref_goal_node:
            ref_dist, ref_time_ms, ref_goal_node = d, t_ms, row.goal_node
            continue
        elapsed_s = (t_ms - ref_time_ms) / 1000.0
        if elapsed_s < LIVELOCK_CHECK_INTERVAL_S:
            continue
        progress = ref_dist - d
        if moving and progress < LIVELOCK_MIN_PROGRESS_M:
            if stall_start_ms < 0:
                stall_start_ms = ref_time_ms
                min_d = max_d = d
                n_msgs = 1
            else:
                min_d = d if min_d is None else min(min_d, d)
                max_d = d if max_d is None else max(max_d, d)
                n_msgs += 1
            stall_s = (t_ms - stall_start_ms) / 1000.0
            if not alerted and stall_s >= LIVELOCK_CONFIRM_DURATION_S:
                out_rows.append({
                    "type": "livelock", "robot_id": robot_id, "run_id": run_id,
                    "window_start": pd.Timestamp(stall_start_ms, unit="ms"),
                    "window_end": pd.Timestamp(t_ms, unit="ms"),
                    "min_dist": float(min_d), "max_dist": float(max_d),
                    "stall_duration_s": stall_s, "n_msgs": int(n_msgs),
                })
                alerted = True
        else:
            stall_start_ms, alerted, min_d, max_d, n_msgs = -1, False, None, None, 0
        ref_dist, ref_time_ms, ref_goal_node = d, t_ms, row.goal_node

    state.update((ref_dist, ref_time_ms, stall_start_ms, alerted, ref_goal_node))
    state.setTimeoutTimestamp(last_event_ms + int(LIVELOCK_STATE_TIMEOUT_S * 1000))

    if out_rows:
        return iter([pd.DataFrame(out_rows)])
    return iter([pd.DataFrame(columns=[f.name for f in LIVELOCK_OUTPUT_SCHEMA.fields])])

DEADLOCK_WINDOW, DEADLOCK_SLIDE = "20 seconds", "10 seconds"

# ---------------------------------------------------------------- previsione
# Un preavviso (guasto "preavviso_intermittente", vedi kafka_bridge.py
# /synthetic_generator.py) porta un canale oltre una soglia "morbida" solo a
# raffiche saltuarie, non in modo continuo come i guasti "duri" gia'
# esistenti -- l'anomalia non e' ancora rilevabile a soglia fissa (il valore
# torna nominale fra una raffica e l'altra) ma il PATTERN delle raffiche e'
# un indicatore che il robot si sta avvicinando a un guasto vero. Si conta,
# per canale e per robot, quante volte il valore ha superato la soglia
# morbida negli ultimi PREAVVISO_WINDOW_S secondi (event time); se il numero
# di raffiche osservate supera PREAVVISO_MIN_CROSSINGS si emette una
# previsione. Stesso operatore stateful del livelock (applyInPandasWithState),
# stesso motivo: serve memoria fra i micro-batch, non solo il messaggio
# corrente.
#
# La soglia morbida e' derivata da quella dura con un margine fisso (non
# un'altra calibrazione a parte): abbastanza per intercettare l'escursione di
# una raffica senza scattare sul rumore nominale (vedi DEFAULT_THRESHOLDS).
PREAVVISO_SOFT_MARGIN = {
    "motor_temp": -10.0,      # soglia dura - 10 (es. 55 -> 45)
    "motor_current": -0.5,    # soglia dura - 0.5 (es. 2.5 -> 2.0)
    "battery_pct": 10.0,      # soglia dura + 10 (es. 20 -> 30, "sotto" e' il guasto)
}
PREAVVISO_DIRECTION = {"motor_temp": "above", "motor_current": "above", "battery_pct": "below"}
PREAVVISO_WINDOW_S = 60.0
PREAVVISO_MIN_CROSSINGS = 3  # almeno 3 raffiche osservate nella finestra per confermare un trend, non rumore isolato
PREAVVISO_STATE_TIMEOUT_S = 90.0
# Stima conservativa del lead time riportata all'utente (non una regressione
# vera come in predictive/forecast_failures.py -- qui il segnale e' a
# raffiche, non un trend continuo adatto a un fit lineare stabile): il tempo
# restante prima che il guasto pieno "confermi" e' assunto pari alla finestra
# di osservazione stessa, un limite superiore prudente, non una previsione di
# precisione.
PREAVVISO_LEAD_TIME_ESTIMATE_S = PREAVVISO_WINDOW_S

PREAVVISO_CHANNELS = ["motor_temp", "motor_current", "battery_pct"]

PREAVVISO_STATE_SCHEMA = StructType([
    StructField("crossings_ms", ArrayType(ArrayType(LongType()))),  # una lista di timestamp per canale, stesso ordine di PREAVVISO_CHANNELS
    StructField("alerted_channels", ArrayType(StringType())),
])

PREAVVISO_OUTPUT_SCHEMA = StructType([
    StructField("type", StringType()),
    StructField("robot_id", StringType()),
    StructField("run_id", StringType()),
    StructField("channel", StringType()),
    StructField("current_value", DoubleType()),
    StructField("critical_threshold", DoubleType()),
    StructField("lead_time_s", DoubleType()),
    StructField("n_crossings", IntegerType()),
])


def make_previsione_state_func(hard_thresholds):
    soft_thresholds = {
        ch: hard_thresholds[f"{ch}_threshold_c" if ch == "motor_temp" else
                            f"{ch}_threshold_a" if ch == "motor_current" else
                            "battery_low_threshold_pct"] + PREAVVISO_SOFT_MARGIN[ch]
        for ch in PREAVVISO_CHANNELS
    }
    hard_by_channel = {
        "motor_temp": hard_thresholds["motor_temp_threshold_c"],
        "motor_current": hard_thresholds["motor_current_threshold_a"],
        "battery_pct": hard_thresholds["battery_low_threshold_pct"],
    }

    def _crosses(channel, value):
        if value is None:
            return False
        soft = soft_thresholds[channel]
        return value > soft if PREAVVISO_DIRECTION[channel] == "above" else value < soft

    def _fn(key, pdf_iter, state):
        (robot_id,) = key
        pdf = pd.concat(pdf_iter, ignore_index=True)

        if state.hasTimedOut:
            state.remove()
            return iter([pd.DataFrame(columns=[f.name for f in PREAVVISO_OUTPUT_SCHEMA.fields])])

        pdf = pdf.sort_values("event_time")

        if state.exists:
            crossings_ms, alerted_channels = state.get
        else:
            crossings_ms, alerted_channels = [[] for _ in PREAVVISO_CHANNELS], []
        crossings_ms = [list(c) for c in crossings_ms]
        alerted_channels = list(alerted_channels)

        out_rows = []
        last_event_ms = 0
        run_id = None

        for row in pdf.itertuples():
            t_ms = int(row.event_time.timestamp() * 1000)
            last_event_ms = max(last_event_ms, t_ms)
            run_id = getattr(row, "run_id", None) or run_id
            cutoff_ms = t_ms - int(PREAVVISO_WINDOW_S * 1000)

            for i, channel in enumerate(PREAVVISO_CHANNELS):
                value = getattr(row, channel)
                crossings_ms[i] = [t for t in crossings_ms[i] if t >= cutoff_ms]
                if _crosses(channel, value):
                    crossings_ms[i].append(t_ms)
                n = len(crossings_ms[i])
                if n >= PREAVVISO_MIN_CROSSINGS and channel not in alerted_channels:
                    out_rows.append({
                        "type": "previsione", "robot_id": robot_id, "run_id": run_id,
                        "channel": channel, "current_value": float(value) if value is not None else None,
                        "critical_threshold": float(hard_by_channel[channel]),
                        "lead_time_s": PREAVVISO_LEAD_TIME_ESTIMATE_S, "n_crossings": int(n),
                    })
                    alerted_channels.append(channel)
                elif n == 0 and channel in alerted_channels:
                    # tornato stabilmente nominale (nessuna raffica nella
                    # finestra): si permette una nuova previsione in futuro
                    # se le raffiche riprendono.
                    alerted_channels.remove(channel)

        state.update((crossings_ms, alerted_channels))
        state.setTimeoutTimestamp(last_event_ms + int(PREAVVISO_STATE_TIMEOUT_S * 1000))

        if out_rows:
            return iter([pd.DataFrame(out_rows)])
        return iter([pd.DataFrame(columns=[f.name for f in PREAVVISO_OUTPUT_SCHEMA.fields])])

    return _fn


def load_graph(config_dir):
    with open(os.path.join(config_dir, "warehouse_graph.json")) as f:
        graph = json.load(f)
    node_pos = {n["id"]: (n["x"], n["y"]) for n in graph["nodes"]}
    return node_pos, graph["edges"]


def all_pairs_shortest_path(node_pos, edges):
    """Floyd-Warshall: grafo piccolo (~10 nodi), il costo O(n^3) e'
    trascurabile e si calcola una volta sola all'avvio del job."""
    nodes = list(node_pos.keys())
    INF = float("inf")
    dist = {a: {b: (0.0 if a == b else INF) for b in nodes} for a in nodes}
    for e in edges:
        length = float(e["length"])
        if length < dist[e["from"]][e["to"]]:
            dist[e["from"]][e["to"]] = length
            dist[e["to"]][e["from"]] = length
    for k in nodes:
        for i in nodes:
            for j in nodes:
                via = dist[i][k] + dist[k][j]
                if via < dist[i][j]:
                    dist[i][j] = via
    return dist


def nearest_node(node_pos, x, y):
    return min(node_pos, key=lambda n: math.hypot(node_pos[n][0] - x, node_pos[n][1] - y))


def make_dist_to_goal_udf(node_pos_bc, dist_table_bc, edge_lookup_bc):
    """Distanza continua sul grafo dal punto attuale al goal_node: proietta
    (x, y) sull'arco corrente (`current_edge`, gia' nello schema) per sapere
    quanto manca a ciascuno dei due nodi estremi, poi somma la distanza-su-
    grafo (Floyd-Warshall) da quel nodo al goal, e prende il minimo dei due
    percorsi.

    Necessario perche' un arco da 10m percorso a velocita' di crociera
    (~0.2 m/s) richiede ~50s: se si agganciasse (x, y) al solo nodo piu'
    vicino (versione precedente), dist_to_goal resterebbe piatto per circa
    meta' di quel tempo -- piu' della finestra di 30s del rilevatore di
    livelock -- facendo scattare falsi positivi su robot che si stanno
    muovendo normalmente. Verificato con dati reali: vedi
    docs/passi/07-detection-streaming.md."""
    def _dist(x, y, current_edge, goal_node):
        if goal_node is None or x is None or y is None:
            return None
        node_pos = node_pos_bc.value
        dist_table = dist_table_bc.value
        edge = edge_lookup_bc.value.get(current_edge)

        if edge is None:
            # arco sconosciuto/mancante: fallback al nodo piu' vicino
            n = nearest_node(node_pos, x, y)
            d = dist_table.get(n, {}).get(goal_node)
            return float(d) if d is not None and d != float("inf") else None

        x1, y1 = node_pos[edge["from"]]
        x2, y2 = node_pos[edge["to"]]
        dx, dy = x2 - x1, y2 - y1
        seg_len_sq = dx * dx + dy * dy
        t = 0.0 if seg_len_sq == 0 else max(0.0, min(1.0, ((x - x1) * dx + (y - y1) * dy) / seg_len_sq))
        length = edge["length"]

        candidates = []
        d_to = dist_table.get(edge["to"], {}).get(goal_node)
        if d_to is not None and d_to != float("inf"):
            candidates.append((1 - t) * length + d_to)
        d_from = dist_table.get(edge["from"], {}).get(goal_node)
        if d_from is not None and d_from != float("inf"):
            candidates.append(t * length + d_from)
        return float(min(candidates)) if candidates else None

    return F.udf(_dist, DoubleType())


def make_threshold_reasons_udf(thresholds):
    temp_t = thresholds["motor_temp_threshold_c"]
    current_t = thresholds["motor_current_threshold_a"]
    battery_t = thresholds["battery_low_threshold_pct"]

    def _reasons(motor_temp, motor_current, battery_pct):
        reasons = []
        if motor_temp is not None and motor_temp > temp_t:
            reasons.append("motor_temp")
        if motor_current is not None and motor_current > current_t:
            reasons.append("motor_current")
        if battery_pct is not None and battery_pct < battery_t:
            reasons.append("battery_pct")
        return reasons

    return F.udf(_reasons, ArrayType(StringType()))


def make_isolation_forest_udf(model_bc):
    @F.pandas_udf(IntegerType())
    def _predict(motor_temp: pd.Series, motor_current: pd.Series, battery_pct: pd.Series,
                 v_lin: pd.Series, min_obstacle_dist: pd.Series) -> pd.Series:
        model = model_bc.value
        frame = pd.DataFrame({
            "motor_temp": motor_temp, "motor_current": motor_current,
            "battery_pct": battery_pct, "v_lin": v_lin,
            "min_obstacle_dist": min_obstacle_dist,
        }).fillna({"v_lin": 0.0, "min_obstacle_dist": 3.5})[FEATURES]
        preds = model.predict(frame)  # -1 = anomalia, 1 = normale
        return pd.Series((preds == -1).astype(int))

    return _predict


def to_kafka(df, topic, key_col=None):
    # key_col resta ANCHE nel payload (non solo nella key di Kafka): un
    # consumer che legge solo il value (es. kafka-console-consumer di
    # default) non deve perdere robot_id.
    out = df.select(F.to_json(F.struct(*df.columns)).alias("value"), *([F.col(key_col).alias("key")] if key_col else []))
    (out.write.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", topic)
        .save())


def main():
    # Il default di Spark (200 partizioni di shuffle) e' pensato per cluster
    # con decine di core: qui detection_job gira con 2 core totali e i due
    # shuffle (groupBy per-robot del livelock, groupBy+window del deadlock)
    # hanno al massimo una manciata di chiavi per micro-batch (un robot_id o
    # un current_edge per messaggio) -- con 200 partizioni, ogni trigger
    # schedula ~200 task quasi tutti vuoti invece di ~4, overhead puro che
    # si somma alla contesa con Gazebo (vedi start-master.sh). Impostato a
    # un multiplo piccolo di spark.cores.max invece che al default.
    spark = (
        SparkSession.builder.appName("shf-detection")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    node_pos, edges = load_graph(CONFIG_DIR)
    dist_table = all_pairs_shortest_path(node_pos, edges)
    edge_lookup = {e["id"]: e for e in edges}
    node_pos_bc = spark.sparkContext.broadcast(node_pos)
    dist_table_bc = spark.sparkContext.broadcast(dist_table)
    edge_lookup_bc = spark.sparkContext.broadcast(edge_lookup)

    with open(os.path.join(CONFIG_DIR, "experiment.json")) as f:
        experiment = json.load(f)
    model = load_or_train_model(MODEL_PATH, experiment["health_channels_nominal"])
    model_bc = spark.sparkContext.broadcast(model)

    thresholds, is_adaptive = load_thresholds(ADAPTIVE_THRESHOLDS_PATH)
    source = "adattive (" + ADAPTIVE_THRESHOLDS_PATH + ")" if is_adaptive else "default"
    print(f"Soglie di salute in uso ({source}): {thresholds}")

    dist_to_goal_udf = make_dist_to_goal_udf(node_pos_bc, dist_table_bc, edge_lookup_bc)
    threshold_reasons_udf = make_threshold_reasons_udf(thresholds)
    isolation_forest_udf = make_isolation_forest_udf(model_bc)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", "telemetry")
        .option("startingOffsets", "latest")
        .load()
    )

    telemetry = (
        raw.select(F.from_json(F.col("value").cast("string"), TELEMETRY_SCHEMA).alias("m"))
        .select("m.*")
        .withColumn("event_time", (F.col("ts") / 1000).cast(TimestampType()))
        .withColumn("dist_to_goal", dist_to_goal_udf("x", "y", "current_edge", "goal_node"))
    )

    # ---------------------------------------------------------------- salute
    health = (
        telemetry
        .withColumn("if_anomaly", isolation_forest_udf(
            "motor_temp", "motor_current", "battery_pct", "v_lin", "min_obstacle_dist"))
        .withColumn("threshold_reasons", threshold_reasons_udf("motor_temp", "motor_current", "battery_pct"))
        .withColumn("health_anomaly", (F.size("threshold_reasons") > 0) | (F.col("if_anomaly") == 1))
    )

    def write_health_batch(batch_df, _batch_id):
        if batch_df.rdd.isEmpty():
            return
        fleet_state = batch_df.select(
            "ts", "robot_id", "run_id", "x", "y", "theta", "v_lin", "v_ang",
            "battery_pct", "motor_current", "motor_temp", "min_obstacle_dist",
            "task_state", "current_edge", "goal_node", "health_anomaly",
        )
        to_kafka(fleet_state, "fleet_state", key_col="robot_id")

        anomalies = batch_df.filter("health_anomaly").select(
            F.lit("salute").alias("type"), "ts", "robot_id", "run_id",
            "threshold_reasons", "if_anomaly",
            "motor_temp", "motor_current", "battery_pct",
        )
        if not anomalies.rdd.isEmpty():
            to_kafka(anomalies, "anomalies", key_col="robot_id")

    health_query = (
        health.writeStream
        .foreachBatch(write_health_batch)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/health")
        .trigger(processingTime="2 seconds")
        .start()
    )

    # -------------------------------------------------------------- livelock
    livelock = (
        telemetry
        .select("robot_id", "run_id", "event_time", "dist_to_goal", "task_state", "goal_node")
        .withWatermark("event_time", "30 seconds")
        .groupBy("robot_id")
        .applyInPandasWithState(
            _livelock_state_func,
            outputStructType=LIVELOCK_OUTPUT_SCHEMA,
            stateStructType=LIVELOCK_STATE_SCHEMA,
            outputMode="append",
            timeoutConf=GroupStateTimeout.EventTimeTimeout,
        )
    )

    def write_anomaly_batch(batch_df, _batch_id):
        if batch_df.rdd.isEmpty():
            return
        to_kafka(batch_df, "anomalies")

    livelock_query = (
        livelock.writeStream
        .foreachBatch(write_anomaly_batch)
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/livelock")
        .trigger(processingTime="10 seconds")
        .start()
    )

    # -------------------------------------------------------------- deadlock
    deadlock_windowed = (
        telemetry.filter(F.col("task_state") == "blocked")
        .withWatermark("event_time", "20 seconds")
        .groupBy(F.window("event_time", DEADLOCK_WINDOW, DEADLOCK_SLIDE), "current_edge")
        .agg(F.collect_set("robot_id").alias("robots"), F.first("run_id", ignorenulls=True).alias("run_id"))
    )
    deadlock_candidates = deadlock_windowed.filter(F.size("robots") >= 2).select(
        F.lit("deadlock").alias("type"),
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "current_edge", "robots", "run_id",
    )
    deadlock_query = (
        deadlock_candidates.writeStream
        .foreachBatch(write_anomaly_batch)
        .outputMode("append")  # stessa motivazione di livelock_query sopra
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/deadlock")
        .trigger(processingTime="10 seconds")
        .start()
    )

    # ------------------------------------------------------------ previsione
    previsione_state_func = make_previsione_state_func(thresholds)
    previsione = (
        telemetry
        .select("robot_id", "run_id", "event_time", "motor_temp", "motor_current", "battery_pct")
        .withWatermark("event_time", "30 seconds")
        .groupBy("robot_id")
        .applyInPandasWithState(
            previsione_state_func,
            outputStructType=PREAVVISO_OUTPUT_SCHEMA,
            stateStructType=PREAVVISO_STATE_SCHEMA,
            outputMode="append",
            timeoutConf=GroupStateTimeout.EventTimeTimeout,
        )
    )
    previsione_query = (
        previsione.writeStream
        .foreachBatch(write_anomaly_batch)
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/previsione")
        .trigger(processingTime="5 seconds")
        .start()
    )

    _ = (health_query, livelock_query, deadlock_query, previsione_query)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
