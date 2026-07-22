#!/usr/bin/env python3
"""Job PySpark Structured Streaming di persistenza.

Consuma i topic Kafka `telemetry`, `anomalies`, `injected_faults` e li
scrive su Parquet (storico), nelle directory nominate in CLAUDE.md:
`/data/telemetry`, `/data/anomalies`, `/data/injected_faults`.

A differenza di detection_job.py (real-time, legge solo i messaggi nuovi
da quando parte), questo job legge `startingOffsets=earliest`: il suo
scopo e' costruire l'archivio storico completo, non reagire in tempo
reale. Per lo stesso motivo il checkpoint vive sotto /data (il volume
persistente), non /tmp come in detection_job.py: se il checkpoint andasse
perso a un riavvio, il job rileggerebbe tutto da earliest e duplicherebbe
righe gia' scritte nei Parquet -- qui la correttezza dello storico conta
piu' che nel job real-time.

`anomalies` e' partizionato per `type` (salute/livelock/deadlock/previsione):
le query del layer TAG e degli script di valutazione filtrano quasi sempre
per tipo, e il partitioning evita di scansionare tutto il dataset ogni
volta. `telemetry`/`injected_faults` non sono partizionati.

`run_id` (isolamento fra run diversi) e' una colonna normale in tutte e tre
le tabelle, NON una chiave di partizione: partizionare anche per `run_id`
e' stato provato e scartato -- aggiungere una colonna di partizione a una
directory Parquet che ha gia' dati scritti con uno schema di partizione
diverso (qui: `type=X/*.parquet` piatto, dati precedenti a questa modifica)
rompe la lettura dell'INTERA tabella ("Conflicting partition column names",
non un problema per-file: Spark pretende uno schema di partizione uniforme
su tutta la directory). Filtrare per `run_id` via `WHERE` in SQL resta
comunque efficiente quanto basta per i volumi di questo progetto, senza il
rischio di rompere lo storico esistente ogni volta che si aggiunge una
colonna.
"""
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from schemas import ANOMALIES_SCHEMA, INJECTED_FAULTS_SCHEMA, TELEMETRY_SCHEMA  # noqa: E402

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
DATA_DIR = os.environ.get("DATA_DIR", "/data")
CHECKPOINT_DIR = os.environ.get("CHECKPOINT_DIR", "/data/_checkpoints")
TRIGGER = os.environ.get("PERSIST_TRIGGER", "10 seconds")
# Se il checkpoint va perso/resettato, il job riparte da `earliest` su un
# topic che nel frattempo puo' aver accumulato milioni di messaggi
# (telemetry, con molti robot/run nel tempo): senza un limite, il PRIMO
# micro-batch prova a leggerli tutti in un colpo solo, restando
# silenziosamente "fermo" (nessun batch commesso, quindi nessun file
# scritto) anche per molti minuti su una macchina con poche risorse. Un
# tetto per trigger fa avanzare il checkpoint a pezzi, con progresso
# visibile fin dal primo batch.
MAX_OFFSETS_PER_TRIGGER = os.environ.get("PERSIST_MAX_OFFSETS_PER_TRIGGER", "20000")


def read_topic(spark, topic, schema):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .load()
    )
    return raw.select(F.from_json(F.col("value").cast("string"), schema).alias("m")).select("m.*")


def start_parquet_sink(df, name, partition_by=None):
    writer = (
        df.writeStream
        .format("parquet")
        .option("path", os.path.join(DATA_DIR, name))
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, f"persist-{name}"))
        .trigger(processingTime=TRIGGER)
    )
    if partition_by:
        writer = writer.partitionBy(partition_by)
    return writer.start()


def main():
    spark = SparkSession.builder.appName("shf-persistence").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    telemetry = read_topic(spark, "telemetry", TELEMETRY_SCHEMA)
    anomalies = read_topic(spark, "anomalies", ANOMALIES_SCHEMA)
    injected_faults = read_topic(spark, "injected_faults", INJECTED_FAULTS_SCHEMA)

    queries = [
        start_parquet_sink(telemetry, "telemetry"),
        start_parquet_sink(anomalies, "anomalies", partition_by="type"),
        start_parquet_sink(injected_faults, "injected_faults"),
    ]

    _ = queries
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
