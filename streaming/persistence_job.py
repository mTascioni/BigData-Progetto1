#!/usr/bin/env python3
"""Job PySpark Structured Streaming di persistenza (Passo 8).

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

`anomalies` e' partizionato per `type` (salute/livelock/deadlock): le
query del Passo 10 (TAG) e del Passo 13 (eval) filtrano quasi sempre per
tipo, e il partitioning evita di scansionare tutto il dataset ogni volta.
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


def read_topic(spark, topic, schema):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
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
