#!/usr/bin/env python3
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from schemas import ANOMALIES_SCHEMA, INJECTED_FAULTS_SCHEMA, TELEMETRY_SCHEMA

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
DATA_DIR = os.environ.get("DATA_DIR", "/data")
CHECKPOINT_DIR = os.environ.get("CHECKPOINT_DIR", "/data/_checkpoints")
TRIGGER = os.environ.get("PERSIST_TRIGGER", "10 seconds")
MAX_OFFSETS_PER_TRIGGER = os.environ.get("PERSIST_MAX_OFFSETS_PER_TRIGGER", "20000")

def read_topic(spark, topic, schema):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .option("failOnDataLoss", "false")
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
