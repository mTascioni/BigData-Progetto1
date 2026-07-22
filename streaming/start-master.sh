#!/bin/bash
set -e

/opt/bitnami/scripts/spark/run.sh &
MASTER_PID=$!

echo "[start-master] in attesa che il master Spark sia pronto..."
until curl -sf http://localhost:8080 > /dev/null 2>&1; do
  sleep 2
done
echo "[start-master] master pronto, avvio query_service.py e detection_job.py"

(
  set +e
  while true; do
    /opt/bitnami/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --conf spark.cores.max=2 \
      /opt/shf/streaming/query_service.py >> /tmp/query_service.log 2>&1
    echo "[start-master] query_service.py terminato (exit $?), riavvio tra 5s..." >> /tmp/query_service.log
    sleep 5
  done
) &

(
  set +e
  while true; do
    /opt/bitnami/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
      --conf spark.jars.ivy=/opt/bitnami/spark/ivy-cache \
      --conf spark.cores.max=2 \
      --conf spark.sql.streaming.stateStore.stateSchemaCheck=false \
      /opt/shf/streaming/detection_job.py >> /tmp/detection_job.log 2>&1
    echo "[start-master] detection_job.py terminato (exit $?), riavvio tra 5s..." >> /tmp/detection_job.log
    sleep 5
  done
) &

wait "$MASTER_PID"
