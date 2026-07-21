#!/bin/bash
# Avvia il master Spark (comportamento originale dell'immagine bitnami,
# invariato) e, in aggiunta, i due servizi Spark persistenti del progetto:
# query_service.py (layer TAG, Passo 10) e detection_job.py (detection
# real-time, Passo 7). Cosi' l'intera pipeline parte con un solo
# `docker compose up`, senza dover lanciare nulla a mano via `docker exec`
# dopo il boot (vedi docs/passi/01-scaffold-infrastruttura.md).
#
# Usato solo dal servizio spark-master in docker-compose.yml (override di
# `command:`); spark-worker resta sul CMD originale dell'immagine.
set -e

/opt/bitnami/scripts/spark/run.sh &
MASTER_PID=$!

echo "[start-master] in attesa che il master Spark sia pronto..."
until curl -sf http://localhost:8080 > /dev/null 2>&1; do
  sleep 2
done
echo "[start-master] master pronto, avvio query_service.py e detection_job.py"

# Split 2/10 fra query_service e detection_job (era 3/8): rivisto durante
# la costruzione della suite di test (test/) del 2026-07-21 -- con 3
# query streaming concorrenti (salute/livelock/deadlock) 8 core non
# bastavano piu' sotto carico, causando micro-batch in ritardo (10s
# diventavano 15-19s) e simulazione delle catene di test/detection meno
# reali. query_service e' interattivo/a bassa frequenza, 2 core bastano.
/opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.cores.max=2 \
  /opt/shf/streaming/query_service.py > /tmp/query_service.log 2>&1 &

# stateStore.stateSchemaCheck=false: il livelock (Passo 7, fix del
# 2026-07-21) usa applyInPandasWithState per lo stato per-robot; il
# controllo rigido di compatibilita' dello schema di stato di Spark da'
# un falso positivo ("StateSchemaNotCompatible") su operatori stateful
# Python anche a schema invariato fra un batch e l'altro -- riproducibile
# in modo deterministico, non un problema di dati/logica. Disattivato
# solo per questo (nessun altro job del progetto usa stato arbitrario).
/opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
  --conf spark.jars.ivy=/opt/bitnami/spark/ivy-cache \
  --conf spark.cores.max=10 \
  --conf spark.sql.streaming.stateStore.stateSchemaCheck=false \
  /opt/shf/streaming/detection_job.py > /tmp/detection_job.log 2>&1 &

# il container vive quanto il master: se muore lui, muore il container
# (docker lo puo' riavviare/segnalare), coerente col comportamento originale.
wait "$MASTER_PID"
