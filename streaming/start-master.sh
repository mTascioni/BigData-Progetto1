#!/bin/bash
# Avvia il master Spark (comportamento originale dell'immagine bitnami,
# invariato) e, in aggiunta, i due servizi Spark persistenti del progetto:
# query_service.py (layer TAG) e detection_job.py (detection real-time).
# Cosi' l'intera pipeline parte con un solo `docker compose up`, senza
# dover lanciare nulla a mano via `docker exec` dopo il boot.
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

# Il master Spark risponde su :8080 non appena parte, ma Kafka puo'
# metterci ancora qualche secondo a essere davvero pronto (KRaft, creazione
# dei topic). spark-submit, per una query Kafka Structured Streaming, deve
# risolvere le partizioni del topic all'avvio -- se Kafka non e' ancora
# pronto lancia UnknownTopicOrPartitionException e l'intero processo muore,
# senza retry. Un ciclo di retry attorno a ciascun spark-submit fa si' che
# un fallimento transitorio all'avvio si autocorregga invece di lasciare la
# pipeline morta finche' qualcuno non se ne accorge e rilancia a mano.

# Split 2/2 fra query_service e detection_job: su Gazebo + flotta reale i
# core dati a Spark competono direttamente con Gazebo per la CPU della
# stessa macchina, e l'operatore stateful del livelock
# (applyInPandasWithState) apre un processo Python separato per
# micro-batch -- con il volume dati bassissimo della flotta reale (4-8
# robot a 2Hz) il costo non e' processare i dati, e' l'overhead di
# scheduling di Spark stesso (mitigato anche da shuffle.partitions in
# detection_job.py). query_service e' interattivo/a bassa frequenza, 2
# core bastano comunque.
(
  set +e   # altrimenti "set -e" ereditato dallo script uccide la subshell al primo fallimento, prima che il loop possa ritentare
  while true; do
    /opt/bitnami/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --conf spark.cores.max=2 \
      /opt/shf/streaming/query_service.py >> /tmp/query_service.log 2>&1
    echo "[start-master] query_service.py terminato (exit $?), riavvio tra 5s..." >> /tmp/query_service.log
    sleep 5
  done
) &

# stateStore.stateSchemaCheck=false: il livelock usa applyInPandasWithState
# per lo stato per-robot; il controllo rigido di compatibilita' dello
# schema di stato di Spark da' un falso positivo ("StateSchemaNotCompatible")
# su operatori stateful Python anche a schema invariato fra un batch e
# l'altro -- riproducibile in modo deterministico, non un problema di
# dati/logica. Disattivato solo per questo (nessun altro job del progetto
# usa stato arbitrario).
(
  set +e   # vedi nota sopra: senza questo "set -e" uccide la subshell al primo crash, niente retry
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

# il container vive quanto il master: se muore lui, muore il container
# (docker lo puo' riavviare/segnalare), coerente col comportamento originale.
wait "$MASTER_PID"
