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

# Bug reale trovato il 2026-07-21 (non solo in questa sandbox: capita a
# chiunque avvii tutto lo stack insieme con `docker compose up`): il master
# Spark risponde su :8080 non appena parte, ma Kafka puo' metterci ancora
# qualche secondo a essere davvero pronto (KRaft, creazione dei topic).
# spark-submit, per una query Kafka Structured Streaming, deve risolvere le
# partizioni del topic all'avvio -- se Kafka non e' ancora pronto lancia
# UnknownTopicOrPartitionException e l'intero processo muore, UNA VOLTA SOLA,
# senza nessun retry (a differenza dei consumer Node del backend, che gia'
# ritentano ogni 5s per lo stesso motivo, vedi fleetStateStore.js). Risultato
# visibile all'utente: la dashboard resta vuota a tempo indeterminato (nessun
# messaggio arriva mai su fleet_state) anche se ROS/Gazebo funzionano
# normalmente -- sembra che "la simulazione parta con molto ritardo", ma in
# realta' e' questo processo che non e' mai partito per davvero. Fix: un
# ciclo di retry attorno a ciascun spark-submit, stesso principio dei
# consumer Node, cosi' un fallimento transitorio all'avvio si autocorregge
# invece di lasciare la pipeline morta finche' qualcuno non se ne accorge e
# rilancia a mano.

# Split 2/4 fra query_service e detection_job (era 3/8, poi 2/10 costruendo
# test/ il 2026-07-21, ora 2/4 lo stesso giorno): quel 10 era tarato per il carico
# PESANTE del generatore sintetico (migliaia di robot-token, Passo 12/13),
# non per la flotta ROS reale. Sulla flotta reale (4-8 robot) e' un problema
# diverso e piu' serio: 10 core dati a Spark competono direttamente con
# Gazebo per la CPU della stessa macchina, e l'operatore stateful del
# livelock (applyInPandasWithState, Passo 7) apre un processo Python
# separato per micro-batch -- osservato empiricamente decine di processi
# "pyspark.daemon" concorrenti anche con soli 4 robot reali. Risultato
# osservato il 2026-07-21: micro-batch da 2s che ne impiegavano 30-65,
# fleet_state quasi fermo per decine di secondi alla volta -> sulla
# dashboard sembra che "la simulazione vada a scatti", ma la causa reale e'
# contesa di CPU fra Spark e Gazebo, non un problema di rete o di canvas.
# query_service e' interattivo/a bassa frequenza, 2 core bastano comunque.
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

# stateStore.stateSchemaCheck=false: il livelock (Passo 7, fix del
# 2026-07-21) usa applyInPandasWithState per lo stato per-robot; il
# controllo rigido di compatibilita' dello schema di stato di Spark da'
# un falso positivo ("StateSchemaNotCompatible") su operatori stateful
# Python anche a schema invariato fra un batch e l'altro -- riproducibile
# in modo deterministico, non un problema di dati/logica. Disattivato
# solo per questo (nessun altro job del progetto usa stato arbitrario).
(
  set +e   # vedi nota sopra: senza questo "set -e" uccide la subshell al primo crash, niente retry
  while true; do
    /opt/bitnami/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
      --conf spark.jars.ivy=/opt/bitnami/spark/ivy-cache \
      --conf spark.cores.max=4 \
      --conf spark.sql.streaming.stateStore.stateSchemaCheck=false \
      /opt/shf/streaming/detection_job.py >> /tmp/detection_job.log 2>&1
    echo "[start-master] detection_job.py terminato (exit $?), riavvio tra 5s..." >> /tmp/detection_job.log
    sleep 5
  done
) &

# il container vive quanto il master: se muore lui, muore il container
# (docker lo puo' riavviare/segnalare), coerente col comportamento originale.
wait "$MASTER_PID"
