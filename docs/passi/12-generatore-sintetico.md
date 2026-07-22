# Passo 12 — Generatore sintetico per lo sweep di scalabilità

**Obiettivo:** robot come token sul grafo, stesso schema messaggi, a volume alzabile (decine di migliaia di msg/s) — Gazebo non arriva a quei ritmi. Solo per stressare Kafka+Spark.
**Deliverable atteso:** il carico per l'esperimento di scalabilità (Passo 13).

Non tocca ROS/Gazebo: la sorgente dati resta quella, questo è solo un generatore di carico per l'esperimento di *efficiency* del Passo 13 — throughput/latenza al variare del carico, punto di rottura. Niente scenari (deadlock/livelock) né guasti: quelli sono già coperti dalla pipeline ROS reale (Passi 5-6) e dal loro ground truth in `injected_faults`.

## Cosa è stato costruito

**`generator/synthetic_generator.py`** — script standalone (no ROS/rospy, solo `confluent-kafka` + stdlib, eseguito nel container `ros` che ha già la libreria installata dal Passo 4). Ogni "robot-token" (`VirtualRobot`) percorre il grafo di `config/warehouse_graph.json` arco per arco a velocità costante — stessa fonte di verità di roadmap usata da `graph_navigator.py` (Passo 3) e `kafka_bridge.py` (Passo 4) — scegliendo il prossimo nodo a caso fra i vicini (evita di tornare subito indietro se ci sono alternative). Nessuna fisica: la posizione è interpolata linearmente sull'arco corrente in base al tempo trascorso da quando lo ha imboccato. I canali di salute sono sintetizzati con la stessa formula nominale+rumore di `kafka_bridge.py`, leggendo `health_channels_nominal` da `config/experiment.json` (stessa fonte di verità, nessun parametro duplicato a mano). Il messaggio pubblicato su Kafka (`telemetry`, key=`robot_id`) rispetta esattamente lo schema condiviso con `kafka_bridge.py` — verificato in questo passo consumando direttamente dal topic.

Il carico è controllato da due manopole indipendenti:
- `--num-robots`: quanti robot-token (scala orizzontale della flotta)
- `--hz`: quante telemetrie/s pubblica ciascuno

Il throughput aggregato di targa è `num_robots * hz`. Schedulazione con una coda a priorità (`heapq`, chiave = prossimo istante di invio per robot) invece di uno scan lineare ad ogni tick: scala bene anche con decine di migliaia di robot-token. Il producer Kafka è configurato per throughput (`linger.ms`, `batch.num.messages`, `queue.buffering.max.messages` alzato) con retry su `BufferError` invece di un crash. Lo script stampa periodicamente il throughput realmente raggiunto e un riepilogo finale (messaggi totali, msg/s medi, errori) — dato grezzo per il punto di rottura del Passo 13.

**`docker-compose.yml`** — nuovo volume `./generator:/opt/shf/generator` sul servizio `ros` (stesso pattern con cui `./streaming` è montato su `spark-master`).

## Cosa NON fa (deliberatamente)

Non simula `task_state=blocked/charging` né guasti: l'obiettivo è generare **volume** con uno schema valido, non scenari realistici (quelli sono già coperti dalla pipeline ROS reale). Non fa multiprocessing: se il collo di bottiglia diventa il singolo processo Python (GIL + `json.dumps` per messaggio) prima ancora di Kafka/Spark, la soluzione più semplice è lanciare più istanze in parallelo con prefissi `--robot-id-prefix` diversi, non complicare questo script — coerente con "niente astrazioni oltre il necessario".

## Verifica

1. **Correttezza dello schema**: run piccola (`--num-robots 2 --hz 2 --duration-s 6`), messaggi consumati live direttamente dal topic `telemetry` (`kafka-console-consumer.sh`, senza `--from-beginning` per non pescare fra le decine di migliaia di righe reali già presenti dai passi precedenti) — tutti i campi dello schema presenti, posizione che avanza in modo plausibile lungo l'arco (`C-H`, `B-I`), `theta` coerente con la direzione di marcia, `battery_pct` che scende lentamente, `motor_current`/`motor_temp` intorno ai nominali.
2. **Throughput controllato**: `--num-robots 1000 --hz 5` (target 5000 msg/s) → **5000 msg/s medi** raggiunti esattamente, 0 `BufferError`, su 15s (75012 messaggi totali).
3. **Volume "decine di migliaia di msg/s"**: `--num-robots 5000 --hz 10` (target 50000 msg/s) → **36686 msg/s medi** (picco 43809 msg/s nei primi 5s, sceso a 32342 msg/s negli ultimi, sintomo del processo singolo che satura prima di raggiungere la targa) su 550359 messaggi in 15s, ancora 0 `BufferError` (il producer ha retto il carico, il limite è nel processo Python che lo alimenta) — target "decine di migliaia di msg/s" del piano raggiunto e il tetto del singolo processo osservato empiricamente, dato utile da riusare come baseline al Passo 13.
4. Confermato via `kafka-get-offsets.sh` che gli offset del topic `telemetry` sono avanzati coerentemente con i messaggi inviati.

## Nota operativa: interazione con `detection_job`/dashboard attivi

Durante la verifica, `detection_job.py` (Passo 7) era ancora in esecuzione per la dashboard del Passo 11: la run da 5000 robot-token ha quindi prodotto anche 5000 righe di `fleet_state`, finite nello stato in memoria del backend e quindi (potenzialmente) sulla mappa della dashboard insieme ai 3 robot reali. Nessun errore o crash, ma la vista live diventerebbe illeggibile. Per gli esperimenti di scalabilità veri e propri (Passo 13) ha senso **non** tenere la dashboard aperta, o usare un prefisso `--robot-id-prefix` dedicato per poter distinguere/filtrare a colpo d'occhio i robot di carico da quelli reali. Nella sessione di verifica, il backend è stato riavviato dopo il test per svuotare lo stato accumulato (nessuna perdita: `fleet_state` non è mai la fonte di verità, quella è Parquet).

## Stato

- `generator/synthetic_generator.py` — nuovo.
- `docker-compose.yml` — nuovo volume su `ros`.

Comando di esempio per rilanciare un carico:

```bash
docker exec shf-ros bash -c \
  "python3 /opt/shf/generator/synthetic_generator.py --num-robots 5000 --hz 10 --duration-s 30"
```

## Prossimo passo

Passo 13 — Valutazione sperimentale (`eval/`): efficiency (throughput/latenza vs carico usando questo generatore, punto di rottura, latenza onset→alert) ed effectiveness (precision/recall/F1 della detection vs `injected_faults`, accuratezza delle previsioni, execution accuracy del layer TAG sulle ~20-30 domande di riferimento).
