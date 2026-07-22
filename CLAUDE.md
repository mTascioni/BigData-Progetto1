# Self Healing Fleet

Pipeline Big Data: ingerisce telemetria continua da una flotta di AGV in un magazzino, fa **detection real-time** (anomalie di salute + deadlock/livelock) e **analisi predittiva offline** dei guasti su serie storiche, si adatta riducendo i falsi positivi, e si interroga in linguaggio naturale (text-to-SQL).

Progetto **Big Data**, non di robotica: il robot è solo la sorgente dati. V: Volume, Velocity, Veracity (+ Value con la predittiva). Corso: Topic 7 (IoT Stream Analytics).

Piano operativo passo-passo: vedi `PLAN.md`.

## Regole del progetto (invarianti)

- **Lo schema del messaggio di telemetria è l'unica fonte di verità.** Ogni componente (ponte ROS, generatore, Spark, storage, TAG) si conforma a esso.
- **La sorgente dati è ROS + Gazebo + TurtleBot3** (requisito, per dati realistici). È la parte più delicata: va messa in piedi e de-rischiata per prima.
- **I canali di salute (`motor_temp`, `motor_current`, `battery_pct`) non esistono nel TurtleBot3 in simulazione**: vengono sintetizzati nel nodo-ponte (nominale + eventuale firma di guasto).
- **Quattro tecnologie richieste, tutte presenti almeno in versione minima:** Kafka, Spark Structured Streaming, previsione su time series, LLM. Se si è a corto di tempo si riduce la *profondità*, non la *presenza*.
- **Real-time e offline sono entrambi richiesti:** detection in streaming (real-time) + previsione dei guasti su storico (offline/predittiva).
- **La dashboard è guidata dalla pipeline** (Spark → topic Kafka `fleet_state` → backend Node → websocket), così vede tutti i robot.
- **Ground truth:** ogni guasto iniettato va loggato in `injected_faults`; è la base per precision/recall.
- **Si costruisce un passo alla volta** seguendo `PLAN.md`.
- **Eccezione deliberata (Passo 14): un'anomalia di salute rilevata su un robot reale chiude l'anello.** Fino al Passo 13 il sistema era puramente diagnostico (detect/predict/alert, nessuna azione verso ROS). Su richiesta esplicita dell'utente (demo dal vivo), per la sola **flotta reale** un'anomalia manda il robot colpito verso un nodo di riparazione e dispaccia un robot di riserva dedicato (R4) sulla sua missione — comando minimo ("vai al nodo X"), non robotica di rimedio: resta coerente con "il robot è solo la sorgente dati" perché il *contenuto* della decisione (quale nodo, quale missione) è logica applicativa nel backend, non nel robot. Il generatore sintetico (Passo 12) non è toccato da questo anello. Vedi `docs/passi/14-flotta-reale-e-self-healing.md`.
- **Reazione differenziata previsione/guasto persistente (Passo 15).** L'anello del Passo 14 si è biforcato: una **previsione** di guasto (rilevata in streaming da raffiche intermittenti/trend che superano una soglia "morbida", non ancora quella dura) attiva la riparazione preventiva + dispaccio riserva di prima; un **guasto persistente confermato** (soglia dura) non manda più il robot in riparazione automaticamente — lo ferma dov'è (`~nav_control` `{"cmd":"freeze"}`) e aspetta che l'operatore lo veda in dashboard e lo decommissioni esplicitamente (il robot sparisce da mappa/tabelle, mai più scelto come riserva). Motivo: un guasto già confermato è per definizione troppo tardi per una manovra preventiva. Vedi `docs/passi/15-previsione-live-deposito-run-id.md`.
- **Eccezione al "nessun cambio di topologia" (Passo 15).** Il grafo del magazzino ha guadagnato tre nodi dedicati (`DEPOSITO`, `RISERVA1`, `RISERVA2`, fuori dai corridoi principali) per `repair_node`/`reserve_node`/gli start_node dei robot di riserva, che prima coincidevano con nodi di transito di robot reali (collisione fisica osservata). Vedi lo stesso doc sopra.
- **`DEPOSITO`/`RISERVA1`/`RISERVA2` non sono nodi percorribili dal generatore sintetico (Passo 16).** Sono dedicati alla flotta reale (repair/reserve); il generatore sintetico li esclude sia dallo spawn iniziale sia dal random walk (`kind` in `{"repair","reserve"}`, filtrato in `build_adjacency()`), altrimenti un robot-token normale (nessun guasto) poteva nascerci o finirci per puro caso. Vedi `docs/passi/16-perturbazioni-streaming-live-fix-riserve.md`.
- **Lo stato di riparazione/riserva della flotta reale è per-run, non per processo (Passo 16).** `dispatchedReserves`/`inRepair`/`decommissioned` (`fleetStateStore.js`) sono resettati esplicitamente a ogni `POST /sim/start`: senza reset, una riserva già usata (o un robot decommissionato) in un run precedente restava inutilizzabile per sempre anche dopo un riavvio pulito della simulazione ROS/Gazebo. Vedi lo stesso doc sopra.

## Stack

- ROS1 Noetic + Gazebo + TurtleBot3 — sorgente dati
- `rospy` + `confluent-kafka` — nodo-ponte ROS → Kafka
- Kafka — ingestion (topic `telemetry` partizionato per `robot_id`)
- PySpark Structured Streaming — detection real-time
- Regressione lineare (Python, numpy) — previsione time-series dei guasti (offline). *(ARIMA provato per primo, poi semplificato: il segnale è quasi lineare per costruzione, vedi `docs/passi/09-analisi-predittiva.md`.)*
- Parquet — storico, su un volume Docker condiviso (`/data`, montato sui container Spark)
- Spark SQL — esecuzione dell'SQL del layer TAG sui Parquet, via un servizio Spark persistente dedicato (`streaming/query_service.py`, HTTP interno). *(Non DuckDB come pensato inizialmente: riusa il cluster Spark già in piedi, vedi `docs/passi/10-layer-tag.md`.)*
- Qwen-Coder (Qwen2.5-Coder-32B-Instruct) via il router Inference Providers di Hugging Face (`https://router.huggingface.co/v1/chat/completions`, API OpenAI-compatibile, non self-hosted) — text-to-SQL (no CrewAI)
- **Node.js (Express) — backend + dashboard:** serve la pagina, consuma `fleet_state` via `kafkajs` e fa push in websocket, espone l'endpoint TAG (`POST /api/tag`) che chiama il router HF e poi `query_service.py` per eseguire l'SQL
- Frontend: HTML + canvas 2D + JS
- Docker / docker-compose — orchestrazione

## Contratti condivisi

Messaggio di telemetria (JSON, un tick per robot):

```json
{
  "ts": 1721400000000,
  "robot_id": "R3",
  "run_id": "a1b2c3d4",
  "x": 12.4, "y": 3.1, "theta": 1.57,
  "v_lin": 0.22, "v_ang": 0.0,
  "cmd_v_lin": 0.25, "cmd_v_ang": 0.0,
  "battery_pct": 74.0, "motor_current": 1.8, "motor_temp": 41.5,
  "min_obstacle_dist": 0.9,
  "task_state": "moving",
  "current_edge": "C-F",
  "goal_node": "H"
}
```

`run_id` (Passo 15): id della sessione/run corrente (generatore sintetico o avvio della flotta reale), nullable per compatibilità con dati storici precedenti al campo. Isola l'analisi (previsione, query TAG, eval) fra run diversi — `robot_id` da solo non basta, i robot-token del generatore lo riusano a ogni run.

`task_state` in { idle, moving, blocked, charging }.
Origine campi: posa/velocità/odometria/`min_obstacle_dist` da Gazebo; canali di salute sintetizzati nel ponte.

Mappa a grafo (`config/warehouse_graph.json`): `nodes` (id, x, y, kind), `edges` (id, from, to, capacity, length); `capacity=1` = corsia singola. I robot la seguono come roadmap; (x,y) mappato sull'arco occupato (`current_edge`). Include (Passo 15) `DEPOSITO`/`RISERVA1`/`RISERVA2`, fuori dai corridoi principali, per repair_node/reserve_node/gli start_node dei robot di riserva.

Storage (directory Parquet): `telemetry/`, `anomalies/`, `injected_faults/`, `predictions/` — sottocartelle di un volume Docker condiviso (`/data`), non del repo. `anomalies` partizionata per `type`; `run_id` (Passo 15) è una colonna normale ovunque, filtrata via `WHERE`, non una chiave di partizione (aggiungerla come partizione romperebbe la lettura dello storico preesistente, scritto con uno schema di partizione diverso — verificato).

Guasti di salute (firme parametriche): `deriva_termica` (rampa+plateau), `spike_corrente` (picco), `batteria_collasso` (discesa ripida), `sensore_bloccato` (freeze), `preavviso_intermittente` (Passo 15: raffiche saltuarie oltre una soglia morbida, non un guasto pieno continuo — segnale per la previsione live in streaming).
Guasti comportamentali (scenari su corridoi + task): `deadlock`, `livelock`.
Anomalie di tipo `previsione` (Passo 15): non un guasto iniettato, un segnale calcolato in streaming (`detection_job.py`) quando un canale supera una soglia morbida abbastanza spesso da suggerire un guasto imminente — stesso topic `anomalies`, `type="previsione"`.
Perturbazioni (Passo 16): `rumore_sensore` — rumore gaussiano extra su un canale, **non un guasto**: iniettabile in modo reattivo come i guasti (stessa via `POST /api/fleet-control/fault`), ma deliberatamente esclusa da `injected_faults` (ground truth), cosi' un'eventuale anomalia "salute" rilevata durante la sua finestra resta un falso positivo per `offline/adaptive_thresholds.py`, non un vero positivo — serve a generare falsi positivi controllati per verificare che il sistema li impari a filtrare.

## Struttura del repo

```
self-healing-fleet/
  docker-compose.yml
  config/
    warehouse_graph.json
    experiment.json     # fleet, tasks/scenari, fault_schedule (= ground truth)
  ros/                  # launch, roadmap navigation, nodo-ponte ROS->Kafka
  generator/            # generatore sintetico (scale test) + injection
  streaming/            # job PySpark: detection (real-time), persistenza su Parquet, query_service TAG
  predictive/           # previsione time-series dei guasti (offline)
  offline/              # soglie adattive
  backend/              # Node: endpoint TAG, consumer fleet_state, websocket
  dashboard/            # frontend canvas 2D + UI query NL
  eval/                 # script esperimenti + domande di riferimento (Passo 13, numeri per la tesina)
  test/                 # suite pytest pass/fail: verifica di correttezza, non produce numeri per la tesina
```
