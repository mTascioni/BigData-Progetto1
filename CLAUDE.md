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

`task_state` in { idle, moving, blocked, charging }.
Origine campi: posa/velocità/odometria/`min_obstacle_dist` da Gazebo; canali di salute sintetizzati nel ponte.

Mappa a grafo (`config/warehouse_graph.json`): `nodes` (id, x, y, kind), `edges` (id, from, to, capacity, length); `capacity=1` = corsia singola. I robot la seguono come roadmap; (x,y) mappato sull'arco occupato (`current_edge`).

Storage (directory Parquet): `telemetry/`, `anomalies/`, `injected_faults/`, `predictions/` — sottocartelle di un volume Docker condiviso (`/data`), non del repo.

Guasti di salute (firme parametriche): `deriva_termica` (rampa+plateau), `spike_corrente` (picco), `batteria_collasso` (discesa ripida), `sensore_bloccato` (freeze).
Guasti comportamentali (scenari su corridoi + task): `deadlock`, `livelock`.

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
