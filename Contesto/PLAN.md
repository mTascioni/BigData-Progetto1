# Piano operativo — Self Healing Fleet

Passi ordinati, ciascuno con il suo deliverable. Contesto e invarianti: vedi `CLAUDE.md`.

## Mappatura ai requisiti (Topic 7)

| Requisito | Dove è coperto |
|---|---|
| Ingest di stream continui da sensori | Telemetria TurtleBot → nodo-ponte → Kafka (passi 3-4) |
| Real-time analytics | Detection in Spark Structured Streaming (passo 7) |
| Offline analytics su storico → analisi predittiva | Previsione time-series dei guasti (passo 9) |
| Kafka / Spark streaming | Ingestion / Structured Streaming |
| Prediction algorithms su time series | ARIMA/Prophet o LSTM (passo 9) |
| LLM (open source) | TAG text-to-SQL con Qwen (passo 10) |
| Esperimenti: effectiveness + efficiency | Valutazione (passo 13) |

## Passi

### 1. Scaffold + infrastruttura
Struttura del repo e `docker-compose.yml` con Kafka, Spark, servizio Node e container ROS (Noetic + TurtleBot3; noVNC per la GUI in debug).
**Produce:** i container si avviano, Kafka raggiungibile.

### 2. Contratti dati
`config/warehouse_graph.json` (magazzino piccolo con almeno un corridoio a corsia singola) e `config/experiment.json` (flotta, scenari, `fault_schedule`). Fissa lo schema del messaggio.
**Produce:** file di configurazione + contratto messaggi.

### 3. Bring-up ROS/Gazebo (de-rischia subito)
Un **singolo** TurtleBot3 in Gazebo (Noetic), headless, che naviga sul grafo mandando goal nodo per nodo (`move_base`). Verifica posa/odometria/lidar.
**Produce:** un robot che si muove sul grafo e pubblica sui topic ROS.

### 4. Nodo-ponte ROS → Kafka
`rospy` che si sottoscrive ai topic, compone il messaggio nello schema condiviso (sintetizzando i canali di salute nominali) e pubblica su Kafka (`telemetry`, partizionato per `robot_id`).
**Produce:** dati reali in Kafka. *(Suggerimento: registra un rosbag e riproducilo per sviluppare a valle senza tenere Gazebo sempre acceso.)*

### 5. Multi-robot + scenari
Scala a N TurtleBot (namespacing) e imposta gli scenari `deadlock`/`livelock` (corridoi a corsia singola + task opposti). Tieni N modesto: uno stack di navigazione per robot è pesante in CPU.
**Produce:** flotta reale con conflitti di traffico riproducibili.

### 6. Layer di fault injection
Nel nodo-ponte: legge il `fault_schedule` e, per un robot con guasto di salute attivo, somma la firma alla telemetria prima di pubblicare. Logga in `injected_faults`.
**Produce:** guasti controllati + ground truth.

### 7. Detection in streaming — REAL-TIME (PySpark)
Consuma `telemetry`. Salute: soglie + Isolation Forest. Livelock: finestra in cui il robot è attivo ma la distanza sul grafo dal `goal_node` non cala + revisite di nodi. Deadlock: >=2 robot `blocked` in mutua contesa sugli archi su finestra. Scrive `anomalies` e lo stato flotta su `fleet_state`.
**Produce:** anomalie in tempo reale + stato per la dashboard.

### 8. Persistenza + soglie adattive
Persisti `telemetry`, `anomalies`, `injected_faults` su Parquet/Delta. Job che tara soglie adattive per ridurre i falsi positivi, con feedback verso lo streaming.
**Produce:** storico + adattamento.

### 9. Analisi predittiva su time series — OFFLINE
Sui canali di salute storici, allena un modello di previsione (ARIMA/Prophet o LSTM) che prevede il degrado e **quando** una metrica supererà la soglia critica → predice quali robot si guasteranno e con quanto anticipo (remaining useful life). Scrive in `predictions/`.
**Produce:** previsioni di guasto con lead time.

### 10. Layer TAG (text-to-SQL, LLM)
Endpoint Node: prompt (schema intero + few-shot) → Qwen su HuggingFace → SQL → esecuzione con `duckdb` sui Parquet → righe. Guardia: solo `SELECT` + retry sull'errore. ~20-30 domande di riferimento per la valutazione.
**Produce:** "chiedi a parole → risposta dai dati".

### 11. Dashboard
Frontend (HTML + canvas 2D + JS) servito dal backend Node. Il backend consuma `fleet_state` da Kafka e fa push in websocket; la pagina disegna la mappa a grafo e i robot live. Pannelli (incl. robot a rischio dalle previsioni) + casella di query NL verso l'endpoint TAG. Sim headless: la pagina è la visualizzazione.
**Produce:** vista live della flotta + interrogazione.

### 12. Generatore sintetico per lo sweep di scalabilità
Robot come token sul grafo, **stesso schema messaggi**, a volume alzabile (decine di migliaia di msg/s) — Gazebo non arriva a quei ritmi. Solo per stressare Kafka+Spark.
**Produce:** il carico per l'esperimento di scalabilità.

### 13. Valutazione sperimentale — EFFECTIVENESS + EFFICIENCY
`eval/`:
- **Efficiency:** scalabilità (throughput/latency vs carico, punto di rottura), latenza onset->alert.
- **Effectiveness:** detection (precision/recall/F1 vs `injected_faults`, falsi positivi nel tempo); previsione (accuratezza forecast + lead time); execution accuracy del TAG.
**Produce:** i numeri per il report.

---

**Se sei in ritardo:** riduci la profondità, non la presenza. Le quattro tecnologie richieste (Kafka, Spark streaming, previsione time-series, LLM) restano almeno in versione minima. Gli esperimenti non si tagliano.
