# Piano operativo — Self Healing Fleet

Passi ordinati, ciascuno con il suo deliverable. Contesto e invarianti: vedi `CLAUDE.md`.

## Stato di avanzamento

**Tutti e 13 i passi completati (2026-07-21).** Documentazione dettagliata di ciascuno — scelte tecniche, problemi incontrati, comandi di verifica — in `docs/passi/01-...md` fino a `13-...md`.

**Passo 14, estensione oltre il piano originale (2026-07-21):** su richiesta dell'utente per una demo dal vivo, un mondo Gazebo con corridoi fisici stretti sui choke point del grafo (deadlock/livelock ora osservabili su dati ROS reali, non solo sintetici), iniezione di guasti live sui robot reali dentro il topic `telemetry` vero, e un primo anello di "self healing" reale: un'anomalia di salute rilevata manda il robot in riparazione e dispaccia un quarto robot di riserva (R4) sulla sua missione. Eccezione deliberata e documentata all'invariante "solo diagnosi" (vedi `CLAUDE.md`). Dettagli in `docs/passi/14-flotta-reale-e-self-healing.md`.

**2026-07-21**, prima di iniziare il Passo 13: corretto un bug reale di falsi positivi nella detection di livelock (Passo 7, vedi `docs/passi/07-detection-streaming.md`), tre correzioni alla dashboard (Passo 11, vedi `docs/passi/11-dashboard.md`), e l'intera pipeline ora si avvia con un solo `docker compose up -d` senza comandi manuali (vedi `docs/passi/01-scaffold-infrastruttura.md`, sezione "Avvio a comando singolo").

Aggiunta anche una suite di test pass/fail in `test/` (distinta dal futuro `eval/` del Passo 13: qui si verifica correttezza, non si producono numeri per la tesina) — 23 test su schema dei messaggi, ground truth dei guasti, precision/recall della detection, accuratezza delle previsioni, execution accuracy del TAG, throughput/latenza. Costruendola è emerso un **secondo bug indipendente** di falsi positivi nel livelock (`outputMode("update")` valutava finestre ancora parzialmente popolate), corretto con `outputMode("append")` — vedi `docs/passi/07-detection-streaming.md` e `test/README.md`. Tutti i 23 test passano.

Due deviazioni dal piano originale, decise in corso d'opera (vedi `docs/passi/10-layer-tag.md` per i dettagli):
- **Passo 9**: previsione con **regressione lineare** invece di ARIMA/Prophet/LSTM (il segnale è quasi lineare per costruzione, ARIMA provato per primo e poi sostituito — non un vincolo rigido del piano).
- **Passo 10**: **niente DuckDB** — l'SQL generato dal LLM gira su **Spark SQL** (nuovo servizio `streaming/query_service.py`), riusando il cluster già in piedi. Qwen-Coder chiamato via il router Inference Providers di Hugging Face (`https://router.huggingface.co/v1/chat/completions`), non self-hosted.

## Mappatura ai requisiti (Topic 7)

| Requisito | Dove è coperto |
|---|---|
| Ingest di stream continui da sensori | Telemetria TurtleBot → nodo-ponte → Kafka (passi 3-4) |
| Real-time analytics | Detection in Spark Structured Streaming (passo 7) |
| Offline analytics su storico → analisi predittiva | Previsione time-series dei guasti (passo 9) |
| Kafka / Spark streaming | Ingestion / Structured Streaming |
| Prediction algorithms su time series | regressione lineare su trend di salute (passo 9, ARIMA provato e poi semplificato) |
| LLM (open source) | TAG text-to-SQL con Qwen2.5-Coder-32B via router HuggingFace (passo 10) |
| Esperimenti: effectiveness + efficiency | Valutazione (passo 13) |

## Passi

### 1. Scaffold + infrastruttura ✅
Struttura del repo e `docker-compose.yml` con Kafka, Spark, servizio Node e container ROS (Noetic + TurtleBot3; noVNC per la GUI in debug).
**Produce:** i container si avviano, Kafka raggiungibile. → `docs/passi/01-scaffold-infrastruttura.md`

### 2. Contratti dati ✅
`config/warehouse_graph.json` (magazzino piccolo con almeno un corridoio a corsia singola) e `config/experiment.json` (flotta, scenari, `fault_schedule`). Fissa lo schema del messaggio.
**Produce:** file di configurazione + contratto messaggi. → `docs/passi/02-contratti-dati.md`

### 3. Bring-up ROS/Gazebo (de-rischia subito) ✅
Un **singolo** TurtleBot3 in Gazebo (Noetic), headless, che naviga sul grafo mandando goal nodo per nodo (`move_base`). Verifica posa/odometria/lidar.
**Produce:** un robot che si muove sul grafo e pubblica sui topic ROS. → `docs/passi/03-bringup-ros-gazebo.md`

### 4. Nodo-ponte ROS → Kafka ✅
`rospy` che si sottoscrive ai topic, compone il messaggio nello schema condiviso (sintetizzando i canali di salute nominali) e pubblica su Kafka (`telemetry`, partizionato per `robot_id`).
**Produce:** dati reali in Kafka. *(Suggerimento: registra un rosbag e riproducilo per sviluppare a valle senza tenere Gazebo sempre acceso.)* → `docs/passi/04-nodo-ponte-kafka.md`

### 5. Multi-robot + scenari ✅
Scala a N TurtleBot (namespacing) e imposta gli scenari `deadlock`/`livelock` (corridoi a corsia singola + task opposti). Tieni N modesto: uno stack di navigazione per robot è pesante in CPU.
**Produce:** flotta reale con conflitti di traffico riproducibili. → `docs/passi/05-multi-robot-scenari.md`

### 6. Layer di fault injection ✅
Nel nodo-ponte: legge il `fault_schedule` e, per un robot con guasto di salute attivo, somma la firma alla telemetria prima di pubblicare. Logga in `injected_faults`.
**Produce:** guasti controllati + ground truth. → `docs/passi/06-fault-injection.md`

### 7. Detection in streaming — REAL-TIME (PySpark) ✅
Consuma `telemetry`. Salute: soglie + Isolation Forest. Livelock: finestra in cui il robot è attivo ma la distanza sul grafo dal `goal_node` non cala + revisite di nodi. Deadlock: >=2 robot `blocked` in mutua contesa sugli archi su finestra. Scrive `anomalies` e lo stato flotta su `fleet_state`.
**Produce:** anomalie in tempo reale + stato per la dashboard. → `docs/passi/07-detection-streaming.md`

### 8. Persistenza + soglie adattive ✅
Persisti `telemetry`, `anomalies`, `injected_faults` su Parquet/Delta. Job che tara soglie adattive per ridurre i falsi positivi, con feedback verso lo streaming.
**Produce:** storico + adattamento. → `docs/passi/08-persistenza-soglie-adattive.md`

### 9. Analisi predittiva su time series — OFFLINE ✅
Sui canali di salute storici, allena un modello di previsione che prevede il degrado e **quando** una metrica supererà la soglia critica → predice quali robot si guasteranno e con quanto anticipo (remaining useful life). Scrive in `predictions/`.
**Realizzato con:** regressione lineare (non ARIMA/Prophet/LSTM come indicato inizialmente — provato ARIMA per primo, poi semplificato di comune accordo: il segnale è quasi lineare per costruzione, il vincolo del piano non era rigido sulla tecnica esatta).
**Produce:** previsioni di guasto con lead time. → `docs/passi/09-analisi-predittiva.md`

### 10. Layer TAG (text-to-SQL, LLM) ✅
Endpoint Node: prompt (schema intero + few-shot) → Qwen su HuggingFace → SQL → esecuzione → righe. Guardia: solo `SELECT` + retry sull'errore. ~20-30 domande di riferimento per la valutazione (previste al Passo 13).
**Realizzato con:** esecuzione su **Spark SQL** (non `duckdb` come indicato inizialmente — nuovo servizio persistente `streaming/query_service.py`, riusa il cluster già in piedi) e Qwen2.5-Coder-32B chiamato via il router **Inference Providers di Hugging Face** (`https://router.huggingface.co/v1/chat/completions`, non self-hosted — evita GPU/quantizzazione nel container).
**Produce:** "chiedi a parole → risposta dai dati". → `docs/passi/10-layer-tag.md`

### 11. Dashboard ✅
Frontend (HTML + canvas 2D + JS) servito dal backend Node. Il backend consuma `fleet_state` da Kafka e fa push in websocket; la pagina disegna la mappa a grafo e i robot live. Pannelli (incl. robot a rischio dalle previsioni) + casella di query NL verso l'endpoint TAG. Sim headless: la pagina è la visualizzazione.
**Esteso oltre il piano originale** (su richiesta, dopo il completamento iniziale): animazione fluida via dead reckoning lato client, overlay live di deadlock/livelock sulla mappa + log eventi, e un pannello di controllo per avviare/fermare il generatore sintetico (Passo 12) direttamente dalla dashboard — N robot, topologia del magazzino (preset piccolo/medio/grande) e guasti da iniettare (stessa ground truth `injected_faults`) — più una tabella live con tutti i valori di tutti i robot (reali e sintetici). Vedi `docs/passi/11-dashboard.md` per il dettaglio delle tre decisioni di scope prese con l'utente.
**Produce:** vista live della flotta + interrogazione + pannello di controllo esperimenti. → `docs/passi/11-dashboard.md`

### 12. Generatore sintetico per lo sweep di scalabilità ✅
Robot come token sul grafo, **stesso schema messaggi**, a volume alzabile (decine di migliaia di msg/s) — Gazebo non arriva a quei ritmi. Solo per stressare Kafka+Spark.
**Produce:** il carico per l'esperimento di scalabilità. → `docs/passi/12-generatore-sintetico.md`

### 13. Valutazione sperimentale — EFFECTIVENESS + EFFICIENCY ✅
`eval/`:
- **Efficiency:** scalabilità (throughput/latency vs carico, punto di rottura), latenza onset->alert.
- **Effectiveness:** detection (precision/recall/F1 vs `injected_faults`, falsi positivi nel tempo); previsione (accuratezza forecast + lead time); execution accuracy del TAG.
**Esteso oltre il piano originale** (su richiesta): i risultati sono visualizzati in un pannello dedicato in fondo alla dashboard, non solo su file. Dal 2026-07-21 il pannello lancia i run **on-demand** ("Esegui ora") e li mostra **davvero in diretta**, sotto-esperimento per sotto-esperimento man mano che finiscono (barre live, niente più PNG pre-generati/refresh a 30s) — vedi `docs/passi/13-valutazione-sperimentale.md`.
**Produce:** i numeri per il report — precision/recall/F1 detection = 1.00/1.00/1.00 (nella verifica originale; un run di verifica dell'estensione "live" ha misurato 0/0/nan, da ri-controllare prima di citarlo in tesina, vedi doc), errore medio previsione 1.6s, TAG accuracy 21/22 (95%), punto di rottura throughput ~40000 msg/s. → `docs/passi/13-valutazione-sperimentale.md`

---

**Se sei in ritardo:** riduci la profondità, non la presenza. Le quattro tecnologie richieste (Kafka, Spark streaming, previsione time-series, LLM) restano almeno in versione minima. Gli esperimenti non si tagliano.
