# Self Healing Fleet

Progetto Big Data (corso Big Data, Roma Tre, 2026): ingestione continua di telemetria da una flotta di AGV (TurtleBot3 in ROS/Gazebo), detection real-time delle anomalie (salute + deadlock/livelock) con Spark Structured Streaming, previsione offline dei guasti, interrogazione in linguaggio naturale (layer TAG: NL → SQL → esecuzione su Spark SQL → risposta sintetizzata) e dashboard live.

## Regole del progetto

- **Lo schema del messaggio di telemetria è l'unica fonte di verità**: ogni componente (nodo-ponte ROS, generatore sintetico, job Spark, storage, layer TAG) si conforma allo stesso contratto — vedi sotto.
- **La sorgente dati è ROS + Gazebo + TurtleBot3**: il generatore sintetico serve solo per gli esperimenti di scalabilità, non sostituisce la pipeline reale.
- **I canali di salute** (`motor_temp`, `motor_current`, `battery_pct`) non esistono nel TurtleBot3 simulato: sono sintetizzati nel nodo-ponte a partire da parametri nominali configurabili.
- **Ogni guasto iniettato è loggato in `injected_faults`**: è la ground truth su cui si calcolano precision/recall.
- **`run_id`** isola i dati fra run diverse dello stesso robot-token (il generatore sintetico riusa gli stessi id ad ogni run).
- **L'anello di self healing sulla flotta reale** (riparazione preventiva su previsione, freeze su guasto persistente confermato) è cablato solo sui robot ROS reali, non sul generatore sintetico.

Schema del messaggio di telemetria (JSON, un tick per robot, topic Kafka `telemetry`):

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

`task_state` ∈ {`idle`, `moving`, `blocked`, `charging`}.

## Copertura dei requisiti del corso (Topic 7 — IoT Stream Analytics)

| Requisito | Dove è coperto |
|---|---|
| Ingest di stream continui da sensori | Telemetria TurtleBot3 → nodo-ponte ROS → Kafka |
| Real-time analytics | Detection in Spark Structured Streaming |
| Offline analytics su storico → analisi predittiva | Previsione time-series dei guasti (regressione lineare) |
| Kafka / Spark streaming | Ingestion / Structured Streaming |
| Prediction algorithms su time series | Regressione lineare sul trend dei canali di salute |
| LLM (open source) | Layer TAG (text-to-SQL) con Qwen2.5-Coder-32B via router Hugging Face |
| Esperimenti: effectiveness + efficiency | `eval/`, pannello dedicato in dashboard |

## Struttura del progetto

**`ros/`** — tutto ciò che riguarda ROS1 Noetic + Gazebo + TurtleBot3. `catkin_ws/src/shf_bringup/` è il pacchetto ROS vero e proprio: `scripts/` (i nodi Python: `kafka_bridge.py` legge i topic ROS e pubblica su Kafka, `graph_navigator.py` fa muovere ogni robot lungo il grafo, `fleet_control_service.py` espone l'HTTP usato dal backend per avviare/fermare la simulazione e iniettare guasti dal vivo), `launch/` (i file `.launch` che avviano uno o N robot in Gazebo), `worlds/` (la mappa del magazzino in formato Gazebo/SDF), `config/` (parametri di navigazione). `bags/` è dove finiscono eventuali registrazioni ROS bag.

**`streaming/`** — i job Spark Structured Streaming: `detection_job.py` (real-time: salute, deadlock, livelock, previsione) e `persistence_job.py` (batch: scrive tutto su Parquet), più `query_service.py` (il servizio HTTP che espone Spark SQL al backend per il layer TAG), `schemas.py` (gli schemi condivisi), `isolation_forest_model.py`/`train_isolation_forest.py`/`models/` (il modello di anomaly detection).

**`generator/`** — la simulazione sintetica alternativa a ROS/Gazebo: `synthetic_generator.py` (la logica di simulazione a eventi) e `generator_service.py` (il servizio HTTP che la dashboard usa per avviarla/fermarla).

**`predictive/`** — `forecast_failures.py`, l'analisi predittiva offline (regressione lineare sui trend dei canali di salute per stimare quando un robot supererà una soglia).

**`offline/`** — `adaptive_thresholds.py`, il job batch che ricalibra le soglie di salute sullo storico invece di usare valori fissi.

**`eval/`** — la suite di valutazione sperimentale: `run_effectiveness.py`/`run_efficiency.py` (gli esperimenti veri e propri), `eval_service.py` (il servizio HTTP che li lancia on-demand dalla dashboard), `common.py` (helper condivisi), `reference_questions.py` (le domande di riferimento per il layer TAG).

**`test/`** — la suite pytest (23 test) che verifica la correttezza del sistema con scenari costruiti ad hoc, indipendente da `eval/` che invece misura le metriche.

**`backend/`** — il server Node.js/Express: `src/routes/` (gli endpoint HTTP), `src/services/` (la logica: consumo Kafka, layer TAG, guardia SQL, stato flotta in memoria), `src/config/` (le credenziali Hugging Face).

**`dashboard/`** — il frontend: `index.html`, `app.js`, `style.css` — nessun framework, canvas 2D + WebSocket puro.

**`spark/`** — il Dockerfile dell'immagine Spark (master e worker condividono la stessa immagine).

**`config/`** — i file di configurazione condivisi letti da più componenti: `warehouse_graph.json` (il grafo del magazzino), `experiment.json` (robot, missioni, nodi di riserva/riparazione), `presets/` (le topologie alternative per il generatore sintetico).

**Radice** — `docker-compose.yml` (orchestrazione dei 5 container), questo `README.md`.

## Prerequisiti

- Docker + Docker Compose
- Almeno ~6 core e 12GB di RAM liberi per la flotta reale (Gazebo + Kafka + due cluster Spark insieme sono pesanti — misurato: ~2GB per lo Spark master, ~2-4GB per lo worker, ~1GB per Gazebo con 4 robot, escluso il resto dello stack; con meno risorse tutto funziona ma la dashboard può risultare a scatti). Il generatore sintetico da solo (senza la flotta ROS reale) è molto più leggero.
- Un token API di Hugging Face (gratuito), solo per il layer TAG (domande in linguaggio naturale) — il resto della pipeline funziona anche senza

## Scaricare il progetto

```bash
git clone <url-del-repository> self-healing-fleet
cd self-healing-fleet
```

## Configurazione (prima del primo avvio)

Il layer TAG (query synthesis + answer synthesis) chiama un LLM (Qwen2.5-Coder-32B-Instruct) tramite il router **Inference Providers** di Hugging Face — serve un token API valido:

1. Crea un token su https://huggingface.co/settings/tokens (basta un account gratuito; permessi di sola lettura/inference sono sufficienti, non serve un token con permessi di scrittura).
2. Copia il file di esempio e inserisci il **tuo** token al posto del segnaposto:

   ```bash
   cp backend/src/config/HuggingFace_credentials.example.json backend/src/config/HuggingFace_credentials.json
   ```

   ```json
   {
     "hf_api_key": "hf_INCOLLA_QUI_IL_TUO_TOKEN",
     "model": "Qwen/Qwen2.5-Coder-32B-Instruct:fastest"
   }
   ```

`HuggingFace_credentials.json` è già in `.gitignore` (non finisce mai nel repository) — solo `HuggingFace_credentials.example.json` è versionato, come modello da copiare. **Senza questo file l'intera pipeline funziona comunque**: solo la casella "Chiedi ai dati" della dashboard resterà disattivata (`503`), tutto il resto (ingestion, detection, previsione, dashboard live) non dipende da Hugging Face. Se il tuo token non ha accesso al provider dietro `:fastest` per questo modello, prova a togliere il suffisso (`"Qwen/Qwen2.5-Coder-32B-Instruct"`) o a scegliere un altro modello supportato dagli Inference Providers — il backend va riavviato (`docker compose restart backend`) dopo aver modificato il file.

## Avvio

```bash
docker compose build   # la prima volta: ~5-10 minuti (immagine ROS+Gazebo+TurtleBot3 è la più pesante)
docker compose up -d
```

Un solo comando: Kafka, Spark (master+worker), il servizio di query TAG, il job di detection real-time, il generatore sintetico e il backend partono tutti da soli, senza bisogno di altri comandi manuali. Il boot completo richiede circa 1-2 minuti.

**La simulazione ROS/Gazebo (la flotta reale) non parte da sola** — è una scelta deliberata per lasciarti il controllo su quando avviarla e con quanti robot: apri la dashboard (http://localhost:3000), card "Flotta reale — controllo", scegli la scala (`small` = 4 robot R1-R4, `large` = 8 robot R1-R8) e premi "Avvia simulazione" — richiede ~15-20s per essere operativa. In alternativa da riga di comando: `docker exec shf-ros supervisorctl start sim_multi_robot` (scala `small` di default).

## Connettersi

| Cosa | URL |
|---|---|
| **Dashboard** (vista live flotta + query NL + pannello esperimenti) | http://localhost:3000 |
| Gazebo via noVNC (debug visivo della simulazione) | http://localhost:6080/vnc.html |
| Spark master UI | http://localhost:8080 |
| Spark worker UI | http://localhost:8081 |
| Kafka (client esterno, es. per ispezionare i topic) | `localhost:9094` |

## Verificare che sia tutto su

```bash
docker compose ps                                  # tutti i container "Up"
curl -s http://localhost:3000/api/fleet | head -c 200   # robot live (vuoto finche' non avvii la simulazione ROS dalla dashboard, o il generatore sintetico)
curl -s http://localhost:5000/health                # query_service (layer TAG)
curl -s http://localhost:5001/health                # generator_service (pannello esperimenti)
curl -s http://localhost:5002/health                # fleet_control_service (guasti live, avvio/arresto simulazione)
```

## Esperimenti

Dalla dashboard (in fondo alla pagina, card "Risultati sperimentazioni"): bottone "Esegui ora" per effectiveness (precision/recall detection, errore previsione, accuratezza TAG) o efficiency (throughput, latenza) — i risultati compaiono man mano che ogni sotto-esperimento finisce, nessun comando manuale necessario. La simulazione ROS reale (se l'hai avviata) non va fermata: gli esperimenti usano il generatore sintetico, non la flotta reale.

In alternativa, da riga di comando (produce solo CSV, non aggiorna la vista live se non tramite il refresh periodico):

```bash
docker exec shf-ros bash -c "cd /opt/shf/eval && python3 run_effectiveness.py"   # precision/recall, previsione, TAG
docker exec shf-ros bash -c "cd /opt/shf/eval && python3 run_efficiency.py"      # throughput, latenza
```

I risultati (CSV) sono in `/data/eval/` sul volume condiviso.

## Fermare tutto

```bash
docker compose down       # ferma e rimuove i container (i dati persistiti su /data restano nel volume shf-data)
docker compose down -v    # come sopra, ma cancella anche i volumi (Kafka, storico Parquet, cache Ivy) — riparte da zero
```
