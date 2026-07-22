# Self Healing Fleet

Progetto Big Data (corso Big Data, Roma Tre, 2026): ingestione continua di telemetria da una flotta di AGV (TurtleBot3 in ROS/Gazebo), detection real-time delle anomalie (salute + deadlock/livelock) con Spark Structured Streaming, previsione offline dei guasti, interrogazione in linguaggio naturale (layer TAG: NL → SQL → esecuzione su Spark SQL → risposta sintetizzata) e dashboard live.

Documentazione tecnica completa nella relazione ([`docs/relazione/relazione.pdf`](docs/relazione/relazione.pdf)); lo sviluppo passo per passo, con le scelte e i problemi incontrati, è in [`docs/passi/`](docs/passi/).

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

```
docker-compose.yml     # orchestrazione di tutti i servizi
config/                 # grafo del magazzino, esperimento (fleet, guasti), preset per il generatore
ros/                    # ROS Noetic + Gazebo + TurtleBot3: sorgente dati, nodo-ponte verso Kafka
generator/               # generatore sintetico (scale test + guasti) e il suo servizio di controllo HTTP
streaming/               # job PySpark: detection real-time, persistenza su Parquet, query_service (layer TAG)
offline/                 # soglie adattive (riduzione falsi positivi)
predictive/              # previsione offline dei guasti (regressione lineare)
backend/                 # Node/Express: serve la dashboard, endpoint TAG, websocket
dashboard/               # frontend (HTML + canvas 2D + JS), nessun framework
test/                    # suite pytest: verifica di correttezza (schema, ground truth, precision/recall, ...)
eval/                    # esperimenti di valutazione sperimentale: numeri/grafici per la relazione
docs/passi/               # documentazione dettagliata dello sviluppo (scelte, problemi, verifica)
```

## Prerequisiti

- Docker + Docker Compose
- Almeno ~6 core e 12GB di RAM liberi per la flotta reale (Gazebo + Kafka + due cluster Spark insieme sono pesanti — misurato: ~2GB per lo Spark master, ~2-4GB per lo worker, ~1GB per Gazebo con 4 robot, escluso il resto dello stack; con meno risorse tutto funziona ma la dashboard può risultare a scatti, vedi `docs/passi/11-dashboard.md`). Il generatore sintetico da solo (senza la flotta ROS reale) è molto più leggero.
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

Un solo comando: Kafka, Spark (master+worker), il servizio di query TAG, il job di detection real-time, il generatore sintetico e il backend partono tutti da soli, senza bisogno di altri comandi manuali (vedi `docs/passi/01-scaffold-infrastruttura.md`, sezione "Avvio a comando singolo"). Il boot completo richiede circa 1-2 minuti.

**La simulazione ROS/Gazebo (la flotta reale) non parte da sola** — è una scelta deliberata per lasciarti il controllo su quando avviarla e con quanti robot (vedi `docs/passi/01-scaffold-infrastruttura.md`, sezione "La simulazione ROS non parte più da sola"): apri la dashboard (http://localhost:3000), card "Flotta reale — controllo", scegli la scala (`small` = 4 robot R1-R4, `large` = 8 robot R1-R8) e premi "Avvia simulazione" — richiede ~15-20s per essere operativa. In alternativa da riga di comando: `docker exec shf-ros supervisorctl start sim_multi_robot` (scala `small` di default).

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

Per una verifica più a fondo (schema dei messaggi, precision/recall della detection, accuratezza delle previsioni, execution accuracy del TAG): [`test/README.md`](test/README.md).

## Esperimenti

Dalla dashboard (in fondo alla pagina, card "Risultati sperimentazioni"): bottone "Esegui ora" per effectiveness (precision/recall detection, errore previsione, accuratezza TAG) o efficiency (throughput, latenza) — i risultati compaiono man mano che ogni sotto-esperimento finisce, nessun comando manuale necessario. La simulazione ROS reale (se l'hai avviata) non va fermata: gli esperimenti usano il generatore sintetico, non la flotta reale.

In alternativa, da riga di comando (produce solo CSV, non aggiorna la vista live se non tramite il refresh periodico):

```bash
docker exec shf-ros bash -c "cd /opt/shf/eval && python3 run_effectiveness.py"   # precision/recall, previsione, TAG
docker exec shf-ros bash -c "cd /opt/shf/eval && python3 run_efficiency.py"      # throughput, latenza
```

I risultati (CSV) sono in `/data/eval/` sul volume condiviso. Dettagli: [`docs/passi/13-valutazione-sperimentale.md`](docs/passi/13-valutazione-sperimentale.md).

## Fermare tutto

```bash
docker compose down       # ferma e rimuove i container (i dati persistiti su /data restano nel volume shf-data)
docker compose down -v    # come sopra, ma cancella anche i volumi (Kafka, storico Parquet, cache Ivy) — riparte da zero
```
