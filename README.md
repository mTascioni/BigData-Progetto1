# Self Healing Fleet

Progetto Big Data (corso Big Data, Roma Tre, 2026): ingestione continua di telemetria da una flotta di AGV (TurtleBot3 in ROS/Gazebo), detection real-time delle anomalie (salute + deadlock/livelock) con Spark Structured Streaming, previsione offline dei guasti, interrogazione in linguaggio naturale (text-to-SQL) e dashboard live.

Dettagli completi: [`CLAUDE.md`](CLAUDE.md) (schema dati, stack, invarianti) e [`PLAN.md`](PLAN.md) (i 13 passi del piano, più un'estensione — Passo 14, flotta reale e self healing — con link alla documentazione dettagliata di ciascuno in [`docs/passi/`](docs/passi/)).

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
eval/                    # esperimenti del Passo 13: numeri/grafici per la tesina
docs/passi/               # documentazione dettagliata di ogni passo del piano (scelte, problemi, verifica)
```

## Prerequisiti

- Docker + Docker Compose
- Almeno ~4 core e 8GB di RAM liberi (Gazebo + Spark insieme sono pesanti; con meno risorse tutto funziona ma più lentamente)
- Un token API di Hugging Face (gratuito), solo per il layer TAG (domande in linguaggio naturale) — il resto della pipeline funziona anche senza

## Scaricare il progetto

```bash
git clone <url-del-repository> self-healing-fleet
cd self-healing-fleet
```

## Configurazione (prima del primo avvio)

Il layer TAG (Passo 10) chiama Qwen-Coder tramite il router Inference Providers di Hugging Face. Serve un token:

```bash
cp backend/src/config/HuggingFace_credentials.example.json backend/src/config/HuggingFace_credentials.json
```

e inserire il proprio `hf_api_key` nel file appena creato (è già in `.gitignore`, non finisce nel repository). Senza questo file l'intera pipeline funziona comunque — solo la casella "Chiedi ai dati" della dashboard resterà disattivata.

## Avvio

```bash
docker compose build   # la prima volta: ~5-10 minuti (immagine ROS+Gazebo+TurtleBot3 è la più pesante)
docker compose up -d
```

Un solo comando: Kafka, Spark (master+worker), il servizio di query TAG, il job di detection real-time, la simulazione ROS/Gazebo con 3 robot, il generatore sintetico e il backend partono tutti da soli, senza bisogno di altri comandi manuali (vedi `docs/passi/01-scaffold-infrastruttura.md`, sezione "Avvio a comando singolo"). Il boot completo (Gazebo compreso) richiede circa 1-2 minuti.

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
curl -s http://localhost:3000/api/fleet | head -c 200   # robot live (potrebbe essere vuoto per i primi ~30-60s)
curl -s http://localhost:5000/health                # query_service (layer TAG)
curl -s http://localhost:5001/health                # generator_service (pannello esperimenti)
```

Per una verifica più a fondo (schema dei messaggi, precision/recall della detection, accuratezza delle previsioni, execution accuracy del TAG): [`test/README.md`](test/README.md).

## Esperimenti (Passo 13)

```bash
docker exec shf-ros supervisorctl stop sim_multi_robot   # libera CPU, opzionale ma consigliato
docker exec shf-ros bash -c "cd /opt/shf/eval && python3 run_effectiveness.py"   # precision/recall, previsione, TAG
docker exec shf-ros bash -c "cd /opt/shf/eval && python3 run_efficiency.py"      # throughput, latenza
docker exec shf-ros supervisorctl start sim_multi_robot
```

I risultati (CSV + grafici) compaiono automaticamente nel pannello "Risultati sperimentazioni" in fondo alla dashboard, oltre che in `/data/eval/` sul volume condiviso. Dettagli: [`docs/passi/13-valutazione-sperimentale.md`](docs/passi/13-valutazione-sperimentale.md).

## Fermare tutto

```bash
docker compose down       # ferma e rimuove i container (i dati persistiti su /data restano nel volume shf-data)
docker compose down -v    # come sopra, ma cancella anche i volumi (Kafka, storico Parquet, cache Ivy) — riparte da zero
```
