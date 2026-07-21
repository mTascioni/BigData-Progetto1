# Self Healing Fleet

Progetto Big Data (corso Big Data, Roma Tre, 2026): ingestione continua di telemetria da una flotta di AGV (TurtleBot3 in ROS/Gazebo), detection real-time delle anomalie (salute + deadlock/livelock) con Spark Structured Streaming, previsione offline dei guasti, interrogazione in linguaggio naturale (layer TAG: NL → SQL → esecuzione su Spark SQL → risposta sintetizzata) e dashboard live.

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

Il layer TAG (query synthesis + answer synthesis, Passo 10) chiama un LLM (Qwen2.5-Coder-32B-Instruct) tramite il router **Inference Providers** di Hugging Face — serve un token API valido:

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

## Esperimenti (Passo 13)

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
