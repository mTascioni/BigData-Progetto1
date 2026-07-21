# Passo 10 — Layer TAG (text-to-SQL, LLM)

**Obiettivo (da PLAN.md):** endpoint Node: prompt (schema intero + few-shot) → Qwen su HuggingFace → SQL → esecuzione sui Parquet → righe. Guardia: solo `SELECT` + retry sull'errore. ~20-30 domande di riferimento per la valutazione.
**Deliverable atteso:** "chiedi a parole → risposta dai dati".

Quarta e ultima delle tecnologie richieste dal corso (Kafka, Spark, previsione time-series, LLM — tutte e quattro presenti a questo punto). Rispetto al piano originale in `CLAUDE.md`, due decisioni prese insieme all'utente prima di implementare, discusse in conversazione:

1. **Niente DuckDB.** Il piano originale prevedeva `duckdb` (libreria Node embeddable) per eseguire l'SQL generato direttamente nel backend. Deciso di sostituirlo con **Spark SQL**, riusando il cluster già in piedi dai Passi 7-9 invece di aggiungere un nuovo motore. Il backend Node non esegue più l'SQL da solo: chiama un piccolo servizio Spark dedicato (`query_service.py`) via HTTP interno.
2. **Qwen-Coder via il router di Hugging Face (Inference Providers), non self-hosted.** Il piano non specificava se ospitare il modello in locale o chiamarlo via API. Scelto di riusare esattamente il pattern già rodato in un altro progetto dell'utente (endpoint `https://router.huggingface.co/v1/chat/completions`, formato OpenAI-compatibile, Bearer token) — evita di dover gestire GPU/quantizzazione/`nvidia-container-toolkit` nel container, molto più leggero e riproducibile. Modello usato: `Qwen/Qwen2.5-Coder-32B-Instruct:fastest` (verificato disponibile tramite gli Inference Providers, provider Nscale al momento).

## Architettura

Con queste due scelte, la pipeline del progetto acquista la forma di una **architettura lambda**: gli stessi dati grezzi (`telemetry` su Kafka) alimentano sia un percorso *real-time* a bassa latenza (Passo 7, per la dashboard) sia un percorso *batch* più lento ma completo (Passi 8-10, per analisi storiche e query ad-hoc) — con un layer di servizio che li espone entrambi.

```mermaid
flowchart TB
    ROS["ROS + Gazebo + TurtleBot3<br/>(Passi 3-6)"] --> Bridge["kafka_bridge.py"]
    Bridge -->|"telemetry<br/>injected_faults"| Kafka[("Kafka")]

    subgraph Speed["SPEED LAYER — real-time (Passo 7)"]
        Kafka --> Detection["detection_job.py<br/>Spark Structured Streaming"]
        Detection --> FleetState[("fleet_state")]
        Detection --> AnomaliesTopic[("anomalies")]
    end

    subgraph BatchL["BATCH LAYER — offline (Passi 8-9)"]
        Kafka --> Persistence["persistence_job.py<br/>Spark Structured Streaming"]
        AnomaliesTopic --> Persistence
        Persistence --> Parquet[("Parquet: telemetry<br/>anomalies, injected_faults")]
        Parquet --> Adaptive["adaptive_thresholds.py"]
        Adaptive -.feedback soglie.-> Detection
        Parquet --> Predict["forecast_failures.py"]
        Predict --> Predictions[("predictions")]
    end

    subgraph Serving["SERVING LAYER (Passo 10-11)"]
        Parquet --> QueryService["query_service.py<br/>Spark SQL"]
        Predictions --> QueryService
        QueryService <--> Backend["backend Node<br/>endpoint /api/tag"]
        LLM["Qwen2.5-Coder-32B<br/>via router HuggingFace"] <--> Backend
        FleetState -.Passo 11.-> Dashboard["dashboard"]
    end

    Utente(["utente: domanda<br/>in linguaggio naturale"]) --> Backend --> Utente
```

Il layer TAG (questo passo) vive interamente nel **serving layer**: interroga solo lo storico Parquet della batch layer, mai lo stream diretto — coerente con l'idea che "a parole" si interroga cosa è già successo, non l'istante presente (quello lo fa già la dashboard via `fleet_state`, Passo 11).

## Cosa è stato costruito

**`streaming/query_service.py`** — processo Spark persistente (non `spark-submit` per query: costerebbe ~10s di avvio JVM ad ogni domanda, inaccettabile per un endpoint interattivo). Un server HTTP minimale (`http.server`, niente Flask: evita una dipendenza in più per due sole route) espone:
- `GET /health`
- `POST /query {"sql": "..."}` → ricrea le temp view sulle 4 cartelle Parquet (economico: è solo discovery dello schema finché non scatta un'azione, così vede sempre i dati più recenti scritti da `persistence_job.py`), valida che sia una sola `SELECT`/`WITH` senza parole chiave di scrittura (`INSERT`/`DROP`/... — stessa guardia del backend Node, difesa in profondità), esegue con `spark.sql(...).limit(500)` e ritorna righe JSON.

**`backend/src/services/LlmService.js`** — client per il router HuggingFace, adattato da un pattern già in uso in un altro progetto dell'utente (stessa struttura, portato a ES modules).

**`backend/src/services/promptBuilder.js`** — costruisce i messaggi per il LLM: schema completo delle 4 tabelle (inclusi i dettagli di dialetto Spark SQL rilevanti — accesso a STRUCT con la dot notation, `array_contains` per gli ARRAY, `timestamp_millis` per i timestamp epoch), 5 esempi few-shot, regole esplicite (solo `SELECT`, nessuna spiegazione, `LIMIT` ragionevole se non specificato).

**`backend/src/services/sqlGuard.js`** — `extractSql` (ripulisce eventuali blocchi markdown che il modello potrebbe comunque aggiungere) e `validateSelectOnly` (stessa logica lato Node, prima ancora di contattare `query_service`).

**`backend/src/services/tagService.js`** — orchestra il ciclo: prompt → LLM → guardia → esecuzione; se la guardia rifiuta la query o l'esecuzione fallisce, **un retry** (PLAN.md: "guardia + retry sull'errore"), ri-prompting il modello con l'errore ricevuto, poi restituisce il risultato o l'errore finale.

**`backend/src/routes/tag.js` + `server.js`** — `POST /api/tag { "question": "..." }`.

**Credenziali**: `backend/src/config/HuggingFace_credentials.json` (gitignored, con un file `.example.json` committato come template) — per ora usa **lo stesso token** del progetto di riferimento dell'utente (deciso esplicitamente: "intanto usiamo lo stesso token, poi dopo ci penso io a modificare"), da sostituire con un token dedicato quando l'utente vorrà.

## Problema incontrato: worker Spark disconnesso dal master

Durante il wiring ho dovuto ricreare il container `spark-master` (per applicare la nuova porta `5000:5000` nel `docker-compose.yml`). `spark-worker`, già in esecuzione e connesso al master precedente, non si è riconnesso automaticamente al nuovo container (stesso nome DNS `spark-master`, ma identità di rete diversa dopo la ricreazione) — è rimasto con una connessione Netty verso un host ormai inesistente. Sintomo: `query_service.py` restava bloccato con `WARN TaskSchedulerImpl: Initial job has not accepted any resources`, perché il master lo vedeva in stato `WAITING` con **zero worker vivi** (`aliveworkers: 0` nell'API JSON di `http://localhost:8080/json/`). Fix: ricreare anche `spark-worker` per fargli ri-risolvere `spark-master` da capo. Da tenere presente in generale: in un cluster Spark standalone su Compose, ricreare il master richiede quasi sempre di ricreare anche i worker.

## Verifica

### 1. Guardie SQL in isolamento

8 controlli su `extractSql`/`validateSelectOnly` (Node, via `node -e`): `SELECT` accettata, blocco markdown ripulito correttamente, `DROP`/`INSERT` rifiutate, query con `;` in mezzo rifiutata (previene SQL impilate), `WITH ... SELECT` accettata, query vuota rifiutata, e — controllo mirato — `SELECT created_at FROM telemetry` **accettata** (il controllo sulle parole chiave vietate usa confini di parola, altrimenti "created_at" farebbe scattare per errore il blocco su `CREATE`). Tutti verificati.

### 2. Pipeline reale end-to-end

Con `query_service.py` attivo e connesso, 5 domande reali in italiano poste a `POST /api/tag`, **tutte risolte al primo tentativo** (nessun retry necessario):

| Domanda | SQL generato (riassunto) | Esito |
|---|---|---|
| "Quanti messaggi di telemetria ci sono per ciascun robot?" | `GROUP BY robot_id` su `telemetry` | R1: 16767, R3: 3981, R2: 1027, + i robot di test |
| "Quali guasti di tipo deriva_termica... e su quale robot?" | filtro su `injected_faults.fault_type` | F1 su R1 e su R3 (due run distinte, Passi 6 e 9) |
| "Previsioni con il lead time più basso" | `ORDER BY lead_time_s ASC` su `predictions` | le due previsioni reali del Passo 9 (ARIMA e regressione lineare, stesso segnale) |
| "Anomalie salute per motor_current, per robot" | `array_contains(threshold_reasons, 'motor_current')` | vedi sotto |
| "Per ogni robot con guasti iniettati, conteggio guasti e temperatura media" | CTE + `JOIN` fra `injected_faults` e `telemetry` | R1: 5 guasti, 35.0°C medi; R3: 2 guasti, 38.4°C medi |

La quarta domanda ha fatto emergere un dettaglio interessante dello storico reale: `robot_id` risultava `null` per le anomalie `motor_current` più vecchie. Non un bug nuovo — verificato ordinando per `ts` che le righe con `robot_id` presente sono tutte **successive** al fix del Passo 7 (il bug per cui `robot_id` restava solo nella key Kafka, non nel payload), mentre le più vecchie risalgono a *prima* di quel fix. Il layer TAG ha semplicemente esposto fedelmente un artefatto storico reale già noto — buona controprova che funziona onestamente sui dati così come sono, senza nasconderne le imperfezioni.

La quinta domanda (CTE con due sotto-query e una `JOIN`) dimostra che il modello generalizza oltre gli esempi few-shot (nessuno dei 5 esempi nel prompt usa `JOIN` o più di una CTE).

## Stato

- `streaming/query_service.py` — nuovo, in esecuzione persistente (lasciato attivo: il layer TAG ne dipende, come `backend`/`kafka` resta un servizio sempre su, non un job lanciato e fermato come i job Passo 7-9).
- `backend/src/services/{LlmService,promptBuilder,sqlGuard,tagService}.js` — nuovi.
- `backend/src/routes/tag.js`, `backend/src/server.js` — nuovo endpoint `/api/tag`.
- `backend/src/config/HuggingFace_credentials.{json,example.json}` — la prima gitignored.
- `docker-compose.yml` — porta `5000` esposta su `spark-master`, `QUERY_SERVICE_URL` sul `backend`.
- `.gitignore` — aggiunta la nuova entry per le credenziali.

Comando per rilanciare `query_service.py` se il container viene ricreato:

```bash
docker exec -d shf-spark-master bash -c "
  /opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 \
    /opt/shf/streaming/query_service.py > /tmp/query_service.log 2>&1"
```

## Prossimo passo

Passo 11 — Dashboard: frontend HTML + canvas 2D servito dal backend Node, che consuma `fleet_state` da Kafka via websocket per la vista live della flotta, con un pannello robot-a-rischio (da `predictions`) e la casella di query in linguaggio naturale verso l'endpoint `/api/tag` appena costruito.
