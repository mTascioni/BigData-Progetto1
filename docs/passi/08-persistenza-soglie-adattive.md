# Passo 8 — Persistenza + soglie adattive

**Obiettivo (da PLAN.md):** persistere `telemetry`, `anomalies`, `injected_faults` su Parquet/Delta. Job che tara soglie adattive per ridurre i falsi positivi, con feedback verso lo streaming.
**Deliverable atteso:** storico + adattamento.

## Cosa è stato costruito

**`streaming/persistence_job.py`** — job PySpark Structured Streaming (tre query indipendenti, stesso pattern del Passo 7): legge `telemetry`, `anomalies`, `injected_faults` da Kafka con `startingOffsets=earliest` (a differenza di `detection_job.py`, che legge solo `latest`: qui lo scopo è costruire l'archivio storico completo, non reagire in tempo reale) e li scrive su Parquet in `/data/telemetry`, `/data/anomalies`, `/data/injected_faults` — un volume Docker condiviso fra `spark-master`/`spark-worker` (non dentro al repo: sono dati generati a runtime, non sorgenti). `anomalies` è partizionato per `type` (salute/livelock/deadlock), dato che le query successive (Passo 10 TAG, Passo 13 eval) filtrano quasi sempre per tipo.

**`streaming/schemas.py`** (nuovo) — gli schemi Spark (`TELEMETRY_SCHEMA`, `ANOMALIES_SCHEMA`, `INJECTED_FAULTS_SCHEMA`) centralizzati qui, riusati sia da `detection_job.py` (che prima li duplicava) sia da `persistence_job.py`. `ANOMALIES_SCHEMA` è un superset nullable dei campi dei tre tipi di anomalia — `from_json` lascia a `null` i campi non pertinenti a un dato `type`, senza bisogno di tre schemi separati.

**`offline/adaptive_thresholds.py`** (nuovo, nella cartella che `CLAUDE.md` riserva esplicitamente alle "soglie adattive") — script batch (pandas, non Spark: per il volume di dati di questo progetto una `SparkSession` sarebbe overhead inutile) che:

1. Legge lo storico Parquet (`telemetry`, `anomalies` di tipo `salute`, `injected_faults`).
2. Usa `injected_faults` come **ground truth**: un'anomalia di salute è un vero positivo se il suo `ts` cade dentro la finestra `[start_ts, end_ts]` di un guasto reale per quel `robot_id`; altrimenti è un falso positivo.
3. Per ciascun canale (`motor_temp`, `motor_current`, `battery_pct`) con almeno `--min-false-positives` (default 3) falsi positivi, ricalcola la soglia come il 99.5° percentile (0.5° per la batteria, che è una soglia "sotto" non "sopra") dei valori osservati nei periodi **nominali** (fuori da ogni finestra di guasto) — così il rumore nominale reale, non un numero scelto a mano, decide quanto allargare la soglia. Se il percentile nominale non richiede di allargarla (i falsi positivi hanno un'altra causa), la soglia resta invariata: la calibrazione non peggiora mai le cose alla cieca.
4. Scrive `/data/adaptive_thresholds.json`.

**Feedback verso lo streaming**: `detection_job.py` (Passo 7), all'avvio, prova a leggere `/data/adaptive_thresholds.json` (`load_thresholds()`); se il file esiste usa quei valori, altrimenti i default hard-coded. Non è un hot-reload a caldo su una query già in esecuzione (fuori scopo — richiederebbe riavviare gli operatori streaming a metà run) ma un feedback **fra una run e la successiva**: si lancia `persistence_job.py` per un po', poi `adaptive_thresholds.py` per calibrare, poi si rilancia `detection_job.py` che raccoglie le soglie aggiornate. Loggato esplicitamente all'avvio (`Soglie di salute in uso (adattive|default): {...}`) per essere ispezionabile.

## Scelte tecniche e motivazioni

**Percentile su dati nominali, non su tutte le anomalie.** Alzare la soglia guardando dove cadono le anomalie sarebbe circolare (si ricalibra sul proprio stesso rumore di misura, non su cosa è realmente normale). Guardare invece la distribuzione dei valori nei periodi *senza* guasto attivo (noti grazie a `injected_faults`) dà una stima onesta di "quanto rumore nominale c'è davvero", che è esattamente il segnale serve per smettere di scambiarlo per un'anomalia.

**Soglie globali di flotta, non per singolo robot.** `health_channels_nominal` (Passo 2) è già definito a livello di flotta, non per robot — tutti i TurtleBot3 hanno lo stesso profilo nominale sintetico. Calibrare per-robot con questo volume di dati avrebbe rischiato overfitting su pochi campioni a testa; una soglia globale è più robusta ed è coerente con come il resto del progetto tratta i canali di salute.

**pandas invece di Spark per `adaptive_thresholds.py`.** Lo storico accumulato in questo progetto è dell'ordine di decine di migliaia di righe (18835 telemetry, 90 anomalie nel test qui sotto) — abbondantemente nella comfort zone di pandas su una singola macchina. Usare Spark per questo aggiungerebbe la latenza di avvio di una `SparkSession` (~10s) per un conto che pandas fa in meno di un secondo. Se lo storico crescesse di ordini di grandezza (es. col generatore sintetico del Passo 12) si potrebbe riscrivere in PySpark batch riusando `schemas.py` — non necessario ora.

## Problemi incontrati e fix

1. **`params` di `injected_faults` è un oggetto JSON annidato, non una stringa.** Nel primo schema avevo messo `params` come `StringType`, ma `kafka_bridge.py` scrive `params` come oggetto JSON vero (`json.dumps` dell'intero record, `params` incluso come dict). Con lo schema sbagliato `from_json` avrebbe restituito `null` per l'intero campo. Fix: `FAULT_PARAMS_SCHEMA`, uno `StructType` nullable con tutti i campi dei 6 `fault_type` di `fault_signature_schema` (Passo 2) — solo quelli pertinenti al tipo specifico sono valorizzati, gli altri restano `null`.
2. **Permessi sul volume Docker `/data`** — stesso problema già visto per `spark-ivy-cache` al Passo 7: un volume nominato appena creato è `root:root`, non scrivibile dall'utente 1001 del container anche se condivide il gruppo 0 (bit di scrittura di gruppo assente). Fix: `chmod 777 /data` una tantum dopo la prima creazione del volume (documentato qui per chi rifà il setup da zero).
3. **Bug reale: colonna di partizione persa leggendo i singoli file Parquet.** `adaptive_thresholds.py` inizialmente leggeva ogni file `.parquet` di `anomalies` singolarmente con `pd.read_parquet(file)` e li concatenava — ma Spark, quando partiziona per `type`, **non** scrive quella colonna dentro ai file (vive solo nel path, `type=salute/...`). Il risultato: `KeyError: 'type'` al primo utilizzo. Fix: leggere la **directory** con `pd.read_parquet(dir)`, che usa il dataset API di pyarrow e ricostruisce le colonne di partizione Hive-style dal path.

## Verifica

### 1. Logica di calibrazione in isolamento

Stesso approccio dei Passi 6-7: uno script di test (non nel repo) importa `calibrate()` da `adaptive_thresholds.py` e la esercita con `DataFrame` pandas costruiti a mano. 8 controlli, tutti verificati:

- **Falsi positivi veri** (robot senza alcun guasto iniettato, rumore nominale di `motor_current` sistematicamente sopra la soglia di default 2.5): la soglia si alza sopra 2.5 e resta sotto il massimo osservato — coerente.
- Il canale **senza** falsi positivi (`motor_temp`) resta invariato.
- **Sotto la soglia minima** di falsi positivi (2 < 3): nessuna modifica.
- **Anomalie dentro una finestra di guasto vero**: correttamente classificate come veri positivi, nessuna modifica alla soglia.
- **Nessuna telemetria storica**: nessun crash, restano i default.

### 2. Pipeline reale end-to-end

`persistence_job.py` lanciato contro i topic Kafka reali, popolati da tutte le run dei Passi 3-7 (`startingOffsets=earliest`, quindi l'intero backlog):

```
telemetry:        18835 righe
anomalies:            90 righe (partizionate: salute/livelock/deadlock)
injected_faults:       5 righe
```

Schema verificato leggendo il Parquet: tutti i campi attesi presenti, `params` di `injected_faults` correttamente destrutturato per `fault_type` (es. `deriva_termica` ha `ramp_rate_c_per_s`/`plateau_temp_c`/`ramp_duration_s` valorizzati e gli altri campi `null`).

`adaptive_thresholds.py` lanciato su questo storico reale: **nessuna soglia modificata** — il report mostra che il 99.5° percentile del `motor_current` nominale osservato è 1.76A, ben sotto la soglia attuale di 2.5A, quindi allargarla non avrebbe senso (i falsi positivi trovati vengono da altro, non da rumore nominale sottostimato). Risultato corretto e atteso: la maggior parte dei dati storici qui sono run di test pulite, non ci si aspettava una ricalibrazione — la verifica del punto 1 sopra prova che *se* servisse, la funzionerebbe.

**Feedback verso lo streaming verificato**: rilanciato `detection_job.py` dopo aver scritto `/data/adaptive_thresholds.json` — log di avvio:

```
Soglie di salute in uso (adattive (/data/adaptive_thresholds.json)): {'motor_temp_threshold_c': 55.0, 'motor_current_threshold_a': 2.5, 'battery_low_threshold_pct': 20.0}
```

Confermato: il job legge dal file (non dai default hard-coded — la label "adattive" lo dice esplicitamente) anche se i valori in questo caso coincidono con i default (nessuna ricalibrazione necessaria).

## Stato

- `streaming/schemas.py` — nuovo, schemi condivisi.
- `streaming/persistence_job.py` — nuovo.
- `streaming/detection_job.py` — riusa `TELEMETRY_SCHEMA` da `schemas.py`; aggiunta `load_thresholds()` e lettura delle soglie adattive.
- `offline/adaptive_thresholds.py` — nuovo.
- `docker-compose.yml` — nuovo volume `shf-data:/data` su `spark-master`/`spark-worker`; mount di `./offline`.
- `/data/telemetry`, `/data/anomalies`, `/data/injected_faults` — storico reale persistito (volume Docker, non nel repo).

Comandi per rieseguire il ciclo:

```bash
# persistenza (lasciar girare finché serve, poi fermare)
docker exec -d shf-spark-master bash -c "
  /opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
    --conf spark.jars.ivy=/opt/bitnami/spark/ivy-cache \
    /opt/shf/streaming/persistence_job.py > /tmp/persistence_job.log 2>&1"

# calibrazione (batch, una tantum)
docker exec shf-spark-master python3 /opt/shf/offline/adaptive_thresholds.py

# detection con le soglie aggiornate
docker exec -d shf-spark-master bash -c "
  /opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
    --conf spark.jars.ivy=/opt/bitnami/spark/ivy-cache \
    /opt/shf/streaming/detection_job.py > /tmp/detection_job.log 2>&1"
```

## Prossimo passo

Passo 9 — Analisi predittiva su time series (OFFLINE): sui canali di salute storici ora persistiti, allenare un modello di previsione (ARIMA/Prophet o LSTM) che stimi quando una metrica supererà la soglia critica, scrivendo in `predictions/`.
