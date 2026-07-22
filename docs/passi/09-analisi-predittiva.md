# Passo 9 — Analisi predittiva su time series (OFFLINE)

**Obiettivo:** sui canali di salute storici, allenare un modello di previsione (ARIMA/Prophet o LSTM) che preveda il degrado e **quando** una metrica supererà la soglia critica → predire quali robot si guasteranno e con quanto anticipo (remaining useful life). Scrivere in `predictions/`.
**Deliverable atteso:** previsioni di guasto con lead time.

Primo passo puramente "a freddo": nessuno stream, solo storico già persistito (Passo 8) analizzato in batch. È anche la tecnologia "prediction algorithms su time series" richiesta esplicitamente fra le quattro presenze minime del progetto.

**Nota sulla scelta del modello**: il piano indica "ARIMA/Prophet o LSTM". Prima versione di questo passo: implementata con ARIMA (statsmodels). Deciso poi, di comune accordo, di sostituirlo con una **regressione lineare** — la richiesta di "previsione su time series" nel piano non è un vincolo rigido sulla tecnica esatta, e per segnali quasi lineari come le nostre rampe di guasto (Passo 2) una retta ai minimi quadrati è più semplice da spiegare e verificare, e produce risultati praticamente identici (confrontato più sotto: 130.5s di lead time contro i 130.0s che dava ARIMA sullo stesso segnale reale).

## Cosa è stato costruito

**`predictive/forecast_failures.py`** — script batch (pandas, non PySpark: per il volume di dati di questo progetto una `SparkSession` sarebbe overhead puro, stessa scelta già fatta per `offline/adaptive_thresholds.py` al Passo 8). Per ogni coppia `(robot_id, canale)` fra `motor_temp`, `motor_current`, `battery_pct`:

1. Prende la finestra recente (`--lookback-s`, default 300s) di storico da `/data/telemetry` e la ricampiona a bin di 5s (le letture grezze arrivano a ~2Hz, irregolari).
2. **Pre-filtro**: calcola la pendenza (valore finale − iniziale, per minuto) sulla finestra ricampionata. Se è sotto una soglia minima per canale (calibrata sui rate nominali di `config/experiment.json`, Passo 2), si ferma qui — è rumore, non un trend, niente fit e niente falsi allarmi.
3. Solo se c'è un trend vero, allena una **regressione lineare ai minimi quadrati** (`numpy.polyfit`, grado 1) sulla serie ricampionata ed estrapola la retta in avanti (fino a un orizzonte massimo di 30 minuti) per trovare **analiticamente** l'istante in cui incrocia la **soglia critica** del canale — nessun bisogno di generare un forecast punto-per-punto e scandirlo, l'incrocio di una retta si calcola in una riga (`t = (soglia − intercetta) / pendenza`).
4. Le soglie critiche non sono scelte a caso: **coincidono con i valori-obiettivo delle firme di guasto fissate al Passo 2** — `85.0°C` è `plateau_temp_c` di `deriva_termica`, `4.5A` è `peak_a` di `spike_corrente`. È lì che un guasto di quel tipo, lasciato correre, porterebbe la metrica.
5. Se c'è un incrocio (e il trend va nella direzione giusta: es. una temperatura che *scende* non genera mai un allarme "sopra soglia"), scrive una riga di previsione (`robot_id`, `channel`, valore corrente, pendenza, soglia critica, istante di incrocio previsto, **lead time** in secondi, modello) su `/data/predictions/predictions_<timestamp>.parquet`.

## Scelte tecniche e motivazioni

**Regressione lineare invece di ARIMA/Prophet/LSTM.** Prophet richiede `cmdstanpy`/Stan compilato (dipendenza pesante, più fragile da containerizzare in modo riproducibile); un LSTM avrebbe bisogno di molti più dati di quanti questo progetto ne produca in una sessione di test; ARIMA, pur funzionante (vedi sotto), aggiunge un livello di complessità concettuale (ordine `(p,d,q)`, stazionarietà, drift) non necessario per un segnale che è già, di fatto, quasi lineare per costruzione (le firme di guasto del Passo 2 sono rampe lineari verso un plateau). La regressione lineare è la tecnica più semplice possibile per stimare "quando una retta tocca un valore", ed è quello che serve qui.

**Pre-filtro sulla pendenza prima del fit.** Fare il fit su ogni combinazione robot×canale ad ogni run, anche quando il segnale è puro rumore nominale, produrrebbe previsioni spurie quando il rumore casuale imita per caso un breve trend. Il pre-filtro (soglia di pendenza minima, diversa per canale, tarata sui rate nominali reali) taglia alla radice i falsi positivi prima ancora di invocare il modello — indipendente dalla tecnica di fit usata, è rimasto identico nel passaggio da ARIMA a regressione lineare.

## Cosa non ha funzionato al primo tentativo (con ARIMA)

Prima di passare alla regressione lineare, la versione con ARIMA aveva un problema non banale, degno di nota per la tesina anche se il codice attuale non lo usa più: il primo test contro il segnale reale (rampa `deriva_termica` di R3) non produceva **nessuna** previsione nonostante un trend chiaro. Causa: `statsmodels.tsa.arima.model.ARIMA` con differenziazione (`d=1`, necessaria per modellare un trend) ha default `trend='n'` (nessun drift) — senza, il forecast di un `ARIMA(p,1,0)` converge a un livello asintotico costante invece di continuare a estrapolare la rampa. Risolto allora con `trend="t"`; il problema è specifico di ARIMA e non esiste più con la regressione lineare (che per costruzione estrapola sempre la stessa retta, senza convergenze asintotiche indesiderate).

## Verifica

### 1. Logica in isolamento (dati sintetici con trend noto)

Stesso approccio dei passi precedenti: costruita una serie sintetica con una rampa nota (0.15°C/s, gli stessi parametri di `deriva_termica`), una nominale (solo rumore) e una in **raffreddamento** (trend nella direzione sbagliata). 10 controlli, tutti verificati:

- La rampa produce una previsione su `motor_temp` con lead time positivo e plausibile, pendenza stimata vicina a quella iniettata (8.99 vs atteso 9.0°C/min), soglia critica corretta (85.0), modello riportato correttamente come "regressione lineare (OLS)".
- Il lead time calcolato dal codice combacia con un calcolo indipendente fatto a mano nel test.
- Il robot nominale **non** produce alcuna previsione su nessuno dei tre canali (rumore correttamente scartato dal pre-filtro).
- Il robot in raffreddamento **non** genera un falso allarme "sopra soglia" (il trend va nella direzione sbagliata).
- Storia troppo corta → nessuna previsione, nessun crash.

### 2. Segnale reale (rampa `deriva_termica` di R3)

Rilanciata la simulazione di un robot singolo (`R3`) con la finestra di guasto **naturale** di `config/experiment.json` (non accorciata come nei test rapidi dei passi precedenti: `start_time_s=120`, `end_time_s=420`, `ramp_rate_c_per_s=0.15`), lasciata girare fino a catturare un tratto sostanziale della rampa reale, persistita via `persistence_job.py`.

Rieseguita l'analisi predittiva **come se fosse** un istante a metà rampa (`--now-ts`, opzione aggiunta apposta per poter valutare lo storico a un punto nel passato, utile sia per questo test sia in generale per backtesting):

```
robot_id    channel  current_value  slope_per_min  critical_threshold  lead_time_s                      model
      R3 motor_temp         69.48           6.98                85.0        130.5  regressione lineare (OLS)
```

Praticamente identico al risultato ottenuto con ARIMA sullo stesso identico segnale (130.0s) — buona controprova empirica che, per un trend così vicino al lineare, il modello più semplice non perde nulla in accuratezza.

Confrontato con l'andamento reale successivo: il picco effettivamente raggiunto è stato **81.03°C**, non 85°C — la rampa **non ha raggiunto il plateau teorico** perché la finestra del guasto in `config/experiment.json` dura solo 300s (termina a `end_time_s=420`), e a quel ritmo (0.15°C/s × 300s = 45°C sopra il nominale 35°C) il valore atteso a fine finestra è proprio ~80°C — coerente col picco osservato. Questo **non è un difetto del modello predittivo**: un predittore di RUL estrapola necessariamente assumendo che il trend attuale continui, esattamente come farebbe in un caso reale senza intervento — il fatto che *questo* guasto specifico sia "auto-risolto" dopo 300s è una proprietà dell'esperimento controllato (`fault_schedule` con finestre temporali fisse, pensate per essere riproducibili e limitate nel tempo), non della realtà che il modello sta cercando di anticipare. La direzione e l'ordine di grandezza della previsione (pendenza, pochi minuti di lead time) erano corretti; è la premessa "se niente cambia" a non essersi verificata in questo run, proprio perché *qualcosa* (la fine programmata del guasto) è effettivamente cambiato.

### 3. Robustezza: file Parquet corrotti

Durante i test, fermare `persistence_job.py` con `pkill` a metà di un ciclo di scrittura ha lasciato un file `.parquet` da 0 byte in `/data/telemetry` — `pd.read_parquet()` letto direttamente sulla cartella (senza passare dal commit log `_spark_metadata` di Spark, che lo escluderebbe) va in crash su un file del genere. Fix in `load_parquet_dir()`: individua e rimuove i file da 0 byte prima di leggere la cartella (sono comunque vuoti per definizione, cancellarli non perde dati veri).

## Stato

- `predictive/forecast_failures.py` — nuovo; regressione lineare (non più ARIMA).
- `docker-compose.yml` — mount `./predictive:/opt/shf/predictive` su `spark-master`.
- `spark/Dockerfile` — **non serve `statsmodels`**: numpy/pandas/pyarrow, già presenti dal Passo 7, bastano.
- `/data/predictions/` — previsioni reali scritte (Parquet).

Comando per rieseguire:

```bash
docker exec shf-spark-master python3 /opt/shf/predictive/forecast_failures.py \
  --data-dir /data --out /data/predictions --lookback-s 300
# --now-ts <epoch_ms> opzionale, per analizzare lo storico a un istante passato
```

## Prossimo passo

Passo 10 — Layer TAG (text-to-SQL, LLM): endpoint Node che traduce domande in linguaggio naturale in SQL (Qwen-Coder via HuggingFace) eseguito con DuckDB sui Parquet ora disponibili (`telemetry/`, `anomalies/`, `injected_faults/`, `predictions/`) — la quarta e ultima tecnologia richiesta dal progetto.
