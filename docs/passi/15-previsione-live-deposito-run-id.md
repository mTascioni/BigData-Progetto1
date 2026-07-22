# Passo 15 — Previsione live, reazione differenziata, isolamento run, deposito (estensione oltre il piano originale)

**Non è un passo del piano originale.** Nasce da cinque osservazioni concrete dell'utente dopo aver usato il sistema (flotta reale + generatore sintetico insieme), verificate una per una nel codice prima di intervenire — non ipotesi.

## Perché

1. Il generatore sintetico produceva moltissimi falsi positivi di livelock (fino a 50 in 120s) mentre la flotta reale quasi mai.
2. La previsione dei guasti (Passo 9) è uno script batch offline, non un segnale in streaming — l'utente voleva un preavviso live (raffiche saltuarie, non un guasto pieno) che attivasse una riparazione preventiva, con una reazione diversa per un guasto persistente reale (il robot si ferma, l'operatore lo rimuove).
3. Per iniettare un guasto sul generatore serviva scrivere a mano il codice del robot.
4. Non c'era modo di isolare i dati di una simulazione dalla successiva — `robot_id` viene riusato ad ogni run del generatore sintetico.
5. `repair_node`/`reserve_node` coincidevano con lo `start_node` di robot reali (R1, R4): un robot riparato o rimesso in servizio collideva fisicamente con un robot ancora in servizio.

## Decisioni di scope

- **Isolamento dati: `run_id`, non cancellazione fisica** (scelta con l'utente): più coerente con lo spirito "Big Data" (i dati grezzi non si cancellano mai), implementato end-to-end (schema, generatore, flotta reale, `persistence_job.py`, `forecast_failures.py`, query delle previsioni) — non solo un tag sui messaggi ignorato a valle.
- **Fix del livelock, non sostituzione**: il rilevatore esistente (distanza-dal-goal, `applyInPandasWithState`) resta — corretto il bug specifico (confronto fra distanze verso goal diversi), non buttato via per uno più semplice basato su posizione statica. Preserva i numeri già documentati al Passo 13.
- **Reazione differenziata**: una previsione (preavviso, non ancora un guasto) attiva la stessa riparazione preventiva + dispaccio riserva di prima; un guasto persistente confermato (soglia dura) ferma il robot invece di ripararlo in automatico — è già troppo tardi per una manovra preventiva, serve l'operatore.
- **Decommissioning v1**: il robot sparisce da dashboard/pool riserve, non vengono terminati i processi ROS del suo namespace (nessun controllo per-robot esiste in `fleet_control_service.py`, sarebbe un lavoro a parte). Solo per la flotta reale — la rimozione di un singolo robot sintetico non è coperta (fermare l'intero run resta la via per "ripulire" la mappa sintetica).
- **Nodi di deposito**: inverte la decisione "nessun cambio di topologia" del Passo 14 — registrata esplicitamente, non lasciata in silenzio, perché il progetto tiene traccia di queste scelte.

## Cosa è stato costruito

### 1. Fix falsi positivi livelock (`streaming/detection_job.py`)

`_livelock_state_func` ora tiene in stato anche il `goal_node` del campionamento di riferimento (`ref_goal_node`, nuovo campo in `LIVELOCK_STATE_SCHEMA`). Se il `goal_node` della riga corrente differisce da quello in stato, la riga è trattata come un nuovo riferimento (nessun calcolo di progresso quel giro) invece di confrontare la distanza verso un obiettivo con quella verso un altro. Causa esatta confermata: il generatore sintetico assegna un nuovo goal a caso ogni volta che un robot arriva a destinazione, senza mai fermarsi; con `speed_mps` alto (parametro libero, sganciato dalla fisica) un arco si percorre in meno del tempo di un campionamento (`LIVELOCK_CHECK_INTERVAL_S=10s`), quindi quasi ogni campionamento cadeva a cavallo di un cambio goal.

**Verifica**: generatore a `speed_mps=1.0` per 130s (lo scenario che prima produceva lo sciame di falsi positivi) → **0 eventi livelock**. Suite pytest (`test_livelock_vero_positivo_...`, `test_nessun_falso_positivo_livelock_...`, `test_deadlock_vero_positivo_...`) tutti passati dopo il fix.

### 2. Nodi di deposito (`config/warehouse_graph.json`, `config/experiment.json`)

Tre nodi nuovi fuori dal rettangolo dei corridoi principali (x=-10, contro 0-30 del resto del grafo), ciascuno con un raccordo dedicato da A: `DEPOSITO` (repair_node), `RISERVA1` (reserve_node, anche start_node di R4), `RISERVA2` (start_node di R8, scale=large). `pickAvailableReserve()` (`backend/src/services/fleetStateStore.js`) corretto per escludere un candidato con `health_anomaly` attivo o già in riparazione — bug trovato leggendo il codice (mai controllava la salute del candidato), non ancora osservato dal vivo ma confermato possibile.

**Verifica**: flotta reale (`scale=small`) → R4 nasce a (-10, 8) = RISERVA1, isolato dal traffico. Guasto iniettato su R1 → R1 va verso DEPOSITO, R4 dispacciato con la missione di R1 (`goal_node` osservato: "B", il primo nodo della sequenza di R1) senza convergere sulla posizione di nessun altro robot. `return-to-service` verificato puntare a "RISERVA1", non più "J".

### 3. `run_id` end-to-end

Nuovo campo (nullable) in `TELEMETRY_SCHEMA`/`ANOMALIES_SCHEMA`/`INJECTED_FAULTS_SCHEMA`. Generato una volta per run: `generator_service.py` (`/start`) per il generatore sintetico, `fleet_control_service.py` (`/sim/start`, marker file `/tmp/shf_run_id`, stesso meccanismo già usato per la scala) per la flotta reale — letto da `sim_multi_robot.launch` come nuovo `<arg name="run_id">` passato a ogni `kafka_bridge.py` via rosparam. `predictive/forecast_failures.py` raggruppa per `(run_id, robot_id, channel)`, di default analizza solo l'ultimo `run_id` presente nello storico. La query delle previsioni (`backend/src/routes/predictions.js`) filtra allo stesso modo. `query_service.py` legge ora con `mergeSchema=true` (altrimenti un file Parquet scritto prima dell'introduzione di un campo nuovo fa fallire qualunque query che lo referenzia, non solo mischia i dati — bug reale trovato e corretto durante la verifica).

`run_id` **non è una chiave di partizione** in nessuna delle tre tabelle, solo una colonna normale filtrata via `WHERE`. Provato inizialmente a partizionare anche per `run_id` (oltre a `type` per `anomalies`) e scartato: aggiungere una colonna di partizione a una directory Parquet con dati preesistenti scritti a una profondità di partizione diversa (qui: `type=X/*.parquet` piatto, dati precedenti a questa modifica) rompe la lettura dell'**intera** tabella, non solo delle righe vecchie (`AssertionError: Conflicting partition column names detected`) — verificato dal vivo, con la suite pytest che ha smesso di poter interrogare `anomalies` finché non è stato ripristinato lo schema di partizione uniforme (vedi bug reali sotto).

**Verifica**: due run consecutivi del generatore, `run_id` distinti e coerenti su tutti i messaggi di un run (un solo valore visto su un run intero).

### 4. Menù a tendina per il generatore sintetico

`dashboard/app.js`: il campo `.fault-robot` (prima `<input type="text">`) è ora un `<select>` popolato con `SIM00000..SIM0000{N-1}` calcolato dal campo "Numero robot", aggiornato al volo se quel numero cambia; oltre 500 robot resta solo "casuale" (un menù con migliaia di opzioni non sarebbe utile). La flotta reale aveva già il menù.

### 5. Previsione live + reazione differenziata + decommissioning

- **Nuovo tipo di guasto `preavviso_intermittente`** (`kafka_bridge.py`, `synthetic_generator.py`): un'onda quadra periodica tiene un canale oltre una soglia "morbida" solo per `burst_duration_s` ogni `burst_interval_s`, tornando nominale in mezzo — non un guasto continuo.
- **Quarto operatore stateful in streaming** (`detection_job.py`, stesso pattern `applyInPandasWithState` del livelock): per `(robot_id, channel)` conta quante volte il valore ha superato la soglia morbida negli ultimi `PREAVVISO_WINDOW_S=60s`; a `PREAVVISO_MIN_CROSSINGS=3` emette `type="previsione"` su `anomalies`, stessi nomi di campo delle previsioni offline (`channel`, `current_value`, `critical_threshold`, `lead_time_s`) per coerenza con la tabella "Robot a rischio" già in dashboard. `lead_time_s` qui è una stima conservativa fissa (la finestra stessa), non una regressione: il segnale a raffiche non si presta a un fit lineare stabile come il trend continuo di `forecast_failures.py`.
- **Reazione differenziata** (`backend/src/services/fleetStateStore.js`): `onPrevisioneAnomaly` (nuovo, ascolta `anomalyStream.onPrevisione`) fa la riparazione preventiva + dispaccio riserva che prima era agganciata al guasto vero; `onPersistentFailure` (ex `onSaluteAnomaly`, stesso trigger `onSaluteThresholdAnomaly`) ora ferma il robot (`freezeRobot` → `fleet_control_service.py` `/robot/freeze` → `~nav_control {"cmd":"freeze"}` → `MissionRunner.freeze()`, nuovo in `graph_navigator.py`: annulla il goal corrente, non ne assegna uno nuovo) invece di ripararlo in automatico.
- **Decommissioning**: nuova route `POST /api/fleet-control/robot/decommission` → `decommissionRobot()` in `fleetStateStore.js`, rimuove il robot dalla Map in memoria (stesso meccanismo `onRemove`/websocket `"remove"` già usato da `pruneStale`/`pruneSynthetic`) e lo esclude per sempre da `pickAvailableReserve()`. **Bug reale trovato e corretto durante la verifica**: un robot decommissionato continua a pubblicare `fleet_state` (il nodo ROS non sa nulla della decommission), quindi il messaggio successivo lo rimetteva subito nella Map — il consumer ora scarta esplicitamente i messaggi di un `robot_id` già decommissionato.
- Dashboard: nuovo stato "in avaria" (`realRobotStatus()`, quando `health_anomaly` è vero e il robot non sta andando verso `repair_node`/`reserve_node`) con bottone "Decommissiona"; riga evidenziata in rosso.

**Verifica** (flotta reale, `scale=small`): guasto `preavviso_intermittente` su R2 → evento `previsione` (`channel=motor_current`, `current_value=2.14` sopra soglia morbida 2.0 sotto quella dura 2.5, `n_crossings=3`) → log `"previsione di guasto ... su R2: riparazione preventiva + dispaccio riserva (R4)"` confermato. **Bonus non pianificato**: i guasti pre-schedulati a rampa lenta di R1/R3 (`deriva_termica`, `spike_corrente`) hanno anch'essi generato previsioni corrette prima di raggiungere la soglia dura — il meccanismo generalizza bene oltre il solo `preavviso_intermittente`. Guasto istantaneo (`spike_corrente`, `rise_time_s=0.5`) su R4 → log `"guasto persistente ... su R4: robot fermato"` confermato, `task_state=idle` osservato, nessun invio verso `repair_node`. Decommissioning di R4 → sparito da `/api/fleet` e resta sparito (verificato il fix del bug sopra).

## Bug reali trovati durante la verifica (non solo il codice nuovo)

- **Checkpoint Spark obsoleti dopo un cambio di schema di stato**: cambiare `LIVELOCK_STATE_SCHEMA`/l'aggregazione del deadlock (aggiunta di `run_id`) senza cancellare `/tmp/shf-checkpoints` fa fallire silenziosamente `detection_job.py` in crash-loop (`py4j.Py4JException: Received empty command`, causa reale: `InvalidUnsafeRowException`, checkpoint scritto con uno schema di stato diverso). Non specifico a questa sessione — capiterà a chiunque cambi lo stato di un operatore stateful senza pulire i checkpoint. Non risolto strutturalmente (richiederebbe un meccanismo di versionamento dello schema), solo documentato: **dopo un cambio allo stato di `detection_job.py`, cancellare `/tmp/shf-checkpoints` prima di riavviare**.
- **`--` letterale in un commento XML**: introdotto per errore in `sim_multi_robot.launch` (stile dei commenti Python/JS di questo progetto, non valido in XML), causava `RLException: Invalid roslaunch XML syntax`. Corretto, validato con `xml.dom.minidom`.
- **`spark.read.parquet` senza `mergeSchema`**: un file Parquet scritto prima dell'introduzione di `run_id` fa fallire (`UNRESOLVED_COLUMN`) qualunque query che lo referenzia, anche se un file più recente ce l'ha — non basta che il campo sia nullable nello schema logico, Parquet è colonnare per file fisico. Corretto in `query_service.py`.
- **Aggiungere `run_id` come chiave di partizione ha rotto `anomalies` per intero**: il tentativo iniziale di partizionare anche per `run_id` (oltre a `type`) ha prodotto una struttura di cartelle a profondità diversa da quella preesistente (`type=X/*.parquet` piatto, dati precedenti alla modifica, contro `type=X/run_id=Y/*.parquet` per i dati nuovi) — Spark rifiuta di leggere l'intera tabella (`AssertionError: Conflicting partition column names detected`), non solo le righe vecchie. Peggio: anche dopo aver cancellato i file annidati, l'errore persisteva perché `_spark_metadata` (il log dei batch di Structured Streaming per quella directory) ricordava ancora la struttura incoerente dalle scritture precedenti — bisogna cancellare anche quello, non solo i dati fisici, per far ripartire la lettura da una scansione pulita del filesystem. **Decisione finale**: `run_id` resta una colonna normale ovunque, filtrata via `WHERE`, mai una chiave di partizione.

## Limiti noti (dichiarati, non nascosti)

- **Il decommissioning non termina i processi ROS** del robot (nessun controllo per-robot esiste in `fleet_control_service.py`) — resta "fermo e invisibile", non "smontato".
- **La rimozione individuale di un robot sintetico non è coperta** — solo la flotta reale.
- **Più robot guasti contemporaneamente convergono sullo stesso `DEPOSITO`** — osservato dal vivo (R1/R2/R3 tutti inviati lì nello stesso test): un limite già presente prima di questa estensione (un solo `repair_node`), non risolto qui (più bay di riparazione sarebbe un lavoro a parte).
- **`lead_time_s` della previsione live è una stima conservativa fissa**, non una regressione come l'offline — dichiarato esplicitamente nel codice e qui.
- **Previsione e guasto persistente condividono lo stesso debounce (`inRepair`)**: se una previsione ha già azionato la riparazione preventiva su un robot, un guasto persistente confermato subito dopo sullo stesso robot non lo ferma di nuovo (è già in uscita verso il deposito) — scelta deliberata, non un bug: evita azioni doppie sullo stesso robot.

## Stato

- `streaming/schemas.py`, `streaming/detection_job.py`, `streaming/persistence_job.py`, `streaming/query_service.py` — `run_id`, fix livelock, quarto operatore "previsione", `mergeSchema`.
- `generator/synthetic_generator.py`, `generator/generator_service.py` — `run_id`, `preavviso_intermittente`.
- `ros/catkin_ws/src/shf_bringup/scripts/kafka_bridge.py`, `graph_navigator.py`, `fleet_control_service.py` — `run_id`, `preavviso_intermittente`, `MissionRunner.freeze()`, route `/robot/freeze`.
- `ros/catkin_ws/src/shf_bringup/launch/sim_multi_robot.launch`, `ros/supervisord.conf` — arg/marker file `run_id`, start_node R4/R8 spostati.
- `config/warehouse_graph.json`, `config/experiment.json` — nodi `DEPOSITO`/`RISERVA1`/`RISERVA2`.
- `predictive/forecast_failures.py` — isolamento per `run_id`.
- `backend/src/services/{anomalyStream,fleetStateStore,fleetControlService}.js`, `backend/src/routes/{fleetControl,predictions}.js` — `onPrevisione`, reazione differenziata, `decommissionRobot`, fix `pickAvailableReserve`, fix ri-comparsa robot decommissionato.
- `dashboard/{index.html,app.js,style.css}` — menù a tendina generatore, stato "in avaria", bottone decommissiona, badge `run_id`.
- `CLAUDE.md`, `PLAN.md` — nuove invarianti e voce di piano.
