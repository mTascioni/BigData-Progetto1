# Passo 16 — Perturbazioni reattive, streaming live in dashboard, fix riserve/deposito (estensione oltre il piano originale)

**Non è un passo del piano originale.** Nasce dall'uso quotidiano del sistema dopo il Passo 15: due richieste di interattività sulla GUI e tre bug osservati (riserve che sparivano, robot che finivano al deposito senza un guasto vero, il pannello "Robot a rischio" bloccato su un dato di test).

## Perché

1. L'iniezione di guasti sulla flotta reale era già "molto reattiva" (form → azione immediata); mancava l'equivalente per le **perturbazioni** — rumore/instabilità sui sensori, non un guasto vero, per generare falsi positivi controllati e verificare che il sistema li impari a filtrare (`offline/adaptive_thresholds.py`).
2. Nessuna vista sui messaggi grezzi che passano sui topic Kafka: utile per capire/debuggare la pipeline senza aprire un consumer a mano.
3. **Bug**: dopo aver usato una riserva (es. R4) in un run, non ricompariva più nei run successivi della flotta reale — "in generale non ci sono più i ricambi".
4. **Bug**: molti robot, sia nella flotta reale sia nel generatore sintetico, finivano sui nodi `DEPOSITO`/`RISERVA1`/`RISERVA2`.
5. **Bug**: "Robot a rischio" mostrava una riga fissa (`PREDTEST`) mai iniettata dall'utente, e non rifletteva le previsioni viste in "Eventi recenti".

## Decisioni di scope

- **"Perturbazione" ≠ "guasto"**: distinzione da tenere chiara (non ovvia dal solo termine) — rumore gaussiano extra su un canale, non una firma di guasto strutturata. Deliberatamente **esclusa da `injected_faults`** (ground truth): se ci finisse, `offline/adaptive_thresholds.py` la conterebbe come guasto vero invece che come il falso positivo che deve imparare a filtrare.
- **Streaming live campionato, non esaustivo**: telemetry da sola può arrivare a centinaia di msg/s (Passo 12); il pannello serve a "vedere cosa passa", non a processare tutto — throttle a monte (backend) a un messaggio ogni ~200ms per topic, nessuna persistenza (buffer solo client-side, scartato al refresh).
- **Fix mirati, non un redesign**: i tre bug hanno cause diverse (stato in memoria non resettato, random walk del generatore, dato di test non ripulito) — corretti singolarmente invece di un meccanismo unificato.

## Cosa è stato costruito

### 1. Perturbazioni reattive (`rumore_sensore`)

Nuovo `fault_type` in `kafka_bridge.py`/`synthetic_generator.py`: somma rumore gaussiano extra (`PERTURBATION_NOISE_STD_BY_CHANNEL`, calibrato per sconfinare la soglia dura solo occasionalmente — 3-sigma, non ad ogni tick) al canale scelto, per la durata richiesta. Riusa la stessa via reattiva del Passo 14 (`POST /api/fleet-control/fault` → `fleet_control_service.py` → `~fault_inject` → `kafka_bridge.py`), estesa per far passare `params.channel` dal form. **Esclusa da `injected_faults`**: `NON_GROUND_TRUTH_FAULT_TYPES` in entrambi i file, controllata in `_deactivate()`. Nuova sezione dedicata "Perturbazioni (rumore sensori)" nella dashboard (Robot, Canale, Durata), sotto il form guasti esistente.

**Verifica**: perturbazione `motor_current` su R1 (25s) → valori osservati nel range 1.31-1.80 (contro un range nominale ±0.1σ molto più stretto) → nessuna riga in `injected_faults` per R1 con `fault_type=rumore_sensore` (confermate solo le righe `spike_corrente` reali di run precedenti).

### 2. Streaming live dei topic Kafka (`backend/src/services/rawStream.js`)

Nuovo consumer Kafka indipendente (stesso pattern di `anomalyStream.js`/`fleetStateStore.js`, group id univoco per processo), sottoscritto a `telemetry`/`anomalies`/`injected_faults`/`fleet_state`, campionato a un messaggio ogni `MIN_INTERVAL_MS=200` per topic, broadcast via lo stesso websocket `/ws` esistente (nuovo `type="raw"`). Dashboard: nuova sezione con tab per topic e un log scorrevole (`#raw-stream-log`, buffer client-side da 50 righe, pausa/riprendi) — nessuna scrittura su disco né lato backend né lato client.

**Verifica**: client websocket di prova connesso per 8s → 15 messaggi raw ricevuti su 3 topic attivi in quella finestra, coerente col throttling atteso.

### 3. Fix: riserve che non ricomparivano (`backend/src/services/fleetStateStore.js`, `routes/fleetControl.js`)

Causa: `dispatchedReserves`/`inRepair`/`decommissioned` sono `Set` in memoria che vivono per l'intera vita del processo backend, non per singola simulazione — un nuovo avvio della flotta reale (`/sim/start`) ricrea tutto da zero in ROS/Gazebo, ma senza un reset esplicito lato backend una riserva già usata (o un robot decommissionato) in un run precedente restava inutilizzabile per sempre anche nei run successivi. Nuova `resetRealFleetState()`, chiamata da `POST /sim/start` dopo l'avvio.

**Verifica**: R3 → previsione → R4 dispacciato (run A) → sim fermata e riavviata (run B) → nuova previsione su R3 → **R4 ridispacciato di nuovo** (log: `"previsione di guasto ... su R3: riparazione preventiva + dispaccio riserva (R4)"`), confermando che il reset funziona anche su più cicli avvio/stop.

### 4. Fix: robot che finivano al deposito (`generator/synthetic_generator.py`)

Causa (generatore sintetico): `build_adjacency()`/lo spawn iniziale (`rng.choice(list(node_pos))`) trattavano `DEPOSITO`/`RISERVA1`/`RISERVA2` come nodi qualunque del grafo — un robot-token poteva nascerci o finirci per un random walk normale, senza nessun guasto. Fix: `ROUTABLE_KINDS_EXCLUDED = {"repair", "reserve"}` filtra questi nodi (per `kind`, da `warehouse_graph.json`) sia dallo spawn sia dall'adiacenza percorribile — il generatore ora non ha modo di raggiungerli, come previsto (sono nodi dedicati alla flotta reale, Passo 15).

Causa (flotta reale): non un bug di instradamento (i robot con task seguono una `goal_sequence` fissa che non include quei nodi) ma l'effetto combinato del bug #3 sopra — una volta esaurite le riserve, ogni previsione successiva mandava comunque il robot guasto a `DEPOSITO` senza mai un cambio, dando l'impressione di un accumulo anomalo. Il fix #3 lo allevia (le riserve tornano disponibili ad ogni run); **resta però un limite noto già documentato al Passo 15**: più robot guasti nella stessa run convergono comunque sull'unico `DEPOSITO` (una sola baia di riparazione, per design) — non è stato introdotto né risolto da questa estensione.

**Verifica**: run sintetico dopo il fix, 3000 messaggi, nodi/archi osservati: nessun `DEPOSITO`/`RISERVA1`/`RISERVA2`/`A-DEPOSITO`/`A-RISERVA1`/`A-RISERVA2` (prima del fix, lo stesso run li conteneva).

### 5. Fix: "Robot a rischio" bloccato su un dato di test (`dashboard/app.js`, dati)

Causa: un file Parquet di test (`predictions_20260722T105858.parquet`, robot_id `PREDTEST`) creato in una sessione precedente per verificare `mergeSchema` era rimasto in `/data/predictions` — essendo il record con `predicted_at_ts` più recente, la query `LATEST_PREDICTIONS_SQL` lo sceglieva sempre come "ultimo run", nascondendo qualunque previsione vera. Rimosso (insieme al corrispondente file di telemetria sintetica `part-predtest.parquet`).

Causa più profonda: il pannello leggeva **solo** le previsioni offline batch (`predictive/forecast_failures.py`, on-demand), non gli eventi `type="previsione"` già visibili in "Eventi recenti" (streaming, Passo 15). Fix: nuova mappa client-side `livePredictions` (chiave `robot_id:channel`), aggiornata da `handleAnomalyEvent` su ogni evento previsione live, con TTL di 2 minuti (il `lead_time_s` live è una stima fissa, non decade da sola come una vera regressione — senza scadenza resterebbe visibile anche dopo che il trend è rientrato). `refreshPredictions()` ora unisce le righe offline con quelle live (le live vincono su una stessa coppia robot/canale, sono più fresche).

**Verifica**: rimossi gli artefatti di test da `/data`; iniettata una previsione live (vedi Passo 15) → riga corrispondente comparsa immediatamente in "Robot a rischio" senza attendere il prossimo giro di `forecast_failures.py`.

## Bug reali trovati durante la verifica

- **`docker compose restart` non basta per il codice backend/generatore**: sia `backend` sia `ros` copiano il codice nell'immagine al build (`COPY src ./src` / analogo), non lo montano live — un `docker restart` riavvia semplicemente la stessa immagine vecchia. Serve `docker compose build <servizio>` prima di ricreare il container. Per il generatore sintetico, che gira come processo persistente dentro il container `ros` (`generator_service.py`, supervisord), anche un bind mount live non basta: il modulo Python è già importato in memoria, serve `supervisorctl restart generator_service` per far ripartire il processo e ricaricare il codice.
- **Corsa fra previsione e guasto persistente su un guasto molto rapido**: un test con `spike_corrente` a `rise_time_s=2` e `peak_a` appena sopra la soglia dura ha attivato direttamente `onPersistentFailure` (freeze) invece di passare prima da `onPrevisioneAnomaly` — entrambi i percorsi condividono lo stesso debounce (`inRepair`), quindi chi arriva prima "vince". Comportamento accettabile, non un bug da correggere: un guasto che sale da nominale a critico in 2 secondi non lascia comunque margine per una manovra preventiva.
- **`persistence_job.py`, tre problemi concatenati scoperti rigenerando una previsione per i test**: (1) il suo checkpoint (`/data/_checkpoints`) e il log di sink `_spark_metadata` (dentro ciascuna cartella dati) possono disallinearsi se uno dei due viene resettato senza l'altro — successo qui perché una sessione precedente aveva cancellato solo `_spark_metadata/anomalies` per un bug di partizionamento (Passo 15): il job ripartiva "convinto" di aver già scritto batch che in realtà non esistevano su disco, fallendo con `BATCH_METADATA_NOT_FOUND` o, peggio, con batch "committati" ma silenziosamente mai scritti (numerazione batch del sink collisa con vecchie entry). Fix: cancellare **sempre insieme** checkpoint e `_spark_metadata` di tutte e tre le tabelle, mai uno solo. (2) Un checkpoint perso costringe a rileggere da `earliest`: su un topic con un backlog di milioni di messaggi (accumulato in giorni di sviluppo) il primo micro-batch, senza limiti, prova a leggerli tutti insieme e resta bloccato per molti minuti senza log visibili (il job imposta `setLogLevel("WARN")`, quindi tace anche mentre progredisce). Aggiunta una nuova opzione `PERSIST_MAX_OFFSETS_PER_TRIGGER` (default 20000) che limita quanto legge per trigger, rendendo il recupero incrementale e osservabile. (3) **Kafka non ha un volume Docker persistente** in `docker-compose.yml`: un riavvio dei container (qui causato da un blocco del PC durante la sessione) fa ripartire Kafka completamente vuoto, azzerando tutti i topic — il checkpoint di `persistence_job.py`, che vive invece sul volume persistente `shf-data`, si aspetta offset che non esistono più (`Partition ... offset was changed from N to 0`). Non corretto in questa sessione (richiederebbe aggiungere un volume al servizio `kafka`, decisione con implicazioni di spazio disco da valutare a parte) — solo documentato come limite noto qui sotto.

## Limiti noti (dichiarati, non nascosti)

- **Più robot guasti nella stessa run convergono ancora sull'unico `DEPOSITO`** (limite ereditato dal Passo 15, non toccato qui — una sola baia di riparazione per design).
- **Le perturbazioni sul generatore sintetico usano sempre il canale di default** (`motor_current`): il form pre-run del generatore non espone la scelta del canale come invece fa il form reattivo della flotta reale (coerente con gli altri tipi di guasto del generatore, che non espongono sotto-parametri).
- **Streaming live campionato, non esaustivo**: un messaggio ogni ~200ms per topic è sufficiente per "vedere cosa passa" ma non rappresenta il volume reale ad alto carico (Passo 12) — scelta deliberata per non appesantire backend/browser.
- **Kafka senza volume persistente**: un riavvio dei container perde tutta la cronologia dei topic (vedi bug sopra) mentre lo storico Parquet/i checkpoint sopravvivono — asimmetria non risolta in questa sessione.

## Stato

- `ros/catkin_ws/src/shf_bringup/scripts/kafka_bridge.py`, `generator/synthetic_generator.py` — `rumore_sensore`, esclusione da `injected_faults`, fix random walk (esclusione nodi repair/reserve).
- `backend/src/services/{rawStream,fleetStateStore,fleetControlService}.js`, `backend/src/routes/fleetControl.js` — streaming live, `resetRealFleetState`, passthrough `params` per l'iniezione.
- `backend/src/server.js` — wiring del nuovo consumer/websocket.
- `dashboard/{index.html,app.js,style.css}` — form perturbazioni, pannello streaming live, merge previsioni live in "Robot a rischio", opzione mancante `preavviso_intermittente` nei menu guasti.
- Dati: rimossi gli artefatti di test `predictions_20260722T105858.parquet`/`part-predtest.parquet` da `/data`.
- Documentazione di progetto aggiornata con la nuova voce di piano.
