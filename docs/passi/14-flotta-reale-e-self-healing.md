# Passo 14 — Flotta reale in un magazzino fisico, guasti live, self healing vero (estensione oltre i 13 passi)

**Non è un passo del piano originale.** Nasce dall'esigenza di una demo dal vivo al professore: mostrare comportamento "normale" e guasti su robot ROS/Gazebo **veri** (pochi, ma reali), con la possibilità di iniettare un guasto in diretta su un robot reale (dentro il topic `telemetry` vero, non un canale a parte — altrimenti il modello di detection non servirebbe a niente), e vedere il sistema reagire davvero: il robot guasto va in riparazione, un secondo robot tenuto di riserva ne prende in carico la missione.

## Perché

Fino al Passo 13 il "magazzino" esisteva solo come grafo logico (`config/warehouse_graph.json`): Gazebo era un mondo vuoto, `capacity=1` sui due choke point (C-F, C-H) era solo un numero nel JSON senza alcuna geometria fisica a farlo rispettare. Deadlock/livelock sulla flotta reale non erano mai stati osservati per davvero (il conflitto trovato al Passo 5 fu un incidente di scheduling, non i choke point). I guasti sui robot reali erano solo pre-schedulati in `config/experiment.json`, letti una volta sola all'avvio — niente di interattivo, niente di dimostrabile "a comando" davanti a un pubblico.

## Decisioni di scope

- **Grafo**: riusato quello esistente, nessun cambio di topologia — solo due "tunnel" di pareti Gazebo sui choke point già pensati per questo (Passo 2).
- **Guasti che fermano il robot**: qualsiasi anomalia di salute rilevata (all'inizio implementato su `health_anomaly` di `fleet_state`, poi rivisto — vedi "Il falso positivo dell'Isolation Forest" più sotto) — non una whitelist di guasti "gravi".
- **Sostituto**: robot dedicati alla riserva, sempre fermi finché non servono, non scelti fra quelli già in missione (garantisce un sostituto sempre pronto, importante per una demo dal vivo).
- **Ripresa**: manuale da dashboard ("Rimetti in servizio"), nessuna logica automatica di "fine anomalia" in questa versione.
- **Scala della flotta** (richiesta in un secondo momento): non un numero fisso di robot, ma due scenari selezionabili — `small` (R1-R3 attivi + R4 di riserva, quello originale) e `large` (il doppio: R1/R2/R3/R5/R6/R7 attivi + R4/R8 di riserva). Per carichi oltre questo restano i robot-token sintetici del generatore (Passo 12/13) — 8 robot ROS reali sono già pesanti in CPU, andare oltre non è la strada giusta per lo stress test.

## Cosa è stato costruito

### Mondo Gazebo con corridoi fisici (`ros/catkin_ws/src/shf_bringup/worlds/warehouse.world`)

Nuovo file SDF: due coppie di pareti statiche (box, spessore 0.1m, altezza 0.5m, ben oltre l'altezza del lidar del burger) che restringono gli archi C-F e C-H a un corridoio libero di 0.8m, centrato esattamente sulle coordinate x=20 già usate dai nodi C/F/H nel grafo — nessuna nuova coordinata inventata. `sim_multi_robot.launch` punta a questo world invece di `turtlebot3_gazebo/worlds/empty.world`.

Il local costmap di `move_base` (config `turtlebot3_navigation` già in uso, non modificata) ha già un obstacle layer basato sul lidar reale (`/scan`): le pareti vengono viste ed evitate/bloccate senza nessuna modifica alla navigazione.

**Verifica**: un test forzato (due robot comandati verso l'estremità opposta dello stesso corridoio nello stesso momento, via il nuovo canale di controllo) ha prodotto un conflitto fisico reale — un robot si è bloccato per davvero (`task_state="blocked"` osservato su Kafka, non simulato), l'altro è passato. Prima volta che questo tipo di conflitto è osservabile su dati reali in questo progetto.

### Canale di controllo live

Tre pezzi nuovi, tutti minimi:

1. **`ros/catkin_ws/src/shf_bringup/scripts/graph_navigator.py`** (refactor): il loop `send_goal`/`wait_for_result` per-nodo è ora incapsulato in una classe `MissionRunner`, invocabile sia all'avvio (la `goal_sequence` di `experiment.json`, comportamento originale del Passo 3/5) sia da un nuovo subscriber `~nav_control` (`std_msgs/String`, JSON `{"nodes": [...]}`) che può annullare il goal corrente e assegnarne una nuova in qualsiasi momento — stesso meccanismo per "missione programmata" e "missione assegnata dal vivo", non due percorsi di codice separati.
2. **`ros/catkin_ws/src/shf_bringup/scripts/kafka_bridge.py`**: `FaultInjector.inject_live(fault_type, duration_s, params=None)` aggiunge un guasto allo `schedule` interno **a runtime**, con `start_time_s`/`end_time_s` calcolati sull'orologio del bridge — nessuna logica nuova per applicarlo, lo stesso codice che gestisce i guasti pre-schedulati (Passo 6) lo attiva/applica/logga automaticamente al prossimo tick. Il guasto finisce nel topic `telemetry` reale, esattamente come uno schedulato: nessuna scorciatoia per il layer di detection.
3. **`ros/catkin_ws/src/shf_bringup/scripts/fleet_control_service.py`** (nuovo, porta 5002, stesso stile di `generator_service.py` del Passo 12 ma anche nodo `rospy`): ponte HTTP → topic ROS. Route: `POST /fault/inject`, `POST /robot/goto`, `POST /robot/repair`, `POST /robot/return-to-service`, `POST /robot/dispatch-mission`. `backend/src/services/fleetControlService.js` + `backend/src/routes/fleetControl.js` lo espongono come `/api/fleet-control/*`.

### Robot di riserva (R4, e R8 in scale=large)

`config/experiment.json`: `R4`/`R8` aggiunti a `fleet` (start_node su nodi liberi del grafo — nessuno dei 10 nodi coincide con lo start_node di un robot attivo), **nessuna voce in `tasks[]`**: `graph_navigator.py` con `task is None` resta semplicemente in attesa su `~nav_control`, riusando lo stesso meccanismo del punto 1 senza nessun caso speciale per "un robot che parte fermo". `sim_multi_robot.launch` ha un `<group ns="R4">` sempre presente e quattro gruppi `R5`-`R8` condizionati da un nuovo arg `scale` (`if="$(eval arg('scale') == 'large')"`) — stesso `experiment.json` in entrambi gli scenari, gli R5-R8 semplicemente non vengono spawnati in `small`.

### Anello automatico di retroazione

`backend/src/services/fleetStateStore.js`, su un'anomalia di salute su un robot reale (vedi sotto per la fonte esatta del segnale — è cambiata durante la verifica), il backend:
1. Chiama `fleet_control_service` → il robot va al `repair_node` (`config/experiment.json`, campo nuovo).
2. Legge la `goal_sequence` originale del robot da `experiment.json` e la manda a una riserva **disponibile** (identificata dinamicamente: un robot reale senza voce in `tasks[]`, effettivamente attivo ora, non già usata — `pickAvailableReserve()`, gestisce sia R4 sia R8 in scale=large, sceglie quella libera) via `dispatch-mission` — il sostituto rifà l'intera missione da capo, non riprende dal punto esatto di interruzione (semplificazione consapevole, vedi limiti).

### Dashboard

Nuova card "Flotta reale — controllo" (`dashboard/index.html`/`app.js`/`style.css`), accanto al pannello del generatore sintetico: tabella di stato per tutti i robot reali attivi (in servizio / in riparazione / di riserva, dedotto confrontando `goal_node` con `repair_node`/`reserve_node`/`reserve_robot_ids` letti da un nuovo endpoint `GET /api/fleet-control/config`), form per iniettare un guasto live su un robot reale, bottone "Rimetti in servizio" per un robot in riparazione.

## Il falso positivo dell'Isolation Forest (bug reale, trovato e risolto)

Passando allo scenario `large` (8 robot) per estendere la demo, la prima versione dell'anello automatico (basata su `fleet_state.health_anomaly`, l'OR di soglie fisse e Isolation Forest del Passo 7) è andata fuori controllo: quasi tutti gli 8 robot finivano in riparazione entro un minuto dall'avvio, senza che nessun guasto fosse stato iniettato.

**Diagnosi** (passo per passo, con dati reali, non per intuizione):
1. Isolato il campo che scattava: sempre `if_anomaly=1` (Isolation Forest), mai `threshold_reasons` (soglie fisse) — quindi non un guasto vero.
2. Prima ipotesi (sbagliata): i nuovi corridoi stretti del Passo 14 facevano vedere ai robot pareti vicine (`min_obstacle_dist` ~1.1-1.2m) che il modello, allenato su un campione sintetico con ostacoli quasi sempre lontani, non riconosceva come normali. Corretta la proporzione del campionamento sintetico per rappresentare meglio questo caso — miglioramento reale ma non la causa principale, i falsi positivi sono continuati quasi invariati.
3. Guardando i dati grezzi con calma: un robot **completamente idle** (v_lin=0, v_ang=0, temp/corrente nominali), fermo a ~0.45m da qualcosa, veniva segnalato anomalo per **35 tick consecutivi (17.5s)** — troppo sistematico per essere rumore statistico isolato. La causa vera: `generate_nominal_samples()` (`streaming/isolation_forest_model.py`) campionava `v_lin` e `min_obstacle_dist` **in modo indipendente**. Nella realtà non lo sono: un robot fermo resta parcheggiato dov'è, e se quel punto è vicino a una parete/dock/altro robot, il lidar legge distanza corta per **tutta** la sosta, non come evento raro e transitorio. Il campione originale (idle e "vicino a qualcosa" quasi mai insieme) sotto-rappresentava pesantemente questa combinazione più che plausibile.
4. Fix nel modello: `v_lin` e `min_obstacle_dist` ora campionati **correlati** (se il campione è "fermo", metà delle volte è vicino a qualcosa; se "in movimento", la vicinanza riflette corridoio/incrocio). Migliora ma **non elimina** il fenomeno.
5. Fix strutturale, quello risolutivo: l'Isolation Forest ha per costruzione un tasso di falsi positivi (`contamination=0.02` calibra la soglia perché ~2% delle predizioni scattino sempre, anche su dati perfettamente nominali — non è un difetto, è la definizione del parametro). Con 3-4 robot a 2Hz questo tasso produceva pochi eventi, mai abbastanza per notarli; con 8 robot il numero assoluto sale, e per la prima volta (Passo 14) c'è un'**azione reale** collegata, non solo un pallino sulla mappa. **La correzione giusta non era continuare a rincorrere il modello statistico, ma cambiare cosa aziona l'anello automatico**: `backend/src/services/anomalyStream.js` ora espone `onSaluteThresholdAnomaly(...)`, che inoltra solo le anomalie di tipo `salute` con `threshold_reasons` non vuoto (soglie fisse, deterministiche — ogni guasto iniettato le supera sempre per costruzione, es. `spike_corrente` porta `motor_current` ben oltre 2.5A). `fleetStateStore.js` ora aziona la riparazione da lì, non più da `fleet_state.health_anomaly`. L'Isolation Forest resta attiva e visibile ovunque altrove (dashboard, `fleet_state`, `anomalies`) per il suo valore di segnale "morbido" — solo l'azione automatica su un robot reale non dipende più da un segnale che può scattare per rumore statistico.

**Verifica del fix**: finestra pulita di 3 minuti con 8 robot attivi e nessun guasto iniettato → zero eventi di riparazione (prima: 5-7 robot su 8 in riparazione entro 1 minuto). Poi guasto vero iniettato su R3 → esattamente un evento, R3 verso il nodo di riparazione, R4 (riserva) dispaccia con la missione originale di R3 (primo goal osservato: `B`, il primo nodo di `["B","E","F","G"]`). Bonus: un guasto **pre-schedulato** in `fault_schedule` (non iniettato da me) è scattato naturalmente su R1 durante la stessa prova, ed è stato gestito correttamente assegnando l'**altra** riserva disponibile (R8, non R4 che era già occupata) — conferma che la selezione fra più riserve funziona.

## Verifica end-to-end (tutta reale, non simulata)

1. Conflitto fisico forzato su C-F: un robot bloccato per davvero, l'altro passato — confermato.
2. Guasto iniettato dal vivo su un robot reale (dashboard → backend → `fleet_control_service` → `kafka_bridge.py`): visibile nella telemetria reale (`motor_current` salito ben oltre il nominale) — non in un canale separato.
3. Anomalia di salute su soglia fissa rilevata su dati reali → il backend ha mandato automaticamente il robot colpito al nodo di riparazione (osservato: `goal_node` passato al valore di `repair_node` su `fleet_state` reale) e dispacciato una riserva disponibile sulla missione originale (osservato: `goal_node` della riserva avanzare lungo la sequenza del robot guasto, non la propria).
4. "Rimetti in servizio" da dashboard → il robot torna verso il nodo di riserva (osservato su `fleet_state` reale).
5. Verificato in un vero browser (Chromium headless via CDP): pannello popolato, nessun errore console, aggiornamento live dello stato ad ogni robot.
6. Scenario `large` (8 robot) verificato pulito su una finestra di osservazione di 3 minuti senza guasti (zero riparazioni spurie dopo il fix), poi con un guasto vero (repair+dispaccio corretto, solo sul robot giusto). Scenario `small` (default) riverificato intatto dopo — solo R1-R4, nessun effetto collaterale.
7. Suite `test/` rieseguita per intero dopo tutte le modifiche: 22/23 (l'unico fallimento è la stessa flakiness da contesa di risorse già documentata al Passo 13, non una regressione).

## Limiti noti (dichiarati, non nascosti)

- **Una riserva riparte la missione del robot guasto da capo**, non dal punto esatto di interruzione. Tracciare l'avanzamento esatto è un miglioramento naturale successivo.
- **Se una riserva stessa si guasta mentre è in missione**, non c'è un livello ulteriore di riserva oltre R4/R8 in questa versione.
- **Corridoio largo 0.8m**: valore di partenza tarato empiricamente (il costmap locale di `move_base` usa `inflation_radius=1.0` di default), sufficiente per un robot alla volta ma non testato in modo esaustivo su tutte le combinazioni di traiettorie.
- **Soglia di 60s del rilevatore di livelock** (Passo 7, invariata): se durante le prove dal vivo risultasse troppo lenta per il ritmo di una demo, si ritocca la costante `LIVELOCK_CONFIRM_DURATION_S` in `streaming/detection_job.py`, non serve una nuova architettura.
- **L'Isolation Forest resta statisticamente "rumorosa"** per la visualizzazione passiva (dashboard/`fleet_state`/`anomalies`) — solo l'azione automatica ne è stata isolata. Su una flotta molto più grande di 8 robot anche il pallino viola sulla mappa potrebbe lampeggiare più spesso del dovuto; non è stato necessario risolverlo per questa demo.

## Stato

- `ros/catkin_ws/src/shf_bringup/worlds/warehouse.world` — nuovo.
- `ros/catkin_ws/src/shf_bringup/launch/sim_multi_robot.launch` — world file nuovo, arg `scale` (small/large), gruppi `R4`-`R8`.
- `ros/catkin_ws/src/shf_bringup/scripts/graph_navigator.py` — refactor (`MissionRunner` + `~nav_control`).
- `ros/catkin_ws/src/shf_bringup/scripts/kafka_bridge.py` — `FaultInjector.inject_live` + subscriber `~fault_inject`.
- `ros/catkin_ws/src/shf_bringup/scripts/fleet_control_service.py` — nuovo.
- `ros/supervisord.conf` — nuovo programma `fleet_control_service`.
- `streaming/isolation_forest_model.py` — campionamento `v_lin`/`min_obstacle_dist` correlato invece che indipendente; `streaming/models/isolation_forest.pkl` rigenerato.
- `config/experiment.json` — campi `repair_node`/`reserve_node`, R4/R5/R6/R7/R8 aggiunti a `fleet` (R5-R7 con task, R4/R8 senza).
- `backend/src/services/fleetControlService.js`, `backend/src/routes/fleetControl.js` — nuovi (incl. `GET /config` con `reserve_robot_ids`).
- `backend/src/services/anomalyStream.js` — `onSaluteThresholdAnomaly` (solo anomalie di salute su soglia fissa).
- `backend/src/services/fleetStateStore.js` — anello automatico (`onSaluteAnomaly`, `pickAvailableReserve`, `clearRepairFlag`), non più agganciato a `health_anomaly`.
- `backend/src/server.js` — nuova route `/api/fleet-control`.
- `dashboard/{index.html,style.css,app.js}` — card "Flotta reale — controllo".
- `docker-compose.yml` — porta 5002, env `FLEET_CONTROL_SERVICE_URL`.
- Documentazione di progetto aggiornata con l'eccezione deliberata al "solo diagnosi".

**Aggiornamento (2026-07-21)**: `fleet_control_service.py` esteso con `/sim/start`, `/sim/stop`, `/sim/status` — la simulazione ROS/Gazebo non parte più in automatico all'avvio dello stack, va avviata dalla dashboard scegliendo la scala. Dettagli in `docs/passi/01-scaffold-infrastruttura.md` (sezione "La simulazione ROS non parte più da sola").
