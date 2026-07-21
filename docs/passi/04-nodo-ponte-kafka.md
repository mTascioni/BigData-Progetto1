# Passo 4 — Nodo-ponte ROS → Kafka

**Obiettivo (da PLAN.md):** `rospy` che si sottoscrive ai topic, compone il messaggio nello schema condiviso (sintetizzando i canali di salute nominali) e pubblica su Kafka (`telemetry`, partizionato per `robot_id`).
**Deliverable atteso:** dati reali in Kafka. *(Suggerimento del piano: registra un rosbag e riproducilo per sviluppare a valle senza tenere Gazebo sempre acceso — fatto, vedi in fondo.)*

## Cosa è stato costruito

Un nuovo nodo nel pacchetto `shf_bringup`: **`scripts/kafka_bridge.py`**, aggiunto a `sim_single_robot.launch` accanto a `graph_navigator`. Si sottoscrive a:

| Topic ROS | Campi telemetria che alimenta |
|---|---|
| `/odom` | `x`, `y`, `theta` (da quaternione via `tf.transformations.euler_from_quaternion`), `v_lin`, `v_ang` |
| `/cmd_vel` | `cmd_v_lin`, `cmd_v_ang` (quello che `move_base` sta comandando, non quello realizzato) |
| `/scan` | `min_obstacle_dist` (minimo dei ray validi, escludendo `inf`/oltre `range_max`) |
| `/move_base/goal` | `goal_node` (nodo del grafo più vicino alla posizione target del goal action) |
| `/move_base/result` | segna il goal come concluso → il robot torna "senza goal attivo" |

I campi non derivabili da ROS sono sintetizzati usando i parametri fissati al Passo 2 in `config/experiment.json` (`health_channels_nominal`), letto all'avvio insieme a `config/warehouse_graph.json`:

- **`battery_pct`**: integrata nel tempo (non ricampionata da zero ogni tick) con il drain/charge rate del `task_state` corrente (`drain_rate_moving_pct_per_min`, `drain_rate_idle_pct_per_min`, `charge_rate_pct_per_min`), clampata [0,100].
- **`motor_current`**, **`motor_temp`**: campionati ad ogni tick da una gaussiana centrata sul valore nominale (`nominal_a`/`nominal_c` ± `noise_std_*`). Nessuna firma di guasto qui — l'iniezione arriva al Passo 6, che andrà a sommarsi a questi valori nominali.
- **`current_edge`**: `(x,y)` proiettato (clampato) su ciascun arco del grafo, arco con distanza minima vince — implementa alla lettera l'invariante "(x,y) mappato sull'arco occupato" di `CLAUDE.md`.
- **`task_state`**: euristica locale a partire dai segnali grezzi — `idle` se non c'è un goal attivo (`charging` se in quel momento il robot è entro 0.5m da un nodo di kind `charging`); se il goal è attivo, `moving` finché `|v_lin|` o `|v_ang|` superano una soglia entro gli ultimi 5s, altrimenti `blocked`. **Nota:** questa è solo un'euristica locale e istantanea, non la detection di deadlock/livelock vera e propria (quella guarda alla flotta nel suo insieme su una finestra temporale ed è compito dello Structured Streaming job del Passo 7).

`ts` usa `time.time()` (orologio reale), non il tempo simulato di Gazebo (`rospy.get_time()`/`/clock`): con `use_sim_time=true` il tempo ROS riparte da zero ad ogni run e non è un epoch valido; inoltre a valle (Kafka/Spark) tutto gira in tempo reale, quindi i timestamp di telemetria devono restare comparabili con l'orologio reale del resto della pipeline, esattamente come farebbe un robot vero.

Pubblicazione: `confluent-kafka` (`pip3 install confluent-kafka`, aggiunto al Dockerfile insieme a `python3-pip`), topic `telemetry`, **key = `robot_id`** (non partizione esplicita: il partizionamento di default di Kafka sull'hash della key garantisce che tutti i messaggi dello stesso robot finiscano sempre sulla stessa partizione, che è la proprietà richiesta — "partizionato per `robot_id`"). Rate di pubblicazione configurabile (`publish_hz`, default 2Hz).

## Scelte tecniche e motivazioni

**`/move_base/goal` invece di un topic custom per `goal_node`.** Il bridge potrebbe calcolare da solo il prossimo nodo obiettivo rileggendo `experiment.json` e tenendo un proprio stato di avanzamento — ma duplicherebbe la logica già in `graph_navigator.py`. Invece si "ascolta" il topic actionlib standard che `move_base` già espone quando riceve un goal: qualunque nodo lo mandi (oggi `graph_navigator`, domani magari un pianificatore diverso), il bridge lo intercetta senza modifiche. Meno codice, meno duplicazione, disaccoppiato da chi genera i goal.

**Battery integrata nel tempo, non ricampionata.** Motor current/temp sono processi stazionari (rumore attorno a un nominale) e si possono ricampionare ad ogni tick senza stato. La batteria no: è un processo cumulativo (si scarica/carica), quindi il bridge mantiene `self.battery_pct` come stato persistente tra i tick e lo aggiorna in base al `task_state` e al tempo reale trascorso (`dt_s` misurato con `rospy.get_time()`, che con `use_sim_time` corrisponde comunque al tempo simulato di Gazebo — corretto per l'integrazione, visto che Gazebo qui gira a real-time factor ~1).

**Producer asincrono, `poll(0)` per ciclo.** `confluent-kafka` è a callback: `produce()` accoda il messaggio e ritorna subito, la consegna reale avviene in background e il callback di errore (`_kafka_error_cb`) viene invocato da `poll()`. Scelto `poll(0)` (non bloccante) dentro il loop `rospy.Rate`, con un `flush()` finale allo shutdown del nodo — così il bridge non rallenta mai il proprio ciclo di pubblicazione per aspettare gli ACK di Kafka.

## Problema incontrato e fix

Primo avvio: `kafka_bridge` crashava subito con `TypeError: _kafka_error_cb() takes 2 positional arguments but 3 were given`. Il callback di `Producer.produce()` in `confluent-kafka` ha firma `(err, msg)`, non solo `(err,)` — corretto aggiungendo il parametro `msg` (non usato, ma richiesto dalla firma). Ricostruita l'immagine e rilanciato: nessun altro errore.

## Verifica

Topic Kafka creato esplicitamente (non affidandosi all'auto-creazione, per controllare il numero di partizioni):

```bash
docker exec shf-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic telemetry --partitions 3 --replication-factor 1
```

Simulazione lanciata come al Passo 3 (`roslaunch shf_bringup sim_single_robot.launch`, ora con `kafka_bridge` incluso), in parallelo `rosbag record` sui topic grezzi. Dump completo dei messaggi Kafka del topic `telemetry` e analisi con uno script Python (non salvato nel repo, verifica una tantum):

- **3404 messaggi** ricevuti su Kafka nell'arco della run.
- **Schema**: ogni messaggio ha tutti e 15 i campi del contratto di `CLAUDE.md`, tipi coerenti (numeri, stringhe, `null` per `goal_node`/`min_obstacle_dist` quando non ancora disponibili).
- **`goal_node`**, sequenza delle transizioni osservate: `None → B → C → F → G → J → H → C → B → A` — combacia esattamente con la `goal_sequence` di `R1` in `config/experiment.json`.
- **`current_edge`**, transizioni osservate: `A-B → B-C → C-F → F-G → G-J → (C-D) → G-J → H-J → (G-J) → H-J → C-H → B-C → B-E → A-B` — segue correttamente il percorso; le brevi oscillazioni fra `G-J`/`H-J`/`C-D` intorno ai nodi `J`/`D` sono un **limite noto e atteso** dell'euristica "arco più vicino per distanza pura": vicino a un nodo dove più archi si toccano (o, per `D`, per una coincidenza geometrica — il segmento `G-J` passa esattamente per il punto `(30,0)` dove si trova il nodo `D`) la proiezione più vicina può oscillare tra due archi per pochi tick. Non impatta questo passo (che deve solo far arrivare dati reali su Kafka); da tenere presente se il Passo 7 userà `current_edge` per la detection di deadlock/livelock — soluzione rimandata a quel passo (es. isteresi: cambiare `current_edge` solo se il nuovo arco resta il più vicino per più tick consecutivi).
- **`task_state`**: nessun falso `blocked` nella run (0 su 3404 messaggi) — coerente con un singolo robot senza conflitti di traffico.
- **`battery_pct`**: 100.0 → 94.91 nell'arco della run (drain coerente con i rate nominali e col tempo passato in `moving` vs `idle`).
- **Partizionamento**: verificato con `--property print.partition=true` che tutti i messaggi di `R1` cadono in modo consistente sulla stessa partizione (0) — comportamento atteso del partizionamento per key. Con più robot attivi (Passo 5) ci si aspetta la key a distribuirli sulle 3 partizioni.
- **Nessun errore/crash** nel log del bridge per l'intera run.

Un piccolo artefatto nei dati raccolti: un salto di 72s nei `ts` a metà dump, dovuto al crash-e-restart durante il debug del bug del callback (due run concatenate nello stesso topic, essendo partito da `--from-beginning`) — non un problema del bridge, solo la cronologia della sessione di test.

**Rosbag registrato** per sviluppo futuro senza dover tenere Gazebo acceso (suggerimento del piano):

```bash
docker exec -d shf-ros bash -c "rosbag record -O r1_baseline.bag /odom /scan /cmd_vel /move_base/goal /move_base/result /tf"
```

Salvato in `ros/bags/r1_baseline.bag` (montato su `./ros/bags` sull'host, escluso da git). `rosbag info`: 29:05 minuti, 170593 messaggi, tutti i topic attesi presenti (`/odom` 52371, `/scan` 8729, `/cmd_vel` 4735, `/tf` 104741, `/move_base/goal` 8, `/move_base/result` 9 — il primo goal manca perché la registrazione è partita qualche secondo dopo il lancio, non un problema per l'uso previsto del bag).

## Stato

- `ros/catkin_ws/src/shf_bringup/scripts/kafka_bridge.py` — nuovo nodo.
- `ros/catkin_ws/src/shf_bringup/{package.xml,CMakeLists.txt}` — dipendenze `nav_msgs`/`sensor_msgs`, script installato.
- `ros/catkin_ws/src/shf_bringup/launch/sim_single_robot.launch` — nodo `kafka_bridge` aggiunto con `kafka_bootstrap`/`publish_hz` come argomenti.
- `ros/Dockerfile` — `python3-pip` + `confluent-kafka==2.4.0`.
- `docker-compose.yml` — mount `./ros/bags:/root/bags` sul servizio `ros`.
- Topic Kafka `telemetry` creato (3 partizioni, replication factor 1).
- `ros/bags/r1_baseline.bag` — rosbag di riferimento per sviluppo offline.

## Prossimo passo

Passo 5 — Multi-robot + scenari: scalare a N TurtleBot con namespacing (ogni robot con il proprio `move_base`/`kafka_bridge`/`graph_navigator` nel proprio namespace ROS) e far emergere davvero gli scenari `deadlock-1`/`livelock-1` già dichiarati in `config/experiment.json` sui corridoi `C-F`/`C-H`.
