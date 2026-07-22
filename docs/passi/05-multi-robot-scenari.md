# Passo 5 — Multi-robot + scenari

**Obiettivo:** scala a N TurtleBot (namespacing) e imposta gli scenari `deadlock`/`livelock` (corridoi a corsia singola + task opposti). Tieni N modesto: uno stack di navigazione per robot è pesante in CPU.
**Deliverable atteso:** flotta reale con conflitti di traffico riproducibili.

## Cosa è stato costruito

Tre TurtleBot3 (`R1`, `R2`, `R3`, come in `config/experiment.json`) nello stesso mondo Gazebo, ciascuno nel proprio namespace ROS, con il proprio `move_base`/`graph_navigator`/`kafka_bridge` indipendenti:

- **`ros/catkin_ws/src/shf_bringup/scripts/gen_robot_description.sh`**: genera l'URDF del TurtleBot3 via `xacro` e inietta il namespacing nei plugin Gazebo (diff_drive, IMU, lidar) e nei nomi dei frame TF (`odom`, `base_footprint`, `base_scan`, `imu_link` → `R1/odom`, `R1/base_footprint`, ...).
- **`launch/move_base_no_map.launch`**: esteso con argomenti `odom_frame`/`base_frame`/`base_scan_frame` (default non-prefissati, per restare compatibile col Passo 3/4 a singolo robot) che sovrascrivono i frame nei costmap.
- **`launch/sim_multi_robot.launch`**: un solo `gzserver` headless condiviso, tre `<group ns="R1|R2|R3">` che spawnano ciascuno il proprio robot (posizioni di spawn = `start_node` di `config/experiment.json`: R1@A(0,0), R2@D(30,0), R3@I(10,-10)) e avviano la pipeline completa (move_base + graph_navigator + kafka_bridge) namespaced.

## Perché serve patchare l'URDF (il problema che TurtleBot3 non risolve da solo)

I pacchetti `turtlebot3_description`/`turtlebot3_simulations` **non supportano nativamente il multi-robot**: i plugin Gazebo del diff_drive, dell'IMU e del lidar non hanno un tag `<robotNamespace>` nello xacro di base, e pubblicano topic (`/cmd_vel`, `/odom`, `/scan`, `/imu`) e frame TF (`odom`, `base_footprint`, `base_scan`, `imu_link`) sempre **globali**, indipendentemente dal `<group ns="...">` del launch file — perché questi plugin girano dentro `gzserver` (un unico processo per l'intero mondo), non come nodi roslaunch namespaced. Anche il file ufficiale `turtlebot3_gazebo/launch/multi_turtlebot3.launch` (letto per riferimento) soffre dello stesso limite se preso alla lettera. Inoltre **tf2 non prefissa mai automaticamente i `frame_id`** in base al namespace roslaunch (a differenza dei nomi dei topic): un nodo in `/R1` che pubblica un frame `"odom"` scrive letteralmente `"odom"` nel `/tf` globale, non `"R1/odom"`.

Per questo `gen_robot_description.sh`:
1. Inietta `<robotNamespace>R1</robotNamespace>` in ogni blocco `<plugin>` (namespacing dei topic — meccanismo standard di `gazebo_ros_pkgs`).
2. Riscrive esplicitamente via `sed` i valori dei tag che diventano `frame_id` (`odometryFrame`, `robotBaseFrame`, `frameName`) con il prefisso del robot, **senza fare affidamento sul comportamento automatico di `tf_prefix`** dei plugin (non garantito/verificabile a priori per questa versione di `gazebo_ros_pkgs`): esplicito e ispezionabile è meglio che implicito.
3. `robot_state_publisher` riceve `tf_prefix` allo stesso modo (pattern standard, identico a `multi_turtlebot3.launch`) per la catena cinematica statica (`base_link`→ruote→lidar→imu).
4. `move_base_no_map.launch` riceve gli stessi frame prefissati via i nuovi argomenti `odom_frame`/`base_frame`/`base_scan_frame`, sovrascrivendo dopo il caricamento degli YAML (compresa la chiave annidata `costmap_common_params_*.yaml`'s `scan.sensor_frame`, facile da dimenticare).

## Problemi incontrati e fix

1. **`cmd_vel_topic` assoluto (`/cmd_vel`) in `move_base_no_map.launch`.** Bug latente dal Passo 3 (irrilevante allora, a singolo robot senza namespace): un topic con `/` iniziale non viene mai risolto relativamente al namespace del gruppo, quindi con 3 robot avrebbero tutti scritto sullo stesso `/cmd_vel` globale. Cambiato il default a `cmd_vel` (relativo).
2. **`graph_navigator.py` mandava i goal nel frame `"odom"` fisso.** Con `move_base` in ascolto sul frame `R1/odom`, i goal in `"odom"` venivano rifiutati (`ERROR: The goal pose passed to this planner must be in the R1/odom frame. It is instead in the odom frame.`) — loop di errori, nessun movimento. Aggiunto il parametro `~odom_frame` (default `"odom"`, compatibile col Passo 3/4) e passato `R1/odom` esplicitamente da `sim_multi_robot.launch`.
3. Verificato (non un bug, solo un controllo) che l'URDF generato da `gen_robot_description.sh` sia XML valido e con i frame correttamente rinominati, prima di usarlo nel launch — un warning di xacro (`--inorder` deprecato) finiva su stderr e poteva sembrare un errore fatale se catturato insieme allo stdout: rimosso `--inorder` (default già in Melodic+) per pulizia.

## Verifica

```bash
docker exec -d shf-ros bash -c "source /root/catkin_ws/devel/setup.bash && roslaunch shf_bringup sim_multi_robot.launch"
```

- **Nodi ROS**: `rosnode list` mostra i 4 nodi attesi per ciascun robot (`/R1/graph_navigator`, `/R1/kafka_bridge`, `/R1/move_base`, `/R1/robot_state_publisher`, e lo stesso per R2/R3) più `/gazebo` — nessun nodo collide sul nome.
- **TF**: `rosrun tf tf_monitor` conferma frame separati e prefissati per ciascun robot (`R1/base_footprint`, `R2/base_footprint`, `R3/base_footprint`, ...); `tf_echo R1/odom R1/base_footprint` risolve correttamente la catena.
- **Movimento indipendente**: le posizioni su `/R1/odom`, `/R2/odom`, `/R3/odom` evolvono in modo indipendente e coerente con i rispettivi task (verificato via `rostopic echo`).
- **Kafka**: telemetria di tutti e 3 i robot verificata sul topic `telemetry`. Partizionamento per `robot_id` confermato — con solo 3 chiavi su 3 partizioni si osserva una collisione di hash attesa (R1 → partizione 0, sia R2 che R3 → partizione 2, partizione 1 vuota): comportamento corretto di Kafka (l'hash della key non garantisce partizioni distinte con cardinalità bassa), non un bug — la proprietà che conta davvero (ordine garantito per singolo robot) resta valida.
- **Nessun errore/crash** nei nodi ROS per l'intera run (unica eccezione: gli `ERROR` del frame sbagliato **prima** del fix del punto 2 sopra, spariti dopo la correzione).

## Il conflitto di traffico trovato

L'ipotesi di partenza era che `R1` e `R2` (che percorrono l'anello di storage in direzioni opposte, cfr. `scenarios` in `config/experiment.json`) si scontrassero su uno dei due corridoi a corsia singola (`C-F`, `C-H`). Il calcolo a mano (velocità ~0.2 m/s, anello di 60m, offset di partenza R1=0s/R2=5s) indicava invece che i due si sarebbero incrociati a metà dell'arco largo `G-J` (`capacity: 2`) — **verificato empiricamente sul primo run**: nessun conflitto lì, entrambi passano senza intoppi.

È emerso però un conflitto reale e più interessante, non previsto in fase di progettazione dello scenario: **`R3` completa il proprio task (`B,E,F,G`) e resta "parcheggiato" fermo esattamente sul nodo `G`** (task_state torna `idle`, nessuna logica di "liberare" i nodi condivisi). Quando più tardi `R2` deve raggiungere lo stesso nodo `G` (task `C,H,J,G,F,C,D`), il local planner di `move_base` non riesce a completare l'avvicinamento finale — il corpo di `R3` (più il raggio di inflazione di 1.0m) occupa fisicamente il goal:

```
1784541656251  R3 idle, fermo su (29.96, 10.02)  ~ nodo G
...
1784541657273..666622  R2 fermo su (30.46, 8.64), ~1.4m da R3, task_state -> blocked
[WARN] Clearing both costmaps outside a square (3.00m) large centered on the robot.
[WARN] Rotate recovery behavior started.
[ERROR] Aborting because a valid plan could not be found. Even after executing all recovery behaviors
[WARNING] R2: nodo 'G' NON raggiunto (stato move_base=4)   # 4 = ABORTED
```

`kafka_bridge` ha classificato correttamente la fase di stallo come `task_state: "blocked"` (velocità nulla per >5s con un goal attivo) prima ancora che `move_base` abortisse formalmente — la telemetria su Kafka porta già il segnale utile per una futura detection.

**Perché è riproducibile, non un caso isolato:** il tempismo è strutturale, non casuale. `R3` deve percorrere 4 archi da 10m (~180-200s dal suo `start_time_s=10`) per arrivare a `G`; `R2` deve percorrere 4 archi (10+10+10+20m, quest'ultimo il lato lungo dell'anello) per arrivarci a sua volta (~230-255s dal suo `start_time_s=5`). `R3` finisce sempre prima, con margine — quindi il conflitto si ripresenta ad ogni run con questi task, non è un artefatto di una singola esecuzione fortunata/sfortunata.

**Nota per il Passo 7:** i due corridoi a corsia singola `C-F`/`C-H` restano corretti e pronti (verificati strutturalmente al Passo 3/4 con un solo robot); semplicemente, con l'attuale programmazione dei task, il primo conflitto reale che emerge è di un tipo diverso — un nodo condiviso occupato da un robot che ha terminato, non due robot in attesa reciproca su un arco. Entrambi i tipi di conflitto sono `task_state: "blocked"` in telemetria e sono materiale valido per la detection dello Structured Streaming job (deadlock/livelock su finestra), che dovrà gestire genericamente "un robot fermo con un goal attivo per una finestra di tempo", indipendentemente dalla causa specifica.

## Stato

- `ros/catkin_ws/src/shf_bringup/scripts/gen_robot_description.sh` — nuovo, genera URDF namespaced.
- `ros/catkin_ws/src/shf_bringup/launch/move_base_no_map.launch` — `cmd_vel_topic` relativo, frame parametrizzati.
- `ros/catkin_ws/src/shf_bringup/launch/sim_multi_robot.launch` — nuovo, 3 robot namespaced.
- `ros/catkin_ws/src/shf_bringup/scripts/graph_navigator.py` — parametro `~odom_frame` aggiunto.
- `config/experiment.json` e `config/warehouse_graph.json` — **non modificati** in questo passo: il conflitto trovato è emerso dal design esistente, non ha richiesto di forzare artificialmente i tempi.

## Prossimo passo

Passo 6 — Layer di fault injection: nel nodo-ponte, leggere `fault_schedule` da `config/experiment.json` e sommare la firma di guasto alla telemetria di salute per il robot/finestra temporale giusti, loggando in `injected_faults` (ground truth per il Passo 13).
