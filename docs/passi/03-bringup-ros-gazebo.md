# Passo 3 — Bring-up ROS/Gazebo (de-rischia subito)

**Obiettivo (da PLAN.md):** un **singolo** TurtleBot3 in Gazebo (Noetic), headless, che naviga sul grafo mandando goal nodo per nodo (`move_base`). Verifica posa/odometria/lidar.
**Deliverable atteso:** un robot che si muove sul grafo e pubblica sui topic ROS.

## Cosa è stato costruito

Un pacchetto catkin `shf_bringup` (in `ros/catkin_ws/src/shf_bringup/`, copiato e compilato nell'immagine `ros` al build-time) con:

- **`launch/sim_single_robot.launch`**: avvia Gazebo headless (`gzserver`, nessun `gzclient`) sul mondo vuoto di `turtlebot3_gazebo`, spawna un TurtleBot3 `burger`, avvia `robot_state_publisher` (tf del robot) e `move_base`.
- **`launch/move_base_no_map.launch`**: variante del `move_base.launch` standard di `turtlebot3_navigation`, con un costmap globale senza mappa statica al posto di `map_server` + `AMCL`.
- **`config/global_costmap_no_map.yaml`**: costmap globale nel frame `odom` (non `map`), dimensione fissa 60×40m che copre l'intera bounding box del magazzino, nessun `static_layer`.
- **`scripts/graph_navigator.py`**: nodo `rospy` che legge `config/warehouse_graph.json` e `config/experiment.json` (montati nel container in sola lettura), ricava la `goal_sequence` del robot richiesto e la invia a `move_base` un nodo alla volta via `actionlib`, aspettando il completamento di ciascun goal prima di inviare il successivo.

## Scelte tecniche e motivazioni

**Nessuna mappa statica, nessun AMCL/SLAM.** Il grafo del magazzino (Passo 2) è un roadmap **logico**, non una mappa metrica delle pareti: `CLAUDE.md` descrive i robot che "seguono la roadmap" e mappano `(x,y)` sull'arco occupato, non che debbano localizzarsi rispetto a ostacoli reali. Costruire/salvare una mappa via SLAM per poi fare AMCL sarebbe lavoro extra non richiesto da questo passo (il progetto è di Big Data, non di robotica — vedi invariante in `CLAUDE.md`). Scelta: `global_costmap` senza `static_layer`, frame `odom`, dimensione fissa abbastanza grande da coprire tutto il magazzino; il robot si fida della propria odometria (accurata in simulazione). `move_base` pianifica ed evita ostacoli dinamici rilevati dal lidar tramite `obstacle_layer`/`inflation_layer`, sufficiente per un solo robot in un mondo vuoto.

**Mondo Gazebo vuoto, nessuna parete fisica lungo il grafo.** Le "corsie singole" (`capacity: 1`) del grafo sono un vincolo *logico* che verrà fatto rispettare dalla logica applicativa (detection deadlock/livelock, Passo 7), non da muri fisici in Gazebo. Costruire una geometria 3D fedele al grafo non è necessario per il deliverable di questo passo ("un robot che si muove e pubblica sui topic") e avrebbe aggiunto complessità (modellazione, collisioni) senza valore per gli obiettivi Big Data del progetto.

**`dwa_local_planner` mancante nell'immagine, aggiunto via apt.** `ros-noetic-desktop-full` + i pacchetti `turtlebot3*` installati al Passo 1 non includono `dwa_local_planner` (verificato con `rospack find`, mancante; verificati invece presenti `move_base`, `base_local_planner`, `costmap_2d`, `amcl`, `map_server`, `turtlebot3_navigation`, `turtlebot3_slam`, tutti dipendenze transitive del pacchetto `ros-noetic-turtlebot3`, non dichiarate esplicitamente nel Dockerfile del Passo 1). Aggiunto `ros-noetic-dwa-local-planner` al Dockerfile per poter riusare i parametri DWA già forniti da `turtlebot3_navigation` (`dwa_local_planner_params_burger.yaml`) invece di scriverne di nuovi a mano.

**Config montata da bind mount, codice ROS copiato nell'immagine.** `config/*.json` è dato, non codice: montato in sola lettura (`./config:/workspace/config:ro`) così cambiare `experiment.json` non richiede un rebuild dell'immagine. Il pacchetto `shf_bringup` invece è `COPY`-ato e compilato (`catkin_make`) al build time, per un'immagine autosufficiente e riproducibile senza dipendere da bind mount del codice sorgente.

## Problema incontrato e fix

Nel primo giro di test il robot non raggiungeva il nodo `J`: `move_base` era ancora attivo (stato `ACTIVE`) quando lo script troncava l'attesa a un timeout fisso di 60s. Causa doppia:

1. **Refuso nel Passo 2**: l'arco `G-J` era dichiarato `"length": 10` in `config/warehouse_graph.json`, ma le coordinate reali di `G` (30,10) e `J` (30,-10) distano 20m — è il lato est del magazzino che chiude l'anello direttamente, non una tratta corta. Corretto a `"length": 20`. Scritto ed eseguito uno script di validazione che confronta, per ogni arco, la `length` dichiarata con la distanza euclidea reale tra i nodi: dopo il fix, tutti gli 11 archi coincidono.
2. **Timeout fisso troppo corto nello script**: a ~0.15-0.2 m/s (velocità osservata del burger con questi parametri DWA), 20m richiedono più di 60s. Sostituito il timeout fisso con uno proporzionale alla distanza del segmento (`distanza / 0.15 + 20s`, minimo 60s).

Dopo il fix, rifatto il giro completo da zero: tutti i 9 nodi raggiunti, incluso `J`.

## Verifica

Comando di lancio (dentro il container `ros`, con `roscore`/noVNC già attivi da supervisord):

```bash
docker exec -d shf-ros bash -c "source /root/catkin_ws/devel/setup.bash && roslaunch shf_bringup sim_single_robot.launch"
```

**Topic ROS pubblicati e verificati raggiungibili** (`rostopic list`/`rostopic echo`): `/odom`, `/scan`, `/imu`, `/joint_states`, `/tf`, `/gazebo/model_states`, oltre a tutti i topic di `move_base` (costmap, piani, stato dell'azione).

- **Posa/odometria**: posizione iniziale (0,0) = nodo `A`; posizione finale dopo il giro completo `x=0.012, y=0.018` — coincide (entro pochi cm) col nodo `A` di partenza, come atteso per un percorso ad anello chiuso.
- **Lidar**: `/scan` pubblica 360 raggi per messaggio, `range_min=0.12m`, `range_max=3.5m` (valori nominali del LDS del TurtleBot3 burger) — canale funzionante.
- **Navigazione nodo per nodo**, log reale del nodo `graph_navigator` per il robot `R1` (task da `config/experiment.json`, sequenza `B→C→F→G→J→H→C→B→A`):

  ```
  R1: move_base pronto, inizio la sequenza di 9 nodi: ['B','C','F','G','J','H','C','B','A']
  R1: invio goal verso il nodo 'B' (10.0, 0.0)   → R1: nodo 'B' raggiunto
  R1: invio goal verso il nodo 'C' (20.0, 0.0)   → R1: nodo 'C' raggiunto
  R1: invio goal verso il nodo 'F' (20.0, 10.0)  → R1: nodo 'F' raggiunto      (corridoio a corsia singola C-F)
  R1: invio goal verso il nodo 'G' (30.0, 10.0)  → R1: nodo 'G' raggiunto
  R1: invio goal verso il nodo 'J' (30.0, -10.0) → R1: nodo 'J' raggiunto      (tratta piu' lunga, 20m)
  R1: invio goal verso il nodo 'H' (20.0, -10.0) → R1: nodo 'H' raggiunto      (corridoio a corsia singola C-H)
  R1: invio goal verso il nodo 'C' (20.0, 0.0)   → R1: nodo 'C' raggiunto
  R1: invio goal verso il nodo 'B' (10.0, 0.0)   → R1: nodo 'B' raggiunto
  R1: invio goal verso il nodo 'A' (0.0, 0.0)    → R1: nodo 'A' raggiunto
  R1: sequenza di task completata
  ```

  Nessun errore/traceback nel log completo della run (`grep -iE 'error|traceback|exception'` senza risultati).
- **Risorse**: con Gazebo headless + `move_base` attivi, il container usa ~2 core CPU e ~340MB di RAM (`docker stats`) — margine ampio prima di scalare a più robot (Passo 5).

Al termine della verifica la simulazione è stata fermata (`gzserver`, `move_base`, `graph_navigator` terminati) per liberare risorse; il container `ros` resta comunque attivo con `roscore`/noVNC come impostato al Passo 1, pronto per essere rilanciato con lo stesso comando.

## Stato

- `ros/catkin_ws/src/shf_bringup/` — pacchetto catkin creato e compilato nell'immagine.
- `ros/Dockerfile` — aggiunto `ros-noetic-dwa-local-planner`, `COPY` + `catkin_make` del workspace.
- `docker-compose.yml` — aggiunto il mount `./config:/workspace/config:ro` sul servizio `ros`.
- `config/warehouse_graph.json` — corretta la `length` dell'arco `G-J` (10 → 20).
- `ros/catkin_ws/src/shf_bringup/scripts/graph_navigator.py` — timeout per-goal reso proporzionale alla distanza invece che fisso.

## Prossimo passo

Passo 4 — Nodo-ponte ROS → Kafka: un nodo `rospy` che si sottoscrive a `/odom`, `/scan`, `/cmd_vel` (e a `move_base`/al `graph_navigator` per `task_state`/`current_edge`/`goal_node`), sintetizza i canali di salute nominali (usando i parametri di `health_channels_nominal` fissati al Passo 2) e pubblica su Kafka nello schema di telemetria condiviso.
