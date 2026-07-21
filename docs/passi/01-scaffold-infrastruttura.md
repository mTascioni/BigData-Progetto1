# Passo 1 — Scaffold + infrastruttura

**Obiettivo (da PLAN.md):** struttura del repo e `docker-compose.yml` con Kafka, Spark, servizio Node e container ROS (Noetic + TurtleBot3; noVNC per la GUI in debug).
**Deliverable atteso:** i container si avviano, Kafka raggiungibile.

## Struttura del repo creata

```
BigData-Progetto1/
  docker-compose.yml
  backend/
    Dockerfile
    package.json
    src/server.js        # placeholder Express con GET /health
  ros/
    Dockerfile
    supervisord.conf
    xstartup
    entrypoint.sh
  config/            (vuota, popolata al Passo 2)
  generator/         (vuota, popolata da un passo successivo)
  streaming/         (vuota, popolata al Passo 7)
  predictive/        (vuota, popolata al Passo 9)
  offline/           (vuota, popolata al Passo 8)
  dashboard/         (vuota, popolata al Passo 11)
  eval/              (vuota, popolata al Passo 13)
  docs/passi/        (questo file e i successivi)
```

Le cartelle non ancora popolate contengono un `.gitkeep` per essere tracciate da git fin da subito, secondo la struttura di repo fissata in `CLAUDE.md`.

## Servizi in `docker-compose.yml`

| Servizio | Immagine | Ruolo in questo passo |
|---|---|---|
| `kafka` | `apache/kafka:3.7.0` | Broker in modalità KRaft (senza Zookeeper), un solo nodo che fa sia da broker che da controller |
| `spark-master` | `bitnamilegacy/spark:3.5.6` | Master dello standalone cluster Spark |
| `spark-worker` | `bitnamilegacy/spark:3.5.6` | Worker registrato sul master, eseguirà i job di detection dal Passo 7 |
| `backend` | build locale da `backend/` | Scaffold Node/Express, solo endpoint `/health` per ora |
| `ros` | build locale da `ros/` | ROS Noetic desktop-full + pacchetti TurtleBot3 + stack noVNC (Xtigervnc + websockify) per debug grafico |

## Scelte tecniche e motivazioni

**Kafka in KRaft mode, non Zookeeper.** Un solo container invece di due (broker+zookeeper): meno servizi da orchestrare, meno RAM, e KRaft è ormai la modalità raccomandata da Apache Kafka anche per setup piccoli/didattici. Il broker espone il listener interno (`kafka:9092`, usato dagli altri container) e uno esterno (`localhost:9094`, per debug da host).

**Spark: `bitnamilegacy/spark:3.5.6` invece di `bitnami/spark:3.5`.** Il tag `bitnami/spark:3.5` indicato inizialmente non esiste più: Bitnami ha spostato le immagini gratuite fuori manutenzione dal namespace `bitnami/*` (ormai riservato al loro catalogo a pagamento) al namespace `bitnamilegacy/*`. Verificato interrogando la Docker Registry HTTP API v2 (`registry-1.docker.io`) prima di scegliere il tag, per essere sicuri che l'immagine sia effettivamente disponibile e pinnata su una versione precisa (non una tag mobile) — importante per la riproducibilità dell'esperimento.

**Container ROS costruito da zero (non un'immagine noVNC di terze parti).** Base `osrf/ros:noetic-desktop-full`, sopra la quale vengono installati via apt i pacchetti `ros-noetic-turtlebot3*` e uno stack noVNC minimale (Xtigervnc + websockify + fluxbox), orchestrato da `supervisord` con tre processi: `vncserver`, `novnc`, `roscore`. Scelta per trasparenza e riproducibilità: niente immagini di terze parti non verificate, tutto ricostruibile da un Dockerfile leggibile. Il container espone la porta `6080` (client noVNC via browser, `http://localhost:6080/vnc.html`) e la `11311` (roscore).

**Backend Node scaffold minimale.** Solo `GET /health`, nessuna logica applicativa: la logica di consumo Kafka/websocket/TAG arriva nei passi dedicati (10-11). Serve solo a verificare che il container si builda e comunica con la rete Docker.

## Problemi incontrati e soluzioni

1. **Tag Spark inesistente.** `docker compose pull` falliva su `bitnami/spark:3.5` (`not found`). Interrogata la Docker Registry API (`/v2/bitnami/spark/tags/list`, risposta vuota) e poi `/v2/bitnamilegacy/spark/tags/list`, che elenca ancora le versioni storiche. Risolto pinnando `bitnamilegacy/spark:3.5.6` (ultima patch 3.5.x disponibile).
2. **tigervncserver rifiuta di partire.** Con `-SecurityTypes None` (nessuna password, scelta deliberata: è un container di debug locale, non esposto pubblicamente) il server si rifiuta di avviarsi senza il flag esplicito `--I-KNOW-THIS-IS-INSECURE`. Aggiunto il flag in `supervisord.conf`. Nota per la relazione/uso: questa configurazione è accettabile solo per debug locale in rete fidata, non andrebbe usata così su un host esposto.

## Comandi eseguiti e verifica

```bash
docker compose config -q                 # validazione sintattica del compose
docker compose pull kafka spark-master spark-worker
docker compose build ros                 # build immagine ROS+TurtleBot3+noVNC (~qualche minuto)
docker compose build backend
docker compose up -d
```

Verifiche eseguite, tutte con esito positivo:

- **Kafka raggiungibile**: creata e listata una topic di prova con `kafka-topics.sh --bootstrap-server localhost:9092` dentro il container (poi rimossa).
- **Spark**: `curl http://localhost:8080` (UI master) → `200`; `curl http://localhost:8081` (UI worker) → `200`; la UI del master riporta `Workers (1)`, quindi il worker si è registrato correttamente sul master.
- **Backend**: `curl http://localhost:3000/health` → `{"status":"ok","service":"self-healing-fleet-backend"}`.
- **ROS**: `rostopic list` (dentro il container) → `/rosout`, `/rosout_agg`, quindi `roscore` è attivo; `rospack find turtlebot3_gazebo` e `rospack find turtlebot3_description` risolvono correttamente i pacchetti TurtleBot3; variabile `TURTLEBOT3_MODEL=burger` impostata.
- **noVNC**: `curl http://localhost:6080/vnc.html` → `200`; processi `Xtigervnc`, `websockify`, `roscore` tutti attivi e stabili (verificato via `ps aux` nel container, nessun riavvio dopo il fix del flag `--I-KNOW-THIS-IS-INSECURE`).

Stato finale dei container:

```
NAME               IMAGE                        STATUS         PORTS
shf-backend        self-healing-fleet-backend   Up             0.0.0.0:3000->3000/tcp
shf-kafka          apache/kafka:3.7.0           Up             0.0.0.0:9094->9094/tcp
shf-ros            self-healing-fleet-ros       Up             0.0.0.0:6080->6080/tcp, 0.0.0.0:11311->11311/tcp
shf-spark-master   bitnamilegacy/spark:3.5.6    Up             0.0.0.0:7077->7077/tcp, 0.0.0.0:8080->8080/tcp
shf-spark-worker   bitnamilegacy/spark:3.5.6    Up             0.0.0.0:8081->8081/tcp
```

Utilizzo risorse a regime (`docker stats`): ~850 MB di RAM totali sui 5 container, CPU trascurabile a riposo — margine ampio prima di aggiungere Gazebo (Passo 3), che sarà il consumatore di risorse più pesante.

## Aggiornamento (2026-07-21): avvio a comando singolo

Dal Passo 11 in poi la pipeline aveva accumulato diversi processi da lanciare a mano via `docker exec` dopo `docker compose up` (`detection_job.py`, `query_service.py`, `generator_service.py`, la simulazione ROS multi-robot) — segnalato dall'utente come scomodo: "docker compose up" avviava solo l'infrastruttura di base (Kafka, Spark, backend), non la pipeline funzionante. Da questo aggiornamento, **un solo `docker compose up -d` basta**, senza alcun comando manuale successivo.

**`spark-master`**: nuovo script `streaming/start-master.sh`, referenziato come `command:` del servizio in `docker-compose.yml` (override solo per `spark-master`, `spark-worker` resta sul comportamento originale dell'immagine). Avvia il master Spark (comportamento bitnami invariato) in background, attende che risponda su `:8080`, poi lancia anche `query_service.py` e `detection_job.py` — gli stessi comandi che finora andavano rilanciati a mano dopo ogni riavvio del container (vedi i "Comando per rilanciare" nei Passi 7/10). Lo script vive nella cartella già montata `./streaming`, nessun rebuild dell'immagine necessario per modificarlo.

**`ros`**: `supervisord.conf` (già gestiva `roscore`/VNC) esteso con due nuovi `[program:...]`: `generator_service` (Passo 11 estensione) e `sim_multi_robot` (la simulazione multi-robot del Passo 5), entrambi con `autorestart=true`. A differenza dello script Spark, `supervisord.conf` è copiato nell'immagine (non montato): modificarlo richiede un rebuild (`docker compose build ros`).

**Bug di robustezza trovato durante la verifica**: con tutto lo stack avviato insieme dal nulla, il backend a volte tentava di connettersi a Kafka/creare i consumer *prima* che Kafka fosse davvero pronto (topic non ancora creati), falliva una volta sola e restava morto per sempre — nessun retry. Fix in `fleetStateStore.js`/`anomalyStream.js`: entrambi i consumer ora ritentano ogni 5s finché non riescono a connettersi, invece di arrendersi al primo errore.

**Verificato per davvero**: `docker compose down && docker compose up -d` da zero, **nessun comando manuale successivo** — dopo l'attesa naturale di boot (Kafka + Spark + Gazebo, circa 1-2 minuti), `curl` su `/health` di `generator_service` (`:5001`) e `query_service` (`:5000`) rispondono `ok`, e `/api/fleet` del backend mostra già i 3 robot reali in movimento, tutto senza toccare `docker exec`.

## Aggiornamento (2026-07-21): la simulazione ROS non parte più da sola

Revisione parziale di quanto sopra, su richiesta esplicita dell'utente: si era accorto che i robot reali si muovevano già prima ancora di aprire la dashboard, e si aspettava di poter decidere lui quando farli partire (e con quale scala, small/large — Passo 14). L'avvio automatico di `sim_multi_robot` (unico fra i programmi supervisord elencati sopra) è stato quindi disattivato: **Kafka, Spark, `generator_service`, `fleet_control_service`, `eval_service` restano ad avvio automatico** (nessuna azione richiesta per averli disponibili), ma la simulazione ROS/Gazebo ora **richiede un comando esplicito** dell'utente — dalla dashboard (card "Flotta reale — controllo", nuovo selettore di scala + bottoni Avvia/Ferma) o da riga di comando (`supervisorctl start/stop sim_multi_robot` dentro il container `ros`).

Realizzato riusando `fleet_control_service.py` (già presente dal Passo 14) invece di creare un nuovo servizio: due nuove route, `POST /sim/start {"scale": "small"|"large"}` e `POST /sim/stop`, che richiamano `supervisorctl start/stop sim_multi_robot` — lo stesso identico meccanismo già usato da `test/conftest.py` per mettere in pausa la simulazione durante i test, quindi nessuna incompatibilità con la suite esistente. Un dettaglio tecnico non ovvio: il comando di un programma supervisord è statico nel file di configurazione, non può ricevere argomenti diversi ad ogni `start`; la scala scelta viene quindi scritta in un file marcatore (`/tmp/shf_scale`) **prima** di invocare `supervisorctl start`, e il comando del programma lo legge (`SCALE=$(cat /tmp/shf_scale 2>/dev/null || echo small)`) prima di lanciare `roslaunch ... scale:=$SCALE`. Un tentativo di avvio mentre la simulazione è già in corso (o di stop mentre è già ferma) risponde `409`, stesso pattern già in uso per il generatore sintetico (Passo 12).

**Verificato**: stato iniziale `STOPPED` dopo boot pulito; avvio con `scale=small` (~16s, il tempo di `startsecs` di supervisord) → 4 robot reali (R1-R4) compaiono in `/api/fleet`; un secondo avvio mentre già in corso → `409`; stop → i robot spariscono; avvio con `scale=large` → tutti e 8 i robot (R1-R8) compaiono entro la finestra di boot naturale di Gazebo.

## Correzione (2026-07-21): detection_job.py/query_service.py senza retry all'avvio

Segnalato indirettamente dall'utente ("la simulazione parte molto in ritardo"), diagnosticato scaricando e riavviando l'intero stack più volte per riprodurlo. **Causa reale**: `streaming/start-master.sh` attende che il master Spark risponda su `:8080` prima di lanciare `query_service.py`/`detection_job.py`, ma non aspetta (né ritenta) che **Kafka** sia davvero pronto — a differenza dei consumer Node del backend (`fleetStateStore.js`/`anomalyStream.js`, già corretti per lo stesso motivo in un aggiornamento precedente), questi due `spark-submit` falliscono UNA VOLTA SOLA con `UnknownTopicOrPartitionException` se Kafka non è ancora pronto, e restano morti per sempre: Gazebo/ROS continuano a funzionare normalmente, ma `fleet_state` non viene mai scritto — sulla dashboard sembra che "la simulazione non parta", quando in realtà è la pipeline di detection che non è mai partita per davvero.

**Fix**: ciascuno dei due `spark-submit` è ora avviato dentro un ciclo `while true; do ...; sleep 5; done` in una subshell — se il processo termina (per qualunque motivo), riparte da solo dopo 5s. Trovato e corretto un bug nel fix stesso durante la verifica: `set -e` (in cima allo script) si propaga nelle subshell e le uccide al primo fallimento, **prima** che possano ritentare — serve un `set +e` esplicito dentro ciascuna subshell.

**Verificato**: riavvio pulito dell'intero stack più volte, osservato il crash iniziale (riproducibile, dipende dalla velocità con cui Kafka diventa pronto) seguito dal retry automatico e dalla ripresa entro pochi secondi; verificato anche forzando un crash a mano (`kill -9` sul processo) — riparte da solo. Vedi anche `docs/passi/11-dashboard.md` per il problema collegato (allocazione core di `detection_job.py` troppo alta per la flotta reale, causa della "lentezza a scatti" osservata anche dopo che la pipeline parte).

## Prossimo passo

Passo 2 — Contratti dati: `config/warehouse_graph.json` e `config/experiment.json`, fissando lo schema del messaggio di telemetria già definito in `CLAUDE.md`.
