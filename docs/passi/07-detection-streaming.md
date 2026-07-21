# Passo 7 — Detection in streaming (REAL-TIME, PySpark)

**Obiettivo (da PLAN.md):** consumare `telemetry`. Salute: soglie + Isolation Forest. Livelock: finestra in cui il robot è attivo ma la distanza sul grafo dal `goal_node` non cala + revisite di nodi. Deadlock: >=2 robot `blocked` in mutua contesa sugli archi su finestra. Scrivere `anomalies` e lo stato flotta su `fleet_state`.
**Deliverable atteso:** anomalie in tempo reale + stato per la dashboard.

Il passo più corposo finora: un vero job PySpark Structured Streaming, con tre meccanismi di detection indipendenti sulla stessa sorgente Kafka.

## Cosa è stato costruito

**`streaming/detection_job.py`** — il job. Legge `telemetry`, e mantiene tre query streaming indipendenti (stessa sorgente, sink diversi):

1. **Salute** (per messaggio, trigger 2s): soglie statiche (`motor_temp > 55°C`, `motor_current > 2.5A`, `battery_pct < 20%`) **e** un Isolation Forest (scikit-learn) sul vettore `(motor_temp, motor_current, battery_pct, v_lin, min_obstacle_dist)`, applicato via `pandas_udf`. Scrive **ogni** messaggio arricchito su `fleet_state` (stato live per la dashboard) e, se una delle due tecniche segnala anomalia, anche su `anomalies`.
2. **Livelock** (finestra scorrevole 30s/10s per `robot_id`): il robot è `moving` ma su tutta la finestra la distanza-sul-grafo dal `goal_node` non cala (`max-min < 0.5m`) e resta confinato a ≤3 archi distinti — "gira a vuoto" senza avvicinarsi alla meta.
3. **Deadlock** (finestra scorrevole 20s/10s per `current_edge`): se nella finestra compaiono **≥2 robot distinti** con `task_state=blocked` sullo stesso arco.

**`streaming/isolation_forest_model.py`** + **`streaming/train_isolation_forest.py`**: nessuno storico reale è ancora disponibile (arriva al Passo 8), quindi il modello è allenato su un campione sintetico di telemetria "nominale" generato dagli stessi parametri `health_channels_nominal` di `config/experiment.json` (stessa fonte di verità usata dal nodo-ponte per sintetizzare i canali di salute, coerenza fra chi genera i dati nominali e chi allena il detector). Il modello allenato (`streaming/models/isolation_forest.pkl`, ~1.4MB) è committato nel repo per non doverlo riallenare ad ogni avvio; `detection_job.py` lo ricrea comunque al volo se il file manca (fallback robusto).

**"Distanza sul grafo dal goal_node"**: Floyd-Warshall su `config/warehouse_graph.json` (~10 nodi, calcolo trascurabile), eseguito una volta all'avvio del job e distribuito con `broadcast`. Un UDF proietta `(x,y)` sul nodo più vicino e guarda la distanza precalcolata verso `goal_node`.

**`spark/Dockerfile`** (nuovo): fin qui `spark-master`/`spark-worker` usavano l'immagine `bitnamilegacy/spark:3.5.6` diretta; ora la buildano con `pandas`/`numpy`/`pyarrow`/`scikit-learn` aggiunti (servono agli executor per il `pandas_udf`). Il connettore Kafka (`spark-sql-kafka-0-10_2.12:3.5.6`) non è nell'immagine: viene risolto da Maven a runtime via `--packages`, con la cache Ivy su un volume Docker dedicato (`spark-ivy-cache`) per non riscaricarlo ad ogni riavvio.

## Scelte tecniche e motivazioni

**Tre query streaming separate invece di una sola.** Salute è per-messaggio (nessuna finestra), livelock/deadlock sono aggregazioni a finestra con semantiche diverse (per `robot_id` vs per `current_edge`). Impastarle in un'unica query complicherebbe inutilmente la logica; tre query indipendenti (ciascuna col proprio checkpoint) sono più leggibili, e Structured Streaming supporta nativamente più `writeStream` sulla stessa sorgente letta una volta.

**`foreachBatch` invece del sink Kafka nativo diretto.** Permette di scrivere sia su `fleet_state` che (se necessario) su `anomalies` nello stesso micro-batch della query salute, e di fare l'early-exit su batch vuoti — con il sink Kafka dichiarativo puro servirebbero due query separate anche per questa parte, con overhead di lettura Kafka raddoppiato.

**Isolation Forest allenato su dati sintetici, non su `foreachBatch`-online.** Un modello che si auto-allena sui dati che sta anche giudicando rischia di "abituarsi" ai guasti stessi (specialmente con pochi robot). Allenarlo offline sui parametri nominali già noti (Passo 2) è più semplice, deterministico (seed fissato) e coerente con l'architettura: il vero re-training su storico reale è un miglioramento naturale del Passo 8+, non un requisito di questo passo.

**Soglie *e* Isolation Forest, non uno dei due.** Verificato empiricamente (vedi sotto) che si comportano in modo complementare: nello stesso test, la soglia su `motor_temp` scattava mentre l'Isolation Forest no (temperatura isolata alta, ma il resto del vettore "sembrava" nominale), mentre sullo spike di corrente il pattern si è invertito parzialmente. Usarle insieme è più robusto di scegliere una delle due.

## Problemi incontrati e fix (parecchi — prima volta che Spark viene davvero usato)

1. **`bitnamilegacy/spark` senza pandas/numpy/pyarrow/scikit-learn.** Verificato con `python3 -c "import sklearn"` → `ModuleNotFoundError`. Creato `spark/Dockerfile` con `pip3 install` (funziona senza root: l'utente di default, uid 1001 gid 0, ha permessi di scrittura in user-site).
2. **Permessi sul volume Docker per la cache Ivy.** Un volume nominato nuovo viene creato `root:root`, non scrivibile dall'utente 1001 del container nonostante condivida il gruppo 0 (il bit di scrittura di gruppo non era settato). Fix: `chmod 777` sul path del volume dopo la creazione.
3. **`UserGroupInformation`/Kerberos: `LoginException: NullPointerException: invalid null input: name`.** L'immagine bitnami non ha una entry `/etc/passwd` per l'uid 1001 (funziona per i processi avviati dal suo `entrypoint.sh`, che gestisce le cose diversamente, ma non per uno `spark-submit` lanciato a mano via `docker exec`): il login OS-level di Hadoop (usato internamente da Spark anche senza HDFS) non trova un nome utente per l'uid e fallisce. `HADOOP_USER_NAME=spark` **non** basta (il fallimento è nel login OS-level via `UnixLoginModule`, prima che quella variabile venga consultata). Fix definitivo nel Dockerfile: aggiunta una riga `/etc/passwd` per uid 1001.
4. **`pandas_udf` con signature non riconosciuta.** Usando type hint fra virgolette (`"pd.Series"`, per evitare un import a livello di modulo) PySpark non riesce a inferire il tipo di funzione pandas_udf (`UNSUPPORTED_SIGNATURE`): l'inferenza ispeziona gli oggetti-tipo reali, non stringhe. Fix: `import pandas as pd` a livello di modulo e hint non quotati.
5. **`countDistinct` non supportato in streaming.** `Distinct aggregations are not supported on streaming DataFrames/Datasets` — sostituito con `approx_count_distinct` (sufficiente: serve solo a distinguere "pochi archi" da "molti", non un conteggio esatto).
6. **Bug reale: `robot_id` perso nel payload JSON.** La funzione `to_kafka()` escludeva la colonna usata come *key* Kafka dal *value* JSON (per non duplicarla) — risultato, `fleet_state` e le anomalie di tipo `salute` avevano `robot_id` presente solo nella key Kafka, assente dal JSON. Un consumer che legge solo il value (es. `kafka-console-consumer` di default, o un futuro consumer Parquet/dashboard non scritto per estrarre la key) vedeva `robot_id: null`. Trovato durante la verifica end-to-end (non dal test sintetico iniziale, che per caso non lo mostrava). Fix: la key resta *anche* nel payload, stesso pattern già usato in `kafka_bridge.py`.

## Verifica

### 1. Dati sintetici mirati (un caso per ciascun meccanismo)

Prodotti a mano su Kafka (topic `telemetry`) messaggi costruiti apposta:

- **Salute**: `motor_temp=92°C` (soglia 55) → in `anomalies`: `{"type":"salute","threshold_reasons":["motor_temp"],"if_anomaly":0,...}`. Nota: l'Isolation Forest da solo **non** l'ha segnalato (`if_anomaly:0`) — esempio concreto del perché servono entrambe le tecniche.
- **Livelock**: un robot fittizio fermo (in senso di posizione) sul nodo `B`, `goal_node="H"`, `task_state="moving"` per oltre 60s consecutivi → in `anomalies`: `{"type":"livelock","min_dist":20.0,"max_dist":20.0,"stall_duration_s":60.0,"n_msgs":5}`. `min_dist==max_dist==20.0` è anche **corretto aritmeticamente**: il percorso più breve B→H sul grafo è B-C-H, 10+10=20, esattamente il valore calcolato dal Floyd-Warshall del job. *(Esempio aggiornato alla Correzione 3 più sotto: la logica di rilevamento è cambiata da finestre a stato per-robot dopo la stesura iniziale di questa sezione.)*
- **Deadlock**: due robot fittizi entrambi `task_state="blocked"` sull'arco `C-F` → in `anomalies`: `{"type":"deadlock","current_edge":"C-F","robots":["TESTDEAD2","TESTDEAD1"]}` (su più finestre scorrevoli sovrapposte, comportamento atteso di `window(...,slide)`).

### 2. Pipeline reale end-to-end (Gazebo → ROS → Kafka → Spark)

Lanciato un robot reale in Gazebo (`sim_single_robot.launch`, come ai Passi 3-6) con una copia temporanea di `config/experiment.json` che anticipa il guasto `spike_corrente` di R1 a una finestra breve (5-20s, `peak_a=6.0` per renderlo inequivocabile) — **ripristinato subito dopo** (verificato con `diff`, nessuna differenza residua). Osservato su `anomalies`, in ordine cronologico reale:

```
curr=2.329  threshold_reasons=[]              # sotto soglia (2.5), non ancora flaggato
curr=2.731  threshold_reasons=[motor_current]  # supera la soglia
curr=3.238 → 3.561 → 4.068 → 4.551 → 4.983 → 5.423 → 5.89 → 6.0   # rampa verso il picco
curr=6.0    threshold_reasons=[motor_current]  # plateau, per tutta la finestra restante
```

La rampa e il plateau combaciano esattamente con la firma `spike_corrente` iniettata dal nodo-ponte (Passo 6) — prova che l'intera catena, dalla simulazione fisica fino all'anomalia scritta su Kafka da Spark, funziona con dati reali, non solo sintetici.

### Stato dei tre meccanismi rispetto a questo passo

- **Salute**: verificata sia con dati sintetici sia con un guasto reale iniettato in Gazebo.
- **Deadlock**: verificato con dati sintetici (logica e windowing corretti). Non ri-osservato "organicamente" da Gazebo in questo passo — il Passo 5 aveva già mostrato un conflitto di traffico reale (R3 che blocca R2 su un nodo condiviso), ma riprodurlo di nuovo per testare *questo* job avrebbe richiesto un'altra run multi-robot di ~8-10 minuti; la verifica sintetica isola e prova la logica di detection indipendentemente dalla fortuna del timing di Gazebo.
- **Livelock**: verificato con dati sintetici, stessa motivazione.

## Stato

- `streaming/detection_job.py`, `streaming/isolation_forest_model.py`, `streaming/train_isolation_forest.py` — nuovi.
- `streaming/models/isolation_forest.pkl` — modello allenato, committato.
- `spark/Dockerfile` — nuovo (pip packages + fix `/etc/passwd`).
- `docker-compose.yml` — `spark-master`/`spark-worker` ora buildano da `./spark`; mount di `./config` e `./streaming`; nuovo volume `spark-ivy-cache`.
- Topic Kafka `anomalies` e `fleet_state` creati (3 partizioni ciascuno).
- `config/experiment.json` — **invariato** (la finestra di test accorciata era su una copia temporanea).

Comando per lanciare il job (non lasciato in esecuzione dopo la verifica, per liberare risorse):

```bash
docker exec -d shf-spark-master bash -c "
  /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
    --conf spark.jars.ivy=/opt/bitnami/spark/ivy-cache \
    /opt/shf/streaming/detection_job.py > /tmp/detection_job.log 2>&1
"
```

## Correzione (2026-07-21, dopo il Passo 11): falsi positivi di livelock su robot in movimento normale

Segnalato dall'utente osservando la dashboard: robot marcati con l'indicatore di livelock pur muovendosi normalmente, senza altri robot vicini. Verificato con dati reali (non solo rileggendo il codice): un robot che avanza in modo continuo e regolare su un arco da 10m a velocità di crociera (~0.13-0.2 m/s) impiega **~40-75s** ad attraversarlo — più della finestra di 30s del rilevatore.

**Causa**: `dist_to_goal` agganciava `(x, y)` al **nodo più vicino** (`nearest_node`) e guardava la distanza-su-grafo precalcolata da quel nodo al `goal_node` — non una distanza continua lungo l'arco. Per circa metà del tempo di attraversamento di un arco (finché il robot resta più vicino al nodo di partenza che a quello di arrivo), `dist_to_goal` restava **perfettamente costante**, anche con il robot in movimento reale e ininterrotto. Confermato con telemetria reale (R1 su arco F-G, x da 20.0 a 23.33 in 26s, velocità costante) e con gli eventi `anomalies` osservati nella stessa finestra: `min_dist == max_dist` **esatto** (0.0, 10.0, 20.0 — multipli esatti di lunghezze di arco) in quasi ogni evento di livelock, la firma inequivocabile della discretizzazione.

**Fix**: `dist_to_goal` ora calcolato in modo continuo lungo `current_edge` (già nello schema di telemetria) — proietta `(x, y)` sull'arco corrente per sapere quanto manca a ciascuno dei due nodi estremi, somma la distanza-su-grafo (Floyd-Warshall) da quel nodo al goal, e prende il minimo dei due percorsi possibili. Fallback al comportamento precedente (nodo più vicino) solo se `current_edge` è mancante/sconosciuto.

**Verifica**: 150s di osservazione reale su `anomalies` prima del fix → decine di eventi di livelock, quasi tutti falsi positivi su robot in navigazione normale. Stessa finestra di 150s dopo il fix, con i robot ancora attivamente in navigazione (non idle) → **0 eventi di livelock**. Una verifica più completa contro gli scenari di livelock *veri* (`livelock-1` in `config/experiment.json`) resta un buon caso di test per il Passo 13 (execution/detection accuracy).

## Correzione 2 (2026-07-21): secondo bug di falsi positivi, trovato dalla suite di test

Costruendo `test/` (suite di test pass/fail, vedi `test/README.md`) il test `test_nessun_falso_positivo_livelock_su_robot_in_movimento` ha trovato falsi positivi di livelock **anche dopo** la Correzione 1 sopra — un bug indipendente, non lo stesso riapparso.

**Causa**: `livelock_query`/`deadlock_query` usavano `outputMode("update")`. Con una finestra scorrevole watermark-based, "update" riemette una riga **ogni volta che l'aggregato di quel gruppo cambia**, anche mentre la finestra è ancora aperta e parzialmente popolata — non solo quando è completa. Un robot in movimento normale, osservato nei primissimi secondi di una nuova finestra (pochi messaggi ancora arrivati, quindi poco spostamento visibile *finora*), poteva quindi soddisfare "nessun progresso" sulla base di un campione ancora incompleto, anche se la finestra — una volta piena — avrebbe mostrato chilometri di progresso reale. Confermato empiricamente: gli eventi falsi avevano sistematicamente `n_msgs` molto sotto l'atteso (es. 4-8 invece di ~90 per una finestra di 30s a 3Hz), la firma di una finestra valutata a metà.

**Fix**: `outputMode("append")` su entrambe le query — emette una riga solo dopo che il watermark ha chiuso la finestra per davvero, garantendo che `min_dist`/`max_dist` riflettano l'intera finestra. Costo: più latenza prima che un'anomalia compaia (fino a ~un'altra finestra, es. ~60-70s invece di quasi subito per il livelock) — accettabile: meglio un'anomalia corretta con un po' di ritardo che una sbagliata subito. Richiesto anche pulire i checkpoint di livelock/deadlock esistenti (cambiare `outputMode` su un checkpoint di un output mode diverso non è supportato in modo affidabile).

**Verifica**: `test_nessun_falso_positivo_livelock_su_robot_in_movimento` (4 robot-token, 60s, nessun guasto) — falliva sistematicamente prima del fix (falsi positivi ad ogni run), passa in modo ripetibile dopo. I test dei veri positivi (`test_livelock_vero_positivo_*`, `test_deadlock_vero_positivo_*`) confermano che il fix non ha eliminato la capacità di rilevare i casi genuini, solo aggiunto la latenza dovuta al watermark.

Nota a margine trovata durante l'indagine: con 3 query streaming concorrenti (Passo 7) + `query_service.py` (Passo 10) sugli stessi core, lo split originale 8/3 (detection/query_service, Passo 11) non bastava più sotto il carico dei test — i micro-batch restavano indietro (10s ne impiegavano 15-19). Ribilanciato a 10/2 (vedi `docs/passi/01-scaffold-infrastruttura.md`).

## Correzione 3 (2026-07-21): terzo bug di falsi positivi — la Correzione 2 non bastava

Segnalato di nuovo dall'utente osservando la dashboard: robot marcati in livelock durante un sorpasso normale su un corridoio a corsia singola (rallenta, supera l'altro robot, riparte) — comportamento di traffico ordinario, non uno stallo.

**Causa**: anche con la Correzione 2 (finestra chiusa per intero, non parziale), il rilevatore restava una valutazione **su una singola finestra isolata di 30s**: bastava che il progresso netto in *quella* finestra fosse sotto soglia, senza nessun requisito che l'assenza di progresso *persistesse nel tempo*. Un breve accodamento durante un sorpasso rientra facilmente in una finestra da 30s con poco progresso netto, pur essendo un evento normale che si risolve da solo poco dopo — la definizione di livelock (stallo *prolungato*, non un singolo campionamento) non era davvero implementata.

**Primo tentativo di fix (scartato)**: due aggregazioni a finestra incatenate — una prima finestra "candidata" (come prima) e una seconda finestra che contava quante finestre candidate consecutive vedeva per lo stesso robot, richiedendo almeno 2 prima di confermare l'anomalia. Concettualmente corretto, ma si è rivelato rompere su un **bug/limite di Spark**: il watermark della seconda aggregazione restava bloccato all'epoch (1/1/1970) anche con dati reali in arrivo (verificato con `StreamingQuery.lastProgress`, campo `eventTime.watermark`), quindi la finestra di conferma non si chiudeva mai e nessuna anomalia veniva mai emessa. Incatenare più aggregazioni a finestra con watermark separati nella stessa query è un'area nota per essere fragile in Spark Structured Streaming.

**Fix definitivo**: stato esplicito per `robot_id` via `applyInPandasWithState` (un solo operatore stateful, niente finestre concatenate). Per ogni robot si tiene un "checkpoint" di riferimento (distanza + istante) aggiornato ogni `LIVELOCK_CHECK_INTERVAL_S` (10s) di event time; se il progresso da un checkpoint al successivo è sotto soglia, si accumula un contatore di stallo continuo, altrimenti si azzera; l'anomalia scatta solo quando lo stallo continua per almeno `LIVELOCK_CONFIRM_DURATION_S` (60s) *consecutivi*. Validato prima in isolamento (script di debug con tre scenari: fermo tutto il tempo → scatta a 60s esatti; sorpasso breve, ~39s di sosta poi ripartenza → non scatta mai; movimento continuo → non scatta mai) prima di integrarlo nel job.

**Effetto collaterale trovato durante l'integrazione**: Spark applica per default un controllo rigido di compatibilità dello schema di stato fra un micro-batch e l'altro (`StateSchemaNotCompatible`), pensato per operatori Scala/Java; con `applyInPandasWithState` genera un falso positivo anche a schema Python invariato. Disattivato con `--conf spark.sql.streaming.stateStore.stateSchemaCheck=false` (solo per questo job — nessun altro usa stato arbitrario).

**Costo osservato**: `applyInPandasWithState` è più pesante della vecchia aggregazione a finestre (una chiamata Python per robot per micro-batch, andata/ritorno Arrow), specialmente quando il generatore sintetico (Passo 12) crea migliaia di `robot_id` distinti in un run di scala: ciascuno resta nello stato finché non scade (`LIVELOCK_STATE_TIMEOUT_S`, 90s) e va comunque considerato ad ogni batch nel frattempo. Osservato un aumento sensibile della latenza delle altre due query (salute, deadlock) subito dopo uno sweep di throughput pesante — non un bug, ma un costo reale di risorse condivise, mitigato nella suite di test con una pausa di assestamento (`test/test_efficiency.py`) e da tenere presente se si rilanciano gli esperimenti del Passo 13 con carichi molto alti.

**Verifica**: suite `test/` completa (23/23) rieseguita con la simulazione ROS reale ferma; `test_livelock_vero_positivo_robot_fermo_ma_task_state_moving` (conferma a ≥60s) e `test_nessun_falso_positivo_livelock_su_robot_in_movimento` (4 robot-token, 60s, nessun guasto — non scatta mai) entrambi verdi. Verificato anche in dashboard reale (Chromium headless via CDP): un sorpasso simulato dal generatore non produce più l'anello viola di livelock.

## Aggiornamento (2026-07-21): servizio persistente, avviato in automatico

Questo job non viene più lanciato a mano e fermato dopo la verifica come descritto sotto: dal Passo 11 in poi la dashboard ne dipende in tempo reale, quindi resta sempre attivo, come `query_service` (Passo 10). Dal Passo 13 in poi parte anche in automatico all'avvio dello stack — vedi `docs/passi/01-scaffold-infrastruttura.md`, sezione "Avvio a comando singolo".

## Prossimo passo

Passo 8 — Persistenza + soglie adattive: scrivere `telemetry`, `anomalies`, `injected_faults` su Parquet, e un job che tara le soglie di questo passo sullo storico per ridurre i falsi positivi.
