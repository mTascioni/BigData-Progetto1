# Passo 13 — Valutazione sperimentale (EFFECTIVENESS + EFFICIENCY)

**Obiettivo (da PLAN.md):** `eval/` — efficiency (scalabilità throughput/latency vs carico, punto di rottura, latenza onset→alert) ed effectiveness (detection precision/recall/F1 vs `injected_faults`, accuratezza previsione + lead time, execution accuracy del TAG).
**Deliverable atteso:** i numeri per il report.

Ultimo passo del piano. Distinto da `test/` (Passo pre-13, suite pass/fail per verificare la correttezza — vedi `test/README.md`): qui si producono **numeri e grafici riusabili nella tesina**, non asserzioni vero/falso. Su richiesta esplicita dell'utente, i risultati sono anche visualizzati "in diretta" in un pannello dedicato in fondo alla dashboard (non nella vista principale, per non intralciarla), con grafici incorporati e download di CSV/PNG.

## Cosa è stato costruito

**`eval/common.py`** — helper condivisi (stesso stile di `test/conftest.py`: client Kafka, `query_sql`/`ask_tag` verso i servizi, controllo del generatore). In più: `new_run_dir(tipo)` crea una cartella `/data/eval/<tipo>_<timestamp>/` sul volume Docker condiviso `shf-data` (stesso volume di telemetria/Parquet — CLAUDE.md, storage su volume condiviso), `save_fig`/`new_fig` per i grafici (matplotlib, backend `Agg`: nessun display, solo file), `update_index(...)` che aggiorna un indice condiviso `/data/eval/index.json` letto dal backend.

**`eval/reference_questions.py`** — 22 domande di riferimento in linguaggio naturale con la relativa query SQL "di verità diretta" scritta a mano (stesso principio di `test/test_tag_accuracy.py`: la verità si ricalcola sugli stessi dati al momento della domanda, niente valori attesi fissi che scadrebbero appena lo storico cresce).

**`eval/run_effectiveness.py`** — tre esperimenti, ciascuno scrive CSV + un grafico PNG:
1. **Detection**: 8 robot-token (generatore, Passo 12), 3 con un guasto `spike_corrente` noto, 5 senza. Confusion matrix su `anomalies` (type=salute) → precision/recall/F1.
2. **Previsione**: 3 scenari di trend sintetico (motor_temp, motor_current, battery_pct) con pendenza nota, crossing calcolato analiticamente, confrontato con l'output di `predictive/forecast_failures.py` → errore assoluto sul lead time.
3. **TAG**: le 22 domande di riferimento, confronto risposta-vs-verità con lo stesso confronto tollerante (per valore, non per fraseggio) di `test/test_tag_accuracy.py` → execution accuracy.

**`eval/run_efficiency.py`** — due esperimenti:
1. **Sweep di throughput**: generatore da 500 a 60000 msg/s target (50→6000 robot-token a 10Hz), throughput raggiunto per ciascun livello, punto di rottura (dove il raggiunto scende sotto l'80% del target).
2. **Latenza onset→alert**: 5 prove di iniezione+rilevamento di un guasto, tempo dall'attivazione reale (`injected_faults.start_ts`) alla ricezione del primo alert.

**Backend** (`backend/src/routes/eval.js`): `GET /api/eval/results` (legge `/data/eval/index.json`), `GET /api/eval/files/:runId/:filename` (serve CSV/PNG di un run specifico, con validazione anti-path-traversal sui nomi). Il volume `shf-data` è montato in sola lettura sul backend — gli script `eval/` (eseguiti nel container `ros`) sono gli unici a scriverci.

**Dashboard**: due nuove card in fondo alla pagina ("Risultati sperimentazioni — effectiveness/efficiency"), sotto le card già esistenti del Passo 11/12 — non nella vista principale, come richiesto. Ogni card mostra: metadati del run più recente, alcune statistiche chiave come tile, i grafici incorporati, i link di download per tutti i CSV/PNG. Polling ogni 30s: appena un nuovo run `eval/` scrive `index.json`, il pannello lo mostra senza bisogno di ricaricare la pagina ("in diretta", come richiesto).

## Verifica

Eseguiti per davvero entrambi gli script (non solo scritti), con la simulazione ROS reale messa in pausa nel frattempo (stessa logica di `test/`, evita contesa di CPU con Gazebo — rimessa su alla fine).

**Effectiveness**:
- Detection: **precision=1.00, recall=1.00, F1=1.00** (TP=3, FP=0, FN=0, TN=5) — nessun falso positivo/negativo su questo run, coerente con i due fix di livelock del Passo 7 (dist_to_goal continuo + outputMode append).
- Previsione: errore medio assoluto **1.59s** sul lead time, su previsioni di centinaia di secondi (es. atteso 500s, previsto 498.4s) — la regressione lineare è molto precisa su trend puliti.
- TAG: **21/22 (95%)** corrette. L'unica domanda "sbagliata" ("qual è la temperatura media" per i robot con guasti) si è rivelata un'**ambiguità della domanda**, non un errore del TAG: la query di riferimento calcola la media su tutta la telemetria, il TAG ha scelto (in modo altrettanto legittimo) di calcolarla solo sulle anomalie di temperatura rilevate. Lasciata così nella tesina come esempio genuino dei limiti dell'interrogazione in linguaggio naturale, invece di correggere la domanda per farla "tornare".

**Efficiency**:
- Throughput: scaling pulito (100% raggiunto) fino a **20000 msg/s**; punto di rottura individuato a **40000 msg/s target** (raggiunto 30854, 77%); a 60000 target il raggiunto scende addirittura a 29596 (49%) — il collo di bottiglia è il processo Python singolo del generatore (già discusso in `docs/passi/12-generatore-sintetico.md`), non Kafka/Spark.
- Latenza onset→alert: **3 prove su 5 riuscite**, latenza media misurata 30.2s (la misura è volutamente prudente/larga — vedi nota sotto — non è la vera latenza minima del sistema). 2 prove su 5 non hanno trovato un alert entro la finestra di raccolta, verosimilmente per carico residuo subito dopo lo sweep di throughput che la precede nello stesso run.

Entrambi i pannelli verificati in un vero browser (Chromium headless via CDP): le 5 immagini si caricano correttamente, le statistiche mostrano i numeri giusti, i link di download rispondono `200`, il controllo anti-path-traversal rifiuta un tentativo di attraversamento (`400`).

## Nota sulla misura di latenza

Il numero "~30s" di latenza onset→alert **non è la vera latenza minima** del sistema (che nel Passo 11 era stata osservata sui pochi secondi, coerente col trigger di 2s della query di salute): la tecnica di misura usata negli script (`collect_messages` con un consumer sottoscritto in anticipo ma interrogato *dopo* un'attesa sequenziale su un altro consumer) misura "quanto ci ho messo io ad accorgermene", non "quanto ci ha messo il sistema a produrlo" — un limite del metodo di misura, dichiarato esplicitamente nei commenti degli script, non un problema di prestazioni reali. Una misura più stretta (consumo dei due topic in parallelo, non in sequenza) è un miglioramento naturale se questi numeri servono per la tesina in modo più rigoroso.

## Stato

- `eval/common.py`, `eval/reference_questions.py`, `eval/run_effectiveness.py`, `eval/run_efficiency.py` — nuovi.
- `backend/src/routes/eval.js` — nuovo.
- `backend/src/server.js` — nuova route `/api/eval`.
- `dashboard/{index.html,style.css,app.js}` — pannello "Risultati sperimentazioni".
- `docker-compose.yml` — `./eval` e volume `shf-data` montati nel container `ros` (scrittura), `shf-data:ro` nel backend (lettura).
- `ros/Dockerfile` — aggiunto `matplotlib`.

Comando per rilanciare gli esperimenti:

```bash
docker exec shf-ros supervisorctl stop sim_multi_robot   # libera CPU, opzionale ma consigliato
docker exec shf-ros bash -c "cd /opt/shf/eval && python3 run_effectiveness.py"
docker exec shf-ros bash -c "cd /opt/shf/eval && python3 run_efficiency.py"
docker exec shf-ros supervisorctl start sim_multi_robot
```

## Chiusura del piano

Con questo passo si chiudono tutti e 13 i passi di `PLAN.md`. Le quattro tecnologie richieste dal corso (Kafka, Spark Structured Streaming, previsione time-series, LLM) sono tutte presenti e verificate; entrambe le categorie di esperimenti richieste (effectiveness, efficiency) hanno numeri reali, riproducibili con un comando, visibili sia da riga di comando (CSV) sia dalla dashboard.
