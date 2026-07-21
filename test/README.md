# Suite di test — verifica di correttezza ed effectiveness/efficiency

Non è il Passo 13 del piano (`eval/`, ancora da fare: più domande di riferimento, esperimenti, numeri e grafici per la tesina). Questa cartella è una suite di test **pass/fail** con `pytest`, pensata per rispondere a una domanda diversa e più immediata: *il sistema funziona correttamente adesso?* — schema dei messaggi, ground truth dei guasti, precision/recall della detection, accuratezza delle previsioni, execution accuracy del layer TAG, throughput/latenza sotto carico.

## Come si esegue

Gira dentro il container `ros` (ha già `pytest`/`confluent-kafka`/`pandas`/`numpy`/`pyarrow`, vede tutti gli altri servizi sulla rete Docker del progetto):

```bash
docker exec shf-ros bash -c "cd /opt/shf/test && python3 -m pytest -v"
```

Con `-s` si vedono anche i numeri stampati dai test di efficiency (throughput/latenza misurati). La suite mette in pausa la simulazione ROS/Gazebo reale per tutta la sessione (non serve, usa il generatore sintetico del Passo 12 per avere robot controllati) e la rimette su alla fine — se `supervisorctl` non è raggiungibile prosegue comunque, solo più lento. Impiega **~7 minuti** in totale, principalmente per i test di detection che devono aspettare che le finestre scorrevoli si chiudano davvero (vedi sotto).

## Cosa copre

| File | Cosa verifica |
|---|---|
| `test_schema.py` | I messaggi che circolano DAVVERO sui topic (non fixture statiche) rispettano lo schema di CLAUDE.md |
| `test_fault_ground_truth.py` | Un guasto iniettato dal generatore produce un record `injected_faults` corretto (tipo, timing, parametri) |
| `test_detection_effectiveness.py` | Veri/falsi positivi sui tre meccanismi di detection (salute, livelock, deadlock) |
| `test_prediction_accuracy.py` | La regressione lineare (Passo 9) prevede correttamente il lead time su un trend sintetico noto analiticamente |
| `test_tag_accuracy.py` | Le risposte del layer TAG confrontate con query SQL scritte a mano sugli stessi dati |
| `test_efficiency.py` | Throughput a carico crescente, latenza onset→alert |

## Due bug reali trovati costruendo questa suite (2026-07-21)

Il punto di questo esercizio, non un dettaglio a margine: **`test_nessun_falso_positivo_livelock_su_robot_in_movimento`** ha scoperto un secondo bug di falsi positivi nella detection del livelock, indipendente da quello già corretto in precedenza lo stesso giorno (vedi `docs/passi/07-detection-streaming.md`):

1. `dist_to_goal` agganciato al nodo più vicino invece che continuo lungo l'arco (fix precedente).
2. **`outputMode("update")` sulle query di livelock/deadlock** valutava la condizione "nessun progresso" anche su una finestra scorrevole ancora **parzialmente popolata** (pochi messaggi visti finora, quindi poco movimento apparente solo perché la finestra non si era ancora riempita) — non solo quando la finestra era completa. Fix: `outputMode("append")`, che emette una riga solo dopo che il watermark ha chiuso la finestra per davvero. Costo: più latenza (fino a ~un'altra finestra) prima che un'anomalia compaia — i test dei veri positivi ne tengono conto nei tempi di attesa.

Il primo fix (a mano, senza questa suite) non aveva scoperto il secondo bug — un'ulteriore controprova del perché vale la pena avere test automatici oltre alla verifica manuale.

## Note operative

- **Un solo run del generatore alla volta**: ogni test usa una fixture `autouse` che ferma qualunque run residuo prima e dopo (`conftest.py::_ensure_generator_idle`).
- **Iscriversi al topic PRIMA di scatenare l'azione**: il join di un gruppo consumer Kafka può richiedere alcuni secondi; ogni test che osserva l'effetto di un'azione (guasto iniettato, messaggio sintetico) crea il consumer con `start_consumer()` e lo alimenta con `collect_messages()` *dopo* aver innescato l'azione, mai il contrario — altrimenti si perdono gli eventi più veloci.
- **Allocazione dei core Spark rivista**: durante la costruzione di questa suite, `query_service.py`/`detection_job.py` sono stati ribilanciati da 3/8 a 2/10 core (vedi `streaming/start-master.sh`) — con 3 query streaming concorrenti sotto il carico dei test, 8 core non bastavano e i micro-batch restavano indietro.
- I test di efficiency stampano numeri reali (throughput raggiunto, latenza) ma con soglie di successo volutamente larghe: individuano regressioni vere (crash, throughput vicino a zero), non sostituiscono la valutazione sperimentale del Passo 13.
