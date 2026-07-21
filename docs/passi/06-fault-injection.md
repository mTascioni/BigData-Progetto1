# Passo 6 — Layer di fault injection

**Obiettivo (da PLAN.md):** nel nodo-ponte, leggere il `fault_schedule` e, per un robot con guasto di salute attivo, sommare la firma alla telemetria prima di pubblicare. Loggare in `injected_faults`.
**Deliverable atteso:** guasti controllati + ground truth.

## Cosa è stato costruito

Una classe `FaultInjector` in `kafka_bridge.py`, istanziata una volta per robot (filtra da subito `fault_schedule` sugli eventi che riguardano il proprio `robot_id`), con due fasi richiamate ad ogni tick del bridge:

1. **`update_battery_multiplier()`** — chiamata *prima* di aggiornare `battery_pct`: attiva/disattiva i guasti schedulati (confrontando il tempo trascorso dall'avvio del nodo con `start_time_s`/`end_time_s`) e ritorna il moltiplicatore di drain da usare in questo tick (1.0 se nessun `batteria_collasso` è attivo).
2. **`apply_to_message(message)`** — chiamata *dopo* aver costruito il messaggio di telemetria nominale (compreso `battery_pct` già aggiornato con il moltiplicatore corretto): applica la firma degli altri tre guasti di salute direttamente sui campi del messaggio.

Le firme implementate, coerenti con `fault_signature_schema` fissato al Passo 2:

| Guasto | Meccanismo implementato |
|---|---|
| `deriva_termica` | `motor_temp` = nominale-con-rumore + `ramp_rate_c_per_s × tempo_dall'attivazione`, clampato a `plateau_temp_c` |
| `spike_corrente` | `motor_current` interpola linearmente verso `peak_a` in `rise_time_s`, poi resta fisso a `peak_a` per il resto della finestra |
| `batteria_collasso` | il drain rate di `battery_pct` (moving o idle/blocked, non la ricarica) è moltiplicato per `drain_rate_multiplier` |
| `sensore_bloccato` | il canale `frozen_channel` viene congelato al valore live catturato nell'istante di attivazione, per tutta la finestra |

Perché la fase batteria è separata dalle altre tre: `battery_pct` è un valore **integrato nel tempo** (stato persistente, Passo 4), non ricampionato ogni tick come `motor_current`/`motor_temp` — il moltiplicatore deve essere noto *prima* di far avanzare l'integrazione, mentre le altre firme si applicano come post-processing su un valore già campionato. Da qui il design a due fasi invece di un singolo metodo `apply(message)`.

Ogni transizione di stato (attivazione/disattivazione) viene loggata: **una riga per istanza di guasto**, pubblicata su Kafka al momento della **disattivazione** (quando sia `start_ts` che `end_ts` reali — wall-clock, non tempo simulato — sono noti), topic `injected_faults`, key = `robot_id` (stesso schema di partizionamento di `telemetry`):

```json
{
  "fault_id": "F2", "robot_id": "R1", "fault_type": "spike_corrente",
  "start_time_s": 200, "end_time_s": 260,
  "params": {"peak_a": 4.5, "rise_time_s": 5, "hold_duration_s": 55},
  "start_ts": 1784543015970, "end_ts": 1784543025491
}
```

`flush_active()` chiude (con `end_ts` = adesso) anche i guasti eventualmente ancora attivi se il nodo viene fermato prima della fine naturale della finestra — per non perdere ground truth se una run viene interrotta.

## Perché loggare `injected_faults` su Kafka invece che leggere `fault_schedule` direttamente in fase di valutazione

`config/experiment.json` contiene già lo schedule *pianificato* (tempi relativi all'avvio dell'esperimento). `injected_faults` è il registro di quello che è *realmente* successo: timestamp assoluti (wall-clock) invece che relativi, utile se l'avvio effettivo del nodo diverge dal piano nominale (ritardi di startup, run interrotte). Pubblicarlo su Kafka (stesso pattern di `telemetry`) invece che scriverlo su file locale lo rende immediatamente coerente con l'architettura già decisa: verrà persistito su Parquet dallo stesso job del Passo 8, pronto per il confronto con `anomalies` nella valutazione di precision/recall del Passo 13.

## Verifica

### 1. Test della logica in isolamento

Prima di un test end-to-end (che coi tempi reali del `fault_schedule` — il più lungo dura fino a 560s — avrebbe richiesto quasi 10 minuti di simulazione), la logica di `FaultInjector` è stata verificata in isolamento: uno script Python (non nel repo, solo per la verifica) importa la classe da `kafka_bridge.py`, sostituisce `time.time` con un orologio finto controllabile a piacere, e un `Producer` finto che registra le chiamate a `produce()` invece di parlare con Kafka. 15 asserzioni, tutte verificate:

- nessun guasto attivo → nessun effetto sul messaggio, moltiplicatore batteria 1.0;
- `deriva_termica`: rampa esatta (`nominale + rate×Δt`) durante la finestra, disattivazione puntuale a `end_time_s`;
- evento `injected_faults` scritto una sola volta per istanza, con `start_ts < end_ts` e i `params` originali;
- `spike_corrente`: interpolazione lineare corretta a metà della `rise_time_s`, valore fisso al picco dopo;
- `batteria_collasso`: moltiplicatore ritornato correttamente durante la finestra;
- `sensore_bloccato`: il valore congelato resta quello dell'istante di attivazione anche se il valore "live" sottostante cambia nel frattempo;
- tutti e 4 i guasti risultano loggati su `injected_faults` a fine test.

### 2. Integrazione reale (Gazebo + Kafka)

Creato il topic `injected_faults` (3 partizioni, come `telemetry`). Per non aspettare ~10 minuti, la verifica end-to-end ha usato una copia temporanea di `config/experiment.json` con le stesse 4 firme ma finestre accorciate a pochi secondi ciascuna, tutte sul robot `R1` in sequenza (5-15s, 20-30s, 35-45s, 50-60s) — **il file reale è stato ripristinato subito dopo** (verificato con `diff`, nessuna differenza residua). Lanciata `sim_single_robot.launch`:

- **Timing di attivazione**: i 4 log `guasto '...' ATTIVATO/disattivato` sono comparsi esattamente agli istanti schedulati (verificato sul log dedicato del nodo, non su quello aggregato di `roslaunch` — che soffre dello stesso ritardo di buffering dello stdout già notato al Passo 3).
- **`injected_faults` su Kafka**: 4 record consumati, uno per guasto, con `start_ts`/`end_ts` coerenti e distanza reale di ~10s ciascuno, `params` intatti.
- **`telemetry` durante le finestre attive** (dump e ispezione visiva):
  - `spike_corrente`: `motor_current` sale da ~1.5A a 4.5A in rampa (1.84 → 2.16 → 2.50 → ... → 4.50), poi **resta esattamente a 4.50** per tutto il resto della finestra, e torna a ~1.5A un tick dopo la disattivazione — andamento da manuale.
  - `batteria_collasso`: `battery_pct` scende da 99.70 a 99.06 in ~10s durante la finestra (~3.8%/min, coerente con l'atteso 0.5%/min × 8), contro un drain quasi piatto (99.73→99.70 nello stesso intervallo di tempo) subito prima della finestra — differenza netta e ben visibile.
  - `deriva_termica`: attivazione confermata puntualmente (log + evento `injected_faults`), ma l'effetto sul valore non è visivamente distinguibile dal rumore in questa prova abbreviata: `ramp_rate_c_per_s=0.15` è tarato per la finestra reale di 300s dell'esperimento, su 10s di test aggiunge solo ~1.5°C contro un rumore di ±1°C — la correttezza della formula è comunque garantita dal test unitario (punto 1), che la verifica con parametri dedicati.
  - `sensore_bloccato`: attivazione confermata, ma nel mondo Gazebo vuoto usato per questi test `min_obstacle_dist` è già costantemente al valore massimo (3.5, nessun ostacolo reale in vista) — congelare un valore già costante non produce un effetto visivamente distinguibile in *questo* mondo. Il meccanismo di freeze in sé (valore congelato mentre il "vivo" sottostante cambia) è verificato dal test unitario.
- **Nessun errore/crash** nel nodo per l'intera prova.

## Stato

- `ros/catkin_ws/src/shf_bringup/scripts/kafka_bridge.py` — aggiunta la classe `FaultInjector` e la sua integrazione in `KafkaBridge` (`__init__`, `_update_battery`, `spin`).
- Topic Kafka `injected_faults` creato (3 partizioni, replication factor 1).
- `config/experiment.json` — **invariato** rispetto al Passo 2 (le finestre accorciate usate per il test erano su una copia temporanea, mai committata).

## Prossimo passo

Passo 7 — Detection in streaming (REAL-TIME, PySpark): consumare `telemetry`, rilevare le anomalie di salute (soglie + Isolation Forest) e i conflitti comportamentali (deadlock/livelock, sulla finestra e sulla logica di `task_state`/`current_edge` già presenti in telemetria dai Passi 4-5), scrivere `anomalies` e lo stato flotta su `fleet_state`.
