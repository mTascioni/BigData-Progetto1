# Passo 2 — Contratti dati

**Obiettivo (da PLAN.md):** `config/warehouse_graph.json` (magazzino piccolo con almeno un corridoio a corsia singola) e `config/experiment.json` (flotta, scenari, `fault_schedule`). Fissa lo schema del messaggio.
**Deliverable atteso:** file di configurazione + contratto messaggi.

Lo schema del messaggio di telemetria era già fissato in `CLAUDE.md` (invariante di progetto); questo passo fissa i due file di configurazione che completano il contratto dati: la mappa a grafo e la definizione dell'esperimento (che è anche il ground truth).

## `config/warehouse_graph.json`

Magazzino a 10 nodi: due dock (`A` carico, `D` scarico), una stazione di ricarica (`I`), due junction sull'aisle principale (`B`, `C`) e un anello di storage (`E`, `F`, `G`, `J`, `H`) raggiungibile dall'aisle principale tramite due corridoi a corsia singola: `C-F` e `C-H` (`capacity: 1`). Tutti gli altri archi hanno `capacity: 2` (doppio senso libero).

```
        E --- F --- G
        |     |     |
A - B - C     |     J
        |     |     |
        I     H --- +
              |
              C  (corsia singola C-F e C-H)
```

**Perché un anello con due choke point invece di un singolo corridoio:** un solo corridoio a corsia singola basta per bloccare un robot alla volta, ma per generare un vero **deadlock** (attesa circolare, cfr. tassonomia in `Contesto/self-healing-fleet-relazione.pdf` sez. 6) servono almeno due archi contesi in un ciclo: due robot che percorrono l'anello in direzioni opposte possono restare bloccati ciascuno sul proprio arco a corsia singola, in attesa che l'altro liberi il proprio. Lo stesso schema, con una policy di priorità simmetrica, dà luogo a **livelock** (i due si cedono il passo a vicenda senza mai attraversare). Questa geometria è pensata apposta per lo scenario del Passo 5.

L'edge `C-F` e il nodo `H` sono stati scelti apposta uguali all'esempio di messaggio di telemetria già presente in `CLAUDE.md` (`"current_edge": "C-F"`, `"goal_node": "H"`), così l'esempio nel contratto messaggi resta valido rispetto alla mappa reale.

## `config/experiment.json`

Contiene, come richiesto dal piano:

- **`fleet`**: 3 robot (`R1`, `R2`, `R3`), con nodo di partenza (`A`, `D`, `I` — dock/dock/ricarica).
- **`tasks`**: sequenza di goal nodo-per-nodo per ciascun robot. `R1` percorre l'anello in senso orario, `R2` in senso antiorario, `R3` fa un giro più corto passando per lo storage nord — verificato che ogni coppia di nodi consecutivi in ciascuna sequenza corrisponda a un arco reale del grafo (nessun salto che richiederebbe path-planning implicito).
- **`scenarios`**: due scenari dichiarativi (`deadlock-1`, `livelock-1`) che documentano quali robot e quali archi sono coinvolti nel conflitto sull'anello — la logica che li fa effettivamente emergere a runtime (parametri del planner/policy di attesa) è demandata al Passo 5.
- **`health_channels_nominal`**: i parametri nominali (drain rate batteria, corrente motore, temperatura motore) che il nodo-ponte dovrà usare per **sintetizzare** i canali di salute assenti nel TurtleBot3 simulato (Passo 4/6) — fissati qui per essere l'unica fonte di verità anche per questi valori, coerentemente con l'invariante di progetto.
- **`fault_signature_schema`**: per ciascuno dei 4 guasti di salute e 2 comportamentali, canale/meccanismo interessato e nome dei parametri attesi. Serve da contratto per chi implementerà l'iniezione (Passo 6): il generatore/bridge legge un'istanza in `fault_schedule` e sa esattamente quali chiavi aspettarsi in base al `fault_type`.
- **`fault_schedule`**: 4 guasti di esempio, uno per ogni tipo di guasto di salute (`deriva_termica` su R3, `spike_corrente` su R1, `batteria_collasso` su R2, `sensore_bloccato` su R3), con finestra temporale e parametri coerenti con lo schema — sarà la ground truth da cui calcolare precision/recall al Passo 13.

## Verifica di coerenza incrociata

Scritto ed eseguito uno script Python (non salvato nel repo, solo per la verifica una tantum) che controlla:

1. Sintassi JSON valida per entrambi i file.
2. Ogni `start_node` in `fleet` esiste come nodo del grafo.
3. Ogni nodo in ogni `goal_sequence` esiste come nodo del grafo, **e** ogni coppia di nodi consecutivi nella sequenza corrisponde a un arco reale (percorso valido arco-per-arco, non solo nodi esistenti).
4. Ogni `robot_id` referenziato in `tasks`/`scenarios`/`fault_schedule` esiste in `fleet`.
5. Ogni `involved_edges` in `scenarios` esiste come arco del grafo.
6. Ogni `fault_type` in `fault_schedule` esiste in `fault_signature_schema`, e per i guasti di categoria `salute` l'insieme di chiavi in `params` combacia esattamente con quello dichiarato nello schema.
7. Il grafo è connesso (BFS da un nodo qualsiasi raggiunge tutti gli altri).

Esito: **nessuna inconsistenza trovata**, grafo connesso, tutte le `goal_sequence` sono percorsi validi.

## Stato

- `config/warehouse_graph.json` — creato e validato.
- `config/experiment.json` — creato e validato.
- Il file `config/.gitkeep` è stato rimosso ora che la cartella contiene contenuto reale.

## Prossimo passo

Passo 3 — Bring-up ROS/Gazebo: un singolo TurtleBot3 in Gazebo (headless) che naviga sul grafo appena definito nodo per nodo (`move_base`), partendo verosimilmente dal nodo `A` seguendo il task di `R1`.
