// Costruisce il prompt per il text-to-SQL (Passo 10): schema intero delle
// 4 tabelle Parquet + few-shot, in dialetto Spark SQL (il motore che
// esegue davvero la query, streaming/query_service.py).

const SCHEMA = `Hai a disposizione 4 tabelle Spark SQL (viste su file Parquet), tutte relative a una flotta di robot AGV (TurtleBot3) in un magazzino:

TABELLA telemetry (un tick per robot, ~2 al secondo):
  ts BIGINT              -- epoch millisecondi
  robot_id STRING        -- es. 'R1', 'R2', 'R3'
  x DOUBLE, y DOUBLE, theta DOUBLE       -- posizione e orientamento sul piano
  v_lin DOUBLE, v_ang DOUBLE             -- velocita' lineare/angolare reale
  cmd_v_lin DOUBLE, cmd_v_ang DOUBLE     -- velocita' comandata
  battery_pct DOUBLE     -- percentuale carica batteria [0,100]
  motor_current DOUBLE   -- ampere
  motor_temp DOUBLE      -- gradi Celsius
  min_obstacle_dist DOUBLE  -- metri, distanza minima da un ostacolo (lidar)
  task_state STRING      -- 'idle' | 'moving' | 'blocked' | 'charging'
  current_edge STRING    -- id arco del grafo magazzino su cui si trova, es. 'C-F'
  goal_node STRING       -- id nodo obiettivo corrente, es. 'H' (NULL se nessun goal attivo)

TABELLA anomalies (eventi di anomalia rilevati in tempo reale, Spark Structured Streaming):
  type STRING             -- 'salute' | 'livelock' | 'deadlock'
  ts BIGINT                -- epoch ms (solo per type='salute')
  robot_id STRING          -- (solo per type='salute' e 'livelock')
  threshold_reasons ARRAY<STRING>  -- quali soglie superate, es. ['motor_temp'] (solo type='salute')
  if_anomaly INT            -- 1 se l'Isolation Forest ha segnalato anomalia, altrimenti 0 (solo type='salute')
  motor_temp DOUBLE, motor_current DOUBLE, battery_pct DOUBLE  -- valori al momento dell'anomalia (solo type='salute')
  window_start STRING, window_end STRING  -- finestra temporale ISO8601 (solo type='livelock'/'deadlock')
  min_dist DOUBLE, max_dist DOUBLE, stall_duration_s DOUBLE, n_msgs BIGINT, n_moving BIGINT  -- (solo type='livelock'; stall_duration_s = per quanti secondi consecutivi non c'e' stato progresso prima dell'alert, minimo 60)
  current_edge STRING       -- arco conteso (solo type='deadlock')
  robots ARRAY<STRING>      -- robot coinvolti nel conflitto (solo type='deadlock')

TABELLA injected_faults (ground truth: guasti iniettati apposta per i test, Passo 6):
  fault_id STRING, robot_id STRING
  fault_type STRING       -- 'deriva_termica' | 'spike_corrente' | 'batteria_collasso' | 'sensore_bloccato'
  start_time_s DOUBLE, end_time_s DOUBLE   -- finestra pianificata, secondi relativi all'esperimento
  params STRUCT<ramp_rate_c_per_s: DOUBLE, plateau_temp_c: DOUBLE, ramp_duration_s: DOUBLE,
                peak_a: DOUBLE, rise_time_s: DOUBLE, hold_duration_s: DOUBLE,
                drain_rate_multiplier: DOUBLE, trigger_pct: DOUBLE,
                frozen_channel: STRING, freeze_duration_s: DOUBLE>  -- accedi con params.nome_campo
  start_ts BIGINT, end_ts BIGINT   -- finestra REALE (epoch ms) di quando il guasto e' stato attivo

TABELLA predictions (previsioni offline di guasto, Passo 9):
  robot_id STRING, channel STRING   -- channel: 'motor_temp' | 'motor_current' | 'battery_pct'
  predicted_at_ts BIGINT   -- epoch ms, quando e' stata fatta la previsione
  current_value DOUBLE, slope_per_min DOUBLE, critical_threshold DOUBLE
  predicted_crossing_ts BIGINT   -- epoch ms previsto di superamento soglia critica
  lead_time_s DOUBLE       -- secondi di anticipo sulla previsione (\"remaining useful life\")
  model STRING, n_points BIGINT

Note sul dialetto SQL (Spark SQL):
- Per convertire un timestamp epoch-ms in data leggibile: timestamp_millis(colonna).
- Per accedere a un campo di una STRUCT: params.peak_a (non params->>'peak_a').
- Per controllare se un valore e' in un ARRAY: array_contains(threshold_reasons, 'motor_temp').
- Stringhe fra apici singoli.`;

const FEW_SHOT = [
  {
    question: "Quali robot hanno la batteria sotto il 20% adesso?",
    sql: "SELECT robot_id, battery_pct, ts FROM telemetry WHERE battery_pct < 20 ORDER BY ts DESC LIMIT 50",
  },
  {
    question: "Quante anomalie di salute per motor_temp sono state rilevate per ogni robot?",
    sql: "SELECT robot_id, COUNT(*) AS n FROM anomalies WHERE type = 'salute' AND array_contains(threshold_reasons, 'motor_temp') GROUP BY robot_id ORDER BY n DESC",
  },
  {
    question: "Quali guasti di tipo deriva_termica sono stati iniettati e con quale plateau di temperatura?",
    sql: "SELECT fault_id, robot_id, start_time_s, end_time_s, params.plateau_temp_c AS plateau_temp_c FROM injected_faults WHERE fault_type = 'deriva_termica'",
  },
  {
    question: "Qual e' il robot con il lead time di previsione piu' corto (piu' vicino a guastarsi)?",
    sql: "SELECT robot_id, channel, lead_time_s FROM predictions ORDER BY lead_time_s ASC LIMIT 1",
  },
  {
    question: "Quanti eventi di deadlock sono stati rilevati sull'arco C-F?",
    sql: "SELECT COUNT(*) AS n FROM anomalies WHERE type = 'deadlock' AND current_edge = 'C-F'",
  },
];

export function buildMessages(question) {
  const fewShotText = FEW_SHOT.map(
    (ex) => `Domanda: ${ex.question}\nSQL: ${ex.sql}`
  ).join("\n\n");

  const system = `Sei un traduttore da domande in linguaggio naturale (italiano) a query Spark SQL, per un sistema di monitoraggio di una flotta di robot AGV.

${SCHEMA}

Esempi:

${fewShotText}

Regole:
- Rispondi SOLO con la query SQL, una singola istruzione SELECT (o WITH ... SELECT). Nessuna spiegazione, nessun testo prima o dopo, nessun blocco di codice markdown.
- Solo SELECT: mai INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, TRUNCATE o altri comandi che modificano i dati.
- Se la domanda e' ambigua, fai la scelta piu' ragionevole invece di chiedere chiarimenti.
- Usa sempre un LIMIT ragionevole (es. 100) se la domanda non specifica quante righe vuole, per evitare risultati enormi.`;

  return [
    { role: "system", content: system },
    { role: "user", content: question },
  ];
}

// Terzo stadio del layer TAG (answer synthesis): senza questo passaggio il
// sistema e' un text-to-SQL (query synthesis + execution, restituisce righe
// grezze), non un TAG vero e proprio -- la definizione di TAG (Biswal et al.,
// "TAG: A Unified Framework for Table-Augmented Generation", 2024) richiede
// che l'LLM rielabori il risultato in una risposta, non solo generi la query.
const MAX_ROWS_FOR_SYNTHESIS = 30;

export function buildAnswerSynthesisMessages(question, sql, rows) {
  const truncated = rows.length > MAX_ROWS_FOR_SYNTHESIS;
  const sample = truncated ? rows.slice(0, MAX_ROWS_FOR_SYNTHESIS) : rows;

  const system = `Sei un assistente che risponde in italiano, in linguaggio naturale, a domande sui dati di una flotta di robot AGV in un magazzino.

Ti vengono forniti: la domanda originale, la query Spark SQL che e' stata eseguita, e le righe che quella query ha restituito (in JSON). Rispondi SOLO in base a questi dati -- non inventare valori assenti dalle righe fornite. Se le righe sono vuote, dillo esplicitamente ("nessun risultato trovato per...") invece di inventare una risposta.${
    truncated
      ? ` Attenzione: ti sono state fornite solo le prime ${MAX_ROWS_FOR_SYNTHESIS} righe su ${rows.length} totali; se rilevante, menziona che il risultato completo e' piu' ampio.`
      : ""
  }

Rispondi in poche frasi, in modo diretto e naturale. Non ripetere la query SQL, non descrivere lo schema della tabella: vai dritto alla risposta.`;

  const user = `Domanda: ${question}\n\nQuery eseguita:\n${sql}\n\nRighe risultato (JSON):\n${JSON.stringify(sample)}`;

  return [
    { role: "system", content: system },
    { role: "user", content: user },
  ];
}

export function buildRetryMessages(previousMessages, failedSql, errorMessage) {
  return [
    ...previousMessages,
    { role: "assistant", content: failedSql },
    {
      role: "user",
      content: `Quella query ha dato questo errore eseguendola su Spark SQL:\n${errorMessage}\n\nCorreggi la query. Rispondi di nuovo SOLO con la query SQL corretta, nessun'altra spiegazione.`,
    },
  ];
}
