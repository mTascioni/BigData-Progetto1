import { Kafka } from "kafkajs";

const KAFKA_BOOTSTRAP = process.env.KAFKA_BOOTSTRAP || "kafka:9092";
const STALE_AFTER_MS = 15000; // oltre questo senza un nuovo fleet_state, il robot e' rimosso
const PRUNE_INTERVAL_MS = 5000;

const kafka = new Kafka({ clientId: "shf-backend", brokers: [KAFKA_BOOTSTRAP] });
// group id univoco per processo: la dashboard vuole sempre lo stato piu'
// recente, non un replay dell'offset dell'ultimo consumer fermato.
const consumer = kafka.consumer({ groupId: `shf-backend-dashboard-${Date.now()}` });

const robots = new Map(); // robot_id -> { ...ultimo fleet_state, _receivedAt }
const updateListeners = new Set(); // callback(robotState)
const removeListeners = new Set(); // callback(robotId)

export function getSnapshot() {
  return Array.from(robots.values()).map(({ _receivedAt, ...state }) => state);
}

export function onUpdate(callback) {
  updateListeners.add(callback);
  return () => updateListeners.delete(callback);
}

export function onRemove(callback) {
  removeListeners.add(callback);
  return () => removeListeners.delete(callback);
}

// Robot reali (flotta ROS, config/experiment.json): id "R" + numero, es. R1.
// Tutto il resto e' un robot-token del generatore sintetico (Passo 12),
// qualunque sia il prefisso scelto (default "SIM").
const REAL_ROBOT_ID_RE = /^R\d+$/;

// Un nuovo run del generatore parte "a video" con lo stato del run
// precedente ancora in memoria (nessun messaggio esplicito di fine, solo
// pruneStale() dopo STALE_AFTER_MS): per chi guarda la dashboard sembra che
// premere "Avvia" non faccia nulla. Il backend lo richiama esplicitamente
// all'avvio di un nuovo run (routes/generator.js) cosi' i robot-token del
// run precedente spariscono subito, non dopo 15s.
export function pruneSynthetic() {
  for (const [robotId] of robots) {
    if (!REAL_ROBOT_ID_RE.test(robotId)) {
      robots.delete(robotId);
      for (const callback of removeListeners) callback(robotId);
    }
  }
}

// Un robot del generatore sintetico (Passo 12) che finisce un run, o un
// robot ROS che sparisce, altrimenti resterebbe per sempre come "fantasma"
// nello stato in memoria -- non c'e' nessun messaggio esplicito di "fine",
// solo l'assenza di nuovi fleet_state.
function pruneStale() {
  const cutoff = Date.now() - STALE_AFTER_MS;
  for (const [robotId, entry] of robots) {
    if (entry._receivedAt < cutoff) {
      robots.delete(robotId);
      for (const callback of removeListeners) callback(robotId);
    }
  }
}

const RETRY_DELAY_MS = 5000;

export async function start() {
  // Vedi la stessa nota in anomalyStream.js: all'avvio "a comando singolo"
  // (Passo 13) Kafka puo' non essere ancora pronto, si ritenta finche' non
  // va a buon fine invece di restare morto per sempre dopo un solo fallimento.
  for (;;) {
    try {
      await consumer.connect();
      await consumer.subscribe({ topic: "fleet_state", fromBeginning: false });
      await consumer.run({
        eachMessage: async ({ message }) => {
          if (!message.value) return;
          let state;
          try {
            state = JSON.parse(message.value.toString());
          } catch {
            return;
          }
          if (!state.robot_id) return;
          robots.set(state.robot_id, { ...state, _receivedAt: Date.now() });
          for (const callback of updateListeners) callback(state);
        },
      });
      setInterval(pruneStale, PRUNE_INTERVAL_MS);
      console.log(`[fleetStateStore] consumer Kafka connesso (topic fleet_state, ${KAFKA_BOOTSTRAP})`);
      return;
    } catch (err) {
      console.error(`[fleetStateStore] avvio consumer fallito (${err.message}), riprovo tra ${RETRY_DELAY_MS / 1000}s...`);
      await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY_MS));
    }
  }
}
