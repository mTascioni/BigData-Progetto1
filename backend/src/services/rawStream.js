import { Kafka } from "kafkajs";

// Pannello dashboard "streaming live" -- mostra cosa passa sui topic Kafka
// in tempo reale, senza processarlo ne' salvarlo (nessun impatto sulla
// pipeline di detection/persistenza gia' esistente). Un consumer
// indipendente in piu', stesso pattern di anomalyStream.js/fleetStateStore.js:
// group id univoco per processo, cosi' non interferisce con gli offset dei
// consumer "veri".
const KAFKA_BOOTSTRAP = process.env.KAFKA_BOOTSTRAP || "kafka:9092";
const TOPICS = ["telemetry", "anomalies", "injected_faults", "fleet_state"];

const kafka = new Kafka({ clientId: "shf-backend-rawstream", brokers: [KAFKA_BOOTSTRAP] });
const consumer = kafka.consumer({ groupId: `shf-backend-rawstream-${Date.now()}` });

const listeners = new Set();

export function onRawMessage(callback) {
  listeners.add(callback);
  return () => listeners.delete(callback);
}

// telemetry da sola puo' arrivare a centinaia di messaggi/s con molti robot
// (sweep di scala del generatore sintetico): senza un limite, il pannello
// spingerebbe altrettanti messaggi websocket a ogni client per un pannello
// che serve solo a "vedere cosa passa", non a processarlo tutto. Si
// campiona ad al piu' un messaggio ogni MIN_INTERVAL_MS per topic.
const MIN_INTERVAL_MS = 200;
const lastForwardedAt = {};

const RETRY_DELAY_MS = 5000;

export async function start() {
  for (;;) {
    try {
      await consumer.connect();
      for (const topic of TOPICS) {
        await consumer.subscribe({ topic, fromBeginning: false });
      }
      await consumer.run({
        eachMessage: async ({ topic, message }) => {
          if (!message.value) return;
          const now = Date.now();
          if (now - (lastForwardedAt[topic] || 0) < MIN_INTERVAL_MS) return;
          lastForwardedAt[topic] = now;
          let value;
          try {
            value = JSON.parse(message.value.toString());
          } catch {
            return;
          }
          for (const callback of listeners) callback({ topic, value, ts: now });
        },
      });
      console.log(`[rawStream] consumer Kafka connesso (topic ${TOPICS.join(", ")}, ${KAFKA_BOOTSTRAP})`);
      return;
    } catch (err) {
      console.error(`[rawStream] avvio consumer fallito (${err.message}), riprovo tra ${RETRY_DELAY_MS / 1000}s...`);
      await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY_MS));
    }
  }
}
