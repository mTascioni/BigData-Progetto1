import { Kafka } from "kafkajs";

const KAFKA_BOOTSTRAP = process.env.KAFKA_BOOTSTRAP || "kafka:9092";
const TOPICS = ["telemetry", "anomalies", "injected_faults", "fleet_state"];

const kafka = new Kafka({ clientId: "shf-backend-rawstream", brokers: [KAFKA_BOOTSTRAP] });
const consumer = kafka.consumer({ groupId: `shf-backend-rawstream-${Date.now()}` });

const listeners = new Set();

export function onRawMessage(callback) {
  listeners.add(callback);
  return () => listeners.delete(callback);
}

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
